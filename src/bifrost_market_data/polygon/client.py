"""Async Polygon REST client (httpx) with rate limiting, retry, and pagination."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx

from bifrost_market_data.polygon import endpoints as ep
from bifrost_market_data.polygon.errors import PolygonAPIError, PolygonRateLimitError
from bifrost_market_data.polygon.rate_limit import TokenBucket, get_tier_profile

logger = logging.getLogger(__name__)

_TRANSIENT_STATUS = frozenset({408, 425, 500, 502, 503, 504})
_LOGIC_ERROR_STATUS = frozenset({"ERROR", "NOT_AUTHORIZED", "FAILED"})


def redact_url(url: str) -> str:
    """Strip apiKey from URL for safe logging."""
    try:
        parsed = urlparse(url)
        q = [(k, v if k.lower() != "apikey" else "***") for k, v in parse_qsl(parsed.query, keep_blank_values=True)]
        return urlunparse(parsed._replace(query=urlencode(q)))
    except Exception:
        return "<unparseable-url>"


def _polygon_body_error_message(data: object) -> str | None:
    if not isinstance(data, dict):
        return None
    status = str(data.get("status") or "").upper()
    if status in _LOGIC_ERROR_STATUS:
        return str(data.get("error") or data.get("message") or status)
    err = data.get("error")
    if isinstance(err, str) and err.strip():
        # Some endpoints return error without status=ERROR
        if status and status not in ("OK", "DELAYED", "SUCCESS"):
            return err
    return None


def _backoff_seconds(attempt: int) -> float:
    return min(0.4 * (2**attempt), 30.0)


def parse_retry_after(value: str | None, *, now: datetime | None = None) -> float | None:
    """Parse ``Retry-After`` as delta-seconds or HTTP-date. Returns seconds to wait."""
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(raw)
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        base = now if now is not None else datetime.now(timezone.utc)
        return max(0.0, (when - base).total_seconds())
    except (TypeError, ValueError, IndexError, OverflowError):
        return None


class PolygonClient:
    """httpx-based async Polygon client.

    Auth is query ``apiKey``. Rate limiting is enforced via a shared ``TokenBucket``.
    """

    def __init__(
        self,
        api_key: str,
        *,
        tier: str = "developer",
        rest_base: str = ep.DEFAULT_REST_BASE,
        timeout: float = 60.0,
        max_retries: int = 5,
        limiter: TokenBucket | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        key = (api_key or "").strip()
        if not key:
            raise ValueError("api_key is required")
        self.api_key = key
        self.tier = get_tier_profile(tier).name
        self.rest_base = (rest_base or ep.DEFAULT_REST_BASE).rstrip("/")
        self.max_retries = max(1, int(max_retries))
        self._limiter = limiter or get_tier_profile(tier).make_bucket()
        self._http = httpx.AsyncClient(
            base_url=self.rest_base,
            timeout=httpx.Timeout(timeout),
            transport=transport,
            headers={"Accept": "application/json", "User-Agent": "bifrost-market-data/0.1"},
        )
        self._closed = False

    @property
    def limiter(self) -> TokenBucket:
        return self._limiter

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            await self._http.aclose()

    async def close(self) -> None:
        await self.aclose()

    async def __aenter__(self) -> PolygonClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def _request_once(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        absolute_url: str | None = None,
    ) -> tuple[int, Any]:
        await self._limiter.acquire()
        query = dict(params or {})
        query["apiKey"] = self.api_key
        if absolute_url:
            # next_url may already include apiKey; force ours
            parsed = urlparse(absolute_url)
            existing = dict(parse_qsl(parsed.query, keep_blank_values=True))
            existing["apiKey"] = self.api_key
            url = urlunparse(parsed._replace(query=urlencode(existing)))
            response = await self._http.get(url)
        else:
            response = await self._http.get(path, params=query)

        status = response.status_code
        try:
            data: Any = response.json()
        except Exception:
            data = {"error": response.text[:500] if response.text else "non-json body"}

        if status == 429:
            retry_after = parse_retry_after(response.headers.get("Retry-After"))
            raise PolygonRateLimitError(
                "rate limited by Polygon",
                retry_after=retry_after,
                url=redact_url(str(response.url)),
                body=data,
            )

        return status, data

    async def _request(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        absolute_url: str | None = None,
    ) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                status, data = await self._request_once(path, params, absolute_url=absolute_url)
            except PolygonRateLimitError as e:
                last_error = e
                wait = e.retry_after if e.retry_after is not None else _backoff_seconds(attempt)
                logger.warning("Polygon 429; sleeping %.2fs (attempt %s)", wait, attempt + 1)
                await asyncio.sleep(wait)
                continue
            except (httpx.TransportError, httpx.TimeoutException) as e:
                last_error = e
                wait = _backoff_seconds(attempt)
                logger.warning(
                    "Polygon transport error %s; sleeping %.2fs (attempt %s)",
                    type(e).__name__,
                    wait,
                    attempt + 1,
                )
                await asyncio.sleep(wait)
                continue

            url_for_err = absolute_url or path
            if status in _TRANSIENT_STATUS:
                last_error = PolygonAPIError(
                    f"transient HTTP {status}",
                    status_code=status,
                    url=redact_url(url_for_err),
                    body=data,
                )
                wait = _backoff_seconds(attempt)
                await asyncio.sleep(wait)
                continue

            if status >= 400:
                msg = _polygon_body_error_message(data) or f"HTTP {status}"
                raise PolygonAPIError(msg, status_code=status, url=redact_url(url_for_err), body=data)

            logic_err = _polygon_body_error_message(data)
            if logic_err:
                raise PolygonAPIError(
                    logic_err,
                    status_code=status,
                    url=redact_url(url_for_err),
                    body=data,
                )
            return data

        if isinstance(last_error, PolygonAPIError):
            raise last_error
        raise PolygonAPIError(
            f"exhausted retries: {last_error}",
            status_code=None,
            url=redact_url(absolute_url or path),
        ) from last_error

    async def _paginate(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        max_pages: int = 20,
        results_key: str = "results",
    ) -> dict[str, Any]:
        """Follow ``next_url`` and merge ``results`` lists. Returns a single aggregate payload."""
        if max_pages < 1:
            raise ValueError("max_pages must be >= 1")

        first = await self._request(path, params)
        if not isinstance(first, dict):
            # marketstatus/upcoming returns a bare list
            if isinstance(first, list):
                return {"results": first, "status": "OK", "pages": 1}
            raise PolygonAPIError("unexpected response type", body=first, url=path)

        all_results: list[Any] = list(first.get(results_key) or [])
        next_url = first.get("next_url")
        pages = 1
        truncated = False

        while next_url and pages < max_pages:
            page = await self._request(path, absolute_url=str(next_url))
            pages += 1
            if not isinstance(page, dict):
                raise PolygonAPIError("unexpected page type", body=page, url=str(next_url))
            all_results.extend(page.get(results_key) or [])
            next_url = page.get("next_url")

        if next_url:
            truncated = True
            logger.warning(
                "Polygon pagination truncated at max_pages=%s path=%s remaining_next_url=%s",
                max_pages,
                path,
                redact_url(str(next_url)),
            )

        out = dict(first)
        out[results_key] = all_results
        out["next_url"] = None
        out["pages"] = pages
        out["truncated"] = truncated
        return out

    # ---- high-level helpers (P4 ingest) ----

    async def fetch_stock_aggs(
        self,
        symbol: str,
        *,
        from_value: str | int,
        to_value: str | int,
        multiplier: int = 1,
        timespan: str = "day",
        adjusted: bool | None = None,
        max_pages: int = 200,
    ) -> dict[str, Any]:
        """GET ``/v2/aggs/ticker/{}/range/...`` (stock or option ticker).

        ``adjusted`` defaults by ticker type: equities ``true``; ``O:`` / ``I:`` omitted.
        Pass an explicit bool to override.
        """
        path = ep.aggs_range_path(
            symbol,
            multiplier=multiplier,
            timespan=timespan,
            from_value=from_value,
            to_value=to_value,
        )
        params = ep.aggs_range_params(ticker=symbol, adjusted=adjusted)
        return await self._paginate(path, params, max_pages=max_pages)

    async def fetch_options_contracts(
        self,
        *,
        underlying_ticker: str | None = None,
        expiration_date: str | None = None,
        expired: bool | None = False,
        max_pages: int = 20,
    ) -> dict[str, Any]:
        """GET ``/v3/reference/options/contracts`` with pagination."""
        path = ep.options_contracts_path()
        params = ep.options_contracts_params(
            underlying_ticker=underlying_ticker,
            expiration_date=expiration_date,
            expired=expired,
        )
        return await self._paginate(path, params, max_pages=max_pages)

    async def fetch_options_snapshot(
        self,
        underlying: str,
        *,
        expiration_date: str | None = None,
        contract_type: str | None = None,
        max_pages: int = 500,
    ) -> dict[str, Any]:
        """GET ``/v3/snapshot/options/{underlying}`` with pagination."""
        path = ep.options_snapshot_path(underlying)
        params = ep.options_snapshot_params(
            expiration_date=expiration_date,
            contract_type=contract_type,
        )
        return await self._paginate(path, params, max_pages=max_pages)

    async def fetch_reference_tickers(
        self,
        *,
        market: str = "stocks",
        active: bool = True,
        locale: str = "us",
        ticker_type: str | None = "CS",
        cursor: str | None = None,
        max_pages: int = 1,
    ) -> dict[str, Any]:
        """GET ``/v3/reference/tickers``. Default single page (cursor-driven by caller)."""
        path = ep.reference_tickers_path()
        params = ep.reference_tickers_params(
            market=market,
            active=active,
            locale=locale,
            ticker_type=ticker_type,
            cursor=cursor,
        )
        return await self._paginate(path, params, max_pages=max_pages)

    async def fetch_ticker_details(self, ticker: str) -> dict[str, Any]:
        path = ep.ticker_details_path(ticker)
        data = await self._request(path, {})
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected ticker details payload", body=data, url=path)
        return data

    async def fetch_financials(
        self,
        ticker: str,
        *,
        timeframe: str | None = None,
        limit: int = 100,
        max_pages: int = 5,
    ) -> dict[str, Any]:
        path = ep.financials_path()
        params = ep.financials_params(ticker=ticker, limit=limit, timeframe=timeframe)
        return await self._paginate(path, params, max_pages=max_pages)

    async def fetch_splits(self, ticker: str | None = None, *, max_pages: int = 5) -> dict[str, Any]:
        path = ep.splits_path()
        params = ep.splits_params(ticker=ticker)
        return await self._paginate(path, params, max_pages=max_pages)

    async def fetch_dividends(self, ticker: str | None = None, *, max_pages: int = 5) -> dict[str, Any]:
        path = ep.dividends_path()
        params = ep.dividends_params(ticker=ticker)
        return await self._paginate(path, params, max_pages=max_pages)

    async def fetch_market_status_upcoming(self) -> dict[str, Any]:
        path = ep.market_status_upcoming_path()
        data = await self._request(path, {})
        if isinstance(data, list):
            return {"results": data, "status": "OK", "pages": 1, "truncated": False}
        if isinstance(data, dict):
            return data
        raise PolygonAPIError("unexpected market status payload", body=data, url=path)

    async def fetch_grouped_daily(
        self,
        date_str: str,
        *,
        locale: str = "us",
        market: str = "stocks",
        adjusted: bool = True,
    ) -> dict[str, Any]:
        """GET ``/v2/aggs/grouped/locale/.../market/.../{date}`` (single response, no pagination)."""
        path = ep.grouped_daily_path(date_str, locale=locale, market=market)
        params = ep.grouped_daily_params(adjusted=adjusted)
        data = await self._request(path, params)
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected grouped daily payload", body=data, url=path)
        out = dict(data)
        out.setdefault("pages", 1)
        out.setdefault("truncated", False)
        return out

    async def fetch_stock_snapshot_all(
        self,
        *,
        include_otc: bool = False,
        max_pages: int = 50,
    ) -> dict[str, Any]:
        """GET ``/v2/snapshot/locale/us/markets/stocks/tickers`` (paginated via ``tickers``)."""
        path = ep.stock_snapshot_all_path()
        params = ep.stock_snapshot_all_params(include_otc=include_otc)
        return await self._paginate(
            path, params, max_pages=max_pages, results_key="tickers"
        )

    async def fetch_stock_snapshot_single(self, ticker: str) -> dict[str, Any]:
        """GET ``/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}``."""
        path = ep.stock_snapshot_single_path(ticker)
        data = await self._request(path, {})
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected stock snapshot payload", body=data, url=path)
        out = dict(data)
        out.setdefault("pages", 1)
        out.setdefault("truncated", False)
        return out

    async def fetch_stock_gainers_losers(
        self,
        direction: str,
        *,
        include_otc: bool = False,
    ) -> dict[str, Any]:
        """GET ``/v2/snapshot/locale/us/markets/stocks/{gainers|losers}``."""
        path = ep.stock_gainers_losers_path(direction)
        params = ep.stock_gainers_losers_params(include_otc=include_otc)
        data = await self._request(path, params)
        if not isinstance(data, dict):
            raise PolygonAPIError(
                "unexpected gainers/losers payload", body=data, url=path
            )
        out = dict(data)
        out.setdefault("pages", 1)
        out.setdefault("truncated", False)
        return out

    # ---- pass-through REST (P5 Wave 5-A) ----

    async def get_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Public wrapper around ``_request`` for thin API pass-through routes."""
        return await self._request(path, params)

    async def fetch_open_close(
        self,
        ticker: str,
        date_str: str,
        *,
        adjusted: bool = True,
    ) -> dict[str, Any]:
        path = ep.open_close_path(ticker, date_str)
        data = await self._request(path, ep.open_close_params(adjusted=adjusted))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected open-close payload", body=data, url=path)
        return data

    async def fetch_prev_agg(
        self,
        ticker: str,
        *,
        adjusted: bool = True,
    ) -> dict[str, Any]:
        path = ep.prev_agg_path(ticker)
        data = await self._request(path, ep.prev_agg_params(adjusted=adjusted))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected prev agg payload", body=data, url=path)
        return data

    async def fetch_news(self, **params: Any) -> dict[str, Any]:
        path = ep.news_path()
        data = await self._request(path, ep.news_params(**params))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected news payload", body=data, url=path)
        return data

    async def fetch_related_companies(self, ticker: str) -> dict[str, Any]:
        path = ep.related_companies_path(ticker)
        data = await self._request(path, {})
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected related companies payload", body=data, url=path)
        return data

    async def fetch_reference_tickers_query(self, **params: Any) -> dict[str, Any]:
        path = ep.reference_tickers_path()
        query = ep.reference_tickers_search_params(**params)
        return await self._paginate(path, query, max_pages=1)

    async def fetch_ticker_types(
        self,
        *,
        asset_class: str | None = None,
        locale: str | None = None,
    ) -> dict[str, Any]:
        path = ep.ticker_types_path()
        data = await self._request(path, ep.ticker_types_params(asset_class=asset_class, locale=locale))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected ticker types payload", body=data, url=path)
        return data

    async def fetch_ticker_detail(
        self,
        ticker: str,
        *,
        date: str | None = None,
    ) -> dict[str, Any]:
        path = ep.ticker_details_path(ticker)
        data = await self._request(path, ep.ticker_detail_params(date=date))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected ticker detail payload", body=data, url=path)
        return data

    async def fetch_market_conditions(
        self,
        *,
        asset_class: str | None = None,
        data_type: str | None = None,
        limit: int = 1000,
    ) -> dict[str, Any]:
        path = ep.conditions_path()
        return await self._paginate(
            path,
            ep.conditions_params(asset_class=asset_class, data_type=data_type, limit=limit),
            max_pages=1,
        )

    async def fetch_market_exchanges(
        self,
        *,
        asset_class: str | None = None,
        locale: str | None = None,
    ) -> dict[str, Any]:
        path = ep.exchanges_path()
        data = await self._request(path, ep.exchanges_params(asset_class=asset_class, locale=locale))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected exchanges payload", body=data, url=path)
        return data

    async def fetch_market_status_now(self) -> dict[str, Any]:
        path = ep.market_status_now_path()
        data = await self._request(path, {})
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected market status payload", body=data, url=path)
        return data

    async def fetch_financial_statement(self, kind: str, **params: Any) -> dict[str, Any]:
        path = ep.financial_statement_path(kind)
        query = ep.financial_statement_params(**params)
        data = await self._request(path, query)
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected financial statement payload", body=data, url=path)
        return data

    async def fetch_ratios(self, **params: Any) -> dict[str, Any]:
        path = ep.ratios_path()
        data = await self._request(path, ep.ratios_params(**params))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected ratios payload", body=data, url=path)
        return data

    async def fetch_short_interest(self, **params: Any) -> dict[str, Any]:
        path = ep.short_interest_path()
        data = await self._request(path, ep.short_interest_params(**params))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected short interest payload", body=data, url=path)
        return data

    async def fetch_short_volume(self, **params: Any) -> dict[str, Any]:
        path = ep.short_volume_path()
        data = await self._request(path, ep.short_volume_params(**params))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected short volume payload", body=data, url=path)
        return data

    async def fetch_float(self, **params: Any) -> dict[str, Any]:
        path = ep.float_path()
        data = await self._request(path, ep.float_params(**params))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected float payload", body=data, url=path)
        return data

    async def fetch_edgar_index(self, **params: Any) -> dict[str, Any]:
        path = ep.edgar_index_path()
        data = await self._request(path, ep.edgar_index_params(**params))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected edgar index payload", body=data, url=path)
        return data

    async def fetch_10k_sections(self, **params: Any) -> dict[str, Any]:
        path = ep.filing_10k_sections_path()
        data = await self._request(path, ep.filing_10k_sections_params(**params))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected 10-K sections payload", body=data, url=path)
        return data

    async def fetch_8k_text(self, **params: Any) -> dict[str, Any]:
        path = ep.filing_8k_text_path()
        data = await self._request(path, ep.filing_8k_text_params(**params))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected 8-K text payload", body=data, url=path)
        return data

    async def fetch_13f_filings(self, **params: Any) -> dict[str, Any]:
        path = ep.filing_13f_path()
        data = await self._request(path, ep.filing_13f_params(**params))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected 13-F payload", body=data, url=path)
        return data

    async def fetch_risk_factors(self, **params: Any) -> dict[str, Any]:
        path = ep.filing_risk_factors_path()
        data = await self._request(path, ep.filing_risk_factors_params(**params))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected risk factors payload", body=data, url=path)
        return data

    async def fetch_risk_categories(self, **params: Any) -> dict[str, Any]:
        path = ep.filing_risk_categories_path()
        data = await self._request(path, ep.filing_risk_categories_params(**params))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected risk categories payload", body=data, url=path)
        return data

    async def fetch_form_3(self, **params: Any) -> dict[str, Any]:
        path = ep.filing_form_3_path()
        data = await self._request(path, ep.insider_filing_params(**params))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected form-3 payload", body=data, url=path)
        return data

    async def fetch_form_4(self, **params: Any) -> dict[str, Any]:
        path = ep.filing_form_4_path()
        data = await self._request(path, ep.insider_filing_params(**params))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected form-4 payload", body=data, url=path)
        return data

    async def fetch_indicator(
        self,
        indicator: str,
        ticker: str,
        **params: Any,
    ) -> dict[str, Any]:
        path = ep.indicator_path(indicator, ticker)
        data = await self._request(path, ep.indicator_params(**params))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected indicator payload", body=data, url=path)
        return data

    async def fetch_last_trade(self, options_ticker: str) -> dict[str, Any]:
        path = ep.last_trade_path(options_ticker)
        data = await self._request(path, {})
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected last trade payload", body=data, url=path)
        return data

    async def fetch_option_quotes(self, options_ticker: str, **params: Any) -> dict[str, Any]:
        path = ep.option_quotes_path(options_ticker)
        data = await self._request(path, ep.option_ticks_params(**params))
        if not isinstance(data, dict):
            raise PolygonAPIError("unexpected option quotes payload", body=data, url=path)
        return data

    async def fetch_option_trades(self, options_ticker: str, **params: Any) -> dict[str, Any]:
        """GET /v3/trades/{optionsTicker} with next_url pagination.

        Pass ``max_pages`` (default 50) to bound REST page walks for busy contracts.
        """
        path = ep.option_trades_path(options_ticker)
        max_pages = int(params.pop("max_pages", 50) or 50)
        tick_keys = (
            "timestamp_gte",
            "timestamp_lte",
            "limit",
            "sort",
            "order",
        )
        tick_params = {k: params[k] for k in tick_keys if k in params and params[k] is not None}
        query = ep.option_ticks_params(**tick_params)
        return await self._paginate(path, query, max_pages=max_pages)
