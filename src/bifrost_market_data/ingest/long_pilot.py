"""TD-172/142 exact pilot: 82 ranges, 120 target days, append-only raw bars.

Run by an approved domain writer action, not the broad option-depth planner.
No retries of this command without reconciling its receipt and existing keys.
"""
from __future__ import annotations

import asyncio
import csv
import fcntl
import hashlib
import math
import time
from datetime import date
from pathlib import Path
from typing import Any

from bifrost_market_data.ingest._upsert import (
    as_float,
    as_int,
    epoch_ms_to_date,
    parse_option_ticker,
)
from bifrost_market_data.ingest.option_daily import _COLS
from bifrost_market_data.polygon.client import PolygonClient
from bifrost_market_data.polygon.errors import PolygonAPIError

START, END = date(2026, 8, 10), date(2026, 8, 14)
SYMBOLS = {'SPY', 'NVDA', 'AAPL'}


def load_pilot(path: Path, digest: str) -> list[dict[str, Any]]:
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != digest:
        raise ValueError('pilot scope digest mismatch')
    out = []
    seen = set()
    for r in csv.DictReader(content.decode().splitlines()):
        days = [date.fromisoformat(d) for d in r['target_sessions'].split(',')]
        parsed = parse_option_ticker(r['option_ticker'])
        if (r['symbol'] not in SYMBOLS or parsed['underlying'] != r['symbol']
                or str(parsed['expiry']) != r['expiry'] or not days
                or any(d < START or d > END for d in days)
                or any(not 60 <= (parsed['expiry']-d).days <= 220 for d in days)
                or r['start'] != str(min(days)) or r['end'] != str(max(days))
                or r['option_ticker'] in seen or len(days) != len(set(days))):
            raise ValueError('pilot exceeds approved symbol/date/expiry scope')
        seen.add(r['option_ticker'])
        out.append({**r, 'days': days})
    if len(out) != 82 or sum(len(r['days']) for r in out) != 120:
        raise ValueError('pilot must be exactly 82 ranges / 120 target days')
    return out


class PilotClient(PolygonClient):
    """Counts every HTTP attempt including retries and pagination, ≤1/s history."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.attempts = {'total': 0, 'bars': 0, 'reference': 0}
        self.last_attempt = 0.

    async def _request_once(self, path, params=None, *, absolute_url=None):
        kind = 'reference' if '/reference/options/contracts' in (absolute_url or path) else 'bars'
        if self.attempts['total'] >= 500 or self.attempts[kind] >= {'bars': 100, 'reference': 50}[kind]:
            raise RuntimeError('pilot HTTP budget exhausted; stop and reconcile')
        await asyncio.sleep(max(0., 1.-(time.monotonic()-self.last_attempt)))
        self.last_attempt = time.monotonic()
        self.attempts['total'] += 1
        self.attempts[kind] += 1
        return await super()._request_once(path, params, absolute_url=absolute_url)


def append_bar(conn, request, bar) -> str:
    day = epoch_ms_to_date(bar['t'])
    if day not in request['days']:
        return 'outside_target'
    parsed = parse_option_ticker(request['option_ticker'])
    prices = [as_float(bar.get(k)) for k in ('o', 'h', 'l', 'c')]
    if any(v is None or not math.isfinite(v) or v < 0 for v in prices) or prices[-1] <= 0:
        return 'invalid_bar'
    volume = as_int(bar.get('v'))
    if volume is not None and volume < 0:
        return 'invalid_bar'
    values = (parsed['option_ticker'], request['symbol'], parsed['expiry'], parsed['strike'],
              parsed['option_right'], day, *prices, volume, as_float(bar.get('vw')), as_int(bar.get('n')))
    with conn.cursor() as cur:
        cur.execute(f"""INSERT INTO raw_market.option_daily ({','.join(_COLS)})
            VALUES ({','.join(['%s']*len(_COLS))})
            ON CONFLICT (option_ticker,bar_date) DO NOTHING""", values)
        return 'inserted' if cur.rowcount else 'existing_preserved'


async def run_pilot(conn, client: PilotClient, requests, *, receipt: Path, lock: Path):
    import json
    from collections import Counter
    report = {'requests': [], 'status': 'running'}
    with lock.open('a') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with conn.cursor() as cur:
            cur.execute('SELECT current_user, current_database()')
            if tuple(cur.fetchone()) != ('data_writer','bifrost_golden_source'):
                raise ValueError('pilot requires the Market Data domain writer')
            cur.execute('SELECT pg_try_advisory_lock(17214282)')
            if not cur.fetchone()[0]:
                raise RuntimeError('another pilot coordinator is active')
        # No history worker overlap: defer rather than steal capacity from EOD.
        with conn.cursor() as cur:
            cur.execute("""SELECT count(*) FROM ops_jobs.job_ingest
                WHERE status='running' AND kind IN ('option_daily','option_backfill')""")
            if cur.fetchone()[0]:
                raise RuntimeError('another history writer is active; defer pilot')
        catalogues = {}
        try:
            for symbol, expiry in sorted({(r['symbol'], r['expiry']) for r in requests}):
                data = await client.fetch_options_contracts(underlying_ticker=symbol,
                    expiration_date=expiry, expired=date.fromisoformat(expiry) < date.today(), max_pages=5)
                if data.get('truncated'):
                    raise RuntimeError('reference pagination truncated; no bars requested')
                catalogues[symbol, expiry] = {r['ticker'] for r in data.get('results') or []}
            for request in requests:
                with conn.cursor() as cur:
                    cur.execute("""SELECT count(*) FROM ops_jobs.job_ingest
                        WHERE status='running' AND kind IN ('option_daily','option_backfill')""")
                    if cur.fetchone()[0]:
                        raise RuntimeError('history/EOD worker became active; stop pilot and reconcile')
                outcome = Counter()
                if request['option_ticker'] not in catalogues[request['symbol'], request['expiry']]:
                    outcome['reference_unknown'] += len(request['days'])
                else:
                    data = await client.fetch_stock_aggs(request['option_ticker'],
                        from_value=request['start'], to_value=request['end'], max_pages=1)
                    if data.get('truncated'):
                        raise RuntimeError('aggregate pagination truncated')
                    observed = set()
                    for bar in data.get('results') or []:
                        outcome[append_bar(conn, request, bar)] += 1
                        observed.add(epoch_ms_to_date(bar['t']))
                    outcome['no_bar_unknown'] += len(set(request['days'])-observed)
                    conn.commit()
                report['requests'].append({'ticker': request['option_ticker'], 'outcome': dict(outcome)})
                report['attempts'] = client.attempts
                receipt.write_text(json.dumps(report, indent=2))
            report['status'] = 'finished_requires_coverage_acceptance'
        except PolygonAPIError as exc:
            conn.rollback()
            report['status'] = 'stopped'
            report['http_status'] = exc.status_code
            # No vendor body/URL/key in the receipt. 401/403 stop the whole pilot.
            raise
        except Exception:
            conn.rollback()
            report['status'] = 'stopped'
            raise
        finally:
            report['attempts'] = client.attempts
            receipt.write_text(json.dumps(report, indent=2))
