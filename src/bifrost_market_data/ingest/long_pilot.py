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
    def __init__(self, *args, initial_attempts=None, replacement=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.attempts = dict(initial_attempts or {'total': 0, 'bars': 0, 'reference': 0})
        if (set(self.attempts) != {'total','bars','reference'}
                or any(type(v) is not int or v < 0 for v in self.attempts.values())
                or self.attempts['total'] != self.attempts['bars']+self.attempts['reference']):
            raise ValueError('invalid reconciled HTTP budget')
        self.limits = {'total':160, 'bars':100, 'reference':60} if replacement else {'total':500, 'bars':100, 'reference':50}
        self.replacement = replacement
        self.last_attempt = 0.

    async def _request_once(self, path, params=None, *, absolute_url=None):
        kind = 'reference' if '/reference/options/contracts' in (absolute_url or path) else 'bars'
        if self.attempts['total'] >= self.limits['total'] or self.attempts[kind] >= self.limits[kind]:
            raise RuntimeError('pilot HTTP budget exhausted; stop and reconcile')
        await asyncio.sleep(max(0., 1.-(time.monotonic()-self.last_attempt)))
        self.last_attempt = time.monotonic()
        self.attempts['total'] += 1
        self.attempts[kind] += 1
        if self.replacement:
            import json
            print(json.dumps({'http_budget':self.attempts}), flush=True)
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


async def run_pilot(conn, client: PilotClient, requests, *, receipt: Path, lock: Path, control=None):
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
            # Reconcile existing target keys before any vendor HTTP (rerun = zero requests).
            pending = []
            for request in requests:
                with conn.cursor() as cur:
                    cur.execute("SELECT bar_date FROM raw_market.option_daily WHERE option_ticker=%s AND bar_date=ANY(%s)",
                                (request['option_ticker'],request['days']))
                    held = {r[0] for r in cur.fetchall()}
                days = [d for d in request['days'] if d not in held]
                if days:
                    pending.append({**request,'days':days,'start':str(min(days)),'end':str(max(days))})
                report['requests'].append({'ticker':request['option_ticker'],
                    'outcome':{'existing_preserved':len(held)}})
            requests = pending
            if control and requests:
                detail = await client._request('/v3/reference/options/contracts/'+control['ticker'],
                                               {'as_of':control['session']})
                data = await client.fetch_stock_aggs(control['ticker'],from_value=control['session'],
                                                    to_value=control['session'],max_pages=1)
                if (not detail.get('results') or data.get('truncated')
                        or date.fromisoformat(control['session']) not in
                        {epoch_ms_to_date(r['t']) for r in data.get('results') or []}):
                    raise RuntimeError('historical positive control failed')
                report['control'] = {'status':'passed','symbol':control['symbol']}
            groups = {}
            for request in requests:
                for day in request['days']:
                    groups.setdefault((request['symbol'],day,request['expiry']),[]).append(request)
            for (symbol, day, expiry), members in sorted(groups.items()):
                strikes = [parse_option_ticker(r['option_ticker'])['strike'] for r in members]
                data = await client.fetch_options_contracts(underlying_ticker=symbol,
                    expiration_date=expiry, as_of=str(day), expired=False,
                    strike_gte=min(strikes),strike_lte=max(strikes),max_pages=1)
                verify_historical_membership(data,{r['option_ticker'] for r in members})
                catalogues[symbol,day,expiry] = {r['ticker'] for r in data.get('results') or []}
            report['historical_groups_verified'] = len(catalogues)
            for request in requests:
                with conn.cursor() as cur:
                    cur.execute("""SELECT count(*) FROM ops_jobs.job_ingest
                        WHERE status='running' AND kind IN ('option_daily','option_backfill')""")
                    if cur.fetchone()[0]:
                        raise RuntimeError('history/EOD worker became active; stop pilot and reconcile')
                outcome = Counter()
                if any(request['option_ticker'] not in catalogues[request['symbol'], day, request['expiry']] for day in request['days']):
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
                    outcome['listed_no_eligible_aggregate'] += len(set(request['days'])-observed)
                    conn.commit()
                report['requests'].append({'ticker': request['option_ticker'], 'outcome': dict(outcome)})
                report['attempts'] = dict(client.attempts)
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
            report['attempts'] = dict(client.attempts)
            receipt.write_text(json.dumps(report, indent=2))


REPLACEMENT_SHA256 = '7264b1ff5cf2f581ed407c57ff95f6874290c02fd8fd6b891ff2d74ae53e485b'


def load_replacement(path: Path, digest: str, symbol: str):
    if digest != REPLACEMENT_SHA256:
        raise ValueError('replacement must be the reviewed 46/92 manifest')
    # Reuse exact ticker/day/expiry validation without relaxing original 82/120 gate.
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != digest:
        raise ValueError('replacement scope digest mismatch')
    out = []
    seen = set()
    for r in csv.DictReader(content.decode().splitlines()):
        days = [date.fromisoformat(d) for d in r['target_sessions'].split(',')]
        parsed = parse_option_ticker(r['option_ticker'])
        if (r['symbol'] not in SYMBOLS or parsed['underlying'] != r['symbol']
                or str(parsed['expiry']) != r['expiry'] or not days
                or any(d < START or d > END or not 60 <= (parsed['expiry']-d).days <= 220 for d in days)
                or r['start'] != str(min(days)) or r['end'] != str(max(days))
                or r['option_ticker'] in seen or len(set(days)) != len(days)):
            raise ValueError('replacement scope invalid')
        seen.add(r['option_ticker'])
        out.append({**r,'days':days})
    if len(out) != 46 or sum(len(r['days']) for r in out) != 92 or symbol not in SYMBOLS:
        raise ValueError('replacement must be exactly 46 ranges / 92 days')
    subset = [r for r in out if r['symbol']==symbol]
    if (len(subset),sum(len(r['days']) for r in subset)) != {'SPY':(26,44),'NVDA':(8,8),'AAPL':(12,40)}[symbol]:
        raise ValueError('symbol replacement count mismatch')
    return subset


def verify_historical_membership(data, tickers):
    if data.get('truncated') or data.get('next_url'):
        raise RuntimeError('historical reference truncated')
    contracts = {r['ticker']:r for r in data.get('results') or []}
    if data.get('status') not in ('OK','DELAYED','SUCCESS') or not tickers <= contracts.keys():
        raise RuntimeError('contract not listed on historical session')
    if any(contracts[t].get('shares_per_contract') != 100
           or contracts[t].get('additional_underlyings') for t in tickers):
        raise RuntimeError('nonstandard historical contract')
