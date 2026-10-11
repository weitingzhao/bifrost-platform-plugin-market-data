"""Owner-approved depth batch: frozen phases, carried durable HTTP journal."""
from __future__ import annotations

import csv
import asyncio
import hashlib
import json
import os
from datetime import date
from pathlib import Path

from bifrost_market_data.ingest._upsert import parse_option_ticker
from bifrost_market_data.ingest.long_pilot import PilotClient

DEPTH_SHA256 = 'f05a758d2496b851387c4d5918f99748827ae9f0d5cd18e1e8de4e54c75af381'
LIMITS = {'total':220, 'reference':90, 'bars':130}
PHASES = {'SPY_gate':(6,8), 'SPY_primary':(38,100), 'SPY_reserve':(10,12),
          'NVDA_primary':(22,68), 'NVDA_reserve':(6,12),
          'AAPL_primary':(16,80), 'AAPL_reserve':(10,12)}


def load_depth(path: Path, digest: str, phase: str):
    if digest != DEPTH_SHA256 or phase not in PHASES:
        raise ValueError('depth requires the approved manifest and phase')
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != digest:
        raise ValueError('depth manifest changed')
    rows = []
    seen = set()
    for r in csv.DictReader(content.decode().splitlines()):
        p = parse_option_ticker(r['option_ticker'])
        days = [date.fromisoformat(d) for d in r['target_sessions'].split(',')]
        ph = r['execution_phase']
        if (ph not in PHASES or r['symbol'] != ph.split('_')[0]
                or r['symbol'] != p['underlying'] or str(p['expiry']) != r['expiry']
                or r['cohort'] != ('reserve' if ph.endswith('_reserve') else 'primary')
                or not days or len(days) != len(set(days))
                or r['start'] != str(min(days)) or r['end'] != str(max(days))
                or any(not date(2026,6,26) <= d <= date(2026,9,3)
                       or not 180 <= (p['expiry']-d).days <= 220 for d in days)):
            raise ValueError('depth scope invalid')
        for d in days:
            key = p['option_ticker'],d
            if key in seen:
                raise ValueError('duplicate target day across phases')
            seen.add(key)
        rows.append({**r,'days':days})
    if len(rows) != 108 or len(seen) != 292:
        raise ValueError('depth must be exactly 108 ranges / 292 target days')
    for ph, counts in PHASES.items():
        subset = [r for r in rows if r['execution_phase']==ph]
        if (len(subset),sum(len(r['days']) for r in subset)) != counts:
            raise ValueError('depth phase count differs')
    return [r for r in rows if r['execution_phase']==phase]


def validate_checkpoint(checkpoint, phase):
    if checkpoint.get('manifest_sha256') != DEPTH_SHA256 or phase not in PHASES:
        raise ValueError('checkpoint scope invalid')
    counts = checkpoint['attempts']
    if (set(counts) != set(LIMITS) or any(type(v) is not int or not 0 <= v <= LIMITS[k]
                                         for k,v in counts.items())
            or counts['total'] != counts['reference']+counts['bars']):
        raise ValueError('invalid carried HTTP budget')
    accepted = checkpoint['accepted']
    if any(p not in PHASES or v is not True for p,v in accepted.items()):
        raise ValueError('unaccepted prior phase')
    if accepted and counts['total'] == 0:
        raise ValueError('HTTP counter reset after accepted phase')
    if phase == 'SPY_gate' and not accepted and counts['total'] != 0:
        raise ValueError('unfinished gate must be reconciled before restart')
    n = checkpoint['effective_n']
    if (set(n) != {'SPY','NVDA','AAPL'} or any(set(v) != {'90','180'}
            or any(type(x) is not int or not 0 <= x <= 252 for x in v.values()) for v in n.values())):
        raise ValueError('invalid effective session counts')
    if phase != 'SPY_gate' and (not accepted.get('SPY_gate') or n['SPY']['180'] <= 33):
        raise ValueError('SPY effective 180 gate has not improved')
    symbol = phase.split('_')[0]
    if phase.endswith('_reserve'):
        if not accepted.get(symbol+'_primary') or all(n[symbol][str(t)] >= 60 for t in (90,180)):
            raise ValueError('reserve only after primary and only when needed')
    if phase == 'NVDA_primary' and (not accepted.get('SPY_primary') or min(n['SPY'].values()) < 60):
        raise ValueError('SPY depth acceptance required before NVDA')
    if phase == 'AAPL_primary' and (not accepted.get('NVDA_primary') or min(n['NVDA'].values()) < 60):
        raise ValueError('NVDA depth acceptance required before AAPL')
    return counts


class DepthClient(PilotClient):
    """Journal + stdout before each attempt; coordinator must reconcile crash logs."""
    def __init__(self, *args, checkpoint, phase, journal: Path, **kwargs):
        counts = validate_checkpoint(checkpoint,phase)
        super().__init__(*args,initial_attempts=counts,replacement=True,**kwargs)
        self.limits = dict(LIMITS)
        self.journal = journal
        self.budget_lock = asyncio.Lock()

    async def _request_once(self, path, params=None, *, absolute_url=None):
        async with self.budget_lock:
            return await self._charged_request(path,params,absolute_url=absolute_url)

    async def _charged_request(self, path, params=None, *, absolute_url=None):
        kind = 'reference' if '/reference/options/contracts' in (absolute_url or path) else 'bars'
        if self.attempts['total'] >= self.limits['total'] or self.attempts[kind] >= self.limits[kind]:
            raise RuntimeError('depth HTTP budget exhausted; stop and reconcile')
        projected = dict(self.attempts)
        projected['total'] += 1
        projected[kind] += 1
        # Persist and emit before HTTP. A crash conservatively charges this attempt.
        with self.journal.open('w') as handle:
            json.dump(projected,handle)
            handle.flush()
            os.fsync(handle.fileno())
        print('DEPTH_HTTP:'+json.dumps(projected),flush=True)
        return await super()._request_once(path,params,absolute_url=absolute_url)
