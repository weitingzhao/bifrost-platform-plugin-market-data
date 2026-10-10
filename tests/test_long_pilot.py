import asyncio
import csv
import hashlib
from datetime import datetime, timezone

import httpx
import pytest

from bifrost_market_data.ingest.long_pilot import PilotClient, append_bar, load_pilot


def test_pilot_rejects_unapproved_size_and_changed_digest(tmp_path):
    p = tmp_path/'pilot.csv'
    with p.open('w') as f:
        w = csv.DictWriter(f, fieldnames=['symbol','option_ticker','expiry','start','end','target_sessions'])
        w.writeheader()
        w.writerow(dict(symbol='SPY',option_ticker='O:SPY261218C00675000',expiry='2026-12-18',
                        start='2026-08-10',end='2026-08-10',target_sessions='2026-08-10'))
    digest = hashlib.sha256(p.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match='82 ranges'):
        load_pilot(p, digest)
    with pytest.raises(ValueError, match='digest'):
        load_pilot(p, '0'*64)


def test_http_budget_includes_retries_and_stops_before_extra_attempt():
    async def check():
        client = PilotClient('test-only', transport=httpx.MockTransport(lambda req: httpx.Response(200,json={'results':[]})))
        client.attempts['bars'] = 100
        with pytest.raises(RuntimeError, match='budget'):
            await client._request_once('/v2/aggs/ticker/O:SPY/range')
        assert client.attempts['total'] == 0
        await client.aclose()
    asyncio.run(check())


def test_append_preserves_zero_volume_and_existing_raw_rows():
    from datetime import date
    class Conn:
        rowcount = 0
        def cursor(self): return self
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, args): self.sql, self.args = sql, args
    c = Conn()
    r = dict(option_ticker='O:SPY261218C00675000',symbol='SPY',days=[date(2026,8,10)])
    bar = dict(t=int(datetime(2026,8,10,tzinfo=timezone.utc).timestamp()*1000),o=1,h=2,l=1,c=1,v=0,n=0)
    assert append_bar(c,r,bar) == 'existing_preserved'
    assert c.args[10] == 0 and c.args[12] == 0
    assert 'DO NOTHING' in c.sql and 'UPDATE' not in c.sql
