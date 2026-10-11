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


def test_historical_reference_is_fail_closed():
    from bifrost_market_data.ingest.long_pilot import verify_historical_membership
    c={'ticker':'x','shares_per_contract':100}
    verify_historical_membership({'status':'OK','results':[c]}, {'x'})
    for data, message in [({'status':'OK','results':[]},'not listed'),
                          ({'status':'OK','results':[c],'truncated':True},'truncated'),
                          ({'status':'OK','results':[{**c,'shares_per_contract':10}]},'nonstandard')]:
        with pytest.raises(RuntimeError,match=message):
            verify_historical_membership(data,{'x'})


def test_as_of_and_strike_bounds_reach_actual_http():
    async def check():
        requests=[]
        def handle(req):
            requests.append(req)
            return httpx.Response(200,json={'status':'OK','results':[]})
        client=PilotClient('test-only',transport=httpx.MockTransport(handle))
        await client.fetch_options_contracts(underlying_ticker='SPY',as_of='2026-08-10',
                                           strike_gte=770,strike_lte=778,max_pages=1)
        assert requests[0].url.params['as_of']=='2026-08-10'
        assert requests[0].url.params['strike_price.gte']=='770'
        assert requests[0].url.params['expired']=='false'
        await client.aclose()
    asyncio.run(check())


def test_replacement_http_cap_is_cumulative_across_cohorts():
    async def check():
        client=PilotClient('test-only',replacement=True,
                          initial_attempts={'total':160,'bars':100,'reference':60})
        with pytest.raises(RuntimeError,match='budget'):
            await client._request_once('/v3/reference/options/contracts')
        assert client.attempts['total']==160
        await client.aclose()
    asyncio.run(check())


def test_unapproved_replacement_digest_never_relaxes_original_scope(tmp_path):
    from bifrost_market_data.ingest.long_pilot import load_replacement
    with pytest.raises(ValueError,match='reviewed'):
        load_replacement(tmp_path/'does-not-exist','0'*64,'SPY')
