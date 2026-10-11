import asyncio
import json

import httpx
import pytest

from bifrost_market_data.ingest.depth_pilot import (
    DEPTH_SHA256, DepthClient, load_depth, validate_checkpoint,
)


def checkpoint(total=10, reference=4, bars=6):
    return {'manifest_sha256':DEPTH_SHA256,
            'attempts':{'total':total,'reference':reference,'bars':bars},
            'accepted':{'SPY_gate':True},
            'effective_n':{'SPY':{'90':41,'180':35},'NVDA':{'90':44,'180':43},
                           'AAPL':{'90':41,'180':40}}}


def test_changed_manifest_never_enters_writer(tmp_path):
    with pytest.raises(ValueError,match='approved'):
        load_depth(tmp_path/'absent','0'*64,'SPY_gate')
    with pytest.raises(ValueError,match='approved'):
        load_depth(tmp_path/'absent',DEPTH_SHA256,'unknown')


def test_budget_reset_overflow_and_invalid_counts_rejected():
    for values in [(0,0,0),(221,90,131),(5,2,2),(5,-1,6)]:
        with pytest.raises(ValueError):
            validate_checkpoint(checkpoint(*values),'SPY_primary')


def test_spy_zero_improvement_stops_all_later_phases():
    c = checkpoint()
    c['effective_n']['SPY']['180'] = 33
    for phase in ('SPY_primary','SPY_reserve','NVDA_primary','AAPL_primary'):
        with pytest.raises(ValueError,match='has not improved'):
            validate_checkpoint(c,phase)


def test_depth_needed_before_next_symbol_and_reserve_only_when_needed():
    c = checkpoint()
    with pytest.raises(ValueError,match='SPY depth'):
        validate_checkpoint(c,'NVDA_primary')
    with pytest.raises(ValueError,match='reserve'):
        validate_checkpoint(c,'SPY_reserve')
    c['accepted']['SPY_primary'] = True
    validate_checkpoint(c,'SPY_reserve')
    c['effective_n']['SPY'] = {'90':66,'180':60}
    validate_checkpoint(c,'NVDA_primary')
    with pytest.raises(ValueError,match='reserve'):
        validate_checkpoint(c,'SPY_reserve')
    with pytest.raises(ValueError,match='NVDA depth'):
        validate_checkpoint(c,'AAPL_primary')


def test_journal_is_written_before_http_and_charges_failed_attempt(tmp_path):
    async def check():
        path = tmp_path/'journal.json'
        def handle(req):
            assert json.loads(path.read_text()) == {'total':11,'reference':5,'bars':6}
            return httpx.Response(500,json={'status':'ERROR'})
        client = DepthClient('test-only',checkpoint=checkpoint(),phase='SPY_primary',
                             journal=path,transport=httpx.MockTransport(handle))
        await client._request_once('/v3/reference/options/contracts')
        assert client.attempts['total'] == 11
        await client.aclose()
    asyncio.run(check())


def test_concurrent_attempts_cannot_exceed_last_budget_slot(tmp_path):
    async def check():
        client = DepthClient('test-only',checkpoint=checkpoint(219,89,130),
                             phase='SPY_primary',journal=tmp_path/'journal.json',
                             transport=httpx.MockTransport(lambda r:httpx.Response(200,json={})))
        result = await asyncio.gather(*[
            client._request_once('/v3/reference/options/contracts') for _ in range(2)
        ],return_exceptions=True)
        assert sum(isinstance(r,RuntimeError) for r in result)==1
        assert client.attempts == {'total':220,'reference':90,'bars':130}
        await client.aclose()
    asyncio.run(check())


def test_sub_budget_stops_even_when_total_has_room(tmp_path):
    async def check():
        client = DepthClient('test-only',checkpoint=checkpoint(90,90,0),
                             phase='SPY_primary',journal=tmp_path/'journal.json')
        with pytest.raises(RuntimeError,match='budget'):
            await client._request_once('/v3/reference/options/contracts')
        assert not (tmp_path/'journal.json').exists()
        await client.aclose()
    asyncio.run(check())
