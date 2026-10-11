"""One accepted depth phase; private frozen manifest and reconciled checkpoint."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path

from bifrost_market_data.ingest.depth_pilot import DepthClient, load_depth, validate_checkpoint
from bifrost_market_data.ingest.long_pilot import run_pilot

CONTROL_SHA256 = '05e079b5dd6383e4414babae245848ce0b7a9c73ca193e0ed1c37e002eca22df'

def main():
    p = argparse.ArgumentParser()
    for name in ('manifest','checkpoint','controls','receipt','lock'):
        p.add_argument('--'+name,type=Path,required=True)
    for name in ('sha256','checkpoint-sha256','controls-sha256','phase'):
        p.add_argument('--'+name,required=True)
    p.add_argument('--execute',action='store_true')
    a = p.parse_args()
    if a.controls_sha256 != CONTROL_SHA256:
        raise ValueError('controls must be the reviewed three read-only targets')
    requests = load_depth(a.manifest,a.sha256,a.phase)
    for path,digest in [(a.checkpoint,a.checkpoint_sha256),(a.controls,a.controls_sha256)]:
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise ValueError('private checkpoint/control changed')
    checkpoint = json.loads(a.checkpoint.read_text())
    validate_checkpoint(checkpoint,a.phase)
    controls = json.loads(a.controls.read_text())
    control = next(x for x in controls if x['symbol']==a.phase.split('_')[0])
    control = {**control,'ticker':control['option_ticker']}
    if a.phase != 'SPY_gate' and a.phase != 'NVDA_primary' and a.phase != 'AAPL_primary':
        control = None
    if not a.execute:
        print(json.dumps({'status':'validated_no_http_no_writes','phase':a.phase,'ranges':len(requests)}))
        return
    import os

    import psycopg2

    from bifrost_market_data.config import load_config, postgres_connect_kwargs
    cfg = load_config()
    poly = cfg.get('polygon') or {}
    key = os.environ.get('POLYGON_API_KEY') or os.environ.get('MASSIVE_API_KEY') or poly.get('api_key')
    conn = psycopg2.connect(**postgres_connect_kwargs(cfg,statement_timeout='20s'),connect_timeout=10)
    async def execute():
        async with DepthClient(key,tier=poly.get('tier','starter'),max_retries=3,
                               checkpoint=checkpoint,phase=a.phase,journal=a.receipt.with_suffix('.http.json')) as client:
            await run_pilot(conn,client,requests,receipt=a.receipt,lock=a.lock,control=control)
    try:
        asyncio.run(execute())
    finally:
        conn.close()
        if a.receipt.exists():
            print('DEPTH_REPORT:'+json.dumps({'phase':a.phase,'manifest_sha256':a.sha256,
                                            **json.loads(a.receipt.read_text())}),flush=True)


if __name__ == '__main__':
    main()
