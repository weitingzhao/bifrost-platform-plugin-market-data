"""Execute one reviewed replacement cohort; prior HTTP counts must be reconciled."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path

from bifrost_market_data.ingest.long_pilot import PilotClient, load_replacement, run_pilot

INPUT_SHA256 = 'a819658b305d1983249aaddaa2de7dffce87887b6de8f0eeb1161e1c0c6c3d95'


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--manifest',type=Path,required=True)
    p.add_argument('--sha256',required=True)
    p.add_argument('--symbol',choices=['SPY','NVDA','AAPL'],required=True)
    p.add_argument('--controls',type=Path,required=True)
    p.add_argument('--prior-attempts',required=True)
    p.add_argument('--receipt',type=Path,required=True)
    p.add_argument('--lock',type=Path,required=True)
    p.add_argument('--execute',action='store_true')
    a=p.parse_args()
    requests=load_replacement(a.manifest,a.sha256,a.symbol)
    content=a.controls.read_bytes()
    if hashlib.sha256(content).hexdigest()!=INPUT_SHA256:
        raise ValueError('historical control input changed')
    control=next(x for x in json.loads(content)['controls'] if x['symbol']==a.symbol)
    prior=json.loads(a.prior_attempts)
    if not a.execute:
        print(json.dumps({'status':'validated_no_http_no_writes','symbol':a.symbol,'ranges':len(requests)}))
        return
    import os

    import psycopg2

    from bifrost_market_data.config import load_config, postgres_connect_kwargs
    cfg=load_config()
    poly=cfg.get('polygon') or {}
    key=os.environ.get('POLYGON_API_KEY') or os.environ.get('MASSIVE_API_KEY') or poly.get('api_key')
    conn=psycopg2.connect(**postgres_connect_kwargs(cfg,statement_timeout='20s'),connect_timeout=10)
    async def execute():
        async with PilotClient(key,tier=poly.get('tier','starter'),max_retries=3,
                               replacement=True,initial_attempts=prior) as client:
            await run_pilot(conn,client,requests,receipt=a.receipt,lock=a.lock,control=control)
    try:
        asyncio.run(execute())
    finally:
        conn.close()
        if a.receipt.exists():
            print('REPLACEMENT_REPORT:'+json.dumps(json.loads(a.receipt.read_text())),flush=True)


if __name__=='__main__':
    main()
