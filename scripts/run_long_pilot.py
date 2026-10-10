"""Execute the fixed pilot in the approved Market Data writer environment."""
import argparse
import asyncio
from pathlib import Path

from bifrost_market_data.ingest.long_pilot import PilotClient, load_pilot, run_pilot


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--manifest', type=Path, required=True)
    p.add_argument('--sha256', required=True)
    p.add_argument('--receipt', type=Path, required=True)
    p.add_argument('--lock', type=Path, required=True)
    p.add_argument('--execute', action='store_true')
    a = p.parse_args()
    requests = load_pilot(a.manifest, a.sha256)
    if not a.execute:
        print('Validated: 82 ranges, 120 target days; no HTTP or DB writes')
        return
    import os

    import psycopg2

    from bifrost_market_data.config import load_config, postgres_connect_kwargs
    cfg = load_config()
    poly = cfg.get('polygon') or {}
    key = poly.get('api_key') or os.environ.get('POLYGON_API_KEY') or os.environ.get('MASSIVE_API_KEY')
    if not key:
        raise RuntimeError('vendor key must be supplied by the approved domain runtime')
    conn = psycopg2.connect(**postgres_connect_kwargs(cfg))
    async def execute():
        async with PilotClient(key, tier=poly.get('tier','starter')) as client:
            await run_pilot(conn, client, requests, receipt=a.receipt, lock=a.lock)
    try:
        asyncio.run(execute())
    finally:
        conn.close()


if __name__ == '__main__':
    main()
