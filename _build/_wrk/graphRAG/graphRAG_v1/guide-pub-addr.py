"""
guide-pub-addr.py -- Address signature publisher

Fetches transaction signatures for a wallet address via RPC, stores as
a RedisJSON object, and pushes the key onto the Redis work queue.

Usage:
    python guide-pub-addr.py <wallet_address> [--limit 50]
"""

import argparse, sys, time, json, uuid
from graph_common import (
    log, rpc_call, REDIS_QUEUE, REDIS_KEY_PREFIX,
)
import redis as redis_lib
import requests

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
TAG = 'pub-addr'


def main():
    parser = argparse.ArgumentParser(description='Address signature publisher')
    parser.add_argument('wallet', help='Wallet address to fetch signatures for')
    parser.add_argument('--limit', type=int, default=50, help='Max signatures to fetch (default: 50)')
    args = parser.parse_args()

    wallet = args.wallet
    limit = args.limit

    log(TAG, f'Wallet: {wallet}  Limit: {limit}')

    rdb = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    session = requests.Session()

    # 1. Fetch signatures via RPC (pages of 1000, the max per call)
    log(TAG, 'Fetching signatures...')
    page_size = min(limit, 1000)
    msg_size = 100
    fetched = 0
    queued = 0
    before = None

    while fetched < limit:
        params = {'limit': page_size}
        if before:
            params['before'] = before

        try:
            resp = rpc_call(session, 'getSignaturesForAddress', [wallet, params])
            page = resp.get('result', [])
        except Exception as e:
            if '429' in str(e):
                log(TAG, f'  Rate limited, waiting 2s...')
                time.sleep(2)
                continue
            raise

        if not page:
            break

        before = page[-1]['signature']
        fetched += len(page)

        # Filter and queue immediately — don't wait for all pages
        valid = [
            {'signature': s['signature'], 'block_time': s.get('blockTime', 0), 'slot': s.get('slot', 0)}
            for s in page
            if s.get('confirmationStatus') == 'finalized' and s.get('err') is None
        ]

        if valid:
            for i in range(0, len(valid), msg_size):
                chunk = valid[i:i + msg_size]
                key = f'{REDIS_KEY_PREFIX}{uuid.uuid4().hex[:12]}'
                rdb.set(key, json.dumps({
                    'origin': wallet,
                    'created_at': int(time.time()),
                    'count': len(chunk),
                    'signatures': chunk,
                }))
                rdb.lpush(REDIS_QUEUE, key)
            queued += len(valid)

        log(TAG, f'  Page: {len(page)} sigs, {len(valid)} valid (fetched: {fetched}, queued: {queued})')

        if len(page) < page_size:
            break  # last page

        time.sleep(0.2)  # pace between pages

    log(TAG, f'Done: {fetched} fetched, {queued} queued')


if __name__ == '__main__':
    main()
