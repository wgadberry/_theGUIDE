"""
guide-pub-addr-enrich.py -- Publish addresses needing enrichment to Redis queue

Scans ArangoDB for addresses missing metadata (funded_by, label, tags),
batches them into groups of 50, and pushes to Redis queue for consumer.

Usage:
    python guide-pub-addr-enrich.py [--limit 10000] [--batch 50]
"""

import argparse, sys, time, json, uuid
import redis as redis_lib
from graph_common import log, bootstrap_arango, REDIS_KEY_PREFIX

TAG = 'pub-enrich'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_QUEUE = 't16o:addr:enrich'


def main():
    parser = argparse.ArgumentParser(description='Publish addresses needing enrichment')
    parser.add_argument('--limit', type=int, default=10000, help='Max addresses to scan (default: 10000)')
    parser.add_argument('--batch', type=int, default=50, help='Addresses per Redis message (default: 50)')
    args = parser.parse_args()

    limit = args.limit
    batch_size = args.batch

    db = bootstrap_arango()
    rdb = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Find addresses missing enrichment (no funded_by field)
    log(TAG, f'Scanning for unenriched addresses (limit={limit})...')
    addrs = list(db.aql.execute('''
        FOR a IN addresses
            FILTER a.enriched == 0
            FILTER a.type IN ["wallet", "unknown", "pool"]
            FILTER (a.attempt_cnt || 0) < 3
            LIMIT @limit
            RETURN a._key
    ''', bind_vars={'limit': limit}))

    log(TAG, f'Found {len(addrs)} addresses to enrich')

    if not addrs:
        return

    # Batch into groups of 50 and push to queue
    queued = 0
    for i in range(0, len(addrs), batch_size):
        chunk = addrs[i:i + batch_size]
        key = f'{REDIS_KEY_PREFIX}enrich:{uuid.uuid4().hex[:12]}'
        rdb.set(key, json.dumps({
            'addresses': chunk,
            'count': len(chunk),
            'created_at': int(time.time()),
        }))
        rdb.lpush(REDIS_QUEUE, key)
        queued += len(chunk)

    queue_len = rdb.llen(REDIS_QUEUE)
    log(TAG, f'Queued {queued} addresses in {(queued + batch_size - 1) // batch_size} messages ({queue_len} in queue)')


if __name__ == '__main__':
    main()
