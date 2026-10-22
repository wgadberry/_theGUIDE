"""
guide-pub-mysql.py -- Bulk publish existing MySQL tx signatures via Redis list

Reads signatures from the MySQL tx table in chunks, stores each chunk as
a RedisJSON object, and pushes the key onto the Redis work queue.

Usage:
    python guide-pub-mysql.py [--batch 1000] [--offset 0] [--limit 10000]
"""

import argparse, sys, time, json, uuid
import mysql.connector
import redis as redis_lib
from graph_common import (
    log, REDIS_QUEUE, REDIS_KEY_PREFIX,
)

TAG = 'pub-mysql'

with open('../guide-config.json') as f:
    _cfg = json.load(f)

DB_CONFIG = {
    'host': _cfg['DB_HOST'], 'port': _cfg['DB_PORT'],
    'user': _cfg['DB_USER'], 'password': _cfg['DB_PASSWORD'],
    'database': _cfg['DB_NAME'],
}

REDIS_HOST = 'localhost'
REDIS_PORT = 6379


def main():
    parser = argparse.ArgumentParser(description='Bulk publish MySQL tx signatures via Redis')
    parser.add_argument('--batch', type=int, default=1000, help='MySQL SELECT batch size (default: 1000)')
    parser.add_argument('--offset', type=int, default=0, help='Start offset in tx table (default: 0)')
    parser.add_argument('--limit', type=int, default=0, help='Max transactions, 0=all (default: 0)')
    args = parser.parse_args()

    batch_size = args.batch
    msg_size = 100  # sigs per Redis message
    offset = args.offset
    limit = args.limit

    rdb = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    mysql_conn = mysql.connector.connect(**DB_CONFIG)
    cur = mysql_conn.cursor()
    cur.execute('SELECT COUNT(*) FROM tx')
    total = cur.fetchone()[0]
    target = limit if limit > 0 else total
    log(TAG, f'Total in MySQL: {total}  Target: {target}  Batch: {batch_size}  Offset: {offset}')

    published = 0
    end = offset + target

    while offset < end:
        cur.execute(
            'SELECT signature, block_time FROM tx ORDER BY id LIMIT %s OFFSET %s',
            (batch_size, offset)
        )
        rows = cur.fetchall()
        if not rows:
            break

        all_sigs = [
            {'signature': sig, 'block_time': bt or 0, 'slot': 0}
            for sig, bt in rows
        ]

        # Chunk into msg_size messages for better worker distribution
        for i in range(0, len(all_sigs), msg_size):
            chunk = all_sigs[i:i + msg_size]
            key = f'{REDIS_KEY_PREFIX}{uuid.uuid4().hex[:12]}'
            payload = {
                'origin': 'mysql_import',
                'created_at': int(time.time()),
                'count': len(chunk),
                'signatures': chunk,
            }
            rdb.set(key, json.dumps(payload))
            rdb.lpush(REDIS_QUEUE, key)

        published += len(all_sigs)
        offset += batch_size
        log(TAG, f'  [{offset}/{total}] published={published}')

    cur.close()
    mysql_conn.close()

    queue_len = rdb.llen(REDIS_QUEUE)
    log(TAG, f'Done: {published} published, {queue_len} items in queue')


if __name__ == '__main__':
    main()
