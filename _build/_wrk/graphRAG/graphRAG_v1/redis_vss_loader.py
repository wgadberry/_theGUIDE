#!/usr/bin/env python3
"""
redis_vss_loader.py -- Load edge embeddings into Redis Stack (RediSearch VSS)

Replaces qdrant_loader.py. Uses the same CSV + numpy files produced by
qdrant_export.py, loads them into a RediSearch HNSW vector index.

Prerequisites:
    Redis Stack running with the 'search' module (redis/redis-stack:latest)

Usage:
    python redis_vss_loader.py                          # load all data
    python redis_vss_loader.py --batch 1000 --limit 5000  # partial load for testing
    python redis_vss_loader.py --drop                   # drop index + keys, reload
"""

import argparse, csv, sys, time
import numpy as np
import redis
from redis.commands.search.field import (
    VectorField, TagField, NumericField, TextField,
)
from redis.commands.search.index_definition import IndexDefinition, IndexType
from datetime import datetime

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

INDEX_NAME = 't16o_edges'
KEY_PREFIX = 't16o:edge:'

VECTOR_DIM = 384
VECTOR_ALGORITHM = 'HNSW'
DISTANCE_METRIC = 'COSINE'

PAYLOAD_FILE = 'V:/qdrant_payload.csv'
VECTOR_FILE = 'V:/qdrant_vectors.npy'

PAYLOAD_FIELDS = [
    'id', 'signature', 'edge_type', 'from_address', 'from_type',
    'to_address', 'to_type', 'token', 'amount', 'decimals',
    'block_time', 'pool', 'program', 'narrative', 'pattern_hash',
    'edge_category', 'edge_direction',
]


def log(msg):
    print(f'[{datetime.now().strftime("%H:%M:%S")}] {msg}', flush=True)


def create_index(r, drop=False):
    """Create the RediSearch index with HNSW vector field + payload fields."""
    try:
        info = r.ft(INDEX_NAME).info()
        if drop:
            log(f'Dropping existing index: {INDEX_NAME}')
            r.ft(INDEX_NAME).dropindex(delete_documents=True)
        else:
            doc_count = info.get('num_docs', info.get('num_records', '?'))
            log(f'Index exists: {INDEX_NAME} ({doc_count} docs). Use --drop to recreate.')
            return False
    except redis.exceptions.ResponseError:
        pass  # index doesn't exist

    schema = [
        # Vector field — HNSW for approximate KNN
        VectorField('vector',
                     VECTOR_ALGORITHM,
                     {'TYPE': 'FLOAT32', 'DIM': VECTOR_DIM, 'DISTANCE_METRIC': DISTANCE_METRIC,
                      'INITIAL_CAP': 1000000, 'M': 16, 'EF_CONSTRUCTION': 200}),
        # Filterable payload fields
        TagField('edge_type'),
        TagField('from_type'),
        TagField('to_type'),
        TagField('from_address'),
        TagField('to_address'),
        TagField('signature'),
        TagField('edge_category'),
        TagField('edge_direction'),
        TagField('pattern_hash'),
        NumericField('block_time', sortable=True),
        NumericField('decimals'),
        # Full-text searchable
        TextField('narrative'),
        TextField('token'),
        TextField('pool'),
        TextField('program'),
        TextField('amount', no_stem=True),
    ]

    definition = IndexDefinition(prefix=[KEY_PREFIX], index_type=IndexType.HASH)
    r.ft(INDEX_NAME).create_index(schema, definition=definition)
    log(f'Created index: {INDEX_NAME} (HNSW, {VECTOR_DIM}-dim, {DISTANCE_METRIC})')
    return True


def load_data(r, batch_size=500, limit=0):
    """Load CSV payload + numpy vectors into Redis hashes."""
    log(f'Loading vectors from {VECTOR_FILE}')
    vectors = np.load(VECTOR_FILE, mmap_mode='r')
    log(f'Vectors: {vectors.shape[0]:,} x {vectors.shape[1]}')

    log(f'Loading payload from {PAYLOAD_FILE}')
    with open(PAYLOAD_FILE, 'r', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    total = len(rows)
    target = min(limit, total) if limit > 0 else total
    log(f'Payload rows: {total:,}  Target: {target:,}  Batch: {batch_size}')

    loaded = 0
    t0 = time.time()

    for i in range(0, target, batch_size):
        batch_rows = rows[i:i + batch_size]
        batch_vecs = vectors[i:i + batch_size]

        pipe = r.pipeline(transaction=False)
        for row, vec in zip(batch_rows, batch_vecs):
            key = f'{KEY_PREFIX}{row["id"]}'
            mapping = {
                'vector': vec.tobytes(),
                'signature': row['signature'] or '',
                'edge_type': row['edge_type'] or '',
                'from_address': row['from_address'] or '',
                'from_type': row['from_type'] or '',
                'to_address': row['to_address'] or '',
                'to_type': row['to_type'] or '',
                'token': row['token'] or '',
                'amount': row['amount'] or '0',
                'decimals': int(row['decimals']) if row['decimals'] else 0,
                'block_time': int(row['block_time']) if row['block_time'] else 0,
                'pool': row['pool'] or '',
                'program': row['program'] or '',
                'narrative': row['narrative'] or '',
                'pattern_hash': row['pattern_hash'] or '',
                'edge_category': row['edge_category'] or '',
                'edge_direction': row['edge_direction'] or '',
            }
            pipe.hset(key, mapping=mapping)

        pipe.execute()
        loaded += len(batch_rows)

        if loaded % 10000 < batch_size:
            elapsed = time.time() - t0
            rate = loaded / elapsed if elapsed > 0 else 0
            log(f'  [{loaded:,}/{target:,}] {rate:,.0f} docs/sec')

    elapsed = time.time() - t0
    rate = loaded / elapsed if elapsed > 0 else 0
    log(f'Done: {loaded:,} docs loaded in {elapsed:.1f}s ({rate:,.0f} docs/sec)')
    return loaded


def main():
    parser = argparse.ArgumentParser(description='Load edge vectors into Redis Stack VSS')
    parser.add_argument('--batch', type=int, default=500, help='Pipeline batch size (default: 500)')
    parser.add_argument('--limit', type=int, default=0, help='Max docs to load, 0=all (default: 0)')
    parser.add_argument('--drop', action='store_true', help='Drop existing index and reload')
    args = parser.parse_args()

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
    log(f'Connected to Redis {REDIS_HOST}:{REDIS_PORT}')

    # Verify search module
    mods = [m[b'name'].decode() for m in r.module_list()]
    if 'search' not in mods:
        log('ERROR: RediSearch module not loaded. Use redis/redis-stack image.')
        sys.exit(1)
    log(f'Modules: {", ".join(mods)}')

    created = create_index(r, drop=args.drop)
    if not created and not args.drop:
        log('Index already exists. Use --drop to recreate, or data will be upserted.')

    loaded = load_data(r, batch_size=args.batch, limit=args.limit)

    # Verify
    info = r.ft(INDEX_NAME).info()
    log(f'Index info: {info.get("num_docs", "?")} docs indexed')


if __name__ == '__main__':
    main()
