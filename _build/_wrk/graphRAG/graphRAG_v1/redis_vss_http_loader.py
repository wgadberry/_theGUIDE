#!/usr/bin/env python3
"""
redis_vss_http_loader.py -- Load edge embeddings directly from ArangoDB into Redis VSS

No intermediate CSV/numpy files. Queries ArangoDB in batches via python-arango,
pipes each batch straight into Redis hashes with HNSW vector index.

Usage:
    python redis_vss_http_loader.py                     # load all edges
    python redis_vss_http_loader.py --limit 10000       # partial load for testing
    python redis_vss_http_loader.py --drop --batch 5000 # recreate index, larger AQL batches
"""

import argparse, sys, time, struct
import numpy as np
import redis
from redis.commands.search.field import (
    VectorField, TagField, NumericField, TextField,
)
from redis.commands.search.index_definition import IndexDefinition, IndexType
from arango import ArangoClient
from datetime import datetime

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
INDEX_NAME = 't16o_edges'
KEY_PREFIX = 't16o:edge:'
VECTOR_DIM = 384
VECTOR_ALGORITHM = 'HNSW'
DISTANCE_METRIC = 'COSINE'

ARANGO_HOST = 'http://localhost:8529'
ARANGO_DB = 't16o'
ARANGO_USER = 'root'
ARANGO_PASS = 'localdev'

# AQL: join edges -> addresses -> templates, return edge + embedding
EDGE_TEMPLATE_AQL = '''
    FOR e IN edges
        LIMIT @offset, @limit
        LET from_addr = DOCUMENT(e._from)
        LET to_addr = DOCUMENT(e._to)
        LET hp = e.pool != null
        LET template = FIRST(
            FOR t IN templates
                FILTER t.edge_type_code == e.edge_type
                    AND t.from_address_type == from_addr.type
                    AND t.to_address_type == to_addr.type
                    AND t.has_pool == hp
                SORT t.row_count DESC
                LIMIT 1
                RETURN t
        )
        FILTER template != null
        RETURN {
            id: e._key,
            vector: template.embedding,
            signature: e.signature,
            edge_type: e.edge_type,
            from_address: from_addr._key,
            from_type: from_addr.type,
            to_address: to_addr._key,
            to_type: to_addr.type,
            token: e.token,
            amount: e.amount,
            decimals: e.decimals,
            block_time: e.block_time,
            pool: e.pool,
            program: e.program,
            narrative: template.narrative,
            pattern_hash: template.pattern_hash,
            edge_category: template.edge_category,
            edge_direction: template.edge_direction
        }
'''


def log(msg):
    print(f'[{datetime.now().strftime("%H:%M:%S")}] {msg}', flush=True)


# ---------------------------------------------------------------------------
# Redis index setup (same schema as redis_vss_loader.py)
# ---------------------------------------------------------------------------

def create_index(r, drop=False):
    """Create the RediSearch index with HNSW vector field + payload fields."""
    try:
        info = r.ft(INDEX_NAME).info()
        if drop:
            log(f'Dropping existing index: {INDEX_NAME}')
            r.ft(INDEX_NAME).dropindex(delete_documents=True)
        else:
            doc_count = info.get('num_docs', '?')
            log(f'Index exists: {INDEX_NAME} ({doc_count} docs). Use --drop to recreate.')
            return False
    except redis.exceptions.ResponseError:
        pass

    schema = [
        VectorField('vector',
                     VECTOR_ALGORITHM,
                     {'TYPE': 'FLOAT32', 'DIM': VECTOR_DIM, 'DISTANCE_METRIC': DISTANCE_METRIC,
                      'INITIAL_CAP': 500000, 'M': 16, 'EF_CONSTRUCTION': 200}),
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


# ---------------------------------------------------------------------------
# Main pipeline: ArangoDB → Redis
# ---------------------------------------------------------------------------

def load_batch(r, results):
    """Push a batch of AQL results into Redis hashes."""
    if not results:
        return 0

    pipe = r.pipeline(transaction=False)
    loaded = 0

    for row in results:
        vec = row.get('vector')
        if not vec or len(vec) != VECTOR_DIM:
            continue

        key = f'{KEY_PREFIX}{row["id"]}'
        vec_bytes = np.array(vec, dtype=np.float32).tobytes()

        mapping = {
            'vector': vec_bytes,
            'signature': row.get('signature') or '',
            'edge_type': row.get('edge_type') or '',
            'from_address': row.get('from_address') or '',
            'from_type': row.get('from_type') or '',
            'to_address': row.get('to_address') or '',
            'to_type': row.get('to_type') or '',
            'token': row.get('token') or '',
            'amount': str(row.get('amount') or '0'),
            'decimals': int(row.get('decimals') or 0),
            'block_time': int(row.get('block_time') or 0),
            'pool': row.get('pool') or '',
            'program': row.get('program') or '',
            'narrative': row.get('narrative') or '',
            'pattern_hash': row.get('pattern_hash') or '',
            'edge_category': row.get('edge_category') or '',
            'edge_direction': row.get('edge_direction') or '',
        }
        pipe.hset(key, mapping=mapping)
        loaded += 1

    pipe.execute()
    return loaded


def main():
    parser = argparse.ArgumentParser(description='Load ArangoDB edges directly into Redis VSS')
    parser.add_argument('--batch', type=int, default=2000, help='AQL batch size (default: 2000)')
    parser.add_argument('--limit', type=int, default=0, help='Max edges to load, 0=all (default: 0)')
    parser.add_argument('--drop', action='store_true', help='Drop existing index and reload')
    args = parser.parse_args()

    # Connect
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
    log(f'Redis: {REDIS_HOST}:{REDIS_PORT}')

    mods = [m[b'name'].decode() for m in r.module_list()]
    if 'search' not in mods:
        log('ERROR: RediSearch module not loaded. Use redis/redis-stack image.')
        sys.exit(1)

    db = ArangoClient(hosts=ARANGO_HOST).db(ARANGO_DB, username=ARANGO_USER, password=ARANGO_PASS)
    total_edges = db.collection('edges').count()
    total_templates = db.collection('templates').count()
    log(f'ArangoDB: {total_edges:,} edges, {total_templates} templates')

    if total_templates == 0:
        log('ERROR: No templates with embeddings found. Run template generation first.')
        sys.exit(1)

    # Create index
    create_index(r, drop=args.drop)

    # Stream from ArangoDB → Redis
    target = args.limit if args.limit > 0 else total_edges
    offset = 0
    loaded = 0
    skipped = 0
    t0 = time.time()

    log(f'Loading: target={target:,}  batch={args.batch}')

    while offset < target:
        batch_limit = min(args.batch, target - offset)
        try:
            results = list(db.aql.execute(
                EDGE_TEMPLATE_AQL,
                bind_vars={'offset': offset, 'limit': batch_limit},
                ttl=300,
                batch_size=batch_limit,
            ))
        except Exception as e:
            log(f'  AQL error at offset {offset}: {e}')
            break

        if not results:
            # No more edges with matching templates
            if offset < total_edges:
                offset += batch_limit
                skipped += batch_limit
                continue
            break

        batch_loaded = load_batch(r, results)
        loaded += batch_loaded
        skipped += len(results) - batch_loaded  # rows without valid vectors
        offset += batch_limit

        elapsed = time.time() - t0
        rate = loaded / elapsed if elapsed > 0 else 0

        if loaded % 10000 < args.batch:
            log(f'  [{offset:,}/{target:,}] loaded={loaded:,} skipped={skipped:,} ({rate:,.0f}/sec)')

    elapsed = time.time() - t0
    rate = loaded / elapsed if elapsed > 0 else 0
    log(f'Done: {loaded:,} docs loaded, {skipped:,} skipped in {elapsed:.1f}s ({rate:,.0f}/sec)')

    # Verify
    try:
        info = r.execute_command('FT.INFO', INDEX_NAME)
        # info is a flat list: [key, val, key, val, ...]
        info_dict = dict(zip(info[::2], info[1::2]))
        log(f'Index: {info_dict.get(b"num_docs", b"?").decode()} docs indexed')
    except Exception as e:
        # Fallback: count keys directly
        key_count = len(r.keys(f'{KEY_PREFIX}*'.encode()))
        log(f'Index verify failed ({e}), but {key_count:,} keys exist in Redis')


if __name__ == '__main__':
    main()
