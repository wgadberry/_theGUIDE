"""
qdrant_export.py -- Export edge+template data from ArangoDB to disk

Produces two files:
  - qdrant_payload.csv  (edge metadata)
  - qdrant_vectors.npy  (384-dim embeddings, numpy binary)

Usage:
    python qdrant_export.py [--batch 10000]
"""

import argparse, sys, csv, time
import numpy as np
from datetime import datetime
from arango import ArangoClient

ARANGO_HOST = 'http://localhost:8529'
ARANGO_DB = 't16o'
ARANGO_USER = 'root'
ARANGO_PASS = 'localdev'

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
            id: TO_NUMBER(e._key),
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


def main():
    parser = argparse.ArgumentParser(description='Export ArangoDB edges to Qdrant vectors')
    parser.add_argument('--batch', type=int, default=10000, help='AQL batch size (default: 10000)')
    args = parser.parse_args()

    batch_size = args.batch

    db = ArangoClient(hosts=ARANGO_HOST).db(ARANGO_DB, username=ARANGO_USER, password=ARANGO_PASS)
    total_edges = db.collection('edges').count()
    log(f'Edges: {total_edges}  Batch: {batch_size}')

    vectors = []
    offset = 0
    exported = 0

    with open(PAYLOAD_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=PAYLOAD_FIELDS, extrasaction='ignore')
        writer.writeheader()

        while offset < total_edges:
            results = list(db.aql.execute(
                EDGE_TEMPLATE_AQL,
                bind_vars={'limit': batch_size, 'offset': offset},
            ))

            if not results:
                break

            for r in results:
                vec = r.get('vector')
                if vec is None:
                    continue
                vectors.append(vec)
                writer.writerow(r)
                exported += 1

            offset += batch_size
            log(f'  [{offset}/{total_edges}] exported={exported}')

    np.save(VECTOR_FILE, np.array(vectors, dtype=np.float32))
    log(f'Done: {exported} edges')
    log(f'  {PAYLOAD_FILE}: {exported} rows')
    log(f'  {VECTOR_FILE}: {len(vectors)} x 384 vectors')


if __name__ == '__main__':
    main()
