"""
qdrant_loader.py -- Bulk load from CSV + numpy vectors into Qdrant

Usage:
    python qdrant_export.py                # step 1: ArangoDB -> CSV + npy
    python qdrant_loader.py [--batch 500]  # step 2: CSV + npy -> Qdrant
"""

import argparse, sys, csv, time
import numpy as np
from datetime import datetime
from qdrant_client import QdrantClient
from qdrant_client.models import (
    VectorParams, Distance, PointStruct, PayloadSchemaType,
)

QDRANT_HOST = 'localhost'
QDRANT_PORT = 6333
QDRANT_COLLECTION = 't16o_edges'
VECTOR_DIM = 384

PAYLOAD_FILE = 'V:/qdrant_payload.csv'
VECTOR_FILE = 'V:/qdrant_vectors.npy'


def log(msg):
    print(f'[{datetime.now().strftime("%H:%M:%S")}] {msg}', flush=True)


def init_qdrant(client):
    collections = [c.name for c in client.get_collections().collections]
    if QDRANT_COLLECTION in collections:
        client.delete_collection(QDRANT_COLLECTION)

    client.create_collection(
        collection_name=QDRANT_COLLECTION,
        vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
    )

    for field, schema in [
        ('edge_type', PayloadSchemaType.KEYWORD),
        ('from_type', PayloadSchemaType.KEYWORD),
        ('to_type', PayloadSchemaType.KEYWORD),
        ('block_time', PayloadSchemaType.INTEGER),
        ('from_address', PayloadSchemaType.KEYWORD),
        ('to_address', PayloadSchemaType.KEYWORD),
        ('signature', PayloadSchemaType.KEYWORD),
        ('edge_category', PayloadSchemaType.KEYWORD),
    ]:
        client.create_payload_index(
            collection_name=QDRANT_COLLECTION,
            field_name=field,
            field_schema=schema,
        )
    log(f'Created collection: {QDRANT_COLLECTION}')


def main():
    parser = argparse.ArgumentParser(description='Load vectors into Qdrant')
    parser.add_argument('--batch', type=int, default=500, help='Qdrant upsert batch size (default: 500)')
    args = parser.parse_args()

    batch_size = args.batch

    qc = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
    init_qdrant(qc)

    log(f'Loading vectors from {VECTOR_FILE}')
    vectors = np.load(VECTOR_FILE)
    log(f'Vectors: {vectors.shape[0]} x {vectors.shape[1]}')

    log(f'Loading payload from {PAYLOAD_FILE}')
    with open(PAYLOAD_FILE, 'r', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    log(f'Payload rows: {len(rows)}')

    loaded = 0
    for i in range(0, len(rows), batch_size):
        batch_rows = rows[i:i + batch_size]
        batch_vecs = vectors[i:i + batch_size]

        points = []
        for row, vec in zip(batch_rows, batch_vecs):
            # Cast numeric fields
            point_id = int(row['id'])
            payload = {
                'signature': row['signature'],
                'edge_type': row['edge_type'],
                'from_address': row['from_address'],
                'from_type': row['from_type'],
                'to_address': row['to_address'],
                'to_type': row['to_type'],
                'token': row['token'],
                'amount': row['amount'],
                'decimals': int(row['decimals']) if row['decimals'] else 0,
                'block_time': int(row['block_time']) if row['block_time'] else 0,
                'pool': row['pool'] or None,
                'program': row['program'] or None,
                'narrative': row['narrative'],
                'pattern_hash': row['pattern_hash'],
                'edge_category': row['edge_category'],
                'edge_direction': row['edge_direction'],
            }
            points.append(PointStruct(
                id=point_id,
                vector=vec.tolist(),
                payload=payload,
            ))

        if points:
            qc.upsert(collection_name=QDRANT_COLLECTION, points=points)
            loaded += len(points)

        if (i + batch_size) % 10000 < batch_size:
            log(f'  [{i + len(batch_rows)}/{len(rows)}] loaded={loaded}')

    log(f'Done: {loaded} points')
    info = qc.get_collection(QDRANT_COLLECTION)
    log(f'Qdrant: {info.points_count} points')


if __name__ == '__main__':
    main()
