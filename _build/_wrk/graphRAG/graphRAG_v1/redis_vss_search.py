#!/usr/bin/env python3
"""
redis_vss_search.py -- Vector similarity search via Redis Stack (RediSearch VSS)

Replaces Qdrant for: semantic search, similar-edges, address-behavior profiling,
anomaly detection. Uses the same all-MiniLM-L6-v2 embedder (384-dim).

Usage as library:
    from redis_vss_search import RedisVSS
    vss = RedisVSS()
    results = vss.semantic_search("whale transfer suspicious", top_k=10)

Usage as CLI test:
    python redis_vss_search.py                              # run all tests
    python redis_vss_search.py --query "large swap whale"   # single semantic search
    python redis_vss_search.py --similar <signature>         # find similar edges
    python redis_vss_search.py --behavior <address>          # address behavior profile
"""

import argparse, sys, time, struct
import numpy as np
import redis
from redis.commands.search.query import Query
from datetime import datetime

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
INDEX_NAME = 't16o_edges'
KEY_PREFIX = 't16o:edge:'
VECTOR_DIM = 384


def log(msg):
    print(f'[{datetime.now().strftime("%H:%M:%S")}] {msg}', flush=True)


class RedisVSS:
    """Redis Stack vector similarity search client."""

    def __init__(self, host=REDIS_HOST, port=REDIS_PORT):
        self.r = redis.Redis(host=host, port=port, decode_responses=False)
        self._embedder = None

    @property
    def embedder(self):
        if self._embedder is None:
            from sentence_transformers import SentenceTransformer
            self._embedder = SentenceTransformer('all-MiniLM-L6-v2')
        return self._embedder

    def _encode(self, text):
        """Encode text to 384-dim float32 vector bytes."""
        vec = self.embedder.encode(text)
        return np.array(vec, dtype=np.float32).tobytes()

    def _parse_results(self, results):
        """Parse FT.SEARCH results into list of dicts."""
        docs = []
        for doc in results.docs:
            d = {}
            for k, v in doc.__dict__.items():
                if k == 'id':
                    d['id'] = v.decode() if isinstance(v, bytes) else v
                    continue
                if k == 'payload' or k == 'vector':
                    continue
                if isinstance(v, bytes):
                    try:
                        d[k] = v.decode('utf-8')
                    except UnicodeDecodeError:
                        continue
                else:
                    d[k] = v
            if hasattr(doc, '__vector_score'):
                d['score'] = 1.0 - float(doc.__vector_score)
            elif 'vector_score' in doc.__dict__:
                raw = doc.__dict__['vector_score']
                d['score'] = round(1.0 - float(raw), 4)
            docs.append(d)
        return docs

    def semantic_search(self, query_text, top_k=10, edge_type=None, address=None):
        """Natural language search over edge narratives.

        Encodes query text, runs KNN against the HNSW index with optional
        pre-filters on edge_type and from_address.
        """
        t0 = time.time()
        vec_bytes = self._encode(query_text)

        # Build filter expression
        filters = []
        if edge_type:
            filters.append(f'@edge_type:{{{edge_type}}}')
        if address:
            filters.append(f'@from_address:{{{address}}}')

        pre_filter = ' '.join(filters) if filters else '*'

        q = (Query(f'({pre_filter})=>[KNN {top_k} @vector $vec AS vector_score]')
             .sort_by('vector_score')
             .return_fields('signature', 'edge_type', 'from_address', 'from_type',
                            'to_address', 'to_type', 'token', 'amount', 'decimals',
                            'block_time', 'narrative', 'edge_category', 'edge_direction',
                            'vector_score')
             .paging(0, top_k)
             .dialect(2))

        results = self.r.ft(INDEX_NAME).search(q, query_params={'vec': vec_bytes})
        elapsed = round((time.time() - t0) * 1000, 2)

        docs = self._parse_results(results)
        return elapsed, docs

    def similar_edges(self, signature, top_k=10):
        """Find edges similar to a given transaction's edges.

        Looks up the vector for the signature, then runs KNN to find similar.
        """
        t0 = time.time()

        # Step 1: Find the source edge by signature
        find_q = (Query(f'@signature:{{{signature}}}')
                  .return_fields('vector')
                  .paging(0, 1)
                  .dialect(2))

        src = self.r.ft(INDEX_NAME).search(find_q)
        if not src.docs:
            return 0, []

        # Get the raw vector bytes from the hash directly
        doc_key = src.docs[0].id
        if isinstance(doc_key, str):
            doc_key = doc_key.encode()
        vec_bytes = self.r.hget(doc_key, 'vector')
        if not vec_bytes:
            return 0, []

        # Step 2: KNN search excluding the source signature
        q = (Query(f'(-@signature:{{{signature}}})=>[KNN {top_k} @vector $vec AS vector_score]')
             .sort_by('vector_score')
             .return_fields('signature', 'edge_type', 'from_address', 'from_type',
                            'to_address', 'to_type', 'token', 'amount', 'narrative',
                            'edge_category', 'vector_score')
             .paging(0, top_k)
             .dialect(2))

        results = self.r.ft(INDEX_NAME).search(q, query_params={'vec': vec_bytes})
        elapsed = round((time.time() - t0) * 1000, 2)

        docs = self._parse_results(results)
        return elapsed, docs

    def address_behavior(self, address, top_k=10):
        """Find addresses with similar behavioral patterns.

        Averages all edge vectors for an address, then finds KNN matches
        and groups by from_address to find similarly-behaving wallets.
        """
        t0 = time.time()

        # Step 1: Get vectors for this address's outbound edges
        find_q = (Query(f'@from_address:{{{address}}}')
                  .return_fields('vector')
                  .paging(0, 20)
                  .dialect(2))

        src = self.r.ft(INDEX_NAME).search(find_q)
        if not src.docs:
            return 0, []

        # Read raw vectors from hashes
        vectors = []
        for doc in src.docs:
            doc_key = doc.id
            if isinstance(doc_key, str):
                doc_key = doc_key.encode()
            raw = self.r.hget(doc_key, 'vector')
            if raw:
                vec = np.frombuffer(raw, dtype=np.float32)
                vectors.append(vec)

        if not vectors:
            return 0, []

        # Step 2: Average vector = behavioral signature
        avg_vec = np.mean(vectors, axis=0).astype(np.float32).tobytes()

        # Step 3: KNN with more results to aggregate by address
        fetch = top_k * 5
        q = (Query(f'(-@from_address:{{{address}}})=>[KNN {fetch} @vector $vec AS vector_score]')
             .sort_by('vector_score')
             .return_fields('from_address', 'edge_type', 'token', 'vector_score')
             .paging(0, fetch)
             .dialect(2))

        results = self.r.ft(INDEX_NAME).search(q, query_params={'vec': avg_vec})

        # Step 4: Group by address, rank by average similarity
        addr_scores = {}
        for doc in results.docs:
            fa = doc.__dict__.get('from_address', b'')
            if isinstance(fa, bytes):
                fa = fa.decode()
            if not fa or fa == address:
                continue

            score_raw = doc.__dict__.get('vector_score', b'1')
            score = 1.0 - float(score_raw)

            et = doc.__dict__.get('edge_type', b'')
            tk = doc.__dict__.get('token', b'')
            if isinstance(et, bytes): et = et.decode()
            if isinstance(tk, bytes): tk = tk.decode()

            if fa not in addr_scores:
                addr_scores[fa] = {'scores': [], 'edge_types': set(), 'tokens': set()}
            addr_scores[fa]['scores'].append(score)
            addr_scores[fa]['edge_types'].add(et)
            addr_scores[fa]['tokens'].add(tk)

        ranked = sorted(addr_scores.items(),
                        key=lambda x: -sum(x[1]['scores']) / len(x[1]['scores']))[:top_k]

        similar = [{
            'address': a,
            'similarity': round(sum(d['scores']) / len(d['scores']), 4),
            'matching_edges': len(d['scores']),
            'edge_types': list(d['edge_types']),
            'tokens': list(d['tokens']),
        } for a, d in ranked]

        elapsed = round((time.time() - t0) * 1000, 2)
        return elapsed, similar

    def anomaly_search(self, edge_type=None, top_k=10):
        """Find anomalous edges via semantic search for suspicious patterns."""
        query = 'wallet fully drained whale transfer suspicious exit large outflow'
        return self.semantic_search(query, top_k=top_k, edge_type=edge_type)

    def index_info(self):
        """Get index stats."""
        try:
            info = self.r.ft(INDEX_NAME).info()
            return {
                'num_docs': info.get('num_docs', info.get(b'num_docs', '?')),
                'num_records': info.get('num_records', info.get(b'num_records', '?')),
                'index_name': INDEX_NAME,
            }
        except Exception as e:
            return {'error': str(e)}


# ---------------------------------------------------------------------------
# CLI test harness
# ---------------------------------------------------------------------------

def run_tests(vss):
    """Run all query types and print results + timing."""
    info = vss.index_info()
    log(f'Index: {info}')
    print()

    # 1. Semantic search
    log('--- Semantic Search: "large swap whale transfer" ---')
    elapsed, results = vss.semantic_search('large swap whale transfer', top_k=5)
    log(f'  {len(results)} results in {elapsed}ms')
    for r in results[:3]:
        log(f'  [{r.get("score", "?")}] {r.get("edge_type", "?")} '
            f'{r.get("from_address", "?")[:12]}... -> {r.get("to_address", "?")[:12]}...')
        narr = r.get('narrative', '')
        if narr:
            log(f'    {narr[:120]}...')
    print()

    # 2. Semantic search with filter
    log('--- Semantic Search + Filter: edge_type=swap_in ---')
    elapsed, results = vss.semantic_search('large token swap', top_k=5, edge_type='swap_in')
    log(f'  {len(results)} results in {elapsed}ms')
    for r in results[:3]:
        log(f'  [{r.get("score", "?")}] {r.get("edge_type", "?")} {r.get("from_address", "?")[:16]}...')
    print()

    # 3. Similar edges — use signature from first semantic result
    if results:
        sig = results[0].get('signature', '')
        if sig:
            log(f'--- Similar Edges: {sig[:20]}... ---')
            elapsed, results = vss.similar_edges(sig, top_k=5)
            log(f'  {len(results)} results in {elapsed}ms')
            for r in results[:3]:
                log(f'  [{r.get("score", "?")}] {r.get("edge_type", "?")} '
                    f'{r.get("from_address", "?")[:12]}... -> {r.get("to_address", "?")[:12]}...')
            print()

    # 4. Address behavior — use from_address from first semantic result
    if results:
        addr = results[0].get('from_address', '')
        if addr:
            log(f'--- Address Behavior: {addr[:16]}... ---')
            elapsed, similar = vss.address_behavior(addr, top_k=5)
            log(f'  {len(similar)} similar addresses in {elapsed}ms')
            for s in similar[:3]:
                log(f'  [{s["similarity"]}] {s["address"][:16]}... '
                    f'({s["matching_edges"]} edges, types: {s["edge_types"]})')
            print()

    # 5. Anomaly detection
    log('--- Anomaly Detection ---')
    elapsed, results = vss.anomaly_search(top_k=5)
    log(f'  {len(results)} results in {elapsed}ms')
    for r in results[:3]:
        log(f'  [{r.get("score", "?")}] {r.get("edge_type", "?")} '
            f'{r.get("from_address", "?")[:12]}... -> {r.get("to_address", "?")[:12]}...')
    print()

    log('All tests complete.')


def main():
    parser = argparse.ArgumentParser(description='Redis VSS search — test & query CLI')
    parser.add_argument('--query', type=str, help='Run a semantic search with this text')
    parser.add_argument('--similar', type=str, help='Find edges similar to this signature')
    parser.add_argument('--behavior', type=str, help='Find addresses behaving like this one')
    parser.add_argument('--anomalies', action='store_true', help='Run anomaly detection')
    parser.add_argument('--top-k', type=int, default=10, help='Number of results (default: 10)')
    parser.add_argument('--edge-type', type=str, help='Filter by edge type')
    args = parser.parse_args()

    vss = RedisVSS()

    # Single query mode
    if args.query:
        elapsed, results = vss.semantic_search(args.query, top_k=args.top_k, edge_type=args.edge_type)
        log(f'Semantic search: {len(results)} results in {elapsed}ms')
        for r in results:
            print(f'  [{r.get("score", "?")}] {r.get("edge_type")} '
                  f'{r.get("from_address", "")[:16]} -> {r.get("to_address", "")[:16]} '
                  f'| {r.get("narrative", "")[:80]}')
    elif args.similar:
        elapsed, results = vss.similar_edges(args.similar, top_k=args.top_k)
        log(f'Similar edges: {len(results)} results in {elapsed}ms')
        for r in results:
            print(f'  [{r.get("score", "?")}] {r.get("signature", "")[:20]} {r.get("edge_type")}')
    elif args.behavior:
        elapsed, similar = vss.address_behavior(args.behavior, top_k=args.top_k)
        log(f'Address behavior: {len(similar)} similar addresses in {elapsed}ms')
        for s in similar:
            print(f'  [{s["similarity"]}] {s["address"][:20]}... '
                  f'({s["matching_edges"]} edges, types: {s["edge_types"]})')
    elif args.anomalies:
        elapsed, results = vss.anomaly_search(edge_type=args.edge_type, top_k=args.top_k)
        log(f'Anomalies: {len(results)} results in {elapsed}ms')
        for r in results:
            print(f'  [{r.get("score", "?")}] {r.get("edge_type")} '
                  f'{r.get("from_address", "")[:16]} -> {r.get("to_address", "")[:16]}')
    else:
        # Full test suite
        run_tests(vss)


if __name__ == '__main__':
    main()
