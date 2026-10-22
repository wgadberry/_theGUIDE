#!/usr/bin/env python3
"""
ChromaDB Search API — Natural language search over tx_guide edges.

Endpoints:
    GET /api/search?q=<query>&limit=10&token=OILLESS&dex=Raydium
    GET /api/health

Run:
    python chroma-search-api.py                    # Dev mode (port 5300)
    gunicorn -c gunicorn-search.conf.py wsgi-search:app   # Production
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from flask import Flask, jsonify, request
from flask_cors import CORS
import chromadb

try:
    from t16o_exchange.guide.common.config import load_config
    _cfg = load_config()
except ImportError:
    _cfg = {}

# Optional: validate API keys against MySQL (same as gateway)
try:
    import mysql.connector
    from t16o_exchange.guide.common.config import get_db_config
    HAS_DB = True
except ImportError:
    HAS_DB = False

# ── Config ──
CHROMADB_HOST = _cfg.get('CHROMADB_HOST', os.environ.get('CHROMADB_HOST', 'localhost'))
CHROMADB_PORT = int(_cfg.get('CHROMADB_PORT', os.environ.get('CHROMADB_PORT', '8000')))
COLLECTION_NAME = 'tx_guide_edges'
SEARCH_PORT = int(os.environ.get('SEARCH_PORT', '5300'))

# Simple API key validation (fallback when no DB available)
VALID_API_KEYS = {'admin_master_key'}
_api_key_cache = {}


def validate_api_key(api_key):
    """Validate API key — tries DB first, falls back to static list."""
    if api_key in _api_key_cache:
        return _api_key_cache[api_key]

    if HAS_DB:
        try:
            conn = mysql.connector.connect(**get_db_config())
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT id, api_key, is_active FROM tx_api_key WHERE api_key = %s AND is_active = 1",
                (api_key,)
            )
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            if row:
                _api_key_cache[api_key] = True
                return True
        except Exception:
            pass

    valid = api_key in VALID_API_KEYS
    if valid:
        _api_key_cache[api_key] = True
    return valid


def create_app():
    app = Flask(__name__)
    CORS(app)

    # Connect to ChromaDB
    chroma_client = chromadb.HttpClient(host=CHROMADB_HOST, port=CHROMADB_PORT)

    def get_collection():
        return chroma_client.get_collection(COLLECTION_NAME)

    # ── Auth middleware ──
    def require_auth():
        api_key = request.headers.get('X-API-Key')
        if not api_key:
            return jsonify({'error': 'Missing X-API-Key header'}), 401
        if not validate_api_key(api_key):
            return jsonify({'error': 'Invalid API key'}), 401
        return None

    # ── Search Endpoint ──
    @app.route('/api/search', methods=['GET'])
    def search():
        auth_err = require_auth()
        if auth_err:
            return auth_err

        q = request.args.get('q', '').strip()
        if not q:
            return jsonify({'error': 'Missing required parameter: q'}), 400

        limit = min(max(int(request.args.get('limit', 10)), 1), 100)

        # Build metadata filter from optional params
        where_clauses = []

        token = request.args.get('token', '').strip()
        if token:
            where_clauses.append({'token_symbol': token})

        mint = request.args.get('mint', '').strip()
        if mint:
            where_clauses.append({'mint_address': mint})

        address = request.args.get('address', '').strip()
        if address:
            # Match either from or to address
            where_clauses.append({
                '$or': [
                    {'from_address': address},
                    {'to_address': address}
                ]
            })

        edge_type = request.args.get('edge_type', '').strip()
        if edge_type:
            where_clauses.append({'edge_type': edge_type})

        dex = request.args.get('dex', '').strip()
        if dex:
            where_clauses.append({'dex': dex})

        after = request.args.get('after', type=int)
        if after:
            where_clauses.append({'block_time': {'$gte': after}})

        before = request.args.get('before', type=int)
        if before:
            where_clauses.append({'block_time': {'$lte': before}})

        where = None
        if len(where_clauses) == 1:
            where = where_clauses[0]
        elif len(where_clauses) > 1:
            where = {'$and': where_clauses}

        try:
            collection = get_collection()
            results = collection.query(
                query_texts=[q],
                n_results=limit,
                where=where if where else None,
                include=['documents', 'metadatas', 'distances']
            )

            # Format response
            items = []
            if results and results['ids'] and results['ids'][0]:
                for i, doc_id in enumerate(results['ids'][0]):
                    meta = results['metadatas'][0][i] if results['metadatas'] else {}
                    distance = results['distances'][0][i] if results['distances'] else 0
                    # ChromaDB returns distance (lower = better); convert to similarity score
                    score = max(0, 1 - distance)

                    items.append({
                        'score': round(score, 4),
                        'sentence': results['documents'][0][i] if results['documents'] else '',
                        # Slim metadata — all detail is in the sentence
                        'guide_id': meta.get('guide_id', 0),
                        'tx_signature': meta.get('tx_signature', ''),
                        'mint_address': meta.get('mint_address', ''),
                        'token_symbol': meta.get('token_symbol', ''),
                        'token_name': meta.get('token_name', ''),
                        'edge_type': meta.get('edge_type', ''),
                        'edge_category': meta.get('edge_category', ''),
                        'dex': meta.get('dex', ''),
                        'from_address': meta.get('from_address', ''),
                        'to_address': meta.get('to_address', ''),
                        'amount_human': meta.get('amount_human', 0),
                        'block_time': meta.get('block_time', 0),
                        'block_time_utc': meta.get('block_time_utc', ''),
                    })

            return jsonify({
                'query': q,
                'count': len(items),
                'results': items
            })

        except Exception as e:
            return jsonify({'error': str(e)}), 500

    # ── Health Endpoint ──
    @app.route('/api/health', methods=['GET'])
    def health():
        try:
            chroma_client.heartbeat()
            collection = get_collection()
            return jsonify({
                'status': 'ok',
                'chromadb': 'connected',
                'collection': COLLECTION_NAME,
                'document_count': collection.count(),
                'embedding_model': 'all-MiniLM-L6-v2'
            })
        except Exception as e:
            return jsonify({
                'status': 'error',
                'chromadb': str(e)
            }), 500

    return app


if __name__ == '__main__':
    app = create_app()
    print(f"[search-api] Starting on port {SEARCH_PORT}")
    app.run(host='0.0.0.0', port=SEARCH_PORT, debug=True)
