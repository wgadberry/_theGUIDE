#!/usr/bin/env python3
"""
graph_api.py -- Lightweight HTTP API for graphRAG operations

Exposes endpoints the V5 viewer can call to trigger backend processing.
Runs on port 5100.

Endpoints:
    POST /api/queue-addresses   Queue signature fetching for multiple addresses
    GET  /api/queue-status      Check Redis queue depth
    GET  /health                Health check

Usage:
    python graph_api.py
    python graph_api.py --port 5100
"""

import argparse, json, sys, time, uuid, threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from graph_common import (
    log, rpc_call, REDIS_QUEUE, REDIS_KEY_PREFIX,
)
import redis as redis_lib
import requests

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
TAG = 'graph-api'


def queue_address(rdb, session, wallet, limit=1000):
    """Fetch signatures for an address and queue them to Redis.

    Same logic as guide-pub-addr.py but callable as a function.
    Returns (fetched, queued).
    """
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
                time.sleep(2)
                continue
            log(TAG, f'  RPC error for {wallet[:12]}...: {e}')
            break

        if not page:
            break

        before = page[-1]['signature']
        fetched += len(page)

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

        if len(page) < page_size:
            break

        time.sleep(0.1)

    return fetched, queued


def queue_batch_background(rdb, addresses, limit_per_address):
    """Process a batch of addresses in a background thread."""
    session = requests.Session()
    total_fetched = 0
    total_queued = 0

    for i, addr in enumerate(addresses):
        try:
            fetched, queued = queue_address(rdb, session, addr, limit=limit_per_address)
            total_fetched += fetched
            total_queued += queued
            log(TAG, f'  [{i+1}/{len(addresses)}] {addr[:12]}... fetched={fetched} queued={queued}')
        except Exception as e:
            log(TAG, f'  [{i+1}/{len(addresses)}] {addr[:12]}... ERROR: {e}')
        time.sleep(0.2)  # pace between addresses

    log(TAG, f'Batch complete: {len(addresses)} addresses, {total_fetched} fetched, {total_queued} queued')


class GraphAPIHandler(BaseHTTPRequestHandler):
    rdb = None

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors_headers()
        self.end_headers()

    def do_GET(self):
        if self.path == '/health':
            self._json_response({'status': 'ok'})
        elif self.path == '/api/queue-status':
            depth = self.rdb.llen(REDIS_QUEUE)
            self._json_response({'queue_depth': depth})
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path == '/api/queue-addresses':
            body = self._read_body()
            if not body:
                self._json_response({'error': 'empty body'}, 400)
                return

            addresses = body.get('addresses', [])
            limit = body.get('limit', 1000)

            if not addresses:
                self._json_response({'error': 'no addresses provided'}, 400)
                return

            if len(addresses) > 200:
                self._json_response({'error': f'max 200 addresses per request, got {len(addresses)}'}, 400)
                return

            # Process in background — return immediately
            t = threading.Thread(
                target=queue_batch_background,
                args=(self.rdb, addresses, limit),
                daemon=True,
            )
            t.start()

            log(TAG, f'Queued {len(addresses)} addresses (limit={limit}/ea) for background processing')
            self._json_response({
                'status': 'accepted',
                'addresses': len(addresses),
                'limit_per_address': limit,
            })
        else:
            self.send_error(404)

    def _read_body(self):
        length = int(self.headers.get('Content-Length', 0))
        if length == 0:
            return None
        try:
            return json.loads(self.rfile.read(length))
        except Exception:
            return None

    def _cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def _json_response(self, data, code=200):
        self.send_response(code)
        self._cors_headers()
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, format, *args):
        pass  # suppress default access log


def main():
    parser = argparse.ArgumentParser(description='GraphRAG API server')
    parser.add_argument('--port', type=int, default=5100, help='Listen port (default: 5100)')
    args = parser.parse_args()

    rdb = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    rdb.ping()
    log(TAG, f'Redis connected')

    GraphAPIHandler.rdb = rdb

    server = HTTPServer(('0.0.0.0', args.port), GraphAPIHandler)
    log(TAG, f'Listening on http://0.0.0.0:{args.port}')

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()
    log(TAG, 'Stopped')


if __name__ == '__main__':
    main()
