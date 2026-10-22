"""
guide-cons-tx.py -- Transaction processor consumer

Worker threads BRPOP from a Redis list to receive RedisJSON key names.
Each key contains a batch of signatures to process. Workers fetch detail
+ decode from Solscan in batch, extract graph data, and upsert into ArangoDB.

Usage:
    python guide-cons-tx.py [--threads 4] [--batch 50]
    python guide-cons-tx.py --threads=8 --batch=25
"""

import argparse, sys, time, json, signal, threading, zlib
from graph_common import (
    log, bootstrap_arango, get_arango_db,
    fetch_solscan_detail_batch, upsert_graph,
    REDIS_QUEUE,
)
from tx_detail_slim import from_detail_batch
import redis as redis_lib
import requests

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
RUNNING = True


def shutdown(signum, frame):
    global RUNNING
    RUNNING = False
    log('supervisor', 'Shutdown signal received')


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------

def process_batch(tag, db, session, sigs, origin):
    """Process a batch of signatures via Solscan batch APIs.

    Two Solscan calls (detail/multi + actions/multi) per batch of up to 50 sigs.
    Extracts graph data via tx_detail_slim, upserts into ArangoDB.
    Writes transaction docs only on completion.

    Returns (processed_count, failed_count).
    """
    if not sigs:
        return 0, 0

    # Single Solscan call — detail/multi only
    detail_resp = fetch_solscan_detail_batch(session, sigs)

    # Translate to slim format (edges derived from balance changes)
    slim_txs = from_detail_batch(detail_resp)

    # Extract and accumulate graph data
    all_addrs = []
    all_tokens = []
    all_edges = []
    tx_docs = []
    failed_count = 0
    batch_ts = int(time.time())

    for sig in sigs:
        slim = slim_txs.get(sig)
        if not slim:
            failed_count += 1
            continue

        try:
            # Build graph docs from slim edges
            block_time = slim['block_time']
            for e in slim['edges']:
                if e['from']:
                    all_addrs.append({'_key': e['from'], 'type': 'unknown',
                                      'first_seen': block_time, 'last_seen': block_time})
                if e['to']:
                    all_addrs.append({'_key': e['to'], 'type': 'unknown',
                                      'first_seen': block_time, 'last_seen': block_time})
                all_edges.append({
                    '_from': f'addresses/{e["from"]}',
                    '_to': f'addresses/{e["to"]}',
                    'edge_type': e['transfer_type'],
                    'token': f'tokens/{e["token"]}' if e.get('token') else None,
                    'amount': str(e['amount']),
                    'decimals': e.get('decimals', 0),
                    'signature': sig,
                    'block_time': block_time,
                })

            # Addresses from nodes
            for addr, info in slim['nodes'].items():
                all_addrs.append({'_key': addr, 'type': info['type'],
                                  'first_seen': block_time, 'last_seen': block_time})

            # Tokens
            for mint, info in slim['tokens'].items():
                all_tokens.append({'_key': mint, 'decimals': 0,
                                   'symbol': info.get('symbol', ''),
                                   'name': info.get('name', '')})

            # Transaction doc
            tx_doc = {
                '_key': sig,
                'status': 'processed',
                'processed_at': batch_ts,
                'origin': origin,
                'block_time': block_time,
                'fee': slim.get('fee', 0),
                'priority_fee': slim.get('priority_fee', 0),
                'signer': slim['signers'][0] if slim['signers'] else None,
            }

            # Compressed slim for reprocessing
            raw_json = json.dumps(slim, separators=(',', ':')).encode()
            tx_doc['raw_compressed'] = zlib.compress(raw_json, 6).hex()

            tx_docs.append(tx_doc)
        except Exception as e:
            failed_count += 1

    # Batch upsert graph data
    if all_addrs or all_tokens or all_edges:
        upsert_graph(db, all_addrs, all_tokens, all_edges)

    # Write transaction docs only on completion
    if tx_docs:
        db.collection('transactions').insert_many(tx_docs, overwrite=True, silent=True)

    return len(tx_docs), failed_count


# ---------------------------------------------------------------------------
# Worker thread
# ---------------------------------------------------------------------------

class WorkerThread(threading.Thread):
    """BRPOP from Redis list, pull RedisJSON, filter and process."""

    def __init__(self, worker_id, batch_size):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.batch_size = batch_size
        self.tag = f'cons-tx-{worker_id}'
        self.total_processed = 0
        self.total_failed = 0

    def run(self):
        db = get_arango_db()
        rdb = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        session = requests.Session()

        log(self.tag, f'Started (batch={self.batch_size})')

        while RUNNING:
            try:
                # Block up to 5 seconds waiting for work
                result = rdb.brpop(REDIS_QUEUE, timeout=5)
                if result is None:
                    continue

                _, key = result  # (queue_name, value)

                # Pull the RedisJSON object
                raw = rdb.get(key)
                if raw is None:
                    log(self.tag, f'{key} missing')
                    continue

                payload = json.loads(raw)
                origin = payload.get('origin', 'unknown')
                all_sigs = [s['signature'] for s in payload.get('signatures', [])]

                # Delete the key — we've read it
                rdb.delete(key)

                if not all_sigs:
                    continue

                log(self.tag, f'Processing {key} ({len(all_sigs)} sigs)')

                # Batch check: which sigs already exist?
                existing = set()
                for doc in db.collection('transactions').get_many(all_sigs):
                    if doc is not None:
                        existing.add(doc['_key'])

                new_sigs = [s for s in all_sigs if s not in existing]
                if existing:
                    log(self.tag, f'{len(new_sigs)} new, {len(existing)} already processed')

                if not new_sigs:
                    continue

                # Process in configured batch sizes with retry
                MAX_RETRIES = 3
                for i in range(0, len(new_sigs), self.batch_size):
                    if not RUNNING:
                        break
                    batch = new_sigs[i:i + self.batch_size]
                    for attempt in range(MAX_RETRIES):
                        try:
                            processed, failed = process_batch(
                                self.tag, db, session, batch, origin)
                            self.total_processed += processed
                            self.total_failed += failed
                            log(self.tag, f'{processed} processed, {failed} failed '
                                f'(total: {self.total_processed})')
                            break
                        except Exception as e:
                            if attempt < MAX_RETRIES - 1:
                                log(self.tag, f'Batch error (attempt {attempt+1}/{MAX_RETRIES}): {e}')
                                time.sleep(2 * (attempt + 1))
                            else:
                                self.total_failed += len(batch)
                                log(self.tag, f'Batch failed after {MAX_RETRIES} retries: {e}')

            except Exception as e:
                log(self.tag, f'Error: {e}')
                time.sleep(2)

        log(self.tag, f'Stopped. {self.total_processed} processed, '
            f'{self.total_failed} failed')


# ---------------------------------------------------------------------------
# Supervisor
# ---------------------------------------------------------------------------

def main():
    global RUNNING
    parser = argparse.ArgumentParser(description='Transaction processor consumer')
    parser.add_argument('--threads', type=int, default=4, help='Worker thread count (default: 4)')
    parser.add_argument('--batch', type=int, default=50, help='Solscan batch size per API call (default: 50)')
    args = parser.parse_args()

    thread_count = args.threads
    batch_size = args.batch

    log('supervisor', f'Starting {thread_count} worker threads, '
        f'batch_size={batch_size}')

    bootstrap_arango()

    workers = []
    for wid in range(1, thread_count + 1):
        w = WorkerThread(wid, batch_size)
        w.start()
        workers.append(w)

    log('supervisor', f'{thread_count} workers running, waiting for work...')

    try:
        while RUNNING:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    log('supervisor', 'Shutting down workers...')
    RUNNING = False

    for w in workers:
        w.join(timeout=10)

    total_p = sum(w.total_processed for w in workers)
    total_f = sum(w.total_failed for w in workers)
    log('supervisor', f'All workers stopped. '
        f'Total: {total_p} processed, {total_f} failed')


if __name__ == '__main__':
    main()
