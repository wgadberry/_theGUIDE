#!/usr/bin/env python3
"""
get-top-holders.py -- Token holder analysis + synchronous signature processing

Subscribes to Redis command channel t16o:cmd:load-token.
When triggered (by SignalR gateway or CLI):
  1. Check token:<mint> hash in Redis
  2. Fetch top 80 holders from Solscan
  3. Compare holder hash — if match, signal "complete" (fast path)
  4. If mismatch/new: update hash, spawn X threads to fetch+process sigs
  5. Each thread: fetch 1000 sigs per holder, dedup via tx:<sig> Redis keys,
     process new sigs through Solscan detail, write to ArangoDB
  6. Publish progress to t16o:progress:<connectionId> throughout

Usage:
    python get-top-holders.py                         # subscribe to Redis commands
    python get-top-holders.py --mint <address>        # process single token (CLI mode)
    python get-top-holders.py --threads 8             # configure thread count
"""

import argparse, hashlib, json, os, sys, time, uuid, zlib
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import redis as redis_lib
import requests
from arango import ArangoClient

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from graph_common import (
    log, rpc_call, bootstrap_arango, get_arango_db,
    fetch_solscan_detail_batch, fetch_solscan_decode_batch,
    extract_graph, upsert_graph,
    REDIS_QUEUE, REDIS_KEY_PREFIX,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
SOLSCAN_API = 'https://pro-api.solscan.io/v2.0'

_dir = os.path.dirname(os.path.abspath(__file__))
_config_path = os.path.join(os.path.dirname(_dir), 'guide-config.json')
with open(_config_path) as f:
    _cfg = json.load(f)
SOLSCAN_TOKEN = _cfg.get('SOLSCAN_TOKEN', '')

TAG = 'top-holders'
CMD_CHANNEL = 't16o:cmd:load-token'


# ---------------------------------------------------------------------------
# Progress publishing
# ---------------------------------------------------------------------------

def publish_progress(rdb, connection_id, phase, message='', data=None):
    """Publish progress to Redis for the SignalR gateway to route to the client."""
    payload = {
        'connection_id': connection_id,
        'phase': phase,
        'message': message,
    }
    if data:
        payload['data'] = data
    channel = f't16o:progress:{connection_id}'
    rdb.publish(channel, json.dumps(payload, default=str))


# ---------------------------------------------------------------------------
# Solscan holder fetching
# ---------------------------------------------------------------------------

def fetch_top_holders(session, mint, limit=80):
    """Fetch top holders from Solscan, sorted by amount descending."""
    holders = []
    for page in range(1, (limit // 40) + 2):
        try:
            r = session.get(
                f'{SOLSCAN_API}/token/holders',
                params={'address': mint, 'page': page, 'page_size': 40},
                headers={'token': SOLSCAN_TOKEN},
                timeout=30,
            )
            r.raise_for_status()
            data = r.json().get('data', {})
            items = data.get('items', [])
            if not items:
                break
            holders.extend(items)
            if len(holders) >= limit or len(holders) >= (data.get('total', 0)):
                break
        except Exception as e:
            log(TAG, f'Solscan holder error: {e}')
            break
        time.sleep(0.1)

    holders = sorted(holders, key=lambda h: int(h.get('amount', 0)), reverse=True)
    return holders[:limit]


def compute_holder_hash(holders):
    """SHA-256 of concatenated holder addresses in rank order.

    Only hashes addresses, not amounts — amounts change every block
    but the holder roster changes slowly.
    """
    parts = [h.get('owner', '') for h in holders]
    return hashlib.sha256(''.join(parts).encode('utf-8')).hexdigest()


# ---------------------------------------------------------------------------
# Bloom filter for sig dedup
# ---------------------------------------------------------------------------

BLOOM_KEY = 'tx:bloom'
BLOOM_INITIALIZED = False


def ensure_bloom(rdb):
    """Create Bloom filter if it doesn't exist. Capacity for 10M sigs, 0.1% FP rate."""
    global BLOOM_INITIALIZED
    if BLOOM_INITIALIZED:
        return
    try:
        rdb.execute_command('BF.INFO', BLOOM_KEY)
    except Exception:
        # Create with capacity 10M, error rate 0.001
        try:
            rdb.execute_command('BF.RESERVE', BLOOM_KEY, 0.001, 10000000)
        except Exception:
            pass  # already exists race condition
    BLOOM_INITIALIZED = True


def bloom_check_batch(rdb, signatures):
    """Check multiple sigs against Bloom filter. Returns list of bools (True = exists)."""
    if not signatures:
        return []
    try:
        return [bool(x) for x in rdb.execute_command('BF.MEXISTS', BLOOM_KEY, *signatures)]
    except Exception:
        return [False] * len(signatures)


def bloom_add_batch(rdb, signatures):
    """Add multiple sigs to Bloom filter."""
    if not signatures:
        return
    try:
        rdb.execute_command('BF.MADD', BLOOM_KEY, *signatures)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Signature fetching + dedup
# ---------------------------------------------------------------------------

def fetch_sigs_for_address(session, rdb, address, limit=1000):
    """Fetch signatures via RPC with early-exit dedup against Bloom filter.

    Deduplicates per-page instead of post-fetch — if an entire page is
    already known, we've hit the frontier and stop paginating. This avoids
    fetching hundreds of known sigs for active wallets.

    Uses RedisBloom BF.MEXISTS for O(1) batch membership checks instead
    of individual EXISTS commands.
    """
    ensure_bloom(rdb)

    page_size = min(limit, 1000)
    fetched = 0
    before = None
    all_new_sigs = []
    retry_count = 0
    max_retries = 5
    min_interval = 0.15  # target interval between RPC calls

    while fetched < limit:
        params = {'limit': page_size}
        if before:
            params['before'] = before

        t0 = time.time()
        try:
            resp = rpc_call(session, 'getSignaturesForAddress', [address, params])
            page = resp.get('result', [])
            retry_count = 0  # reset on success
        except Exception as e:
            if '429' in str(e):
                retry_count += 1
                if retry_count > max_retries:
                    break
                sleep_time = min(2 ** retry_count, 30)
                time.sleep(sleep_time)
                continue
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

        if not valid:
            if len(page) < page_size:
                break
            continue

        # Early-exit dedup: check this page against Bloom filter
        sig_strs = [s['signature'] for s in valid]
        exists = bloom_check_batch(rdb, sig_strs)
        new_on_page = [s for s, known in zip(valid, exists) if not known]

        if not new_on_page:
            # Entire page was known — we've hit the frontier, stop paginating
            break

        all_new_sigs.extend(new_on_page)

        if len(page) < page_size:
            break

        # Response-time-aware delay
        elapsed = time.time() - t0
        sleep_needed = max(0, min_interval - elapsed)
        if sleep_needed:
            time.sleep(sleep_needed)

    return all_new_sigs


# ---------------------------------------------------------------------------
# Transaction processing (same as guide-cons-tx.py)
# ---------------------------------------------------------------------------

def process_sigs(tag, db, rdb, session, sigs, origin):
    """Process a batch of signatures: detail + decode → extract_graph → ArangoDB + Redis tx:<sig>.

    Uses both Solscan endpoints for full swap/pool/program data.
    """
    if not sigs:
        return 0, 0

    sig_strs = [s['signature'] for s in sigs]
    processed_sigs = []

    # Parallel fetch: detail + decode from Solscan
    detail_resp = fetch_solscan_detail_batch(session, sig_strs)
    decode_map = fetch_solscan_decode_batch(session, sig_strs)

    # Build lookup: sig -> detail item
    detail_map = {}
    for item in (detail_resp.get('data') or []):
        sig = item.get('tx_hash', '')
        if sig:
            detail_map[sig] = item

    all_addrs = []
    all_tokens = []
    all_edges = []
    tx_docs = []
    failed = 0
    batch_ts = int(time.time())

    for sig_info in sigs:
        sig = sig_info['signature']
        detail = detail_map.get(sig)
        decode = decode_map.get(sig)

        if not detail:
            failed += 1
            continue

        try:
            # extract_graph uses both detail + decode for full edge data
            addr_docs, token_docs, edge_docs = extract_graph(sig, detail, decode, origin)

            all_addrs.extend(addr_docs)
            all_tokens.extend(token_docs)
            all_edges.extend(edge_docs)

            block_time = detail.get('block_time', 0)
            signers = detail.get('signer', [])
            if isinstance(signers, str):
                signers = [signers]

            tx_doc = {
                '_key': sig,
                'status': 'processed',
                'processed_at': batch_ts,
                'origin': origin,
                'block_time': block_time,
                'fee': detail.get('fee', 0),
                'priority_fee': detail.get('priority_fee', 0),
                'signer': signers[0] if signers else None,
            }
            tx_docs.append(tx_doc)

            # Mark sig as processed in Redis
            rdb.set(f'tx:{sig}', batch_ts, ex=86400 * 30)  # 30 day TTL
            processed_sigs.append(sig)

        except Exception as e:
            failed += 1

    if all_addrs or all_tokens or all_edges:
        upsert_graph(db, all_addrs, all_tokens, all_edges)

    if tx_docs:
        db.collection('transactions').insert_many(tx_docs, overwrite=True, silent=True)

    # Add processed sigs to Bloom filter for fast future dedup
    if processed_sigs:
        bloom_add_batch(rdb, processed_sigs)

    return len(tx_docs), failed


# ---------------------------------------------------------------------------
# Worker thread: processes a slice of holder addresses
# ---------------------------------------------------------------------------

def process_single_holder(addr, db, rdb, session, sigs_per_holder, origin,
                          connection_id, progress_lock, progress_state):
    """Process one holder address: fetch sigs, dedup, process new ones.

    Designed to be called from a thread pool — each invocation handles
    exactly one holder, allowing maximum concurrency.
    """
    new_count = 0
    proc_count = 0

    try:
        new_sigs = fetch_sigs_for_address(session, rdb, addr, limit=sigs_per_holder)

        if new_sigs:
            for j in range(0, len(new_sigs), 50):
                batch = new_sigs[j:j + 50]
                processed, failed = process_sigs(f'T-{addr[:6]}', db, rdb, session, batch, origin)
                proc_count += processed
            new_count = len(new_sigs)

    except Exception as e:
        log(TAG, f'Error on {addr[:12]}...: {e}')

    # Update shared progress
    with progress_lock:
        progress_state['holders_done'] += 1
        progress_state['total_sigs'] += new_count
        progress_state['total_processed'] += proc_count
        done = progress_state['holders_done']
        total_h = progress_state['total_holders']
        total_s = progress_state['total_sigs']
        total_p = progress_state['total_processed']

    # Publish progress every 3 holders (viewer throttles to 3s refresh)
    if done % 3 == 0 or done == total_h:
        publish_progress(rdb, connection_id, 'processing',
                         f'{done}/{total_h} holders, {total_s} new sigs, {total_p} processed')

    return new_count, proc_count


# ---------------------------------------------------------------------------
# Main pipeline: hash check → thread spawn → progress
# ---------------------------------------------------------------------------

def load_token(rdb, mint, connection_id, threads=40, sigs_per_holder=100, top_holder_count=80):
    """Full load-token pipeline."""
    session = requests.Session()
    db = get_arango_db()
    t0 = time.time()

    HASH_TTL = 300  # 5 minutes — skip Solscan entirely if hash is fresh

    # Step 0: TTL check — if hash was updated recently, skip everything
    token_key = f'token:{mint}'
    stored_updated = rdb.hget(token_key, 'updatedAt')
    if stored_updated:
        try:
            age = int(time.time()) - int(stored_updated)
            if age < HASH_TTL:
                elapsed = round((time.time() - t0) * 1000)
                log(TAG, f'Hash fresh ({age}s old) for {mint[:16]}... skipping')
                publish_progress(rdb, connection_id, 'complete', 'Data ready (cached)',
                                 {'source': 'cache', 'elapsed_ms': elapsed, 'age_seconds': age})
                return
        except (ValueError, TypeError):
            pass

    publish_progress(rdb, connection_id, 'holders', f'Fetching top {top_holder_count} holders...')

    # Step 1: Fetch current top holders
    holders = fetch_top_holders(session, mint, limit=top_holder_count)
    if not holders:
        publish_progress(rdb, connection_id, 'error', 'No holders found for this token')
        return

    current_hash = compute_holder_hash(holders)
    holder_addrs = [h.get('owner', '') for h in holders if h.get('owner')]

    publish_progress(rdb, connection_id, 'holders',
                     f'Fetched {len(holders)} holders, checking hash...',
                     {'holder_count': len(holders)})

    # Step 2: Check stored hash
    stored_hash = rdb.hget(token_key, 'topHolderHash')
    if isinstance(stored_hash, bytes):
        stored_hash = stored_hash.decode()

    # Cache holder data in Redis for the viewer
    rdb.set(f'holders:{mint}', json.dumps(holders), ex=600)  # 10 min TTL

    if stored_hash == current_hash:
        # Fast path — distribution unchanged, update timestamp
        rdb.hset(token_key, 'updatedAt', int(time.time()))
        elapsed = round((time.time() - t0) * 1000)
        publish_progress(rdb, connection_id, 'hash_match',
                         'Holder distribution unchanged — serving cached data',
                         {'elapsed_ms': elapsed, 'hash': current_hash})
        publish_progress(rdb, connection_id, 'complete', 'Data ready',
                         {'source': 'cache', 'elapsed_ms': elapsed, 'hash': current_hash})
        return

    # Hash mismatch or new token — update hash
    log(TAG, f'Hash {"mismatch" if stored_hash else "new"} for {mint[:16]}...')
    rdb.hset(token_key, mapping={
        'topHolderHash': current_hash,
        'holderCount': len(holders),
        'topHolderAddress': holders[0].get('owner', ''),
        'topHolderPct': str(holders[0].get('percentage', 0)),
        'updatedAt': int(time.time()),
    })

    # Also store token metadata from ArangoDB
    try:
        token_doc = db.collection('tokens').get(mint) or {}
        rdb.hset(token_key, mapping={
            'token_symbol': token_doc.get('symbol', ''),
            'token_name': token_doc.get('name', ''),
        })
    except Exception:
        pass

    phase = 'hash_miss'
    publish_progress(rdb, connection_id, phase,
                     f'Holder distribution changed — processing {len(holder_addrs)} addresses with {threads} threads',
                     {'old_hash': stored_hash, 'new_hash': current_hash})

    progress_lock = threading.Lock()
    progress_state = {
        'holders_done': 0,
        'total_holders': len(holder_addrs),
        'total_sigs': 0,
        'total_processed': 0,
    }

    publish_progress(rdb, connection_id, 'processing',
                     f'0/{len(holder_addrs)} holders, {threads} threads ready...')

    # Step 3: Thread pool — each holder is a separate task, threads grab from queue
    # Pre-create DB connections and sessions for the pool (one per thread)
    thread_local = threading.local()

    def get_thread_resources():
        if not hasattr(thread_local, 'db'):
            thread_local.db = get_arango_db()
            thread_local.session = requests.Session()
        return thread_local.db, thread_local.session

    def process_holder(addr):
        db, session = get_thread_resources()
        return process_single_holder(
            addr, db, rdb, session, sigs_per_holder, mint,
            connection_id, progress_lock, progress_state
        )

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = {pool.submit(process_holder, addr): addr for addr in holder_addrs}
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                log(TAG, f'Thread error on {futures[f][:12]}...: {e}')

    # Step 5: Queue new addresses for enrichment
    try:
        unenriched = list(db.aql.execute('''
            FOR addr IN @addrs
                LET doc = DOCUMENT(CONCAT("addresses/", addr))
                FILTER doc != null AND doc.enriched == 0
                RETURN addr
        ''', bind_vars={'addrs': holder_addrs}))

        if unenriched:
            enrich_queue = 't16o:addr:enrich'
            batch_size = 50
            for i in range(0, len(unenriched), batch_size):
                chunk = unenriched[i:i + batch_size]
                key = f'{REDIS_KEY_PREFIX}enrich:{uuid.uuid4().hex[:12]}'
                rdb.set(key, json.dumps({
                    'addresses': chunk,
                    'count': len(chunk),
                    'created_at': int(time.time()),
                }))
                rdb.lpush(enrich_queue, key)
            log(TAG, f'Queued {len(unenriched)} addresses for enrichment')
            publish_progress(rdb, connection_id, 'enriching',
                             f'Queued {len(unenriched)} new addresses for enrichment')
    except Exception as e:
        log(TAG, f'Enrichment queue error: {e}')

    # Step 6: Complete
    elapsed = round((time.time() - t0) * 1000)
    with progress_lock:
        final = progress_state.copy()

    publish_progress(rdb, connection_id, 'complete', 'Processing complete', {
        'source': 'processed',
        'elapsed_ms': elapsed,
        'hash': current_hash,
        'holders_processed': final['total_holders'],
        'new_sigs': final['total_sigs'],
        'processed_txs': final['total_processed'],
    })

    log(TAG, f'Done: {mint[:16]}... {final["total_holders"]} holders, '
        f'{final["total_sigs"]} new sigs, {final["total_processed"]} processed '
        f'in {elapsed}ms')


# ---------------------------------------------------------------------------
# Redis Stream consumer group
# ---------------------------------------------------------------------------

STREAM_NAME = 't16o:stream:load-token'
GROUP_NAME = 't16o:top-holders'
RUNNING = True


def ensure_stream_group(rdb):
    """Create the consumer group if it doesn't exist."""
    try:
        rdb.xgroup_create(STREAM_NAME, GROUP_NAME, id='0', mkstream=True)
        log(TAG, f'Created consumer group: {GROUP_NAME}')
    except Exception as e:
        if 'BUSYGROUP' in str(e):
            pass  # already exists
        else:
            raise


def run_stream_consumer(rdb, consumer_name, default_threads):
    """Read from stream as a consumer group member. Multiple instances share work."""
    ensure_stream_group(rdb)
    bootstrap_arango()

    log(TAG, f'Consumer [{consumer_name}] listening on stream {STREAM_NAME}...')

    while RUNNING:
        try:
            # Block up to 5 seconds waiting for new messages
            messages = rdb.xreadgroup(
                GROUP_NAME, consumer_name,
                {STREAM_NAME: '>'},
                count=1, block=5000,
            )

            if not messages:
                continue

            for stream, entries in messages:
                for msg_id, data in entries:
                    try:
                        mint = data.get('mint', '')
                        connection_id = data.get('connection_id') or f'cli-{uuid.uuid4().hex[:8]}'
                        threads = int(data.get('threads', default_threads))
                        sigs_per_holder = int(data.get('sigs_per_holder', 100))
                        top_holder_count = int(data.get('top_holder_count', 80))

                        if not mint:
                            rdb.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue

                        log(TAG, f'[{consumer_name}] LoadToken {mint[:16]}... '
                            f'(conn={connection_id[:12]}, threads={threads})')

                        # Process in a thread so we can ACK and pick up next message
                        def process_and_ack(mid=msg_id):
                            try:
                                load_token(rdb, mint, connection_id,
                                           threads, sigs_per_holder, top_holder_count)
                            finally:
                                rdb.xack(STREAM_NAME, GROUP_NAME, mid)

                        t = threading.Thread(target=process_and_ack, daemon=True)
                        t.start()

                    except Exception as e:
                        log(TAG, f'Message parse error: {e}')
                        rdb.xack(STREAM_NAME, GROUP_NAME, msg_id)

        except Exception as e:
            log(TAG, f'Stream read error: {e}')
            time.sleep(2)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global RUNNING
    import signal

    parser = argparse.ArgumentParser(description='Token holder analysis + sig processing')
    parser.add_argument('--mint', type=str, help='Process a single token (CLI mode)')
    parser.add_argument('--threads', type=int, default=40, help='Worker threads per token (default: 40)')
    parser.add_argument('--sigs', type=int, default=100, help='Sigs per holder (default: 100)')
    parser.add_argument('--holders', type=int, default=80, help='Top N holders (default: 80)')
    parser.add_argument('--name', type=str, default=None,
                        help='Consumer name (default: auto-generated). Use to run multiple instances.')
    args = parser.parse_args()

    rdb = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    rdb.ping()
    log(TAG, 'Redis connected')

    def shutdown(signum, frame):
        global RUNNING
        RUNNING = False
        log(TAG, 'Shutdown signal received')

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    if args.mint:
        # CLI mode — process single token
        connection_id = f'cli-{uuid.uuid4().hex[:8]}'
        bootstrap_arango()
        load_token(rdb, args.mint, connection_id,
                   threads=args.threads, sigs_per_holder=args.sigs,
                   top_holder_count=args.holders)
    else:
        # Daemon mode — stream consumer group
        consumer_name = args.name or f'worker-{uuid.uuid4().hex[:8]}'
        run_stream_consumer(rdb, consumer_name, args.threads)


if __name__ == '__main__':
    main()
