#!/usr/bin/env python3
"""
get-top-holders-v2.py -- Token holder analysis via Solscan transfer path

Subscribes to Redis stream t16o:stream:load-token (consumer group).
When triggered (by SignalR gateway or CLI):
  1. TTL check — skip entirely if hash is fresh (< 5 min)
  2. Fetch top 80 holders from Solscan token/holders
  3. Hash check — if roster unchanged, fast-path complete
  4. Fetch account/metadata for all holders in parallel (upfront batch)
  5. Thread pool: per-holder account/transfer (1 page, size=100)
     - Dedup via trans_id against Redis tx:<sig> keys
     - Write address docs + edge docs to ArangoDB
     - Cache edges in Redis t16o:edges
  6. Publish progress to t16o:progress:<connectionId> throughout

Rate budget (1,000 req/60s):
  - 5  calls  : token/holders (5 pages × 40)
  - 200 calls : account/metadata (parallel upfront)
  - 1000 calls: account/transfer (5 pages × 200 holders)
  ─────────────────────────────────────────────────
  ~1205 calls per full run — may need pacing on large tokens

Usage:
    python get-top-holders-v2.py                        # stream consumer mode
    python get-top-holders-v2.py --mint <address>       # CLI single token
    python get-top-holders-v2.py --threads 8            # configure thread count
    python get-top-holders-v2.py --name worker-2        # named consumer instance
"""

import argparse, hashlib, json, os, sys, time, uuid
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import redis as redis_lib
import requests
from arango import ArangoClient

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from graph_common import (
    log, bootstrap_arango, get_arango_db,
    upsert_graph,
    REDIS_KEY_PREFIX,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

REDIS_HOST  = 'localhost'
REDIS_PORT  = 6379
SOLSCAN_API = 'https://pro-api.solscan.io/v2.0'

_dir         = os.path.dirname(os.path.abspath(__file__))
_config_path = os.path.join(os.path.dirname(_dir), 'guide-config.json')
with open(_config_path) as f:
    _cfg = json.load(f)
SOLSCAN_TOKEN = _cfg.get('SOLSCAN_TOKEN', '')

TAG         = 'top-holders-v2'
STREAM_NAME = 't16o:stream:load-token'
GROUP_NAME  = 't16o:top-holders'
HASH_TTL    = 300   # seconds — skip run entirely if hash is fresher than this
RUNNING     = True

# Solscan header used by every request in this worker
_HEADERS = {'token': SOLSCAN_TOKEN}


# ---------------------------------------------------------------------------
# Progress publishing
# ---------------------------------------------------------------------------

def publish_progress(rdb, connection_id, phase, message='', data=None):
    """Publish a progress event to the SignalR gateway channel."""
    payload = {'connection_id': connection_id, 'phase': phase, 'message': message}
    if data:
        payload['data'] = data
    rdb.publish(f't16o:progress:{connection_id}', json.dumps(payload, default=str))


# ---------------------------------------------------------------------------
# Step 1 — Fetch top holders
# ---------------------------------------------------------------------------

def fetch_top_holders(session, mint, limit=80):
    """
    Fetch top N holders from Solscan token/holders.
    Returns list of holder dicts sorted by amount desc, capped at `limit`.
    Each item: { address, amount, decimals, owner, rank, value, percentage }
    """
    holders = []
    pages_needed = -(-limit // 40)  # ceiling division

    for page in range(1, pages_needed + 1):
        try:
            r = session.get(
                f'{SOLSCAN_API}/token/holders',
                params={'address': mint, 'page': page, 'page_size': 40},
                headers=_HEADERS,
                timeout=30,
            )
            r.raise_for_status()
            body  = r.json()
            items = (body.get('data') or {}).get('items', [])
            if not items:
                break
            holders.extend(items)
            total = (body.get('data') or {}).get('total', 0)
            if len(holders) >= limit or len(holders) >= total:
                break
        except Exception as e:
            log(TAG, f'token/holders error page={page}: {e}')
            break
        time.sleep(0.06)  # ~16 req/s — polite, stays well under 1k/min

    holders.sort(key=lambda h: int(h.get('amount', 0)), reverse=True)
    return holders[:limit]


def compute_holder_hash(holders):
    """
    SHA-256 of owner addresses in rank order.
    Hashes addresses only — amounts change every block, roster changes slowly.
    """
    joined = ''.join(h.get('owner', '') for h in holders)
    return hashlib.sha256(joined.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Step 2 — account/metadata (parallel upfront batch)
# ---------------------------------------------------------------------------

def fetch_metadata_batch(session, owners):
    """
    Fetch account/metadata for a list of owner addresses in parallel.
    Returns dict: { owner_address -> metadata_dict }

    Fields of interest:
      account_label, account_tags, account_type,
      funded_by.funded_by, funded_by.tx_hash, funded_by.block_time,
      active_age
    """
    results = {}

    def _fetch_one(owner):
        for attempt in range(3):
            try:
                r = session.get(
                    f'{SOLSCAN_API}/account/metadata',
                    params={'address': owner},
                    headers=_HEADERS,
                    timeout=10,
                )
                if r.status_code == 429:
                    time.sleep(min(2 ** (attempt + 1), 8))
                    continue
                r.raise_for_status()
                body = r.json()
                if body.get('success') and body.get('data'):
                    return owner, body['data']
                return owner, {}
            except Exception as e:
                if attempt < 2:
                    time.sleep(1)
                    continue
                log(TAG, f'account/metadata error {owner[:12]}...: {e}')
        return owner, {}

    # Use a modest thread count — all 80 calls at once is ~80 req in a second,
    # well within the 1k/min budget but we cap to avoid overwhelming the endpoint
    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(_fetch_one, owner): owner for owner in owners}
        for f in as_completed(futures):
            owner, meta = f.result()
            results[owner] = meta

    return results


# ---------------------------------------------------------------------------
# Step 3 — account/transfer (per holder, deduped)
# ---------------------------------------------------------------------------

def fetch_transfers_for_holder(session, owner, token_account, mint, max_pages=5):
    """
    Fetch up to max_pages pages (100 transfers each) for this holder's token account.
    Scoped to the specific token_account so we only get edges for this token.

    Returns list of raw transfer dicts from Solscan.
    """
    all_transfers = []

    for page in range(1, max_pages + 1):
        page_data = _fetch_transfer_page(session, owner, token_account, page)
        if not page_data:
            break
        all_transfers.extend(page_data)
        if len(page_data) < 100:
            break  # last page
        time.sleep(0.06)

    return all_transfers


def _fetch_transfer_page(session, owner, token_account, page):
    """Fetch a single page of transfers with retry."""
    for attempt in range(3):
        try:
            r = session.get(
                f'{SOLSCAN_API}/account/transfer',
                params={
                    'address':       owner,
                    'token_account': token_account,
                    'page':          page,
                    'page_size':     100,
                    'sort_by':       'block_time',
                    'sort_order':    'desc',
                },
                headers=_HEADERS,
                timeout=15,
            )
            if r.status_code == 429:
                time.sleep(min(2 ** (attempt + 1), 8))
                continue
            r.raise_for_status()
            body = r.json()
            return body.get('data') or []
        except Exception as e:
            if attempt < 2:
                time.sleep(1)
                continue
            log(TAG, f'account/transfer error {owner[:12]}...: {e}')
    return []


def filter_new_transfers(rdb, transfers):
    """
    Drop any transfer whose trans_id is already in Redis (tx:<sig>).
    Returns only unseen transfers.
    """
    if not transfers:
        return []

    # Batch check with pipeline — one round trip for all trans_ids
    pipe = rdb.pipeline(transaction=False)
    for t in transfers:
        pipe.exists(f'tx:{t["trans_id"]}')
    flags = pipe.execute()

    return [t for t, known in zip(transfers, flags) if not known]


# ---------------------------------------------------------------------------
# Step 4 — Build graph docs from transfer data
# ---------------------------------------------------------------------------

def _classify_address_type_from_meta(meta):
    """Derive address type from Solscan metadata."""
    import re
    tags = [t.lower() for t in (meta.get('account_tags') or [])]
    label = (meta.get('account_label') or '').lower()
    acct_type = meta.get('account_type', '')

    if 'vault' in label and not re.search(r'\([^)]*vault[^)]*\)', label):
        return 'vault'
    if 'pool' in tags:
        return 'pool'
    if 'market' in tags:
        return 'market'
    if any('vault' in t for t in tags):
        return 'vault'
    if 'lptoken' in tags:
        return 'lp_token'
    if 'dex_wallet' in tags:
        return 'dex_wallet'
    if 'arbitrage_bot' in tags:
        return 'bot'
    if acct_type == 'token_account':
        return 'ata'
    if acct_type == 'program':
        return 'program'
    if acct_type == 'mint':
        return 'mint'
    if acct_type == 'address':
        return 'wallet'
    return 'unknown'


# Map Solscan activity_type to our edge_type codes
_ACTIVITY_TO_EDGE_TYPE = {
    'ACTIVITY_SPL_TRANSFER':        'spl_transfer',
    'ACTIVITY_TOKEN_SWAP':          'swap_in',    # refined below by flow
    'ACTIVITY_AGG_TOKEN_SWAP':      'swap_in',
    'ACTIVITY_TOKEN_ADD_LIQUIDITY': 'add_liquidity',
    'ACTIVITY_TOKEN_REMOVE_LIQUIDITY': 'remove_liquidity',
    'ACTIVITY_SPL_BURN':            'burn',
    'ACTIVITY_SPL_MINT':            'mint',
    'ACTIVITY_SPL_CREATE_ACCOUNT':  'create_ata',
    'ACTIVITY_SYSTEM_TRANSFER':     'sol_transfer',
}


def build_graph_from_transfers(transfers, owner, meta):
    """
    Convert raw Solscan transfer records into address docs, token docs,
    transaction docs, and edge docs compatible with upsert_graph.

    Returns (addr_docs, token_docs, tx_docs, edge_docs).

    owner : the holder's wallet address — receives metadata enrichment
    meta  : account/metadata dict for this holder
    """
    addr_map  = {}   # address -> doc
    token_map = {}   # token_address -> doc
    tx_map    = {}   # trans_id -> doc
    edge_docs = []

    def _ensure_addr(address, block_time=0):
        if address not in addr_map:
            doc = {
                '_key': address,
                'type': 'unknown',
                'first_seen': block_time or 0,
                'last_seen': block_time or 0,
                'enriched': 0,
            }
            # Attach metadata only to the holder's own address
            if address == owner and meta:
                doc['type'] = _classify_address_type_from_meta(meta)
                doc['label'] = meta.get('account_label', '')
                doc['tags'] = meta.get('account_tags', [])
                doc['active_age'] = meta.get('active_age')
                doc['enriched'] = 1
                doc['enriched_at'] = int(time.time())
                funded = meta.get('funded_by') or {}
                if funded and funded.get('funded_by'):
                    doc['funded_by'] = funded['funded_by']
                    doc['funded_by_tx'] = funded.get('tx_hash', '')
                    doc['funded_by_time'] = funded.get('block_time')
                else:
                    doc['funded_by'] = 'none'
            addr_map[address] = doc
        else:
            existing = addr_map[address]
            if block_time:
                existing['first_seen'] = min(existing.get('first_seen', 0) or 0, block_time)
                existing['last_seen'] = max(existing.get('last_seen', 0) or 0, block_time)
        return addr_map[address]

    for tx in transfers:
        from_addr = tx.get('from_address', '')
        to_addr   = tx.get('to_address', '')

        if not from_addr or not to_addr:
            continue

        block_time = tx.get('block_time', 0)
        _ensure_addr(from_addr, block_time)
        _ensure_addr(to_addr, block_time)

        # Map activity_type to edge_type
        activity = tx.get('activity_type', '')
        edge_type = _ACTIVITY_TO_EDGE_TYPE.get(activity, 'spl_transfer')

        # Refine swap direction by flow
        if edge_type == 'swap_in' and tx.get('flow') == 'out':
            edge_type = 'swap_out'

        token_addr = tx.get('token_address', '')
        trans_id = tx.get('trans_id', '')

        # Collect token metadata
        if token_addr and token_addr not in token_map:
            token_map[token_addr] = {
                '_key': token_addr,
                'decimals': tx.get('token_decimals', 0),
            }

        # Collect transaction docs
        if trans_id and trans_id not in tx_map:
            tx_map[trans_id] = {
                '_key': trans_id,
                'status': 'processed',
                'processed_at': int(time.time()),
                'origin': owner,
                'block_time': block_time,
                'block_id': tx.get('block_id'),
                'activity_type': activity,
                'signer': from_addr,
            }

        edge = {
            '_from':      f'addresses/{from_addr}',
            '_to':        f'addresses/{to_addr}',
            'edge_type':  edge_type,
            'token':      f'tokens/{token_addr}' if token_addr else None,
            'amount':     str(tx.get('amount', '0')),
            'decimals':   tx.get('token_decimals', 0),
            'signature':  tx.get('trans_id', ''),
            'block_time': block_time,
            # v2 extras — richer data from transfer endpoint
            'activity_type': activity,
            'flow':       tx.get('flow', ''),
            'value':      tx.get('value'),
        }
        edge_docs.append(edge)

    # Create funded_by edge if metadata has funder info
    if meta and owner in addr_map:
        funded = meta.get('funded_by') or {}
        funder = funded.get('funded_by')
        if funder:
            _ensure_addr(funder, funded.get('block_time', 0))
            edge_docs.append({
                '_from': f'addresses/{funder}',
                '_to': f'addresses/{owner}',
                'edge_type': 'funded_by',
                'token': None,
                'amount': '0',
                'decimals': 0,
                'signature': funded.get('tx_hash', ''),
                'block_time': funded.get('block_time', 0),
            })

    return list(addr_map.values()), list(token_map.values()), list(tx_map.values()), edge_docs


# ---------------------------------------------------------------------------
# Step 5 — Mark transfers processed in Redis + optional edge cache
# ---------------------------------------------------------------------------

def mark_transfers_processed(rdb, transfers, batch_ts):
    """
    Write tx:<trans_id> keys to Redis so future runs skip these.
    TTL: 30 days (matching existing worker convention).
    """
    if not transfers:
        return
    pipe = rdb.pipeline(transaction=False)
    for t in transfers:
        pipe.set(f'tx:{t["trans_id"]}', batch_ts, ex=86400 * 30)
    pipe.execute()


def cache_edges_redis(rdb, mint, edge_docs):
    """
    Append new edge signatures to the t16o:edges:<mint> sorted set,
    scored by block_time for range queries.
    """
    if not edge_docs:
        return
    key  = f't16o:edges:{mint}'
    pipe = rdb.pipeline(transaction=False)
    for e in edge_docs:
        score = e.get('block_time') or 0
        sig = e.get('signature', '')
        if sig:
            pipe.zadd(key, {sig: score})
    pipe.execute()
    rdb.expire(key, 86400 * 7)  # 7-day TTL on the edge cache


# ---------------------------------------------------------------------------
# Per-holder worker (called from thread pool)
# ---------------------------------------------------------------------------

def process_single_holder(holder, metadata_map, db, rdb, session, mint,
                           connection_id, progress_lock, progress_state):
    """
    Full pipeline for one holder:
      fetch transfers → dedup → build graph → upsert ArangoDB →
      mark Redis → cache edges → update progress
    """
    owner         = holder.get('owner', '')
    token_account = holder.get('address', '')   # holder's token account address
    new_count     = 0
    proc_count    = 0

    try:
        raw_transfers = fetch_transfers_for_holder(session, owner, token_account, mint)
        new_transfers = filter_new_transfers(rdb, raw_transfers)

        if new_transfers:
            meta = metadata_map.get(owner, {})
            addr_docs, token_docs, tx_docs, edge_docs = build_graph_from_transfers(new_transfers, owner, meta)

            if addr_docs or token_docs or edge_docs:
                upsert_graph(db, addr_docs, token_docs, edge_docs)

            # Upsert transaction docs
            if tx_docs:
                db.collection('transactions').insert_many(
                    tx_docs, overwrite_mode='update', merge=True, silent=True)

            batch_ts = int(time.time())
            mark_transfers_processed(rdb, new_transfers, batch_ts)
            cache_edges_redis(rdb, mint, edge_docs)

            new_count  = len(new_transfers)
            proc_count = len(edge_docs)

    except Exception as e:
        log(TAG, f'process_single_holder error {owner[:12]}...: {e}')

    # Update shared progress counters
    with progress_lock:
        progress_state['holders_done']    += 1
        progress_state['total_transfers'] += new_count
        progress_state['total_edges']     += proc_count
        done     = progress_state['holders_done']
        total_h  = progress_state['total_holders']
        total_t  = progress_state['total_transfers']
        total_e  = progress_state['total_edges']

    if done % 5 == 0 or done == total_h:
        publish_progress(rdb, connection_id, 'processing',
                         f'{done}/{total_h} holders — {total_t} new transfers, {total_e} edges')

    return new_count, proc_count


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def load_token(rdb, mint, connection_id, threads=20, top_holder_count=200):
    """
    Full load-token pipeline for v2 (transfer-path, Solscan only).

    Phases:
      cache_check → holders → metadata_batch → transfer_threads → complete
    """
    session = requests.Session()
    db      = get_arango_db()
    t0      = time.time()

    token_key = f'token:{mint}'

    # ── Phase 0: TTL fast-path ────────────────────────────────────────────
    stored_updated = rdb.hget(token_key, 'updatedAt')
    if stored_updated:
        try:
            age = int(time.time()) - int(stored_updated)
            if age < HASH_TTL:
                elapsed = round((time.time() - t0) * 1000)
                log(TAG, f'Hash fresh ({age}s) for {mint[:16]}... skipping')
                publish_progress(rdb, connection_id, 'complete', 'Data ready (cached)',
                                 {'source': 'cache', 'elapsed_ms': elapsed, 'age_seconds': age})
                return
        except (ValueError, TypeError):
            pass

    # ── Phase 1: Fetch holders ────────────────────────────────────────────
    publish_progress(rdb, connection_id, 'holders',
                     f'Fetching top {top_holder_count} holders...')
    holders = fetch_top_holders(session, mint, limit=top_holder_count)
    if not holders:
        publish_progress(rdb, connection_id, 'error', 'No holders found for this token')
        return

    current_hash  = compute_holder_hash(holders)
    holder_owners = [h.get('owner', '') for h in holders if h.get('owner')]

    publish_progress(rdb, connection_id, 'holders',
                     f'Fetched {len(holders)} holders — checking roster hash...',
                     {'holder_count': len(holders)})

    # Cache raw holder list for viewer
    rdb.set(f'holders:{mint}', json.dumps(holders), ex=600)

    # ── Phase 2: Hash check ───────────────────────────────────────────────
    stored_hash = rdb.hget(token_key, 'topHolderHash')
    if isinstance(stored_hash, bytes):
        stored_hash = stored_hash.decode()

    if stored_hash == current_hash:
        rdb.hset(token_key, 'updatedAt', int(time.time()))
        elapsed = round((time.time() - t0) * 1000)
        log(TAG, f'Hash match for {mint[:16]}... serving cache')
        publish_progress(rdb, connection_id, 'hash_match',
                         'Holder roster unchanged — serving cached data',
                         {'elapsed_ms': elapsed, 'hash': current_hash})
        publish_progress(rdb, connection_id, 'complete', 'Data ready',
                         {'source': 'cache', 'elapsed_ms': elapsed, 'hash': current_hash})
        return

    # Hash mismatch or new token — update stored hash
    log(TAG, f'Hash {"mismatch" if stored_hash else "new"} for {mint[:16]}...')
    rdb.hset(token_key, mapping={
        'topHolderHash':   current_hash,
        'holderCount':     len(holders),
        'topHolderOwner':  holders[0].get('owner', ''),
        'topHolderPct':    str(holders[0].get('percentage', 0)),
        'updatedAt':       int(time.time()),
    })

    # ── Phase 3: account/metadata — parallel upfront batch ───────────────
    publish_progress(rdb, connection_id, 'metadata',
                     f'Fetching metadata for {len(holder_owners)} holders...')
    metadata_map = fetch_metadata_batch(session, holder_owners)
    log(TAG, f'Metadata fetched for {len(metadata_map)} owners')

    publish_progress(rdb, connection_id, 'metadata',
                     f'Metadata ready — starting transfer scan',
                     {'metadata_count': len(metadata_map)})

    # ── Phase 4: Transfer thread pool ────────────────────────────────────
    publish_progress(rdb, connection_id, 'processing',
                     f'0/{len(holders)} holders — {threads} threads ready...')

    progress_lock  = threading.Lock()
    progress_state = {
        'holders_done':    0,
        'total_holders':   len(holders),
        'total_transfers': 0,
        'total_edges':     0,
    }

    thread_local = threading.local()

    def get_thread_resources():
        if not hasattr(thread_local, 'db'):
            thread_local.db      = get_arango_db()
            thread_local.session = requests.Session()
        return thread_local.db, thread_local.session

    def _process(holder):
        t_db, t_session = get_thread_resources()
        return process_single_holder(
            holder, metadata_map, t_db, rdb, t_session, mint,
            connection_id, progress_lock, progress_state,
        )

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = {pool.submit(_process, h): h for h in holders}
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                holder = futures[f]
                log(TAG, f'Thread error on {holder.get("owner","?")[:12]}...: {e}')

    # ── Phase 5: Verify edges are queryable before signaling complete ────
    with progress_lock:
        final = progress_state.copy()

    if final['total_edges'] > 0:
        publish_progress(rdb, connection_id, 'processing',
                         f'Verifying {final["total_edges"]} edges committed...')
        token_ref = f'tokens/{mint}'
        for check in range(10):
            try:
                count = next(db.aql.execute(
                    'FOR e IN edges FILTER e.token == @token '
                    'COLLECT WITH COUNT INTO c RETURN c',
                    bind_vars={'token': token_ref}
                ), 0)
                if count > 0:
                    break
            except Exception:
                pass
            time.sleep(0.3)

    elapsed = round((time.time() - t0) * 1000)

    publish_progress(rdb, connection_id, 'complete', 'Processing complete', {
        'source':             'processed',
        'elapsed_ms':         elapsed,
        'hash':               current_hash,
        'holders_processed':  final['total_holders'],
        'new_transfers':      final['total_transfers'],
        'edges_written':      final['total_edges'],
    })

    log(TAG, f'Done: {mint[:16]}... {final["total_holders"]} holders, '
             f'{final["total_transfers"]} new transfers, {final["total_edges"]} edges '
             f'in {elapsed}ms')


# ---------------------------------------------------------------------------
# Redis Stream consumer group
# ---------------------------------------------------------------------------

def ensure_stream_group(rdb):
    try:
        rdb.xgroup_create(STREAM_NAME, GROUP_NAME, id='0', mkstream=True)
        log(TAG, f'Created consumer group: {GROUP_NAME}')
    except Exception as e:
        if 'BUSYGROUP' not in str(e):
            raise


def run_stream_consumer(rdb, consumer_name, default_threads):
    ensure_stream_group(rdb)
    bootstrap_arango()
    log(TAG, f'Consumer [{consumer_name}] listening on {STREAM_NAME}...')

    while RUNNING:
        try:
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
                        if not mint:
                            rdb.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue

                        connection_id    = data.get('connection_id') or f'cli-{uuid.uuid4().hex[:8]}'
                        threads          = int(data.get('threads', default_threads))
                        top_holder_count = int(data.get('top_holder_count', 80))

                        log(TAG, f'[{consumer_name}] LoadToken {mint[:16]}... '
                                 f'(conn={connection_id[:12]}, threads={threads})')

                        def process_and_ack(mid=msg_id):
                            try:
                                load_token(rdb, mint, connection_id,
                                           threads=threads,
                                           top_holder_count=top_holder_count)
                            finally:
                                rdb.xack(STREAM_NAME, GROUP_NAME, mid)

                        threading.Thread(target=process_and_ack, daemon=True).start()

                    except Exception as e:
                        log(TAG, f'Message parse error: {e}')
                        rdb.xack(STREAM_NAME, GROUP_NAME, msg_id)

        except Exception as e:
            log(TAG, f'Stream read error: {e}')
            time.sleep(2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    global RUNNING
    import signal

    parser = argparse.ArgumentParser(description='Token holder analysis — transfer path v2')
    parser.add_argument('--mint',    type=str, help='Process a single token (CLI mode)')
    parser.add_argument('--threads', type=int, default=20,
                        help='Worker threads per token (default: 20)')
    parser.add_argument('--holders', type=int, default=200,
                        help='Top N holders to process (default: 200)')
    parser.add_argument('--name',    type=str, default=None,
                        help='Consumer name for multi-instance deployments')
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
        connection_id = f'cli-{uuid.uuid4().hex[:8]}'
        bootstrap_arango()
        load_token(rdb, args.mint, connection_id,
                   threads=args.threads,
                   top_holder_count=args.holders)
    else:
        consumer_name = args.name or f'worker-{uuid.uuid4().hex[:8]}'
        run_stream_consumer(rdb, consumer_name, args.threads)


if __name__ == '__main__':
    main()
