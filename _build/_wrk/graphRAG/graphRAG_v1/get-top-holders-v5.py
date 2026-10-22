#!/usr/bin/env python3
"""
get-top-holders-v5.py -- Token holder analysis via graph_service microservice layer

Refactored from v4. All graph logic (address docs, edge docs, token docs,
type classification, funded_by edges, swap edges) delegated to graph_service.py.
This file is pure pipeline orchestration — fetch, dedup, delegate, progress.

Collections (Option A — lean graph):
    addresses   — every Solana account
    edges       — ALL relationships (transfers, swaps, funding)
    tokens      — SPL token metadata
    (no pool_edges, no defi_activities, no transactions)

Usage:
    python get-top-holders-v5.py                        # stream consumer mode
    python get-top-holders-v5.py --mint <address>       # CLI single token
    python get-top-holders-v5.py --threads 20           # configure thread count
    python get-top-holders-v5.py --name worker-2        # named consumer instance
"""

import argparse, hashlib, json, os, sys, time, uuid
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import redis as redis_lib
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from graph_service import (
    GraphService,
    build_address_doc, build_token_doc, build_transfer_edge,
    build_funded_by_edge, build_swap_edges, build_defi_doc,
    build_transaction_doc, classify_address_type,
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

TAG         = 'top-holders-v5'
STREAM_NAME = 't16o:stream:load-token'
GROUP_NAME  = 't16o:top-holders'
HASH_TTL    = 300
RUNNING     = True
_HEADERS    = {'token': SOLSCAN_TOKEN}

from datetime import datetime
def log(tag, msg):
    print(f'[{datetime.now().strftime("%H:%M:%S")}][{tag}] {msg}', flush=True)


# ---------------------------------------------------------------------------
# Progress publishing
# ---------------------------------------------------------------------------

def publish_progress(rdb, connection_id, phase, message='', data=None):
    payload = {'connection_id': connection_id, 'phase': phase, 'message': message}
    if data:
        payload['data'] = data
    rdb.publish(f't16o:progress:{connection_id}', json.dumps(payload, default=str))


# ---------------------------------------------------------------------------
# Solscan API — holders
# ---------------------------------------------------------------------------

def fetch_top_holders(session, mint, limit=80):
    holders = []
    for page in range(1, -(-limit // 40) + 1):
        try:
            r = session.get(f'{SOLSCAN_API}/token/holders',
                            params={'address': mint, 'page': page, 'page_size': 40},
                            headers=_HEADERS, timeout=30)
            r.raise_for_status()
            items = (r.json().get('data') or {}).get('items', [])
            if not items: break
            holders.extend(items)
            if len(holders) >= limit: break
        except Exception as e:
            log(TAG, f'holders error page={page}: {e}')
            break
        time.sleep(0.06)
    holders.sort(key=lambda h: int(h.get('amount', 0)), reverse=True)
    return holders[:limit]


def compute_holder_hash(holders):
    return hashlib.sha256(''.join(h.get('owner', '') for h in holders).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Solscan API — metadata batch
# ---------------------------------------------------------------------------

def fetch_metadata_batch(session, owners):
    results = {}
    def _fetch(owner):
        for attempt in range(3):
            try:
                r = session.get(f'{SOLSCAN_API}/account/metadata',
                                params={'address': owner}, headers=_HEADERS, timeout=10)
                if r.status_code == 429:
                    time.sleep(min(2 ** (attempt + 1), 8)); continue
                r.raise_for_status()
                body = r.json()
                return owner, (body['data'] if body.get('success') and body.get('data') else {})
            except Exception as e:
                if attempt < 2: time.sleep(1); continue
                log(TAG, f'metadata error {owner[:12]}...: {e}')
        return owner, {}

    with ThreadPoolExecutor(max_workers=20) as pool:
        for f in as_completed({pool.submit(_fetch, o): o for o in owners}):
            owner, meta = f.result()
            results[owner] = meta
    return results


# ---------------------------------------------------------------------------
# Solscan API — transfers
# ---------------------------------------------------------------------------

def fetch_transfers(session, owner, token_account, mint, max_pages=5, page_size=100):
    all_tx = []
    for page in range(1, max_pages + 1):
        items = []
        for attempt in range(3):
            try:
                r = session.get(f'{SOLSCAN_API}/account/transfer',
                                params={'address': owner, 'token_account': token_account,
                                        'page': page, 'page_size': page_size,
                                        'sort_by': 'block_time', 'sort_order': 'desc'},
                                headers=_HEADERS, timeout=15)
                if r.status_code == 429:
                    time.sleep(min(2 ** (attempt + 1), 8)); continue
                r.raise_for_status()
                items = r.json().get('data') or []
                all_tx.extend(items)
                break
            except Exception as e:
                if attempt < 2: time.sleep(1); continue
                log(TAG, f'transfer error {owner[:12]}... p={page}: {e}')
                break
        if len(items) < page_size: break  # partial page = last page
        time.sleep(0.06)
    return all_tx


# ---------------------------------------------------------------------------
# Solscan API — defi activities
# ---------------------------------------------------------------------------

def fetch_defi(session, owner, max_pages=5, page_size=100):
    results, token_meta = [], {}
    for page in range(1, max_pages + 1):
        items = []
        for attempt in range(3):
            try:
                r = session.get(f'{SOLSCAN_API}/account/defi/activities',
                                params={'address': owner, 'page': page, 'page_size': page_size,
                                        'sort_by': 'block_time', 'sort_order': 'desc'},
                                headers=_HEADERS, timeout=15)
                if r.status_code == 429:
                    time.sleep(min(2 ** (attempt + 1), 8)); continue
                r.raise_for_status()
                body = r.json()
                items = body.get('data') or []
                results.extend(items)
                token_meta.update((body.get('metadata') or {}).get('tokens') or {})
                break
            except Exception as e:
                if attempt < 2: time.sleep(1); continue
                log(TAG, f'defi error {owner[:12]}... p={page}: {e}')
                break
        if len(items) < page_size: break  # partial page = last page
        time.sleep(0.06)
    return results, token_meta


# ---------------------------------------------------------------------------
# Redis dedup
# ---------------------------------------------------------------------------

def filter_new(rdb, items, prefix='tx'):
    if not items: return []
    pipe = rdb.pipeline(transaction=False)
    for t in items: pipe.exists(f'{prefix}:{t["trans_id"]}')
    return [t for t, known in zip(items, pipe.execute()) if not known]


def mark_processed(rdb, items, batch_ts, prefix='tx'):
    if not items: return
    pipe = rdb.pipeline(transaction=False)
    for t in items: pipe.set(f'{prefix}:{t["trans_id"]}', batch_ts, ex=86400 * 30)
    pipe.execute()


# ---------------------------------------------------------------------------
# Redis caching
# ---------------------------------------------------------------------------

def cache_edges(rdb, mint, edge_docs):
    if not edge_docs: return
    key = f't16o:edges:{mint}'
    pipe = rdb.pipeline(transaction=False)
    for e in edge_docs:
        sig = e.get('signature', '')
        if sig: pipe.zadd(key, {sig: e.get('block_time') or 0})
    pipe.execute()
    rdb.expire(key, 86400 * 7)


# ---------------------------------------------------------------------------
# Per-holder worker — uses graph_service for all DB operations
# ---------------------------------------------------------------------------

def process_holder(holder, metadata_map, gs, rdb, session, mint,
                   connection_id, progress_lock, progress_state,
                   max_pages=5, page_size=100):
    owner = holder.get('owner', '')
    token_account = holder.get('address', '')
    new_tx = new_defi = 0
    meta = metadata_map.get(owner, {})

    try:
        # Transfers
        raw_tx = fetch_transfers(session, owner, token_account, mint, max_pages=max_pages, page_size=page_size)
        new_transfers = filter_new(rdb, raw_tx, 'tx')

        # DeFi
        raw_defi, token_meta = fetch_defi(session, owner, max_pages=max_pages, page_size=page_size)
        new_defi_list = filter_new(rdb, raw_defi, 'defi')

        batch_ts = int(time.time())

        # -- Build docs via graph_service --
        addr_docs = []
        token_docs = []
        edge_docs = []

        # Owner address with full metadata
        addr_docs.append(build_address_doc(owner, 0, meta=meta))

        # Transfer edges
        if new_transfers:
            for tx in new_transfers:
                f = tx.get('from_address', '')
                t = tx.get('to_address', '')
                if not f or not t: continue
                bt = tx.get('block_time', 0)
                # Stub addresses for counterparties
                if f != owner: addr_docs.append(build_address_doc(f, bt))
                if t != owner: addr_docs.append(build_address_doc(t, bt))
                # Token doc
                tok = tx.get('token_address', '')
                if tok: token_docs.append(build_token_doc(tok, decimals=tx.get('token_decimals', 0)))
                # Edge
                edge_docs.append(build_transfer_edge(f, t, tx))

            # Funded_by edge from metadata
            funded = (meta.get('funded_by') or {})
            funder = funded.get('funded_by')
            if funder:
                funder_doc = build_address_doc(funder, funded.get('block_time', 0))
                funder_doc['is_funder'] = True  # bypass 2-edge threshold in enricher
                addr_docs.append(funder_doc)
                edge_docs.append(build_funded_by_edge(funder, owner,
                                                       funded.get('tx_hash', ''),
                                                       funded.get('block_time', 0)))

            mark_processed(rdb, new_transfers, batch_ts, 'tx')
            new_tx = len(new_transfers)

        # DeFi → swap edges
        if new_defi_list:
            for raw in new_defi_list:
                defi_doc = build_defi_doc(raw, owner)
                swap_addrs, swap_edges = build_swap_edges(defi_doc, owner)
                addr_docs.extend(swap_addrs)
                edge_docs.extend(swap_edges)

            mark_processed(rdb, new_defi_list, batch_ts, 'defi')
            new_defi = len(new_defi_list)

        # Token metadata from defi envelope
        if token_meta:
            for mint_addr, info in token_meta.items():
                if mint_addr:
                    token_docs.append(build_token_doc(
                        mint_addr,
                        symbol=info.get('token_symbol', ''),
                        name=info.get('token_name', ''),
                        icon=info.get('token_icon', ''),
                    ))

        # -- Transaction docs (processing ledger) --
        tx_docs = []
        if new_transfers:
            for tx in new_transfers:
                if tx.get('trans_id'):
                    tx_docs.append(build_transaction_doc(tx, origin=owner))
        if new_defi_list:
            for raw in new_defi_list:
                defi_doc = build_defi_doc(raw, owner)
                if defi_doc.get('trans_id'):
                    tx_docs.append(build_transaction_doc(defi_doc, origin=owner))

        # -- Single upsert call --
        if addr_docs or token_docs or edge_docs:
            gs.upsert_graph(addr_docs, token_docs, edge_docs)
        if tx_docs:
            gs.upsert_transactions(tx_docs)

        # Redis cache
        if edge_docs:
            cache_edges(rdb, mint, edge_docs)

    except Exception as e:
        log(TAG, f'holder error {owner[:12]}...: {e}')

    with progress_lock:
        progress_state['holders_done'] += 1
        progress_state['total_transfers'] += new_tx
        progress_state['total_defi'] += new_defi
        done = progress_state['holders_done']
        total_h = progress_state['total_holders']

    if done % 5 == 0 or done == total_h:
        publish_progress(rdb, connection_id, 'processing',
                         f'{done}/{total_h} holders — '
                         f'{progress_state["total_transfers"]} transfers, '
                         f'{progress_state["total_defi"]} defi')

    return new_tx, new_defi


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def load_token(rdb, mint, connection_id, threads=20, top_holder_count=80, max_pages=5, page_size=100):
    session = requests.Session()
    gs = GraphService()
    gs.bootstrap()
    t0 = time.time()
    token_key = f'token:{mint}'

    # Phase 0: TTL
    stored = rdb.hget(token_key, 'updatedAt')
    if stored:
        try:
            age = int(time.time()) - int(stored)
            if age < HASH_TTL:
                log(TAG, f'Hash fresh ({age}s) for {mint[:16]}... skipping')
                publish_progress(rdb, connection_id, 'complete', 'Data ready (cached)',
                                 {'source': 'cache', 'elapsed_ms': 0, 'age_seconds': age})
                return
        except (ValueError, TypeError): pass

    # Phase 1: Holders
    publish_progress(rdb, connection_id, 'holders', f'Fetching top {top_holder_count} holders...')
    holders = fetch_top_holders(session, mint, limit=top_holder_count)
    if not holders:
        publish_progress(rdb, connection_id, 'error', 'No holders found')
        return

    current_hash = compute_holder_hash(holders)
    holder_owners = [h['owner'] for h in holders if h.get('owner')]

    rdb.set(f'holders:{mint}', json.dumps(holders), ex=600)

    # Fetch token metadata (name, symbol) — one authoritative call
    # Uses overwrite (not merge) so it replaces empty-string fields
    try:
        tr = session.get(f'{SOLSCAN_API}/token/meta',
                         params={'address': mint}, headers=_HEADERS, timeout=10)
        if tr.ok:
            tmeta = tr.json().get('data') or {}
            if tmeta and tmeta.get('symbol'):
                gs.db.collection('tokens').insert(
                    build_token_doc(
                        mint,
                        symbol=tmeta.get('symbol', ''),
                        name=tmeta.get('name', ''),
                        decimals=tmeta.get('decimals', 0),
                        icon=tmeta.get('icon', ''),
                    ),
                    overwrite=True, silent=True)
    except Exception as e:
        log(TAG, f'token/meta error: {e}')

    # Phase 2: Hash check
    stored_hash = rdb.hget(token_key, 'topHolderHash')
    if isinstance(stored_hash, bytes): stored_hash = stored_hash.decode()

    if stored_hash == current_hash:
        rdb.hset(token_key, 'updatedAt', int(time.time()))
        elapsed = round((time.time() - t0) * 1000)
        log(TAG, f'Hash match for {mint[:16]}...')
        publish_progress(rdb, connection_id, 'complete', 'Data ready',
                         {'source': 'cache', 'elapsed_ms': elapsed})
        return

    log(TAG, f'Hash {"mismatch" if stored_hash else "new"} for {mint[:16]}...')
    rdb.hset(token_key, mapping={
        'topHolderHash': current_hash, 'holderCount': len(holders),
        'topHolderOwner': holders[0].get('owner', ''),
        'topHolderPct': str(holders[0].get('percentage', 0)),
        'updatedAt': int(time.time()),
    })

    # Phase 3: Metadata
    publish_progress(rdb, connection_id, 'metadata', f'Fetching metadata for {len(holder_owners)} holders...')
    metadata_map = fetch_metadata_batch(session, holder_owners)
    log(TAG, f'Metadata: {len(metadata_map)} owners')

    # Phase 4: Thread pool
    publish_progress(rdb, connection_id, 'processing', f'0/{len(holders)} holders — {threads} threads...')

    progress_lock = threading.Lock()
    progress_state = {'holders_done': 0, 'total_holders': len(holders),
                      'total_transfers': 0, 'total_defi': 0}

    thread_local = threading.local()
    def _get():
        if not hasattr(thread_local, 'gs'):
            thread_local.gs = GraphService()
            thread_local.gs._bootstrapped = True  # main gs already bootstrapped
            thread_local.session = requests.Session()
        return thread_local.gs, thread_local.session

    def _process(h):
        t_gs, t_sess = _get()
        return process_holder(h, metadata_map, t_gs, rdb, t_sess, mint,
                              connection_id, progress_lock, progress_state,
                              max_pages=max_pages, page_size=page_size)

    with ThreadPoolExecutor(max_workers=threads) as pool:
        for f in as_completed({pool.submit(_process, h): h for h in holders}):
            try: f.result()
            except Exception as e: log(TAG, f'Thread error: {e}')

    # Phase 5: Verify + complete
    total_edges = progress_state['total_transfers'] + progress_state['total_defi']
    if total_edges > 0:
        token_ref = f'tokens/{mint}'
        for _ in range(10):
            try:
                if next(gs.db.aql.execute(
                    'FOR e IN edges FILTER e.token==@t LIMIT 1 RETURN 1',
                    bind_vars={'t': token_ref}), None):
                    break
            except: pass
            time.sleep(0.3)

    elapsed = round((time.time() - t0) * 1000)
    publish_progress(rdb, connection_id, 'complete', 'Processing complete', {
        'source': 'processed', 'elapsed_ms': elapsed, 'hash': current_hash,
        'holders_processed': progress_state['total_holders'],
        'new_transfers': progress_state['total_transfers'],
        'new_defi': progress_state['total_defi'],
    })
    log(TAG, f'Done: {mint[:16]}... {progress_state["total_holders"]} holders, '
             f'{progress_state["total_transfers"]} tx, {progress_state["total_defi"]} defi '
             f'in {elapsed}ms')


# ---------------------------------------------------------------------------
# Stream consumer
# ---------------------------------------------------------------------------

def run_consumer(rdb, name, threads):
    try: rdb.xgroup_create(STREAM_NAME, GROUP_NAME, id='0', mkstream=True)
    except: pass

    gs = GraphService()
    gs.bootstrap()
    log(TAG, f'Consumer [{name}] on {STREAM_NAME}')

    while RUNNING:
        try:
            msgs = rdb.xreadgroup(GROUP_NAME, name, {STREAM_NAME: '>'}, count=1, block=5000)
            if not msgs: continue
            for _, entries in msgs:
                for mid, data in entries:
                    mint = data.get('mint', '')
                    if not mint:
                        rdb.xack(STREAM_NAME, GROUP_NAME, mid); continue
                    cid = data.get('connection_id') or f'cli-{uuid.uuid4().hex[:8]}'
                    th = int(data.get('threads', threads))
                    hc = int(data.get('top_holder_count', 80))
                    mp = int(data.get('max_pages', 5))
                    ps = int(data.get('page_size', 100))
                    log(TAG, f'[{name}] LoadToken {mint[:16]}... (conn={cid[:12]}, t={th}, holders={hc}, pages={mp}, pageSize={ps})')

                    def _run(m=mid):
                        try: load_token(rdb, mint, cid, threads=th, top_holder_count=hc,
                                        max_pages=mp, page_size=ps)
                        finally: rdb.xack(STREAM_NAME, GROUP_NAME, m)
                    threading.Thread(target=_run, daemon=True).start()
        except Exception as e:
            log(TAG, f'Stream error: {e}')
            time.sleep(2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    global RUNNING
    import signal

    parser = argparse.ArgumentParser(description='Token holder analysis v5 (graph_service)')
    parser.add_argument('--mint', type=str)
    parser.add_argument('--threads', type=int, default=20)
    parser.add_argument('--holders', type=int, default=80)
    parser.add_argument('--name', type=str, default=None)
    args = parser.parse_args()

    rdb = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    rdb.ping()
    log(TAG, 'Redis connected')

    signal.signal(signal.SIGINT, lambda *_: globals().update(RUNNING=False))
    signal.signal(signal.SIGTERM, lambda *_: globals().update(RUNNING=False))

    if args.mint:
        load_token(rdb, args.mint, f'cli-{uuid.uuid4().hex[:8]}',
                   threads=args.threads, top_holder_count=args.holders)
    else:
        run_consumer(rdb, args.name or f'w-{uuid.uuid4().hex[:6]}', args.threads)


if __name__ == '__main__':
    main()
