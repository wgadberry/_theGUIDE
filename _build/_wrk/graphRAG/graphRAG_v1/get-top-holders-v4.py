#!/usr/bin/env python3
"""
get-top-holders-v4.py -- Token holder analysis with DeFi activity enrichment

v2 foundation + v3 DeFi enhancements. Uses v2's proven graph builder
(compatible with upsert_graph), progress phases, funded_by edges, and
verification loop. Adds v3's account/defi/activities endpoint, defi_activities
collection, token metadata upsert, and Redis defi caches.

Pipeline per run:
  1. TTL check        — skip if hash is fresh (< 5 min)
  2. Fetch top holders from token/holders
  3. Hash check       — fast-path if roster unchanged
  4. account/metadata — parallel upfront batch
  5. Thread pool      — per holder, in parallel:
       a. account/transfer        (up to 5 pages × 100)
       b. account/defi/activities (up to 5 pages × 100)
       c. Dedup both against Redis tx: / defi: keys
       d. Build graph docs (addresses, tokens, transactions, edges, funded_by)
       e. Build defi_activity docs with flattened router legs
       f. Upsert ArangoDB + mark Redis + cache
  6. Verify edges committed, then signal complete

Usage:
    python get-top-holders-v4.py                        # stream consumer mode
    python get-top-holders-v4.py --mint <address>       # CLI single token
    python get-top-holders-v4.py --threads 20           # configure thread count
    python get-top-holders-v4.py --name worker-2        # named consumer instance
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

TAG         = 'top-holders-v4'
STREAM_NAME = 't16o:stream:load-token'
GROUP_NAME  = 't16o:top-holders'
HASH_TTL    = 300
RUNNING     = True
_HEADERS    = {'token': SOLSCAN_TOKEN}


# ---------------------------------------------------------------------------
# Progress publishing (v2 — compatible with viewer OnProgress/OnComplete)
# ---------------------------------------------------------------------------

def publish_progress(rdb, connection_id, phase, message='', data=None):
    payload = {'connection_id': connection_id, 'phase': phase, 'message': message}
    if data:
        payload['data'] = data
    rdb.publish(f't16o:progress:{connection_id}', json.dumps(payload, default=str))


# ---------------------------------------------------------------------------
# Step 1 — Fetch top holders (v2)
# ---------------------------------------------------------------------------

def fetch_top_holders(session, mint, limit=200):
    holders = []
    pages_needed = -(-limit // 40)
    for page in range(1, pages_needed + 1):
        try:
            r = session.get(f'{SOLSCAN_API}/token/holders',
                            params={'address': mint, 'page': page, 'page_size': 40},
                            headers=_HEADERS, timeout=30)
            r.raise_for_status()
            body = r.json()
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
        time.sleep(0.06)
    holders.sort(key=lambda h: int(h.get('amount', 0)), reverse=True)
    return holders[:limit]


def compute_holder_hash(holders):
    return hashlib.sha256(''.join(h.get('owner', '') for h in holders).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Step 2 — account/metadata parallel batch (v2 with retry)
# ---------------------------------------------------------------------------

def fetch_metadata_batch(session, owners):
    results = {}

    def _fetch_one(owner):
        for attempt in range(3):
            try:
                r = session.get(f'{SOLSCAN_API}/account/metadata',
                                params={'address': owner}, headers=_HEADERS, timeout=10)
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
                log(TAG, f'metadata error {owner[:12]}...: {e}')
        return owner, {}

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(_fetch_one, o): o for o in owners}
        for f in as_completed(futures):
            owner, meta = f.result()
            results[owner] = meta
    return results


# ---------------------------------------------------------------------------
# Step 3a — account/transfer (v2 with multi-page + retry)
# ---------------------------------------------------------------------------

def fetch_transfers_for_holder(session, owner, token_account, mint, max_pages=5):
    all_transfers = []
    for page in range(1, max_pages + 1):
        data = _fetch_transfer_page(session, owner, token_account, page)
        if not data:
            break
        all_transfers.extend(data)
        if len(data) < 100:
            break
        time.sleep(0.06)
    return all_transfers


def _fetch_transfer_page(session, owner, token_account, page):
    for attempt in range(3):
        try:
            r = session.get(f'{SOLSCAN_API}/account/transfer',
                            params={'address': owner, 'token_account': token_account,
                                    'page': page, 'page_size': 100,
                                    'sort_by': 'block_time', 'sort_order': 'desc'},
                            headers=_HEADERS, timeout=15)
            if r.status_code == 429:
                time.sleep(min(2 ** (attempt + 1), 8))
                continue
            r.raise_for_status()
            return r.json().get('data') or []
        except Exception as e:
            if attempt < 2:
                time.sleep(1)
                continue
            log(TAG, f'transfer error {owner[:12]}... page={page}: {e}')
    return []


# ---------------------------------------------------------------------------
# Step 3b — account/defi/activities (NEW from v3)
# ---------------------------------------------------------------------------

def fetch_defi_for_holder(session, owner, max_pages=5):
    """Fetch DeFi swap/LP activity. Returns (activities_list, token_metadata_dict)."""
    results = []
    token_metadata = {}
    for page in range(1, max_pages + 1):
        for attempt in range(3):
            try:
                r = session.get(f'{SOLSCAN_API}/account/defi/activities',
                                params={'address': owner, 'page': page, 'page_size': 100,
                                        'sort_by': 'block_time', 'sort_order': 'desc'},
                                headers=_HEADERS, timeout=15)
                if r.status_code == 429:
                    time.sleep(min(2 ** (attempt + 1), 8))
                    continue
                r.raise_for_status()
                body = r.json()
                items = body.get('data') or []
                results.extend(items)
                page_tokens = (body.get('metadata') or {}).get('tokens') or {}
                token_metadata.update(page_tokens)
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(1)
                    continue
                log(TAG, f'defi error {owner[:12]}... page={page}: {e}')
                break
        if not results or len(results) % 100 != 0:
            break
        time.sleep(0.06)
    return results, token_metadata


# ---------------------------------------------------------------------------
# Dedup helpers
# ---------------------------------------------------------------------------

def filter_new_transfers(rdb, transfers):
    if not transfers:
        return []
    pipe = rdb.pipeline(transaction=False)
    for t in transfers:
        pipe.exists(f'tx:{t["trans_id"]}')
    flags = pipe.execute()
    return [t for t, known in zip(transfers, flags) if not known]


def filter_new_defi(rdb, activities):
    if not activities:
        return []
    pipe = rdb.pipeline(transaction=False)
    for a in activities:
        pipe.exists(f'defi:{a["trans_id"]}')
    flags = pipe.execute()
    return [a for a, seen in zip(activities, flags) if not seen]


def mark_transfers_processed(rdb, transfers, batch_ts):
    if not transfers:
        return
    pipe = rdb.pipeline(transaction=False)
    for t in transfers:
        pipe.set(f'tx:{t["trans_id"]}', batch_ts, ex=86400 * 30)
    pipe.execute()


def mark_defi_processed(rdb, activities, batch_ts):
    if not activities:
        return
    pipe = rdb.pipeline(transaction=False)
    for a in activities:
        pipe.set(f'defi:{a["trans_id"]}', batch_ts, ex=86400 * 30)
    pipe.execute()


# ---------------------------------------------------------------------------
# Graph builder (v2 — compatible with upsert_graph)
# ---------------------------------------------------------------------------

import re

_ACTIVITY_TO_EDGE_TYPE = {
    'ACTIVITY_SPL_TRANSFER':              'spl_transfer',
    'ACTIVITY_TOKEN_SWAP':                'swap_in',
    'ACTIVITY_AGG_TOKEN_SWAP':            'swap_in',
    'ACTIVITY_TOKEN_ADD_LIQUIDITY':       'add_liquidity',
    'ACTIVITY_TOKEN_REMOVE_LIQUIDITY':    'remove_liquidity',
    'ACTIVITY_SPL_BURN':                  'burn',
    'ACTIVITY_SPL_MINT':                  'mint',
    'ACTIVITY_SPL_CREATE_ACCOUNT':        'create_ata',
    'ACTIVITY_SYSTEM_TRANSFER':           'sol_transfer',
}


def _classify_type(meta):
    tags = [t.lower() for t in (meta.get('account_tags') or [])]
    label = (meta.get('account_label') or '').lower()
    acct = meta.get('account_type', '')
    if 'vault' in label and not re.search(r'\([^)]*vault[^)]*\)', label):
        return 'vault'
    if 'pool' in tags:      return 'pool'
    if 'market' in tags:    return 'market'
    if any('vault' in t for t in tags): return 'vault'
    if 'lptoken' in tags:   return 'lp_token'
    if 'dex_wallet' in tags: return 'dex_wallet'
    if 'arbitrage_bot' in tags: return 'bot'
    if acct == 'token_account': return 'ata'
    if acct == 'program':   return 'program'
    if acct == 'mint':      return 'mint'
    if acct == 'address':   return 'wallet'
    return 'unknown'


def build_graph_from_transfers(transfers, owner, meta):
    """Returns (addr_docs, token_docs, tx_docs, edge_docs) — v2 format."""
    addr_map, token_map, tx_map, edge_docs = {}, {}, {}, []

    def _ensure_addr(address, block_time=0):
        if address not in addr_map:
            doc = {'_key': address, 'type': 'unknown',
                   'first_seen': block_time or 0, 'last_seen': block_time or 0, 'enriched': 0}
            if address == owner and meta:
                doc['type'] = _classify_type(meta)
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
            ex = addr_map[address]
            if block_time:
                ex['first_seen'] = min(ex.get('first_seen', 0) or 0, block_time)
                ex['last_seen'] = max(ex.get('last_seen', 0) or 0, block_time)

    for tx in transfers:
        f, t = tx.get('from_address', ''), tx.get('to_address', '')
        if not f or not t:
            continue
        bt = tx.get('block_time', 0)
        _ensure_addr(f, bt)
        _ensure_addr(t, bt)

        activity = tx.get('activity_type', '')
        edge_type = _ACTIVITY_TO_EDGE_TYPE.get(activity, 'spl_transfer')
        if edge_type == 'swap_in' and tx.get('flow') == 'out':
            edge_type = 'swap_out'

        tok = tx.get('token_address', '')
        tid = tx.get('trans_id', '')

        if tok and tok not in token_map:
            token_map[tok] = {'_key': tok, 'decimals': tx.get('token_decimals', 0)}
        if tid and tid not in tx_map:
            tx_map[tid] = {'_key': tid, 'status': 'processed', 'processed_at': int(time.time()),
                           'origin': owner, 'block_time': bt, 'block_id': tx.get('block_id'),
                           'activity_type': activity, 'signer': f}

        edge_docs.append({
            '_from': f'addresses/{f}', '_to': f'addresses/{t}',
            'edge_type': edge_type,
            'token': f'tokens/{tok}' if tok else None,
            'amount': str(tx.get('amount', '0')),
            'decimals': tx.get('token_decimals', 0),
            'signature': tid, 'block_time': bt,
            'activity_type': activity, 'flow': tx.get('flow', ''), 'value': tx.get('value'),
        })

    # funded_by edge
    if meta and owner in addr_map:
        funded = meta.get('funded_by') or {}
        funder = funded.get('funded_by')
        if funder:
            _ensure_addr(funder, funded.get('block_time', 0))
            edge_docs.append({
                '_from': f'addresses/{funder}', '_to': f'addresses/{owner}',
                'edge_type': 'funded_by', 'token': None, 'amount': '0', 'decimals': 0,
                'signature': funded.get('tx_hash', ''), 'block_time': funded.get('block_time', 0),
            })

    return list(addr_map.values()), list(token_map.values()), list(tx_map.values()), edge_docs


# ---------------------------------------------------------------------------
# DeFi activity doc builder (from v3)
# ---------------------------------------------------------------------------

def build_defi_docs(activities, owner):
    """Build defi_activities collection docs with flattened router legs."""
    docs = []
    for a in activities:
        tid = a.get('trans_id', '')
        if not tid:
            continue
        routers = a.get('routers') or {}
        child_routers = routers.get('child_routers') or []

        legs = []
        if routers.get('token1') and routers.get('token2'):
            legs.append({
                'token_in': routers['token1'], 'decimals_in': routers.get('token1_decimals'),
                'amount_in': routers.get('amount1'),
                'token_out': routers['token2'], 'decimals_out': routers.get('token2_decimals'),
                'amount_out': routers.get('amount2'),
                'pool_address': routers.get('pool_address', ''),
                'program_address': routers.get('program_address', ''),
                'leg_index': 0,
            })
        for i, leg in enumerate(child_routers, start=1):
            legs.append({
                'token_in': leg.get('token1'), 'decimals_in': leg.get('token1_decimals'),
                'amount_in': leg.get('amount1'),
                'token_out': leg.get('token2'), 'decimals_out': leg.get('token2_decimals'),
                'amount_out': leg.get('amount2'),
                'pool_address': leg.get('pool_address', ''),
                'program_address': leg.get('program_address', ''),
                'leg_index': i,
            })

        docs.append({
            '_key': tid, 'trans_id': tid, 'block_id': a.get('block_id'),
            'block_time': a.get('block_time'), 'time': a.get('time'),
            'activity_type': a.get('activity_type'), 'from_address': a.get('from_address', owner),
            'owner': owner, 'sources': a.get('sources', []), 'platform': a.get('platform', []),
            'value_usd': a.get('value', 0),
            'token_in': routers.get('token1', ''), 'token_out': routers.get('token2', ''),
            'amount_in': routers.get('amount1'), 'amount_out': routers.get('amount2'),
            'pool_address': routers.get('pool_address', ''),
            'legs': legs, 'leg_count': len(legs),
        })
    return docs


def build_swap_edges_from_defi(defi_docs, owner):
    """Create swap_in/swap_out edges from built defi_activity docs.

    Uses the flattened defi_docs (not raw Solscan response) so pool_address,
    token_in, token_out are already extracted.

    For each swap with pool data:
      - swap_in:  owner → pool (token_in)
      - swap_out: pool → owner (token_out)
    Also creates address stubs for pool addresses.
    """
    edge_docs = []
    addr_docs = []

    for d in defi_docs:
        activity_type = d.get('activity_type', '')
        if 'SWAP' not in activity_type:
            continue

        tid = d.get('trans_id', '')
        bt = d.get('block_time', 0)
        from_addr = d.get('from_address', owner)
        token_in = d.get('token_in', '')
        token_out = d.get('token_out', '')
        amount_in = d.get('amount_in')
        amount_out = d.get('amount_out')
        value = d.get('value_usd')

        if not token_in or not token_out or not tid:
            continue

        # Use pool from top-level or first leg
        pool = d.get('pool_address', '')
        legs = d.get('legs') or []
        if not pool and legs:
            pool = legs[0].get('pool_address', '')

        if not pool:
            continue  # skip swaps without pool — can't create meaningful edges

        addr_docs.append({
            '_key': pool, 'type': 'pool',
            'first_seen': bt, 'last_seen': bt, 'enriched': 0,
        })

        # swap_in: owner sends token_in to pool
        edge_docs.append({
            '_from': f'addresses/{from_addr}', '_to': f'addresses/{pool}',
            'edge_type': 'swap_in',
            'token': f'tokens/{token_in}',
            'amount': str(amount_in or '0'),
            'signature': f'{tid}_in', 'block_time': bt,
            'activity_type': activity_type,
            'value': value,
            'pool': f'addresses/{pool}',
        })

        # swap_out: pool sends token_out to owner
        edge_docs.append({
            '_from': f'addresses/{pool}', '_to': f'addresses/{from_addr}',
            'edge_type': 'swap_out',
            'token': f'tokens/{token_out}',
            'amount': str(amount_out or '0'),
            'signature': f'{tid}_out', 'block_time': bt,
            'activity_type': activity_type,
            'value': value,
            'pool': f'addresses/{pool}',
        })

        # Multi-hop legs
        for leg in legs[1:]:
            lp = leg.get('pool_address', '')
            lt_in = leg.get('token_in', '')
            lt_out = leg.get('token_out', '')
            if lp and lt_in and lt_out:
                addr_docs.append({
                    '_key': lp, 'type': 'pool',
                    'first_seen': bt, 'last_seen': bt, 'enriched': 0,
                })
                edge_docs.append({
                    '_from': f'addresses/{from_addr}', '_to': f'addresses/{lp}',
                    'edge_type': 'swap_in', 'token': f'tokens/{lt_in}',
                    'amount': str(leg.get('amount_in', '0')),
                    'signature': f'{tid}_hop{leg["leg_index"]}_in', 'block_time': bt,
                    'activity_type': 'swap_hop',
                })
                edge_docs.append({
                    '_from': f'addresses/{lp}', '_to': f'addresses/{from_addr}',
                    'edge_type': 'swap_out', 'token': f'tokens/{lt_out}',
                    'amount': str(leg.get('amount_out', '0')),
                    'signature': f'{tid}_hop{leg["leg_index"]}_out', 'block_time': bt,
                    'activity_type': 'swap_hop',
                })

    return addr_docs, edge_docs


def upsert_tokens_from_metadata(db, token_metadata):
    """Upsert token docs from defi response metadata.tokens envelope."""
    if not token_metadata:
        return
    docs = [{'_key': mint, 'address': mint,
             'token_name': info.get('token_name', ''),
             'token_symbol': info.get('token_symbol', ''),
             'token_icon': info.get('token_icon', '')}
            for mint, info in token_metadata.items() if mint]
    if docs:
        try:
            db.collection('tokens').import_bulk(docs, on_duplicate='ignore', halt_on_error=False)
        except Exception as e:
            log(TAG, f'token upsert error: {e}')


# ---------------------------------------------------------------------------
# Redis caching
# ---------------------------------------------------------------------------

def cache_edges_redis(rdb, mint, edge_docs):
    if not edge_docs:
        return
    key = f't16o:edges:{mint}'
    pipe = rdb.pipeline(transaction=False)
    for e in edge_docs:
        sig = e.get('signature', '')
        if sig:
            pipe.zadd(key, {sig: e.get('block_time') or 0})
    pipe.execute()
    rdb.expire(key, 86400 * 7)


def cache_defi_redis(rdb, mint, owner, defi_docs):
    if not defi_docs:
        return
    pipe = rdb.pipeline(transaction=False)
    mk, ok = f't16o:defi:{mint}', f't16o:defi:owner:{owner}'
    for d in defi_docs:
        score = d.get('block_time') or 0
        pipe.zadd(mk, {d['_key']: score})
        pipe.zadd(ok, {d['_key']: score})
    pipe.execute()
    rdb.expire(mk, 86400 * 7)
    rdb.expire(ok, 86400 * 7)


# ---------------------------------------------------------------------------
# Bootstrap — ensure defi_activities + tokens collections exist (from v3)
# ---------------------------------------------------------------------------

def bootstrap_defi(db):
    try:
        if not db.has_collection('defi_activities'):
            db.create_collection('defi_activities')
            log(TAG, 'Created defi_activities collection')
        col = db.collection('defi_activities')
        existing = {idx['fields'][0] for idx in col.indexes() if idx.get('fields')}
        for field in ['owner', 'block_time', 'token_in', 'token_out', 'pool_address']:
            if field not in existing:
                col.add_persistent_index(fields=[field], sparse=True)

        if not db.has_collection('tokens'):
            db.create_collection('tokens')
        tok = db.collection('tokens')
        tok_ex = {idx['fields'][0] for idx in tok.indexes() if idx.get('fields')}
        for field in ['token_symbol', 'token_name']:
            if field not in tok_ex:
                tok.add_persistent_index(fields=[field], sparse=True)
    except Exception as e:
        log(TAG, f'bootstrap_defi error: {e}')


# ---------------------------------------------------------------------------
# Per-holder worker
# ---------------------------------------------------------------------------

def process_single_holder(holder, metadata_map, db, rdb, session, mint,
                           connection_id, progress_lock, progress_state):
    owner = holder.get('owner', '')
    token_account = holder.get('address', '')
    new_tx = 0
    new_defi = 0

    try:
        # 3a. Transfers
        raw_transfers = fetch_transfers_for_holder(session, owner, token_account, mint)
        new_transfers = filter_new_transfers(rdb, raw_transfers)

        # 3b. DeFi activities
        raw_defi, token_meta = fetch_defi_for_holder(session, owner)
        new_defi_list = filter_new_defi(rdb, raw_defi)

        # Token metadata from defi envelope
        if token_meta:
            upsert_tokens_from_metadata(db, token_meta)

        batch_ts = int(time.time())

        # Process transfers
        if new_transfers:
            meta = metadata_map.get(owner, {})
            addr_docs, token_docs, tx_docs, edge_docs = build_graph_from_transfers(new_transfers, owner, meta)

            if addr_docs or token_docs or edge_docs:
                upsert_graph(db, addr_docs, token_docs, edge_docs)
            if tx_docs:
                db.collection('transactions').insert_many(
                    tx_docs, overwrite_mode='update', merge=True, silent=True)

            mark_transfers_processed(rdb, new_transfers, batch_ts)
            cache_edges_redis(rdb, mint, edge_docs)
            new_tx = len(new_transfers)

        # Process defi activities
        if new_defi_list:
            defi_docs = build_defi_docs(new_defi_list, owner)
            if defi_docs:
                try:
                    db.collection('defi_activities').import_bulk(
                        defi_docs, on_duplicate='update', halt_on_error=False)
                except Exception as e:
                    log(TAG, f'defi upsert error: {e}')
                cache_defi_redis(rdb, mint, owner, defi_docs)

            # Create swap edges in edges collection from built defi docs
            swap_addrs, swap_edges = build_swap_edges_from_defi(defi_docs, owner)
            if swap_addrs or swap_edges:
                upsert_graph(db, swap_addrs, [], swap_edges)

            mark_defi_processed(rdb, new_defi_list, batch_ts)
            new_defi = len(new_defi_list)

    except Exception as e:
        log(TAG, f'holder error {owner[:12]}...: {e}')

    with progress_lock:
        progress_state['holders_done'] += 1
        progress_state['total_transfers'] += new_tx
        progress_state['total_edges'] += new_tx  # approximate
        progress_state['total_defi'] += new_defi
        done = progress_state['holders_done']
        total_h = progress_state['total_holders']
        total_t = progress_state['total_transfers']
        total_d = progress_state['total_defi']

    if done % 5 == 0 or done == total_h:
        publish_progress(rdb, connection_id, 'processing',
                         f'{done}/{total_h} holders — {total_t} transfers, {total_d} defi')

    return new_tx, new_defi


# ---------------------------------------------------------------------------
# Main pipeline (v2 structure with v3 defi additions)
# ---------------------------------------------------------------------------

def load_token(rdb, mint, connection_id, threads=20, top_holder_count=200):
    session = requests.Session()
    db = get_arango_db()
    t0 = time.time()
    token_key = f'token:{mint}'

    # Phase 0: TTL
    stored_updated = rdb.hget(token_key, 'updatedAt')
    if stored_updated:
        try:
            age = int(time.time()) - int(stored_updated)
            if age < HASH_TTL:
                log(TAG, f'Hash fresh ({age}s) for {mint[:16]}... skipping')
                publish_progress(rdb, connection_id, 'complete', 'Data ready (cached)',
                                 {'source': 'cache', 'elapsed_ms': round((time.time() - t0) * 1000), 'age_seconds': age})
                return
        except (ValueError, TypeError):
            pass

    # Phase 1: Holders
    publish_progress(rdb, connection_id, 'holders', f'Fetching top {top_holder_count} holders...')
    holders = fetch_top_holders(session, mint, limit=top_holder_count)
    if not holders:
        publish_progress(rdb, connection_id, 'error', 'No holders found for this token')
        return

    current_hash = compute_holder_hash(holders)
    holder_owners = [h.get('owner', '') for h in holders if h.get('owner')]

    publish_progress(rdb, connection_id, 'holders',
                     f'Fetched {len(holders)} holders — checking roster hash...',
                     {'holder_count': len(holders)})
    rdb.set(f'holders:{mint}', json.dumps(holders), ex=600)

    # Phase 2: Hash check
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

    log(TAG, f'Hash {"mismatch" if stored_hash else "new"} for {mint[:16]}...')
    rdb.hset(token_key, mapping={
        'topHolderHash': current_hash, 'holderCount': len(holders),
        'topHolderOwner': holders[0].get('owner', ''),
        'topHolderPct': str(holders[0].get('percentage', 0)),
        'updatedAt': int(time.time()),
    })

    # Phase 3: Metadata
    publish_progress(rdb, connection_id, 'metadata',
                     f'Fetching metadata for {len(holder_owners)} holders...')
    metadata_map = fetch_metadata_batch(session, holder_owners)
    log(TAG, f'Metadata fetched for {len(metadata_map)} owners')

    # Phase 4: Bootstrap defi collection + thread pool
    bootstrap_defi(db)

    publish_progress(rdb, connection_id, 'processing',
                     f'0/{len(holders)} holders — {threads} threads ready...')

    progress_lock = threading.Lock()
    progress_state = {
        'holders_done': 0, 'total_holders': len(holders),
        'total_transfers': 0, 'total_edges': 0, 'total_defi': 0,
    }

    thread_local = threading.local()

    def get_thread_resources():
        if not hasattr(thread_local, 'db'):
            thread_local.db = get_arango_db()
            thread_local.session = requests.Session()
        return thread_local.db, thread_local.session

    def _process(holder):
        t_db, t_session = get_thread_resources()
        return process_single_holder(
            holder, metadata_map, t_db, rdb, t_session, mint,
            connection_id, progress_lock, progress_state)

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = {pool.submit(_process, h): h for h in holders}
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                log(TAG, f'Thread error: {e}')

    # Phase 5: Verify + complete
    with progress_lock:
        final = progress_state.copy()

    if final['total_edges'] > 0:
        publish_progress(rdb, connection_id, 'processing',
                         f'Verifying edges committed...')
        token_ref = f'tokens/{mint}'
        for _ in range(10):
            try:
                count = next(db.aql.execute(
                    'FOR e IN edges FILTER e.token == @token COLLECT WITH COUNT INTO c RETURN c',
                    bind_vars={'token': token_ref}), 0)
                if count > 0:
                    break
            except Exception:
                pass
            time.sleep(0.3)

    elapsed = round((time.time() - t0) * 1000)
    publish_progress(rdb, connection_id, 'complete', 'Processing complete', {
        'source': 'processed', 'elapsed_ms': elapsed, 'hash': current_hash,
        'holders_processed': final['total_holders'],
        'new_transfers': final['total_transfers'],
        'edges_written': final['total_edges'],
        'new_defi': final['total_defi'],
    })
    log(TAG, f'Done: {mint[:16]}... {final["total_holders"]} holders, '
             f'{final["total_transfers"]} transfers, {final["total_defi"]} defi in {elapsed}ms')


# ---------------------------------------------------------------------------
# Stream consumer (v2)
# ---------------------------------------------------------------------------

def ensure_stream_group(rdb):
    try:
        rdb.xgroup_create(STREAM_NAME, GROUP_NAME, id='0', mkstream=True)
    except Exception as e:
        if 'BUSYGROUP' not in str(e):
            raise


def run_stream_consumer(rdb, consumer_name, default_threads):
    ensure_stream_group(rdb)
    bootstrap_arango()
    log(TAG, f'Consumer [{consumer_name}] listening on {STREAM_NAME}...')

    while RUNNING:
        try:
            messages = rdb.xreadgroup(GROUP_NAME, consumer_name,
                                      {STREAM_NAME: '>'}, count=1, block=5000)
            if not messages:
                continue
            for stream, entries in messages:
                for msg_id, data in entries:
                    try:
                        mint = data.get('mint', '')
                        if not mint:
                            rdb.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue
                        connection_id = data.get('connection_id') or f'cli-{uuid.uuid4().hex[:8]}'
                        threads = int(data.get('threads', default_threads))
                        top_holder_count = int(data.get('top_holder_count', 200))

                        log(TAG, f'[{consumer_name}] LoadToken {mint[:16]}... '
                                 f'(conn={connection_id[:12]}, threads={threads})')

                        def process_and_ack(mid=msg_id):
                            try:
                                load_token(rdb, mint, connection_id,
                                           threads=threads, top_holder_count=top_holder_count)
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

    parser = argparse.ArgumentParser(description='Token holder analysis v4 (transfers + DeFi)')
    parser.add_argument('--mint', type=str, help='Process a single token (CLI mode)')
    parser.add_argument('--threads', type=int, default=20, help='Worker threads (default: 20)')
    parser.add_argument('--holders', type=int, default=200, help='Top N holders (default: 200)')
    parser.add_argument('--name', type=str, default=None, help='Consumer name')
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
        bootstrap_arango()
        load_token(rdb, args.mint, f'cli-{uuid.uuid4().hex[:8]}',
                   threads=args.threads, top_holder_count=args.holders)
    else:
        run_stream_consumer(rdb, args.name or f'worker-{uuid.uuid4().hex[:8]}', args.threads)


if __name__ == '__main__':
    main()
