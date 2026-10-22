#!/usr/bin/env python3
"""
get-top-holders-v3.py -- Token holder analysis with DeFi activity enrichment

Extends v2 by adding a per-holder account/defi/activities pull alongside the
existing account/transfer pull, processed in the same thread pool pass.

Pipeline per run:
  1. TTL check        — skip if hash is fresh (< 5 min)
  2. Fetch top 80 holders from token/holders
  3. Hash check       — fast-path if roster unchanged
  4. account/metadata — parallel upfront batch (80 calls)
  5. Thread pool      — per holder, in parallel:
       a. account/transfer   (1 page, size=100) — existing path
       b. account/defi/activities (1 page, size=100) — NEW
       c. Dedup both result sets against Redis tx: keys
       d. Build graph docs from transfers
       e. Build defi_activity docs from defi results
       f. Upsert ArangoDB: addresses + edges (from transfers) + defi_activities
       g. Mark Redis, cache edges
  6. Publish progress throughout

Rate budget (1,000 req/60s):
  - 2   calls : token/holders (2 pages × 40)
  - 80  calls : account/metadata
  - 80  calls : account/transfer
  - 80  calls : account/defi/activities  ← NEW
  ─────────────────────────────────────────────────────
  ~242 calls per full run — still well within limit

Usage:
    python get-top-holders-v3.py                        # stream consumer mode
    python get-top-holders-v3.py --mint <address>       # CLI single token
    python get-top-holders-v3.py --threads 8            # configure thread count
    python get-top-holders-v3.py --name worker-2        # named consumer instance
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

TAG         = 'top-holders-v3'
STREAM_NAME = 't16o:stream:load-token'
GROUP_NAME  = 't16o:top-holders'
HASH_TTL    = 300   # seconds — skip run entirely if hash is fresher than this
RUNNING     = True

_HEADERS = {'token': SOLSCAN_TOKEN}


# ---------------------------------------------------------------------------
# Progress publishing
# ---------------------------------------------------------------------------

def publish_progress(rdb, connection_id, phase, message='', data=None):
    payload = {'connection_id': connection_id, 'phase': phase, 'message': message}
    if data:
        payload['data'] = data
    rdb.publish(f't16o:progress:{connection_id}', json.dumps(payload, default=str))


# ---------------------------------------------------------------------------
# Step 1 — Fetch top holders
# ---------------------------------------------------------------------------

def fetch_top_holders(session, mint, limit=80):
    holders      = []
    pages_needed = -(-limit // 40)

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
        time.sleep(0.06)

    holders.sort(key=lambda h: int(h.get('amount', 0)), reverse=True)
    return holders[:limit]


def compute_holder_hash(holders):
    joined = ''.join(h.get('owner', '') for h in holders)
    return hashlib.sha256(joined.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Step 2 — account/metadata (parallel upfront batch)
# ---------------------------------------------------------------------------

def _fetch_one_metadata(session, owner):
    try:
        r = session.get(
            f'{SOLSCAN_API}/account/metadata',
            params={'address': owner},
            headers=_HEADERS,
            timeout=20,
        )
        r.raise_for_status()
        body = r.json()
        return owner, (body.get('data') or {})
    except Exception as e:
        log(TAG, f'metadata error {owner[:12]}...: {e}')
        return owner, {}


def fetch_metadata_batch(session, owners, max_workers=20):
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {pool.submit(_fetch_one_metadata, session, o): o for o in owners}
        for fut in as_completed(futs):
            owner, meta = fut.result()
            results[owner] = meta
    return results


# ---------------------------------------------------------------------------
# Step 3a — account/transfer (existing path)
# ---------------------------------------------------------------------------

def fetch_transfers_for_holder(session, owner, token_account, mint,
                               num_pages=1, page_size=20):
    results = []
    for page in range(1, num_pages + 1):
        params = {
            'address':       owner,
            'token_account': token_account,
            'token':         mint,
            'page':          page,
            'page_size':     page_size,
            'sort_by':       'block_time',
            'sort_order':    'desc',
        }
        try:
            r = session.get(
                f'{SOLSCAN_API}/account/transfer',
                params=params,
                headers=_HEADERS,
                timeout=30,
            )
            r.raise_for_status()
            body  = r.json()
            items = body.get('data') or []
            results.extend(items)
            if len(items) < page_size:
                break   # last page — no point fetching further
        except Exception as e:
            log(TAG, f'transfer error {owner[:12]}... page={page}: {e}')
            break
        if page < num_pages:
            time.sleep(0.06)
    return results


# ---------------------------------------------------------------------------
# Step 3b — account/defi/activities (NEW)
# ---------------------------------------------------------------------------

def fetch_defi_activities_for_holder(session, owner, num_pages=1, page_size=20):
    """
    Fetch DeFi swap/LP activity for a holder address.

    Returns list of activity dicts. Each item shape (from Solscan):
      {
        "block_id":        int,
        "trans_id":        str,
        "block_time":      int,
        "activity_type":   str,   e.g. "ACTIVITY_TOKEN_SWAP"
        "from_address":    str,
        "sources":         [str], program IDs involved
        "platform":        [str],
        "value":           float, USD value
        "routers": {
          "token1":          str,  mint in
          "token1_decimals": int,
          "amount1":         int,
          "token2":          str,  mint out
          "token2_decimals": int,
          "amount2":         int,
          "pool_address":    str,
          "child_routers":   [...] multi-hop legs
        },
        "time": str  ISO timestamp
      }
    """
    results       = []
    token_metadata = {}   # merged metadata.tokens across all pages

    for page in range(1, num_pages + 1):
        params = {
            'address':    owner,
            'page':       page,
            'page_size':  page_size,
            'sort_by':    'block_time',
            'sort_order': 'desc',
        }
        try:
            r = session.get(
                f'{SOLSCAN_API}/account/defi/activities',
                params=params,
                headers=_HEADERS,
                timeout=30,
            )
            r.raise_for_status()
            body  = r.json()
            items = body.get('data') or []
            results.extend(items)
            # Collect token metadata from outer envelope — free enrichment
            page_tokens = (body.get('metadata') or {}).get('tokens') or {}
            token_metadata.update(page_tokens)
            if len(items) < page_size:
                break   # last page
        except Exception as e:
            log(TAG, f'defi/activities error {owner[:12]}... page={page}: {e}')
            break
        if page < num_pages:
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
    return [t for t, seen in zip(transfers, flags) if not seen]


def filter_new_defi(rdb, activities):
    """Same dedup pattern — keyed by trans_id to avoid double-processing."""
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
# Graph builders
# ---------------------------------------------------------------------------

def build_graph_from_transfers(transfers, owner_meta):
    """Unchanged from v2 — builds address docs + edge docs from transfer list."""
    addr_set  = {}
    edge_docs = []

    for t in transfers:
        for addr in [t.get('from_address', ''), t.get('to_address', '')]:
            if addr and addr not in addr_set:
                meta   = owner_meta.get(addr, {})
                addr_set[addr] = {
                    '_key':          addr,
                    'address':       addr,
                    'account_label': meta.get('account_label', ''),
                    'account_tags':  meta.get('account_tags', []),
                    'active_age':    meta.get('active_age'),
                    'enriched':      1 if meta else 0,
                }

        from_addr  = t.get('from_address', '')
        to_addr    = t.get('to_address',   '')
        trans_id   = t.get('trans_id',     '')
        block_time = t.get('block_time',   0)
        flow       = t.get('flow',         '')
        amount     = t.get('amount',       0)
        value      = t.get('value',        0)
        token      = t.get('token_address', '')

        if from_addr and to_addr and trans_id:
            edge_docs.append({
                '_key':       f'{trans_id}',
                '_from':      f'addresses/{from_addr}',
                '_to':        f'addresses/{to_addr}',
                'trans_id':   trans_id,
                'block_time': block_time,
                'flow':       flow,
                'amount':     amount,
                'value':      value,
                'token':      token,
                'edge_type':  'transfer',
            })

    return list(addr_set.values()), edge_docs


def build_defi_activity_docs(activities, owner):
    """
    Build ArangoDB defi_activities collection documents from raw
    account/defi/activities response items.

    Collection: defi_activities
    _key: trans_id  (unique per tx — avoids duplication across holder runs)

    Captures:
      - top-level swap metadata
      - flattened router legs (token_in / token_out / amount / pool)
      - child_routers for multi-hop paths
    """
    docs = []
    for a in activities:
        trans_id   = a.get('trans_id', '')
        if not trans_id:
            continue

        routers       = a.get('routers') or {}
        child_routers = routers.get('child_routers') or []

        # Flatten router legs into a clean list for easy querying
        legs = []
        if routers.get('token1') and routers.get('token2'):
            legs.append({
                'token_in':       routers.get('token1'),
                'decimals_in':    routers.get('token1_decimals'),
                'amount_in':      routers.get('amount1'),
                'token_out':      routers.get('token2'),
                'decimals_out':   routers.get('token2_decimals'),
                'amount_out':     routers.get('amount2'),
                'pool_address':   routers.get('pool_address', ''),
                'program_address': routers.get('program_address', ''),
                'leg_index':      0,
            })
        for i, leg in enumerate(child_routers, start=1):
            legs.append({
                'token_in':       leg.get('token1'),
                'decimals_in':    leg.get('token1_decimals'),
                'amount_in':      leg.get('amount1'),
                'token_out':      leg.get('token2'),
                'decimals_out':   leg.get('token2_decimals'),
                'amount_out':     leg.get('amount2'),
                'pool_address':   leg.get('pool_address', ''),
                'program_address': leg.get('program_address', ''),
                'leg_index':      i,
            })

        docs.append({
            '_key':          trans_id,
            'trans_id':      trans_id,
            'block_id':      a.get('block_id'),
            'block_time':    a.get('block_time'),
            'time':          a.get('time'),
            'activity_type': a.get('activity_type'),
            'from_address':  a.get('from_address', owner),
            'owner':         owner,
            'sources':       a.get('sources', []),
            'platform':      a.get('platform', []),
            'value_usd':     a.get('value', 0),
            'token_in':      routers.get('token1', ''),
            'token_out':     routers.get('token2', ''),
            'amount_in':     routers.get('amount1'),
            'amount_out':    routers.get('amount2'),
            'pool_address':  routers.get('pool_address', ''),
            'legs':          legs,
            'leg_count':     len(legs),
        })

    return docs


def upsert_tokens_from_metadata(db, token_metadata):
    """
    Upsert token docs into the 'tokens' collection from metadata.tokens
    returned by account/defi/activities.  Only creates if not present —
    existing docs are left untouched (on_duplicate='ignore').

    token_metadata shape (keyed by mint address):
      {
        "So111...": {
          "token_address": "So111...",
          "token_name":    "Wrapped SOL",
          "token_symbol":  "WSOL",
          "token_icon":    "https://..."
        }, ...
      }
    """
    if not token_metadata:
        return
    docs = []
    for mint_addr, info in token_metadata.items():
        if not mint_addr:
            continue
        docs.append({
            '_key':         mint_addr,
            'address':      mint_addr,
            'token_name':   info.get('token_name',   ''),
            'token_symbol': info.get('token_symbol', ''),
            'token_icon':   info.get('token_icon',   ''),
        })
    if not docs:
        return
    try:
        col = db.collection('tokens')
        col.import_bulk(docs, on_duplicate='ignore', halt_on_error=False)
        log(TAG, f'token upsert: {len(docs)} tokens processed')
    except Exception as e:
        log(TAG, f'token upsert error: {e}')


def upsert_defi_activities(db, defi_docs):
    """Upsert defi_activity docs into ArangoDB defi_activities collection."""
    if not defi_docs:
        return
    try:
        col = db.collection('defi_activities')
        col.import_bulk(defi_docs, on_duplicate='update', halt_on_error=False)
    except Exception as e:
        log(TAG, f'defi upsert error: {e}')


# ---------------------------------------------------------------------------
# Redis caching
# ---------------------------------------------------------------------------

def cache_edges_redis(rdb, mint, edge_docs):
    if not edge_docs:
        return
    key  = f't16o:edges:{mint}'
    pipe = rdb.pipeline(transaction=False)
    for e in edge_docs:
        score = e.get('block_time') or 0
        pipe.zadd(key, {e['_key']: score})
    pipe.execute()
    rdb.expire(key, 86400 * 7)


def cache_defi_redis(rdb, mint, owner, defi_docs):
    """
    Cache defi activity keys in a per-mint sorted set scored by block_time.
    Also maintains a per-owner defi key set for fast holder lookups.
    """
    if not defi_docs:
        return
    mint_key  = f't16o:defi:{mint}'
    owner_key = f't16o:defi:owner:{owner}'
    pipe      = rdb.pipeline(transaction=False)
    for d in defi_docs:
        score = d.get('block_time') or 0
        pipe.zadd(mint_key,  {d['_key']: score})
        pipe.zadd(owner_key, {d['_key']: score})
    pipe.execute()
    rdb.expire(mint_key,  86400 * 7)
    rdb.expire(owner_key, 86400 * 7)


# ---------------------------------------------------------------------------
# Per-holder worker (called from thread pool)
# ---------------------------------------------------------------------------

def process_single_holder(holder, metadata_map, db, rdb, session, mint,
                           connection_id, progress_lock, progress_state,
                           num_pages=1, page_size=20):
    """
    Full pipeline for one holder:
      fetch transfers + defi activities → dedup → build docs →
      upsert ArangoDB → mark Redis → cache → update progress
    """
    owner         = holder.get('owner', '')
    token_account = holder.get('address', '')
    new_tx_count  = 0
    new_defi_count = 0

    try:
        # ── 3a. Transfers ──────────────────────────────────────────────────
        raw_transfers  = fetch_transfers_for_holder(session, owner, token_account, mint,
                                                       num_pages=num_pages, page_size=page_size)
        new_transfers  = filter_new_transfers(rdb, raw_transfers)

        # ── 3b. DeFi activities ────────────────────────────────────────────
        raw_defi, token_metadata = fetch_defi_activities_for_holder(session, owner,
                                                                     num_pages=num_pages, page_size=page_size)
        new_defi  = filter_new_defi(rdb, raw_defi)

        # Upsert any new tokens found in the metadata envelope
        if token_metadata:
            upsert_tokens_from_metadata(db, token_metadata)

        batch_ts = int(time.time())

        # ── Build + upsert transfer graph ─────────────────────────────────
        if new_transfers:
            owner_meta           = {owner: metadata_map.get(owner, {})}
            addr_docs, edge_docs = build_graph_from_transfers(new_transfers, owner_meta)

            meta = metadata_map.get(owner, {})
            for doc in addr_docs:
                if doc['address'] == owner and meta:
                    doc['account_label'] = meta.get('account_label', '')
                    doc['account_tags']  = meta.get('account_tags', [])
                    doc['active_age']    = meta.get('active_age')
                    funded = meta.get('funded_by') or {}
                    if funded:
                        doc['funded_by']      = funded.get('funded_by', '')
                        doc['funded_by_tx']   = funded.get('tx_hash', '')
                        doc['funded_by_time'] = funded.get('block_time')

            if addr_docs or edge_docs:
                upsert_graph(db, addr_docs, [], edge_docs)

            mark_transfers_processed(rdb, new_transfers, batch_ts)
            cache_edges_redis(rdb, mint, edge_docs)
            new_tx_count = len(new_transfers)

        # ── Build + upsert defi activity docs ─────────────────────────────
        if new_defi:
            defi_docs = build_defi_activity_docs(new_defi, owner)
            if defi_docs:
                upsert_defi_activities(db, defi_docs)
                cache_defi_redis(rdb, mint, owner, defi_docs)
            mark_defi_processed(rdb, new_defi, batch_ts)
            new_defi_count = len(new_defi)

    except Exception as e:
        log(TAG, f'holder error {owner[:12]}...: {e}')

    # ── Progress ───────────────────────────────────────────────────────────
    with progress_lock:
        progress_state['done'] += 1
        done  = progress_state['done']
        total = progress_state['total']

    publish_progress(
        rdb, connection_id, 'holder_done',
        f'Processed {done}/{total}: {owner[:12]}... '
        f'(+{new_tx_count} transfers, +{new_defi_count} defi)',
        {
            'owner':        owner,
            'done':         done,
            'total':        total,
            'new_transfers': new_tx_count,
            'new_defi':     new_defi_count,
        },
    )

    return new_tx_count, new_defi_count


# ---------------------------------------------------------------------------
# Bootstrap — ensure defi_activities collection exists
# ---------------------------------------------------------------------------

def bootstrap_defi_collection(db):
    """Create defi_activities + tokens collections and indexes if not present."""
    try:
        # defi_activities
        if not db.has_collection('defi_activities'):
            db.create_collection('defi_activities')
            log(TAG, 'Created defi_activities collection')
        col = db.collection('defi_activities')
        existing = {idx['fields'][0] for idx in col.indexes() if idx.get('fields')}
        for field in ['owner', 'block_time', 'token_in', 'token_out', 'pool_address']:
            if field not in existing:
                col.add_persistent_index(fields=[field], sparse=True)

        # tokens — create if graph_common hasn't already
        if not db.has_collection('tokens'):
            db.create_collection('tokens')
            log(TAG, 'Created tokens collection')
        tok = db.collection('tokens')
        tok_existing = {idx['fields'][0] for idx in tok.indexes() if idx.get('fields')}
        for field in ['token_symbol', 'token_name']:
            if field not in tok_existing:
                tok.add_persistent_index(fields=[field], sparse=True)
    except Exception as e:
        log(TAG, f'bootstrap_defi error: {e}')


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------

def run_for_mint(mint, connection_id, rdb, session, thread_count=20,
                 holder_limit=200, num_pages=5, page_size=100):
    t0        = time.time()
    token_key = f'token:{mint}'

    # ── Phase 0: TTL check ─────────────────────────────────────────────────
    last_run = rdb.hget(token_key, 'updatedAt')
    if last_run:
        age = time.time() - float(last_run)
        if age < HASH_TTL:
            log(TAG, f'TTL fresh ({age:.0f}s) for {mint[:16]}... skipping')
            publish_progress(rdb, connection_id, 'complete', 'Data ready (cached)',
                             {'source': 'cache', 'elapsed_ms': 0, 'age_seconds': age})
            return

    # ── Phase 1: Fetch holders ─────────────────────────────────────────────
    publish_progress(rdb, connection_id, 'fetching_holders', 'Fetching top holders...')
    holders = fetch_top_holders(session, mint, limit=holder_limit)
    if not holders:
        log(TAG, f'No holders found for {mint}')
        publish_progress(rdb, connection_id, 'error', 'No holders found')
        return

    holder_owners = [h.get('owner', '') for h in holders if h.get('owner')]
    current_hash  = compute_holder_hash(holders)

    publish_progress(rdb, connection_id, 'holders_fetched',
                     f'Fetched {len(holders)} holders',
                     {'count': len(holders), 'hash': current_hash})

    rdb.set(f'holders:{mint}', json.dumps(holders), ex=600)

    # ── Phase 2: Hash check ────────────────────────────────────────────────
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
                         {'source': 'cache', 'elapsed_ms': elapsed})
        return

    rdb.hset(token_key, mapping={
        'topHolderHash':  current_hash,
        'holderCount':    len(holders),
        'topHolderOwner': holders[0].get('owner', ''),
        'topHolderPct':   str(holders[0].get('percentage', 0)),
        'updatedAt':      int(time.time()),
    })

    # ── Phase 3: Metadata batch ────────────────────────────────────────────
    publish_progress(rdb, connection_id, 'metadata',
                     f'Fetching metadata for {len(holder_owners)} holders...')
    metadata_map = fetch_metadata_batch(session, holder_owners)
    publish_progress(rdb, connection_id, 'metadata_done',
                     f'Metadata fetched for {len(metadata_map)} holders')

    # ── Phase 4: Bootstrap DB + thread pool ───────────────────────────────
    db = get_arango_db()
    bootstrap_arango()
    bootstrap_defi_collection(db)

    progress_lock  = threading.Lock()
    progress_state = {'done': 0, 'total': len(holders)}

    publish_progress(rdb, connection_id, 'processing',
                     f'Processing {len(holders)} holders (transfers + defi)...',
                     {'total': len(holders)})

    total_tx   = 0
    total_defi = 0

    with ThreadPoolExecutor(max_workers=thread_count) as pool:
        futs = {
            pool.submit(
                process_single_holder,
                holder, metadata_map, db, rdb, session, mint,
                connection_id, progress_lock, progress_state,
                num_pages, page_size
            ): holder
            for holder in holders
        }
        for fut in as_completed(futs):
            try:
                tx_c, defi_c = fut.result()
                total_tx   += tx_c
                total_defi += defi_c
            except Exception as e:
                log(TAG, f'thread error: {e}')

    elapsed = round((time.time() - t0) * 1000)
    log(TAG, f'Done {mint[:16]}... {total_tx} new transfers, {total_defi} new defi in {elapsed}ms')
    publish_progress(rdb, connection_id, 'complete', 'Processing complete', {
        'source':        'processed',
        'elapsed_ms':    elapsed,
        'new_transfers': total_tx,
        'new_defi':      total_defi,
        'holders':       len(holders),
    })


# ---------------------------------------------------------------------------
# Stream consumer
# ---------------------------------------------------------------------------

def stream_consumer(name, thread_count, holder_limit, num_pages, page_size):
    rdb     = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    session = requests.Session()

    try:
        rdb.xgroup_create(STREAM_NAME, GROUP_NAME, id='0', mkstream=True)
    except redis_lib.exceptions.ResponseError:
        pass  # group already exists

    bootstrap_arango()
    log(TAG, f'Consumer {name} listening on {STREAM_NAME}')

    while RUNNING:
        try:
            msgs = rdb.xreadgroup(GROUP_NAME, name, {STREAM_NAME: '>'}, count=1, block=5000)
            if not msgs:
                continue
            for _, entries in msgs:
                for msg_id, fields in entries:
                    mint          = fields.get('mint', '')
                    connection_id = fields.get('connection_id') or f'cli-{uuid.uuid4().hex[:8]}'
                    threads       = int(fields.get('threads', thread_count))
                    if mint:
                        log(TAG, f'[{name}] LoadToken {mint[:16]}... (conn={connection_id[:12]}, threads={threads})')
                        try:
                            run_for_mint(mint, connection_id, rdb, session,
                                         thread_count=threads,
                                         holder_limit=holder_limit,
                                         num_pages=num_pages,
                                         page_size=page_size)
                        except Exception as e:
                            log(TAG, f'run_for_mint error: {e}')
                    rdb.xack(STREAM_NAME, GROUP_NAME, msg_id)
        except Exception as e:
            log(TAG, f'stream error: {e}')
            time.sleep(2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='get-top-holders v3')
    parser.add_argument('--mint',         help='Token mint address (CLI mode)')
    parser.add_argument('--threads',      type=int, default=20,  help='Thread pool size (default 20)')
    parser.add_argument('--holder-limit', type=int, default=200, help='Max holders to fetch (default 200)')
    parser.add_argument('--num-pages',    type=int, default=5,   help='Pages to fetch per holder per endpoint (default 5)')
    parser.add_argument('--page-size',    type=int, default=100, help='Records per page per holder endpoint (default 100)')
    parser.add_argument('--name',         default=f'worker-{uuid.uuid4().hex[:6]}',
                        help='Consumer name')
    args = parser.parse_args()

    if args.mint:
        rdb     = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        session = requests.Session()
        cid     = str(uuid.uuid4())
        run_for_mint(args.mint, cid, rdb, session,
                     thread_count=args.threads,
                     holder_limit=args.holder_limit,
                     num_pages=args.num_pages,
                     page_size=args.page_size)
    else:
        stream_consumer(args.name, args.threads, args.holder_limit, args.num_pages, args.page_size)


if __name__ == '__main__':
    main()
