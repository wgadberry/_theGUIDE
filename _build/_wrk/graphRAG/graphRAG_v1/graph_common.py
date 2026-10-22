"""
graph_common.py -- Shared config, connections, and helpers for graphRAG workers.
"""

import os, sys, json, itertools, time
from datetime import datetime
from arango import ArangoClient
import redis
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_dir = os.path.dirname(os.path.abspath(__file__))
_parent_dir = os.path.dirname(_dir)
_config_path = os.path.join(_parent_dir, 'guide-config.json')

sys.path.insert(0, _parent_dir)

with open(_config_path) as f:
    _cfg = json.load(f)

RPC_URLS = _cfg.get('RPC_URLs', [_cfg.get('RPC_URL', '')])
RPC_API_KEYS = _cfg.get('RPC_API_KEYS', [])
SOLSCAN_API = _cfg.get('SOLSCAN_API')
SOLSCAN_TOKEN = _cfg.get('SOLSCAN_TOKEN')

ARANGO_HOST = 'http://localhost:8529'
ARANGO_DB = 't16o'
ARANGO_USER = 'root'
ARANGO_PASS = 'localdev'

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

REDIS_QUEUE = 't16o:tx:queue'               # Redis list — LPUSH/BRPOP
REDIS_KEY_PREFIX = 't16o:sigs:'             # RedisJSON key prefix

_rpc_counter = itertools.count()


def log(tag, msg):
    print(f'[{datetime.now().strftime("%H:%M:%S")}][{tag}] {msg}', flush=True)


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------

def bootstrap_arango():
    """One-time database, collection, graph, and index setup.

    Call once from main(), not from worker threads.
    """
    client = ArangoClient(hosts=ARANGO_HOST)
    sys_db = client.db('_system', username=ARANGO_USER, password=ARANGO_PASS)

    if not sys_db.has_database(ARANGO_DB):
        sys_db.create_database(ARANGO_DB)

    db = client.db(ARANGO_DB, username=ARANGO_USER, password=ARANGO_PASS)

    for name in ('addresses', 'tokens', 'transactions'):
        if not db.has_collection(name):
            db.create_collection(name)

    if not db.has_collection('edges'):
        db.create_collection('edges', edge=True)

    if not db.has_graph('t16o_graph'):
        db.create_graph('t16o_graph', edge_definitions=[{
            'edge_collection': 'edges',
            'from_vertex_collections': ['addresses'],
            'to_vertex_collections': ['addresses'],
        }])

    edges = db.collection('edges')
    for fields in [['signature'], ['block_time'], ['token', 'block_time'], ['edge_type']]:
        try:
            edges.add_index({'type': 'persistent', 'fields': fields})
        except Exception:
            pass

    try:
        edges.add_index({
            'type': 'persistent',
            'fields': ['signature', '_from', '_to', 'edge_type', 'token'],
            'unique': True,
        })
    except Exception:
        pass

    txs = db.collection('transactions')
    for fields in [['status'], ['origin', 'block_time']]:
        try:
            txs.add_index({'type': 'persistent', 'fields': fields})
        except Exception:
            pass

    return db


def get_arango_db():
    """Lightweight connection for worker threads. No schema/index setup."""
    client = ArangoClient(hosts=ARANGO_HOST)
    return client.db(ARANGO_DB, username=ARANGO_USER, password=ARANGO_PASS)


def connect_redis():
    """Connect to Redis and ensure the stream + consumer group exist."""
    rdb = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        rdb.xgroup_create(STREAM_TX_PROCESS, CONSUMER_GROUP, id='0', mkstream=True)
    except redis.exceptions.ResponseError as e:
        if 'BUSYGROUP' not in str(e):
            raise
    return rdb


# ---------------------------------------------------------------------------
# RPC helpers
# ---------------------------------------------------------------------------

def rpc_call(session, method, params, timeout=30):
    """Make a JSON-RPC call to a round-robin Chainstack endpoint."""
    idx = next(_rpc_counter) % len(RPC_URLS)
    url = RPC_URLS[idx]
    key = RPC_API_KEYS[idx] if idx < len(RPC_API_KEYS) else None
    headers = {'Content-Type': 'application/json'}
    if key:
        headers['Authorization'] = f'Bearer {key}'

    r = session.post(url, json={
        'id': 1, 'jsonrpc': '2.0', 'method': method, 'params': params,
    }, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def fetch_solscan_detail_batch(session, sigs):
    """Fetch transaction details for multiple sigs via Solscan detail/multi.

    Single endpoint — returns balances, metadata (token names/icons),
    programs, signers. Chunks into groups of 50.

    Returns full response dict with data[] and metadata.
    """
    if not sigs:
        return {'data': [], 'metadata': {}}

    all_data = []
    metadata = {}

    for i in range(0, len(sigs), 50):
        chunk = sigs[i:i + 50]
        params = '&'.join(f'tx[]={s}' for s in chunk)
        try:
            r = session.get(f'{SOLSCAN_API}/transaction/detail/multi?{params}',
                            headers={'token': SOLSCAN_TOKEN}, timeout=30)
            r.raise_for_status()
            resp = r.json()
            all_data.extend(resp.get('data') or [])
            for mint, info in (resp.get('metadata', {}).get('tokens', {}) or {}).items():
                if mint not in metadata:
                    metadata[mint] = info
        except Exception:
            pass

    return {'data': all_data, 'metadata': {'tokens': metadata}}


def fetch_solscan_decode_batch(session, sigs):
    """Fetch decoded actions for multiple sigs via Solscan actions/multi.

    Returns dict keyed by signature: {sig: {transfers: [...], activities: [...]}}.
    """
    if not sigs:
        return {}

    results = {}

    for i in range(0, len(sigs), 50):
        chunk = sigs[i:i + 50]
        params = '&'.join(f'tx[]={s}' for s in chunk)
        try:
            r = session.get(f'{SOLSCAN_API}/transaction/actions/multi?{params}',
                            headers={'token': SOLSCAN_TOKEN}, timeout=30)
            r.raise_for_status()
            resp = r.json()
            for item in (resp.get('data') or []):
                sig = item.get('tx_hash', '')
                if sig:
                    results[sig] = item
        except Exception:
            pass

    return results


# ---------------------------------------------------------------------------
# Graph extraction
# ---------------------------------------------------------------------------

def extract_graph(sig, detail, decode, origin):
    """Extract addresses, tokens, and edges from normalized detail + decode.

    Returns (addr_docs, token_docs, edge_docs).
    """
    block_time = detail.get('block_time', 0)

    signers = detail.get('signer', [])
    if isinstance(signers, str):
        signers = [signers]
    fee_payer = signers[0] if signers else origin

    addresses = {}

    def ensure_addr(pubkey, addr_type='unknown'):
        if not pubkey:
            return
        if pubkey not in addresses:
            addresses[pubkey] = {'type': addr_type}
        elif addr_type != 'unknown':
            addresses[pubkey]['type'] = addr_type

    ensure_addr(fee_payer, 'wallet')

    for sbc in (detail.get('sol_bal_change') or []):
        ensure_addr(sbc.get('address'))

    for tbc in (detail.get('token_bal_change') or []):
        ensure_addr(tbc.get('address'), 'ata')
        ensure_addr(tbc.get('owner'), 'wallet')
        ensure_addr(tbc.get('token_address'), 'mint')

    tokens = {}
    for tbc in (detail.get('token_bal_change') or []):
        mint = tbc.get('token_address', '')
        if mint and mint not in tokens:
            tokens[mint] = {'decimals': tbc.get('decimals', 0)}

    edges = []

    if decode:
        for xfer in (decode.get('transfers') or []):
            src = xfer.get('source_owner') or xfer.get('source', '')
            dst = xfer.get('destination_owner') or xfer.get('destination', '')
            token_addr = xfer.get('token_address', '')
            amount = str(xfer.get('amount', '0'))
            decimals = xfer.get('decimals', 0)
            xfer_type = xfer.get('transfer_type', '')
            program = xfer.get('outer_program_id') or xfer.get('program_id', '')

            if not src or not dst:
                continue

            if 'CLOSE' in xfer_type:
                edge_type = 'close_ata'
            elif 'MINT' in xfer_type:
                edge_type = 'mint'
            elif 'BURN' in xfer_type:
                edge_type = 'burn'
            else:
                edge_type = 'spl_transfer'

            ensure_addr(src)
            ensure_addr(dst)
            if token_addr:
                ensure_addr(token_addr, 'mint')
                if token_addr not in tokens:
                    tokens[token_addr] = {'decimals': decimals}
            if program:
                ensure_addr(program, 'program')

            edges.append({
                '_from': f'addresses/{src}',
                '_to': f'addresses/{dst}',
                'edge_type': edge_type,
                'token': f'tokens/{token_addr}' if token_addr else None,
                'amount': amount,
                'decimals': decimals,
                'signature': sig,
                'block_time': block_time,
                'program': f'addresses/{program}' if program else None,
            })

        for act in (decode.get('activities') or []):
            act_type = act.get('activity_type', '')
            if 'SWAP' not in act_type:
                continue

            data = act.get('data', {})
            account = data.get('account', fee_payer)
            amm_id = data.get('amm_id')
            token_1 = data.get('token_1')
            token_2 = data.get('token_2')
            amount_1 = str(data.get('amount_1', '0'))
            amount_2 = str(data.get('amount_2', '0'))
            dec_1 = data.get('token_decimal_1', 0)
            dec_2 = data.get('token_decimal_2', 0)
            program = act.get('program_id', '')

            if not token_1 or not token_2:
                continue

            ensure_addr(account, 'wallet')
            if amm_id:
                ensure_addr(amm_id, 'pool')
            ensure_addr(token_1, 'mint')
            ensure_addr(token_2, 'mint')
            if program:
                ensure_addr(program, 'program')
            if token_1 not in tokens:
                tokens[token_1] = {'decimals': dec_1}
            if token_2 not in tokens:
                tokens[token_2] = {'decimals': dec_2}

            pool_ref = f'addresses/{amm_id}' if amm_id else None
            prog_ref = f'addresses/{program}' if program else None

            swap_to = amm_id or program
            if swap_to:
                edges.append({
                    '_from': f'addresses/{account}',
                    '_to': f'addresses/{swap_to}',
                    'edge_type': 'swap_in',
                    'token': f'tokens/{token_1}',
                    'amount': amount_1,
                    'decimals': dec_1,
                    'signature': sig,
                    'block_time': block_time,
                    'pool': pool_ref,
                    'program': prog_ref,
                })

            swap_from = amm_id or program
            if swap_from:
                edges.append({
                    '_from': f'addresses/{swap_from}',
                    '_to': f'addresses/{account}',
                    'edge_type': 'swap_out',
                    'token': f'tokens/{token_2}',
                    'amount': amount_2,
                    'decimals': dec_2,
                    'signature': sig,
                    'block_time': block_time,
                    'pool': pool_ref,
                    'program': prog_ref,
                })

            # Multi-hop router edges (Jupiter aggregated swaps)
            for router in (data.get('routers') or []):
                r_t1 = router.get('token_1')
                r_t2 = router.get('token_2')
                r_a1 = str(router.get('amount_1', '0'))
                r_a2 = str(router.get('amount_2', '0'))
                r_d1 = router.get('token_decimal_1', 0)
                r_d2 = router.get('token_decimal_2', 0)

                if not r_t1 or not r_t2 or r_t1 == token_1 and r_t2 == token_2:
                    continue  # skip if same as parent swap

                ensure_addr(r_t1, 'mint')
                ensure_addr(r_t2, 'mint')
                if r_t1 not in tokens:
                    tokens[r_t1] = {'decimals': r_d1}
                if r_t2 not in tokens:
                    tokens[r_t2] = {'decimals': r_d2}

                edges.append({
                    '_from': f'addresses/{account}',
                    '_to': f'addresses/{account}',
                    'edge_type': 'swap_hop',
                    'token': f'tokens/{r_t1}',
                    'amount': r_a1,
                    'decimals': r_d1,
                    'signature': sig,
                    'block_time': block_time,
                    'pool': None,
                    'program': prog_ref,
                    'hop_token_out': f'tokens/{r_t2}',
                    'hop_amount_out': r_a2,
                    'hop_decimals_out': r_d2,
                })

    addr_docs = [
        {'_key': pubkey, 'type': info['type'],
         'first_seen': block_time, 'last_seen': block_time}
        for pubkey, info in addresses.items() if pubkey
    ]
    token_docs = [
        {'_key': mint, 'decimals': info['decimals']}
        for mint, info in tokens.items() if mint
    ]

    return addr_docs, token_docs, edges


# ---------------------------------------------------------------------------
# ArangoDB upserts
# ---------------------------------------------------------------------------

def upsert_graph(db, addr_docs, token_docs, edge_docs):
    """Upsert addresses, tokens, and edges into ArangoDB.

    Uses insert_many with overwrite for all collections — no AQL loops.
    Dedupes within the batch in Python, fires all three writes in parallel.
    """
    from concurrent.futures import ThreadPoolExecutor

    def _upsert_addresses():
        if not addr_docs:
            return
        # Dedupe within batch, keep last_seen as max
        seen = {}
        for doc in addr_docs:
            key = doc['_key']
            if key not in seen:
                seen[key] = doc
            else:
                existing = seen[key]
                if doc['type'] != 'unknown':
                    existing['type'] = doc['type']
                existing['first_seen'] = min(existing['first_seen'], doc['first_seen'])
                existing['last_seen'] = max(existing['last_seen'], doc['last_seen'])

        # insert_many with overwrite_mode='update' + merge=True
        # preserves existing fields (first_seen stays, last_seen updates)
        db.collection('addresses').insert_many(
            list(seen.values()), overwrite_mode='update',
            merge=True, silent=True)

    def _upsert_tokens():
        if not token_docs:
            return
        seen = {}
        for doc in token_docs:
            if doc['_key'] not in seen:
                seen[doc['_key']] = doc

        db.collection('tokens').insert_many(
            list(seen.values()), overwrite_mode='update',
            merge=True, silent=True)

    def _insert_edges():
        if not edge_docs:
            return
        # Unique index handles dedup — silent=True skips constraint violations
        db.collection('edges').insert_many(edge_docs, silent=True, overwrite=False)

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [
            pool.submit(_upsert_addresses),
            pool.submit(_upsert_tokens),
            pool.submit(_insert_edges),
        ]
        for f in futures:
            f.result()
