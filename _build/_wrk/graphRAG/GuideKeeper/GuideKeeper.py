#!/usr/bin/env python3
# =============================================================================
# GuideKeeper.py
# T16O - theGuide ArangoDB Graph Persistence Layer
# Replaces MySQL guide-cons-tx.py pipeline worker
#
# Usage:
#   python GuideKeeper.py                        # stream consumer mode
#   python GuideKeeper.py --tx sample_tx.json    # CLI single tx file
#   python GuideKeeper.py --name gk-1            # named consumer instance
# =============================================================================

import argparse
import json
import logging
import os
import signal
import threading
import time
import uuid

import redis as redis_lib
import ujson
from arango import ArangoClient
from arango.exceptions import DocumentInsertError, CollectionNotFoundError
from datetime import datetime

# ── LOGGING ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [GuideKeeper] %(levelname)s %(message)s'
)
log_sys = logging.getLogger('GuideKeeper')

def log(tag, msg):
    print(f'[{datetime.now().strftime("%H:%M:%S")}][{tag}] {msg}', flush=True)


# ── CONFIG ────────────────────────────────────────────────────
REDIS_HOST  = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT  = int(os.getenv('REDIS_PORT', 6379))
STREAM_NAME = 't16o:stream:guide-keeper'
GROUP_NAME  = 't16o:guide-keeper'
RUNNING     = True
TAG         = 'GuideKeeper'

# ── COLLECTIONS ───────────────────────────────────────────────
DOCUMENT_COLLECTIONS = [
    'address',
    'token',
    'program',
    'pool',
    'tx',
    'token_account'
]

EDGE_COLLECTIONS = [
    'transfer',
    'activity',
    'route'
]


# ── CONNECTION ────────────────────────────────────────────────
def get_db(host: str, port: int, username: str, password: str, db_name: str):
    client = ArangoClient(hosts=f'http://{host}:{port}')
    db = client.db(db_name, username=username, password=password)
    log(TAG, f'Connected to ArangoDB [{db_name}]')
    return db


# ── UPSERT HELPER ─────────────────────────────────────────────
def upsert(collection, doc: dict, update_fields: list = None):
    """
    Insert or update a document by _key.
    update_fields: list of fields to update on conflict.
    If None, only last_seen is updated.
    """
    try:
        existing = collection.get(doc['_key'])
        if existing is None:
            collection.insert(doc)
            return 'inserted'
        else:
            update = {'_key': doc['_key']}
            if update_fields:
                for field in update_fields:
                    if field in doc:
                        update[field] = doc[field]
            # always update last_seen if present
            if 'last_seen' in doc:
                update['last_seen'] = doc['last_seen']
            collection.update(update)
            return 'updated'
    except Exception as e:
        log(TAG, f'Upsert failed on {collection.name} _key={doc["_key"]}: {e}')
        raise


# ── NODE BUILDERS ─────────────────────────────────────────────
def build_address(wallet: str, block_time: int) -> dict:
    return {
        '_key':       wallet,
        'address':    wallet,
        'label':      None,
        'risk_score': None,
        'cluster_id': None,
        'first_seen': block_time,
        'last_seen':  block_time,
        'tags':       []
    }


def build_token(token_address: str, metadata: dict, block_time: int) -> dict:
    return {
        '_key':         token_address,
        'token_address': token_address,
        'token_name':   metadata.get('token_name'),
        'token_symbol': metadata.get('token_symbol'),
        'token_icon':   metadata.get('token_icon'),
        'decimals':     metadata.get('decimals'),
        'first_seen':   block_time,
        'last_seen':    block_time
    }


def build_program(program_id: str, name: str, block_time: int) -> dict:
    return {
        '_key':       program_id,
        'program_id': program_id,
        'name':       name,
        'first_seen': block_time,
        'last_seen':  block_time
    }


def build_pool(activity_data: dict, block_time: int) -> dict:
    return {
        '_key':          activity_data['amm_id'],
        'amm_id':        activity_data['amm_id'],
        'amm_authority': activity_data.get('amm_authoriy'),  # source typo preserved
        'program_id':    activity_data.get('program_id'),
        'token_1':       activity_data.get('token_1'),
        'token_2':       activity_data.get('token_2'),
        'first_seen':    block_time,
        'last_seen':     block_time
    }


def build_token_account(account: str, owner: str, token: str, program_id: str, block_time: int) -> dict:
    return {
        '_key':            account,
        'account_address': account,
        'owner_address':   owner,
        'token_address':   token,
        'program_id':      program_id,
        'first_seen':      block_time,
        'last_seen':       block_time
    }


def build_tx(tx_data: dict, initiator: str, compute_unit_limit: int) -> dict:
    return {
        '_key':               tx_data['tx_hash'],
        'tx_hash':            tx_data['tx_hash'],
        'block_id':           tx_data['block_id'],
        'block_time':         tx_data['block_time'],
        'time':               tx_data['time'],
        'fee':                tx_data['fee'],
        'priority_fee':       tx_data['priority_fee'],
        'compute_unit_limit': compute_unit_limit,
        'initiator':          initiator
    }


# ── EDGE BUILDERS ─────────────────────────────────────────────
def build_transfer(t: dict, tx_hash: str, block_time: int) -> dict:
    key = f"{tx_hash}_{t['outer_ins_index']}_{t['ins_index']}"
    return {
        '_key':             key,
        '_from':            f"token_account/{t['source']}",
        '_to':              f"token_account/{t['destination']}",
        'tx_hash':          tx_hash,
        'outer_ins_index':  t['outer_ins_index'],
        'ins_index':        t['ins_index'],
        'source_owner':     t['source_owner'],
        'destination_owner': t['destination_owner'],
        'transfer_type':    t['transfer_type'],
        'token_address':    t['token_address'],
        'decimals':         t['decimals'],
        'amount':           t['amount'],
        'amount_str':       t['amount_str'],
        'program_id':       t['program_id'],
        'outer_program_id': t.get('outer_program_id'),
        'block_time':       block_time,
        'base_value':       t.get('base_value')
    }


def build_activity_agg(a: dict, tx_hash: str, block_time: int) -> dict:
    key = f"{tx_hash}_{a['outer_ins_index']}_{a['ins_index']}"
    d   = a['data']
    return {
        '_key':                key,
        '_from':               f"address/{d['account']}",
        '_to':                 f"tx/{tx_hash}",
        'tx_hash':             tx_hash,
        'outer_ins_index':     a['outer_ins_index'],
        'ins_index':           a['ins_index'],
        'activity_type':       a['activity_type'],
        'name':                a['name'],
        'program_id':          a['program_id'],
        'account':             d['account'],
        'token_in':            d['token_1'],
        'token_out':           d['token_2'],
        'amount_in':           int(d['amount_1']),
        'amount_in_str':       d['amount_1_str'],
        'amount_out':          int(d['amount_2']),
        'amount_out_str':      d['amount_2_str'],
        'exact_amount_out':    int(d.get('exact_amount_2', 0)),
        'exact_amount_out_str': d.get('exact_amount_2_str'),
        'token_decimal_in':    d['token_decimal_1'],
        'token_decimal_out':   d['token_decimal_2'],
        'platform_fee':        d.get('platform_fee', 0),
        'slippage':            d.get('slippage', 0),
        'block_time':          block_time,
        'is_agg':              True
    }


def build_activity_swap(a: dict, tx_hash: str, block_time: int) -> dict:
    key = f"{tx_hash}_{a['outer_ins_index']}_{a['ins_index']}"
    d   = a['data']
    return {
        '_key':              key,
        '_from':             f"tx/{tx_hash}",
        '_to':               f"pool/{d['amm_id']}",
        'tx_hash':           tx_hash,
        'outer_ins_index':   a['outer_ins_index'],
        'ins_index':         a['ins_index'],
        'activity_type':     a['activity_type'],
        'name':              a['name'],
        'program_id':        a['program_id'],
        'amm_id':            d['amm_id'],
        'account':           d['account'],
        'token_in':          d['token_1'],
        'token_out':         d['token_2'],
        'amount_in':         d['amount_1'],
        'amount_in_str':     d['amount_1_str'],
        'amount_out':        d['amount_2'],
        'amount_out_str':    d['amount_2_str'],
        'token_decimal_in':  d['token_decimal_1'],
        'token_decimal_out': d['token_decimal_2'],
        'token_account_in':  d.get('token_account_1_1'),
        'token_account_out': d.get('token_account_2_2'),
        'owner_in':          d.get('owner_1'),
        'owner_out':         d.get('owner_2'),
        'block_time':        block_time,
        'is_agg':            False
    }


def build_route(router: dict, hop_index: int, tx_hash: str, activity_key: str, block_time: int) -> dict:
    key = f"{tx_hash}_-1_2_{hop_index}"
    return {
        '_key':           key,
        '_from':          f"token/{router['token_1']}",
        '_to':            f"token/{router['token_2']}",
        'tx_hash':        tx_hash,
        'activity_key':   activity_key,
        'hop_index':      hop_index,
        'amm_program_id': router['amm_program_id'],
        'amount_in':      int(router['amount_1']),
        'amount_in_str':  router['amount_1_str'],
        'amount_out':     int(router['amount_2']),
        'amount_out_str': router['amount_2_str'],
        'token_decimal_in':  router['token_decimal_1'],
        'token_decimal_out': router['token_decimal_2'],
        'block_time':     block_time
    }


# ── PROCESS TRANSACTION ───────────────────────────────────────
def process_transaction(db, payload: dict):
    """
    Main orchestration function.
    Processes a single transaction payload from the Solscan API
    and upserts all nodes and edges into ArangoDB.
    Insert order respects node-before-edge dependencies.
    """

    # ── COLLECTION HANDLES ────────────────────────────────────
    col_address       = db.collection('address')
    col_token         = db.collection('token')
    col_program       = db.collection('program')
    col_pool          = db.collection('pool')
    col_token_account = db.collection('token_account')
    col_tx            = db.collection('tx')
    col_transfer      = db.collection('transfer')
    col_activity      = db.collection('activity')
    col_route         = db.collection('route')

    # ── EXTRACT TOP LEVEL ─────────────────────────────────────
    tx_data    = payload
    tx_hash    = tx_data['tx_hash']
    block_time = tx_data['block_time']
    transfers  = tx_data.get('transfers', [])
    activities = tx_data.get('activities', [])
    metadata   = payload.get('metadata', {}).get('tokens', {})

    log(TAG, f'Processing tx {tx_hash}')

    # ── STEP 1 — TOKENS ───────────────────────────────────────
    log(TAG, 'Step 1: Tokens')
    for token_address, meta in metadata.items():
        doc    = build_token(token_address, meta, block_time)
        result = upsert(col_token, doc, update_fields=['token_name', 'token_symbol', 'token_icon'])
        log(TAG, f'  token {token_address[:8]}... {result}')

    # sweep transfers for any token addresses not in metadata
    for t in transfers:
        token_address = t.get('token_address')
        if token_address and not col_token.get(token_address):
            doc = build_token(token_address, {}, block_time)
            upsert(col_token, doc)
            log(TAG, f'  token (no meta) {token_address[:8]}... upserted')

    # ── STEP 2 — PROGRAMS ─────────────────────────────────────
    log(TAG, 'Step 2: Programs')
    program_map = {
        'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4': 'JupiterV6',
        'HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq': 'PancakePool',
        'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc':  'OrcaWhirlpool',
        '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8': 'Raydium',
        'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA':  'TokenProgram',
        'ComputeBudget111111111111111111111111111111':    'ComputeBudget',
        '11111111111111111111111111111111':               'SystemProgram'
    }

    seen_programs = set()

    for a in activities:
        pid = a.get('program_id')
        if pid and pid not in seen_programs:
            name   = program_map.get(pid, 'Unknown')
            doc    = build_program(pid, name, block_time)
            result = upsert(col_program, doc, update_fields=['name'])
            seen_programs.add(pid)
            log(TAG, f'  program {name} {result}')

        outer_pid = a.get('outer_program_id')
        if outer_pid and outer_pid not in seen_programs:
            name   = program_map.get(outer_pid, 'Unknown')
            doc    = build_program(outer_pid, name, block_time)
            result = upsert(col_program, doc, update_fields=['name'])
            seen_programs.add(outer_pid)
            log(TAG, f'  program (outer) {name} {result}')

    for t in transfers:
        for pid_field in ['program_id', 'outer_program_id']:
            pid = t.get(pid_field)
            if pid and pid not in seen_programs:
                name   = program_map.get(pid, 'Unknown')
                doc    = build_program(pid, name, block_time)
                result = upsert(col_program, doc, update_fields=['name'])
                seen_programs.add(pid)
                log(TAG, f'  program (transfer) {name} {result}')

    # ── STEP 3 — ADDRESSES ────────────────────────────────────
    log(TAG, 'Step 3: Addresses')
    seen_addresses = set()

    def upsert_address(wallet):
        if wallet and wallet not in seen_addresses:
            doc = build_address(wallet, block_time)
            upsert(col_address, doc)
            seen_addresses.add(wallet)
            log(TAG, f'  address {wallet[:8]}...')

    for t in transfers:
        upsert_address(t.get('source_owner'))
        upsert_address(t.get('destination_owner'))

    for a in activities:
        d = a.get('data', {})
        upsert_address(d.get('account'))
        upsert_address(d.get('owner_1'))
        upsert_address(d.get('owner_2'))

    # ── STEP 4 — POOLS ────────────────────────────────────────
    log(TAG, 'Step 4: Pools')
    seen_pools = set()

    for a in activities:
        if a['activity_type'] == 'ACTIVITY_TOKEN_SWAP':
            d      = a.get('data', {})
            amm_id = d.get('amm_id')
            if amm_id and amm_id not in seen_pools:
                d['program_id'] = a.get('program_id')
                doc    = build_pool(d, block_time)
                result = upsert(col_pool, doc, update_fields=['amm_authority', 'token_1', 'token_2'])
                seen_pools.add(amm_id)
                log(TAG, f'  pool {amm_id[:8]}... {result}')

    # ── STEP 5 — TOKEN ACCOUNTS ───────────────────────────────
    log(TAG, 'Step 5: Token Accounts')
    seen_accounts = set()

    def upsert_token_account(account, owner, token, program_id):
        if account and account not in seen_accounts:
            doc = build_token_account(account, owner, token, program_id, block_time)
            upsert(col_token_account, doc)
            seen_accounts.add(account)
            log(TAG, f'  token_account {account[:8]}...')

    for t in transfers:
        upsert_token_account(
            t.get('source'),
            t.get('source_owner'),
            t.get('token_address'),
            t.get('program_id')
        )
        upsert_token_account(
            t.get('destination'),
            t.get('destination_owner'),
            t.get('token_address'),
            t.get('program_id')
        )

    # ── STEP 6 — TRANSACTION ──────────────────────────────────
    log(TAG, 'Step 6: Transaction')

    initiator          = None
    compute_unit_limit = 0

    for a in activities:
        if a['activity_type'] == 'ACTIVITY_AGG_TOKEN_SWAP':
            initiator = a['data'].get('account')
        if a['activity_type'] == 'ACTIVITY_COMPUTE_UNIT_LIMIT':
            compute_unit_limit = int(a['data'].get('compute_unit_limit', 0))

    # fallback initiator from first transfer source_owner
    if not initiator and transfers:
        initiator = transfers[0].get('source_owner')

    doc    = build_tx(tx_data, initiator, compute_unit_limit)
    result = upsert(col_tx, doc)
    log(TAG, f'  tx {tx_hash[:16]}... {result}')

    # ── STEP 7 — TRANSFERS (edges) ────────────────────────────
    log(TAG, 'Step 7: Transfers')
    for t in transfers:
        # skip native SOL system transfers with no outer program
        if t.get('outer_program_id') is None and t.get('program_id') == '11111111111111111111111111111111':
            log(TAG, f'  skipping native SOL rent transfer ins_index={t["ins_index"]}')
            continue
        doc    = build_transfer(t, tx_hash, block_time)
        result = upsert(col_transfer, doc)
        log(TAG, f'  transfer {doc["_key"]} {result}')

    # ── STEP 8 — ACTIVITIES (edges) ───────────────────────────
    log(TAG, 'Step 8: Activities')
    agg_activity_key = None

    for a in activities:
        if a['activity_type'] == 'ACTIVITY_COMPUTE_UNIT_LIMIT':
            continue  # already captured on tx, no edge needed

        if a['activity_type'] == 'ACTIVITY_AGG_TOKEN_SWAP':
            doc              = build_activity_agg(a, tx_hash, block_time)
            agg_activity_key = doc['_key']
            result           = upsert(col_activity, doc)
            log(TAG, f'  activity agg {doc["_key"]} {result}')

        elif a['activity_type'] == 'ACTIVITY_TOKEN_SWAP':
            doc    = build_activity_swap(a, tx_hash, block_time)
            result = upsert(col_activity, doc)
            log(TAG, f'  activity swap {doc["_key"]} {result}')

    # ── STEP 9 — ROUTES (edges) ───────────────────────────────
    log(TAG, 'Step 9: Routes')
    for a in activities:
        if a['activity_type'] == 'ACTIVITY_AGG_TOKEN_SWAP':
            routers = a['data'].get('routers', [])
            for hop_index, router in enumerate(routers):
                doc    = build_route(router, hop_index, tx_hash, agg_activity_key, block_time)
                result = upsert(col_route, doc)
                log(TAG, f'  route hop {hop_index} {doc["_key"]} {result}')

    log(TAG, f'Transaction {tx_hash[:16]}... complete')


# ── PROGRESS ──────────────────────────────────────────────────
def publish_progress(rdb, connection_id: str, phase: str, message: str = '', data: dict = None):
    if not connection_id:
        return
    payload = {
        'connection_id': connection_id,
        'phase':         phase,
        'message':       message
    }
    if data:
        payload['data'] = data
    rdb.publish(
        f't16o:progress:{connection_id}',
        ujson.dumps(payload)
    )


# ── DEAD LETTER HANDLER ───────────────────────────────────────
def run_dead_letter_check(rdb, db, name: str, min_idle_ms: int = 30000):
    """
    Periodic check for PEL messages idle too long.
    Reclaims and reprocesses up to 3 times then routes to DLQ.
    """
    log(TAG, f'Dead letter monitor active (idle threshold {min_idle_ms}ms)')

    while RUNNING:
        try:
            pending = rdb.xpending_range(
                STREAM_NAME,
                GROUP_NAME,
                min='-',
                max='+',
                count=10,
                idle=min_idle_ms
            )

            for entry in pending:
                mid        = entry['message_id']
                idle_ms    = entry.get('time_since_delivered', 0)
                owner      = entry.get('consumer', 'unknown')
                deliveries = entry.get('times_delivered', 0)

                if deliveries > 3:
                    log(TAG, f'DLQ: msg {mid} failed {deliveries}x from {owner} — routing to DLQ')
                    rdb.xadd('t16o:stream:guide-keeper:dlq', {
                        'original_id': mid,
                        'consumer':    owner,
                        'deliveries':  deliveries,
                        'idle_ms':     idle_ms
                    })
                    rdb.xack(STREAM_NAME, GROUP_NAME, mid)
                    continue

                log(TAG, f'DLQ: reclaiming msg {mid} idle {idle_ms}ms from {owner}')
                claimed = rdb.xclaim(
                    STREAM_NAME,
                    GROUP_NAME,
                    name,
                    min_idle_ms,
                    [mid]
                )

                for _, fields in claimed:
                    raw = fields.get('tx_data', '')
                    if not raw:
                        rdb.xack(STREAM_NAME, GROUP_NAME, mid)
                        continue
                    try:
                        payload      = ujson.loads(raw)
                        transactions = (
                            payload.get('data', [payload])
                            if isinstance(payload, dict)
                            else [payload]
                        )
                        for tx in transactions:
                            process_transaction(db, tx)
                        rdb.xack(STREAM_NAME, GROUP_NAME, mid)
                        log(TAG, f'DLQ: reclaimed msg {mid} processed OK')
                    except Exception as e:
                        log(TAG, f'DLQ: reclaimed msg {mid} failed again: {e}')

        except Exception as e:
            log(TAG, f'Dead letter check error: {e}')

        time.sleep(15)


# ── STREAM CONSUMER ───────────────────────────────────────────
def run_consumer(rdb, db, name: str):
    """
    Redis Streams consumer loop.
    Reads transaction payloads from t16o:stream:guide-keeper
    and persists each to ArangoDB via process_transaction().

    Expected stream message fields:
        tx_data       — JSON string of full Solscan transaction payload
        connection_id — optional, for progress pub/sub
    """
    try:
        rdb.xgroup_create(STREAM_NAME, GROUP_NAME, id='0', mkstream=True)
        log(TAG, f'Consumer group [{GROUP_NAME}] created')
    except Exception:
        pass  # already exists

    log(TAG, f'Consumer [{name}] listening on {STREAM_NAME}')

    while RUNNING:
        try:
            msgs = rdb.xreadgroup(
                GROUP_NAME,
                name,
                {STREAM_NAME: '>'},
                count=1,
                block=5000
            )

            if not msgs:
                continue

            for _, entries in msgs:
                for mid, data in entries:

                    raw = data.get('tx_data', '')
                    cid = data.get('connection_id') or f'gk-{uuid.uuid4().hex[:8]}'

                    if not raw:
                        log(TAG, f'Empty tx_data on msg {mid} — acking and skipping')
                        rdb.xack(STREAM_NAME, GROUP_NAME, mid)
                        continue

                    try:
                        payload = ujson.loads(raw)
                    except Exception as e:
                        log(TAG, f'JSON parse error on msg {mid}: {e}')
                        rdb.xack(STREAM_NAME, GROUP_NAME, mid)
                        continue

                    def _run(message_id=mid, tx_payload=payload, conn_id=cid):
                        try:
                            transactions = (
                                tx_payload.get('data', [tx_payload])
                                if isinstance(tx_payload, dict)
                                else [tx_payload]
                            )

                            publish_progress(rdb, conn_id, 'processing',
                                             f'Processing {len(transactions)} transaction(s)')

                            success_count = 0
                            fail_count    = 0

                            for tx in transactions:
                                tx_hash = tx.get('tx_hash', 'unknown')
                                try:
                                    process_transaction(db, tx)
                                    success_count += 1
                                    log(TAG, f'[{name}] Persisted {tx_hash[:16]}...')
                                except Exception as e:
                                    fail_count += 1
                                    log(TAG, f'[{name}] Failed {tx_hash[:16]}...: {e}')
                                    publish_progress(rdb, conn_id, 'error',
                                                     f'Failed {tx_hash[:16]}: {str(e)}')

                            publish_progress(rdb, conn_id, 'complete',
                                             'GuideKeeper done',
                                             {
                                                 'success':   success_count,
                                                 'failed':    fail_count,
                                                 'stream_id': message_id
                                             })
                        finally:
                            rdb.xack(STREAM_NAME, GROUP_NAME, message_id)

                    threading.Thread(target=_run, daemon=True).start()

        except Exception as e:
            log(TAG, f'Stream error: {e}')
            time.sleep(2)

    log(TAG, f'Consumer [{name}] shutting down')


# ── MAIN ──────────────────────────────────────────────────────
def main():
    global RUNNING

    parser = argparse.ArgumentParser(description='GuideKeeper — T16O ArangoDB Graph Persistence Layer')
    parser.add_argument('--tx',      type=str, default=None, help='Single tx JSON file (CLI mode)')
    parser.add_argument('--name',    type=str, default=None, help='Consumer instance name')
    parser.add_argument('--host',    type=str, default='localhost')
    parser.add_argument('--port',    type=int, default=8529)
    parser.add_argument('--user',    type=str, default='root')
    parser.add_argument('--pass',    type=str, default='',   dest='password')
    parser.add_argument('--db',      type=str, default='t16o_guide')
    args = parser.parse_args()

    # ── SIGNAL HANDLING ───────────────────────────────────────
    signal.signal(signal.SIGINT,  lambda *_: globals().update(RUNNING=False))
    signal.signal(signal.SIGTERM, lambda *_: globals().update(RUNNING=False))

    # ── ARANGO CONNECTION ─────────────────────────────────────
    db = get_db(
        host     = os.getenv('ARANGO_HOST', args.host),
        port     = int(os.getenv('ARANGO_PORT', args.port)),
        username = os.getenv('ARANGO_USER',  args.user),
        password = os.getenv('ARANGO_PASS',  args.password),
        db_name  = os.getenv('ARANGO_DB',    args.db)
    )

    # ── CLI MODE — single tx file ─────────────────────────────
    if args.tx:
        log(TAG, f'CLI mode: processing {args.tx}')
        with open(args.tx, 'r') as f:
            payload = ujson.load(f)
        for tx in payload.get('data', [payload]):
            process_transaction(db, tx)
        log(TAG, 'CLI mode complete')
        return

    # ── STREAM CONSUMER MODE ──────────────────────────────────
    rdb = redis_lib.Redis(
        host             = os.getenv('REDIS_HOST', REDIS_HOST),
        port             = int(os.getenv('REDIS_PORT', REDIS_PORT)),
        decode_responses = True
    )
    rdb.ping()
    log(TAG, 'Redis connected')

    name = args.name or f'gk-{uuid.uuid4().hex[:6]}'

    # dead letter monitor as background thread
    threading.Thread(
        target = run_dead_letter_check,
        args   = (rdb, db, name),
        daemon = True,
        name   = 'dlq-monitor'
    ).start()

    # main consumer loop — blocks until SIGINT/SIGTERM
    run_consumer(rdb, db, name)


if __name__ == '__main__':
    main()
