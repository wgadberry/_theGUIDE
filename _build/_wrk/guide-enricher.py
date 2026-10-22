#!/usr/bin/env python3
"""
Guide Enricher - Queue Consumer (multi-threaded, config-driven)

Supervisor thread polls config for desired thread/prefetch counts.
Worker threads each own their own DB + RabbitMQ + API connections.

Enriches:
- tx_token: name, symbol, icon, decimals (Phase 0: local backfill, Phase 1: Solscan API)
- tx_pool: program, tokens, token accounts, LP token, label (Phase 0-2)
- tx_program: name, program_type (known programs + Solscan API)

Config keys (config_type='queue'):
    enricher_wrk_cnt_threads           - desired worker thread count (0 = idle)
    enricher_wrk_cnt_prefetch          - RabbitMQ prefetch per worker channel
    enricher_wrk_supervisor_poll_sec   - supervisor config poll interval
    enricher_wrk_poll_idle_sec         - worker sleep when no message available
    enricher_wrk_reconnect_sec         - delay before reconnecting after errors
    enricher_wrk_shutdown_timeout_sec  - max wait for worker thread on shutdown
    enricher_wrk_api_timeout_sec       - Solscan API request timeout
    enricher_wrk_api_delay_sec         - delay between API calls
    enricher_wrk_batch_limit           - max items per enrichment pass
    enricher_wrk_max_attempts          - skip items with more than N failed attempts
    enricher_wrk_db_poll_sec           - supervisor DB poll interval (0 = disabled)

Manual modes (run directly, not as service):
    python guide-enricher.py --enrich tokens
    python guide-enricher.py --enrich pools
    python guide-enricher.py --enrich programs
    python guide-enricher.py --enrich all
    python guide-enricher.py --pool <address>
    python guide-enricher.py --status
    python guide-enricher.py --dry-run
"""

import argparse
import hashlib
import json
import os
import sys
import time
import threading
import uuid
import requests
import pika
import mysql.connector
from mysql.connector import Error as MySQLError
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any


# =============================================================================
# Static config (from common.config → guide-config.json)
# =============================================================================

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from t16o_exchange.guide.common.config import (
    get_db_config, get_rabbitmq_config, get_solscan_config,
    get_queue_names, get_retry_config, nack_with_retry,
    load_db_config, cfg_int, cfg_float,
)

_solscan            = get_solscan_config()
_rmq                = get_rabbitmq_config()
_queues             = get_queue_names('enricher')
_producer_queues    = get_queue_names('producer')
_decoder_queues     = get_queue_names('decoder')
_detailer_queues    = get_queue_names('detailer')
_retry              = get_retry_config()

SOLSCAN_API_BASE    = _solscan['api_base']
SOLSCAN_API_TOKEN   = _solscan['token']
RABBITMQ_HOST       = _rmq['host']
RABBITMQ_PORT       = _rmq['port']
RABBITMQ_USER       = _rmq['user']
RABBITMQ_PASS       = _rmq['password']
RABBITMQ_VHOST      = _rmq['vhost']
RABBITMQ_HEARTBEAT  = _rmq['heartbeat']
RABBITMQ_BLOCKED_TIMEOUT = _rmq['blocked_timeout']
DB_FALLBACK_RETRY_SEC = _retry['db_fallback_retry_sec']
REQUEST_QUEUE       = _queues['request']
RESPONSE_QUEUE      = _queues['response']
DLQ_QUEUE           = _queues['dlq']
DB_CONFIG           = get_db_config()
PRODUCER_REQUEST_QUEUE = _producer_queues['request']
DECODER_REQUEST_QUEUE  = _decoder_queues['request']
DETAILER_REQUEST_QUEUE = _detailer_queues['request']
PRIME_BATCH_SIZE       = 20

MULTI_META_BATCH_SIZE = 50

# Known token symbols → legitimate mint addresses. Any token claiming these
# symbols from a different mint is flagged as a scam.
KNOWN_TOKEN_MINTS = {
    'SOL':  {'So11111111111111111111111111111111111111111', 'So11111111111111111111111111111111111111112'},
    'WSOL': {'So11111111111111111111111111111111111111111', 'So11111111111111111111111111111111111111112'},
    'USDC': {'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'},
    'USDT': {'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'},
}


# =============================================================================
# Helpers
# =============================================================================

def log(tag, msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}][{tag}] {msg}", flush=True)


# get_config_int / get_config_float removed — use load_db_config + cfg_int/cfg_float from common.config


def db_connect():
    return mysql.connector.connect(**DB_CONFIG)


def rmq_connect():
    creds = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, virtual_host=RABBITMQ_VHOST,
        credentials=creds,
        heartbeat=RABBITMQ_HEARTBEAT,
        blocked_connection_timeout=RABBITMQ_BLOCKED_TIMEOUT,
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=DLQ_QUEUE, durable=True,
                     arguments={'x-max-priority': 10, 'x-message-ttl': 86400000})
    ch.queue_declare(queue=REQUEST_QUEUE, durable=True,
                     arguments={'x-max-priority': 10,
                                'x-dead-letter-exchange': '',
                                'x-dead-letter-routing-key': DLQ_QUEUE})
    ch.queue_declare(queue=RESPONSE_QUEUE, durable=True,
                     arguments={'x-max-priority': 10})
    return conn, ch


# =============================================================================
# Request logging (billing)
# =============================================================================

def log_worker_request(cursor, conn, request_id, correlation_id,
                       batch_size, priority, api_key_id):
    if api_key_id is not None:
        cursor.execute(
            "SELECT id FROM tx_request_log "
            "WHERE request_id=%s AND target_worker='enricher' AND api_key_id=%s",
            (request_id, api_key_id))
    else:
        cursor.execute(
            "SELECT id FROM tx_request_log "
            "WHERE request_id=%s AND target_worker='enricher' AND api_key_id IS NULL",
            (request_id,))
    row = cursor.fetchone()
    if row:
        return row['id']

    cursor.execute(
        "INSERT INTO tx_request_log "
        "(request_id, correlation_id, api_key_id, source, target_worker, "
        " action, priority, status, payload_summary) "
        "VALUES (%s,%s,%s,'queue','enricher','enrich',%s,'processing',%s)",
        (request_id, correlation_id, api_key_id, priority,
         json.dumps({'batch_size': batch_size, 'source': 'queue'})))
    conn.commit()
    return cursor.lastrowid


def update_worker_request(cursor, conn, log_id, status, result=None):
    cursor.execute(
        "UPDATE tx_request_log SET status=%s, result_summary=%s, completed_at=NOW() WHERE id=%s",
        (status, json.dumps(result) if result else None, log_id))
    conn.commit()


def log_daemon_request(cursor, conn, action, batch_size):
    request_id = f"daemon-enricher-{uuid.uuid4().hex[:12]}"
    cursor.execute(
        "INSERT INTO tx_request_log "
        "(request_id, correlation_id, api_key_id, source, target_worker, "
        " action, priority, status, payload_summary) "
        "VALUES (%s,%s,NULL,'daemon','enricher',%s,5,'processing',%s)",
        (request_id, request_id, action,
         json.dumps({'batch_size': batch_size})))
    conn.commit()
    log_id = cursor.lastrowid
    log('DAEMON', f"request_log id={log_id}")
    return log_id


# =============================================================================
# Solscan API
# =============================================================================

def solscan_session():
    s = requests.Session()
    s.headers['token'] = SOLSCAN_API_TOKEN
    return s


def fetch_token_meta_multi(session, mint_addresses, api_timeout):
    url = f"{SOLSCAN_API_BASE}/token/meta/multi"
    params = [('address[]', addr) for addr in mint_addresses]
    try:
        r = session.get(url, params=params, timeout=api_timeout)
        r.raise_for_status()
        data = r.json()
        if data.get('success') and data.get('data'):
            return {item['address']: item for item in data['data'] if 'address' in item}
        return {}
    except Exception as e:
        log('API', f"token/meta/multi error: {e}")
        return {}


def fetch_account_metadata_multi(session, addresses, api_timeout):
    url = f"{SOLSCAN_API_BASE}/account/metadata/multi"
    params = [('address[]', addr) for addr in addresses]
    try:
        r = session.get(url, params=params, timeout=api_timeout)
        r.raise_for_status()
        data = r.json()
        if data.get('success') and data.get('data'):
            return {item['account_address']: item for item in data['data'] if 'account_address' in item}
        return {}
    except Exception as e:
        log('API', f"account/metadata/multi error: {e}")
        return {}


def fetch_account_metadata(session, address, api_timeout):
    url = f"{SOLSCAN_API_BASE}/account/metadata"
    try:
        r = session.get(url, params={'address': address}, timeout=api_timeout)
        r.raise_for_status()
        data = r.json()
        if data.get('success') and data.get('data'):
            return data['data']
    except Exception as e:
        log('API', f"account/metadata error for {address[:12]}...: {e}")
    return None


def fetch_pool_info(session, pool_address, api_timeout):
    url = f"{SOLSCAN_API_BASE}/market/info"
    try:
        r = session.get(url, params={'address': pool_address}, timeout=api_timeout)
        r.raise_for_status()
        data = r.json()
        if data.get('success') and data.get('data'):
            return data['data']
    except Exception as e:
        log('API', f"market/info error for {pool_address[:12]}...: {e}")
    return None


# =============================================================================
# Database helpers (shared by enrichment functions)
# =============================================================================

def ensure_address(cursor, conn, address, addr_type='unknown'):
    if not address:
        return None
    cursor.execute("SELECT id FROM tx_address WHERE address = %s", (address,))
    row = cursor.fetchone()
    if row:
        return row['id']
    cursor.execute(
        "INSERT INTO tx_address (address, address_type) VALUES (%s, %s)",
        (address, addr_type))
    conn.commit()
    return cursor.lastrowid


def ensure_program(cursor, conn, program_address):
    if not program_address:
        return None, None
    addr_id = ensure_address(cursor, conn, program_address, 'program')
    cursor.execute("SELECT id FROM tx_program WHERE program_address_id = %s", (addr_id,))
    row = cursor.fetchone()
    if row:
        return row['id'], addr_id
    cursor.execute(
        "INSERT INTO tx_program (program_address_id, program_type) VALUES (%s, 'dex')",
        (addr_id,))
    conn.commit()
    return cursor.lastrowid, addr_id


def ensure_token(cursor, conn, mint_address):
    if not mint_address:
        return None
    cursor.execute("SELECT id, address_type FROM tx_address WHERE address = %s", (mint_address,))
    row = cursor.fetchone()
    if row:
        addr_id = row['id']
        addr_type = row['address_type']
        if addr_type in ('program', 'pool', 'ata'):
            return None
        if addr_type in ('unknown', 'wallet'):
            cursor.execute("UPDATE tx_address SET address_type = 'mint' WHERE id = %s", (addr_id,))
            conn.commit()
    else:
        cursor.execute(
            "INSERT INTO tx_address (address, address_type) VALUES (%s, 'mint')",
            (mint_address,))
        conn.commit()
        addr_id = cursor.lastrowid

    cursor.execute("SELECT id FROM tx_token WHERE mint_address_id = %s", (addr_id,))
    row = cursor.fetchone()
    if row:
        return row['id']
    cursor.execute("INSERT INTO tx_token (mint_address_id) VALUES (%s)", (addr_id,))
    conn.commit()
    return cursor.lastrowid


# =============================================================================
# Auto-Prime: load originating tx for unprimed tokens via Solscan token/meta
# =============================================================================

def compute_sig_hash(signatures: list) -> str:
    sorted_sigs = '|'.join(sorted(signatures))
    return hashlib.sha256(sorted_sigs.encode()).hexdigest()


def publish_cascade_to_workers(ch, signatures, request_id=None, correlation_id=None):
    """Cascade signatures directly to decoder + detailer queues.
    Returns True on success."""
    if not signatures:
        return False
    if not request_id:
        request_id = f"enricher-prime-{uuid.uuid4().hex[:12]}"
    if not correlation_id:
        correlation_id = request_id

    msg = json.dumps({
        'request_id': request_id,
        'correlation_id': correlation_id,
        'action': 'cascade',
        'source_worker': 'enricher-prime',
        'tx_origin': 3,
        'priority': 1,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'batch': {
            'signatures': signatures,
            'batch_num': 1,
            'total_batches': 1,
        },
    })
    body = msg.encode('utf-8')
    props = pika.BasicProperties(delivery_mode=2, content_type='application/json', priority=1)
    try:
        ch.basic_publish(exchange='', routing_key=DECODER_REQUEST_QUEUE, body=body, properties=props)
        ch.basic_publish(exchange='', routing_key=DETAILER_REQUEST_QUEUE, body=body, properties=props)
        return True
    except Exception:
        return False


def prime_unprimed_tokens(tag, session, cursor, conn, ch, api_timeout):
    """Find unprimed tokens, fetch create_tx from Solscan, cascade to decoder+detailer.
    Returns count of tokens primed. Never raises."""
    try:
        cursor.execute("""
            SELECT t.id AS token_id, a.address AS mint_address, t.token_json
            FROM tx_token t
            JOIN tx_address a ON a.id = t.mint_address_id
            WHERE t.primed = 0
            ORDER BY t.id
            LIMIT %s
        """, (PRIME_BATCH_SIZE,))
        rows = cursor.fetchall()
    except Exception as e:
        log(tag, f"[prime] Query error: {e}")
        return 0

    if not rows:
        return 0

    log(tag, f"[prime] {len(rows)} unprimed tokens")

    # Split into tokens that already have token_json (with create_tx) vs need API call
    need_api = []
    have_sig = []
    for row in rows:
        token_json = row.get('token_json')
        if token_json:
            try:
                meta = json.loads(token_json) if isinstance(token_json, str) else token_json
                sig = meta.get('create_tx') or meta.get('first_mint_tx')
                if sig:
                    have_sig.append((row['token_id'], row['mint_address'], sig))
                    continue
            except (json.JSONDecodeError, TypeError):
                pass
        need_api.append(row)

    # Fetch metadata for tokens without cached create_tx
    if need_api:
        mint_addresses = [r['mint_address'] for r in need_api]
        mint_to_row = {r['mint_address']: r for r in need_api}
        log(tag, f"[prime] Calling Solscan token/meta/multi for {len(mint_addresses)} tokens")

        results = fetch_token_meta_multi(session, mint_addresses, api_timeout)

        for mint, meta in results.items():
            sig = meta.get('create_tx') or meta.get('first_mint_tx')
            if sig and mint in mint_to_row:
                have_sig.append((mint_to_row[mint]['token_id'], mint, sig))

                # Cache the token_json for future use
                try:
                    cursor.execute(
                        "UPDATE tx_token SET token_json = COALESCE(token_json, %s) WHERE id = %s",
                        (json.dumps(meta), mint_to_row[mint]['token_id']))
                    conn.commit()
                except Exception:
                    pass

    if not have_sig:
        # Mark all as primed even without sigs (no create_tx available)
        token_ids = [r['token_id'] for r in rows]
        placeholders = ','.join(['%s'] * len(token_ids))
        try:
            cursor.execute(f"UPDATE tx_token SET primed = 1 WHERE id IN ({placeholders})", token_ids)
            conn.commit()
        except Exception:
            pass
        log(tag, f"[prime] No create_tx found for any of {len(rows)} tokens, marked primed")
        return 0

    # Filter out signatures that already exist in tx table
    sigs = [s for _, _, s in have_sig]
    try:
        placeholders = ','.join(['%s'] * len(sigs))
        cursor.execute(f"SELECT signature FROM tx WHERE signature IN ({placeholders})", sigs)
        existing = {r['signature'] for r in cursor.fetchall()}
    except Exception:
        existing = set()

    new_sigs = [(tid, mint, sig) for tid, mint, sig in have_sig if sig not in existing]
    skipped = len(have_sig) - len(new_sigs)
    if skipped:
        log(tag, f"[prime] {skipped} signatures already in DB, skipping")

    # Cascade new signatures to decoder + detailer
    if new_sigs:
        sig_list = [sig for _, _, sig in new_sigs]
        request_id = f"enricher-prime-{uuid.uuid4().hex[:12]}"
        if publish_cascade_to_workers(ch, sig_list, request_id=request_id):
            log(tag, f"[prime] Cascaded {len(sig_list)} create_tx sigs to decoder+detailer")
        else:
            log(tag, f"[prime] Failed to cascade signatures")

    # Mark all processed tokens as primed
    all_token_ids = [r['token_id'] for r in rows]
    placeholders = ','.join(['%s'] * len(all_token_ids))
    try:
        cursor.execute(f"UPDATE tx_token SET primed = 1 WHERE id IN ({placeholders})", all_token_ids)
        conn.commit()
    except Exception as e:
        log(tag, f"[prime] Failed to mark primed: {e}")

    log(tag, f"[prime] {len(all_token_ids)} tokens marked primed, {len(new_sigs)} new sigs cascaded")
    return len(new_sigs)


# =============================================================================
# Batch Claiming (FOR UPDATE SKIP LOCKED)
# =============================================================================

def claim_batch(cursor, conn, table, where_clause, where_params,
                max_attempts, limit, join_clause, select_columns):
    """
    Atomically claim rows via FOR UPDATE SKIP LOCKED, pre-increment
    attempt_cnt, COMMIT to release locks, then fetch full data.
    Enables parallel enricher instances without duplicate API calls.
    """
    # Step 1: Lock + claim IDs
    claim_sql = (
        f"SELECT id FROM {table} "
        f"WHERE {where_clause} AND attempt_cnt < %s "
        f"ORDER BY id LIMIT %s "
        f"FOR UPDATE SKIP LOCKED"
    )
    cursor.execute(claim_sql, where_params + (max_attempts, limit))
    claimed_ids = [row['id'] for row in cursor.fetchall()]

    if not claimed_ids:
        conn.commit()
        return []

    # Step 2: Pre-increment (marks as claimed)
    placeholders = ','.join(['%s'] * len(claimed_ids))
    cursor.execute(
        f"UPDATE {table} SET attempt_cnt = attempt_cnt + 1 "
        f"WHERE id IN ({placeholders})", claimed_ids)

    # Step 3: Release locks
    conn.commit()

    # Step 4: Fetch full data with JOINs (lockless)
    cursor.execute(
        f"SELECT {select_columns} FROM {table} t "
        f"{join_clause} WHERE t.id IN ({placeholders})", claimed_ids)
    return cursor.fetchall()


# =============================================================================
# Token Enrichment
# =============================================================================

def run_token_backfill(tag, cursor):
    """Run sp_tx_token_backfill once. Returns count of updated tokens."""
    try:
        cursor.execute("CALL sp_tx_token_backfill(@updated)")
        try:
            while cursor.nextset():
                pass
        except:
            pass
        cursor.execute("SELECT @updated")
        row = cursor.fetchone()
        bf = (row['@updated'] if isinstance(row, dict) else (row[0] if row else 0)) or 0
        if bf:
            log(tag, f"[tokens] Backfill updated={bf}")
        return bf
    except Exception as e:
        log(tag, f"[tokens] Backfill error: {e}")
        return 0


def enrich_tokens(tag, session, cursor, conn, limit, max_attempts, api_delay, api_timeout,
                   rmq_channel=None, prime_enabled=False):
    stats = {'processed': 0, 'updated': 0, 'failed': 0, 'backfill_updated': 0, 'primed': 0}

    # Phase 1: Solscan API (claim batch with FOR UPDATE SKIP LOCKED)
    # Only enrich tokens that appear in tx_guide (associated with real activity)
    tokens = claim_batch(
        cursor, conn, table='tx_token',
        where_clause="(token_symbol IS NULL OR token_symbol = '' OR token_name IS NULL OR token_name = '' OR decimals IS NULL OR (decimals = 0 AND token_type IS NULL)) AND in_guide = 1",
        where_params=(),
        max_attempts=max_attempts, limit=limit,
        join_clause='JOIN tx_address a ON a.id = t.mint_address_id',
        select_columns='t.id, a.address as mint'
    )

    if not tokens:
        log(tag, "[tokens] No tokens need enrichment")
        return stats

    log(tag, f"[tokens] {len(tokens)} tokens needing metadata")

    for chunk_start in range(0, len(tokens), MULTI_META_BATCH_SIZE):
        chunk = tokens[chunk_start:chunk_start + MULTI_META_BATCH_SIZE]
        mint_to_token = {t['mint']: t for t in chunk}
        chunk_num = chunk_start // MULTI_META_BATCH_SIZE + 1
        total_chunks = (len(tokens) + MULTI_META_BATCH_SIZE - 1) // MULTI_META_BATCH_SIZE

        log(tag, f"  Batch {chunk_num}/{total_chunks}: {len(chunk)} tokens")

        try:
            results = fetch_token_meta_multi(session, list(mint_to_token.keys()), api_timeout)
            log(tag, f"    Got {len(results)} results")

            for mint, token_info in mint_to_token.items():
                token_id = token_info['id']
                response = results.get(mint)

                if response:
                    name = (response.get('name') or '').strip() or None
                    symbol = (response.get('symbol') or '').strip() or None
                    icon = response.get('icon')
                    decimals = response.get('decimals')
                    supply_str = response.get('supply')
                    try:
                        supply = int(float(supply_str)) if supply_str is not None else None
                        if supply is not None and (supply > 18446744073709551615 or supply < 0):
                            supply = None
                    except (ValueError, OverflowError, TypeError):
                        supply = None
                    mint_authority = response.get('mint_authority')

                    # Derive token_type from Metaplex rules:
                    #   decimals >= 1 → fungible
                    #   decimals = 0, no mint authority → nft (supply 0 = burned, 1 = active)
                    #   decimals = 0, supply > 1 → semi_fungible
                    token_type = None
                    if decimals is not None:
                        if decimals >= 1:
                            token_type = 'fungible'
                        elif not mint_authority and supply is not None and supply <= 1:
                            token_type = 'nft'
                        elif supply is not None and supply > 1:
                            token_type = 'semi_fungible'
                        else:
                            token_type = 'unknown'

                    # Scam guard: flag tokens impersonating known symbols
                    if symbol and symbol.upper() in KNOWN_TOKEN_MINTS:
                        legit_mints = KNOWN_TOKEN_MINTS[symbol.upper()]
                        if mint not in legit_mints:
                            log(tag, f"    SCAM: {mint[:16]}... claims {symbol} — flagging")
                            name = f"{name} (Scam)" if name else "(Scam)"
                            symbol = f"{symbol} (Scam)"

                    has_meaningful = bool(name and name.strip()) or bool(symbol and symbol.strip())
                    token_json_str = json.dumps(response)

                    if has_meaningful:
                        # Strip invisible/zero-width chars before checking
                        clean_name = (name or '').strip('\u200e\u200f\u200b\u200c\u200d\u0335\ufeff\u00ad').strip()
                        clean_symbol = (symbol or '').strip('\u200e\u200f\u200b\u200c\u200d\u0335\ufeff\u00ad').strip()
                        all_fields = bool(clean_name) and bool(clean_symbol) and decimals is not None

                        try:
                            if all_fields:
                                cursor.execute("""
                                    UPDATE tx_token
                                    SET token_name = COALESCE(%s, token_name),
                                        token_symbol = COALESCE(%s, token_symbol),
                                        token_icon = COALESCE(%s, token_icon),
                                        decimals = COALESCE(%s, decimals),
                                        supply = COALESCE(%s, supply),
                                        token_type = COALESCE(%s, token_type),
                                        token_json = COALESCE(%s, token_json),
                                        attempt_cnt = CASE
                                            WHEN COALESCE(%s, token_name) != '' AND COALESCE(%s, token_symbol) != ''
                                            THEN 0 ELSE attempt_cnt END
                                    WHERE id = %s
                                """, (name, symbol, icon, decimals, supply, token_type, token_json_str, name, symbol, token_id))
                            else:
                                cursor.execute("""
                                    UPDATE tx_token
                                    SET token_name = COALESCE(%s, token_name),
                                        token_symbol = COALESCE(%s, token_symbol),
                                        token_icon = COALESCE(%s, token_icon),
                                        decimals = COALESCE(%s, decimals),
                                        supply = COALESCE(%s, supply),
                                        token_type = COALESCE(%s, token_type),
                                        token_json = COALESCE(%s, token_json)
                                    WHERE id = %s
                                """, (name, symbol, icon, decimals, supply, token_type, token_json_str, token_id))
                            conn.commit()
                            log(tag, f"    {mint[:16]}... {symbol or 'unnamed'} ({decimals} dec) [{token_type or '?'}]")
                            stats['updated'] += 1
                        except Exception as e:
                            conn.rollback()
                            log(tag, f"  Error updating {mint[:16]}...: {e}")
                            stats['failed'] += 1
                    else:
                        stats['failed'] += 1
                else:
                    stats['failed'] += 1

                stats['processed'] += 1

        except Exception as e:
            log(tag, f"  Error: {e}")
            stats['failed'] += len(chunk)
            stats['processed'] += len(chunk)

        if api_delay > 0 and chunk_start + MULTI_META_BATCH_SIZE < len(tokens):
            time.sleep(api_delay)

    # Auto-prime: fetch create_tx for unprimed tokens and cascade to decoder+detailer
    if rmq_channel and prime_enabled:
        try:
            primed = prime_unprimed_tokens(tag, session, cursor, conn, rmq_channel, api_timeout)
            stats['primed'] = primed
        except Exception as e:
            log(tag, f"[prime] Error (non-fatal): {e}")

    return stats


# =============================================================================
# Pool Enrichment
# =============================================================================

def update_pool_from_api(cursor, conn, pool_address_id, pool_id, api_data, label):
    try:
        program_address = api_data.get('program_id')
        if not program_address:
            return False

        program_id, program_address_id_fk = ensure_program(cursor, conn, program_address)

        tokens_info = api_data.get('tokens_info', [])
        token1_id = token2_id = token_account1_id = token_account2_id = None

        if len(tokens_info) >= 1:
            t1_mint = tokens_info[0].get('token')
            t1_account = tokens_info[0].get('token_account')
            if t1_mint:
                token1_id = ensure_token(cursor, conn, t1_mint)
            if t1_account:
                token_account1_id = ensure_address(cursor, conn, t1_account, 'vault')

        if len(tokens_info) >= 2:
            t2_mint = tokens_info[1].get('token')
            t2_account = tokens_info[1].get('token_account')
            if t2_mint:
                token2_id = ensure_token(cursor, conn, t2_mint)
            if t2_account:
                token_account2_id = ensure_address(cursor, conn, t2_account, 'vault')

        lp_token_id = None
        lp_mint = api_data.get('lp_token')
        if lp_mint:
            lp_token_id = ensure_token(cursor, conn, lp_mint)

        cursor.execute(
            "UPDATE tx_address SET program_id = %s WHERE id = %s",
            (program_address_id_fk, pool_address_id))

        has_all_data = token_account1_id is not None and label is not None
        cursor.execute("""
            UPDATE tx_pool SET
                program_id = COALESCE(%s, program_id),
                token1_id = COALESCE(%s, token1_id),
                token2_id = COALESCE(%s, token2_id),
                token_account1_id = COALESCE(%s, token_account1_id),
                token_account2_id = COALESCE(%s, token_account2_id),
                lp_token_id = COALESCE(%s, lp_token_id),
                pool_label = COALESCE(%s, pool_label),
                attempt_cnt = CASE WHEN %s THEN 0 ELSE attempt_cnt END
            WHERE id = %s
        """, (program_id, token1_id, token2_id,
              token_account1_id, token_account2_id, lp_token_id,
              label, has_all_data, pool_id))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        return False


def enrich_pools(tag, session, cursor, conn, limit, max_attempts, api_delay, api_timeout):
    stats = {'processed': 0, 'updated': 0, 'labels': 0, 'not_found': 0,
             'failed': 0, 'backfill_created': 0, 'backfill_updated': 0}

    # Phase 0: Local backfill — only run when new pool addresses exist without tx_pool records.
    # Skip the expensive token/label backfill (Steps 2+3) on every cycle — those resolve
    # only when new token data arrives, which happens after Phase 1/2 API calls.
    try:
        cursor.execute("""
            SELECT EXISTS(
                SELECT 1 FROM tx_address a
                LEFT JOIN tx_pool p ON p.pool_address_id = a.id
                WHERE a.address_type IN ('pool','lp_token') AND p.id IS NULL
                LIMIT 1
            ) AS needs_work
        """)
        needs_work = cursor.fetchone()
        needs_work = (needs_work.get('needs_work', 0) if isinstance(needs_work, dict) else (needs_work[0] if needs_work else 0))
        if needs_work:
            cursor.execute("CALL sp_tx_pool_backfill(@created, @updated, @accounts)")
            try:
                while cursor.nextset():
                    pass
            except:
                pass
            cursor.execute("SELECT @created, @updated, @accounts")
            row = cursor.fetchone()
            if isinstance(row, dict):
                bf_c, bf_u, bf_a = row.get('@created', 0), row.get('@updated', 0), row.get('@accounts', 0)
            else:
                bf_c, bf_u, bf_a = (row[0] or 0, row[1] or 0, row[2] or 0) if row else (0, 0, 0)
            stats['backfill_created'] = bf_c
            stats['backfill_updated'] = bf_u
            if bf_c or bf_u or bf_a:
                log(tag, f"[pools] Phase 0: backfill created={bf_c}, updated={bf_u}, accounts={bf_a}")
    except Exception as e:
        log(tag, f"[pools] Phase 0: backfill error: {e}")

    # Phase 1: Batch label enrichment via /account/metadata/multi (claimed with FOR UPDATE SKIP LOCKED)
    # Only enrich pools that appear in tx_guide (associated with real activity)
    label_pools = claim_batch(
        cursor, conn, table='tx_pool',
        where_clause="pool_label IS NULL AND pool_address_id IN (SELECT DISTINCT pool_address_id FROM tx_guide WHERE pool_address_id IS NOT NULL)",
        where_params=(),
        max_attempts=max_attempts, limit=limit,
        join_clause='JOIN tx_address a ON a.id = t.pool_address_id',
        select_columns='t.id as pool_id, a.id as address_id, a.address'
    )

    if label_pools:
        log(tag, f"[pools] Phase 1: {len(label_pools)} pools needing labels")

        for chunk_start in range(0, len(label_pools), MULTI_META_BATCH_SIZE):
            chunk = label_pools[chunk_start:chunk_start + MULTI_META_BATCH_SIZE]
            addr_to_pool = {p['address']: p for p in chunk}

            try:
                metadata_map = fetch_account_metadata_multi(session, list(addr_to_pool.keys()), api_timeout)

                for address, pool_info in addr_to_pool.items():
                    pool_id = pool_info['pool_id']
                    meta = metadata_map.get(address)
                    label = meta.get('account_label') if meta else None

                    if label:
                        cursor.execute(
                            "UPDATE tx_pool SET pool_label = %s, attempt_cnt = 0 "
                            "WHERE id = %s AND pool_label IS NULL", (label, pool_id))
                        cursor.execute(
                            "UPDATE tx_address SET label = %s WHERE id = %s AND label IS NULL",
                            (label, pool_info['address_id']))
                        conn.commit()
                        log(tag, f"    {address[:16]}... {label}")
                        stats['labels'] += 1
                        stats['updated'] += 1
                    else:
                        stats['failed'] += 1

                    stats['processed'] += 1

            except Exception as e:
                log(tag, f"  Phase 1 error: {e}")
                stats['failed'] += len(chunk)
                stats['processed'] += len(chunk)

            if api_delay > 0 and chunk_start + MULTI_META_BATCH_SIZE < len(label_pools):
                time.sleep(api_delay)

    # Phase 2: Individual structural enrichment via /market/info (claimed with FOR UPDATE SKIP LOCKED)
    # Only enrich pools that appear in tx_guide (associated with real activity)
    struct_pools = claim_batch(
        cursor, conn, table='tx_pool',
        where_clause="token_account1_id IS NULL AND pool_address_id IN (SELECT DISTINCT pool_address_id FROM tx_guide WHERE pool_address_id IS NOT NULL)",
        where_params=(),
        max_attempts=max_attempts, limit=limit,
        join_clause='JOIN tx_address a ON a.id = t.pool_address_id',
        select_columns='t.id as pool_id, a.id as address_id, a.address'
    )

    if struct_pools:
        log(tag, f"[pools] Phase 2: {len(struct_pools)} pools needing structure")

        for i, pool in enumerate(struct_pools):
            address = pool['address']
            pool_id = pool['pool_id']

            api_data = fetch_pool_info(session, address, api_timeout)

            if not api_data:
                stats['not_found'] += 1
                stats['processed'] += 1
                if api_delay > 0:
                    time.sleep(api_delay)
                continue

            cursor.execute("SELECT pool_label FROM tx_pool WHERE id = %s", (pool_id,))
            row = cursor.fetchone()
            existing_label = row['pool_label'] if row else None

            if update_pool_from_api(cursor, conn, pool['address_id'], pool_id, api_data, existing_label):
                program = api_data.get('program_id', 'unknown')
                log(tag, f"    [{i+1}/{len(struct_pools)}] {address[:20]}... {program[:20]}...")
                stats['updated'] += 1
            else:
                stats['failed'] += 1

            stats['processed'] += 1
            if api_delay > 0 and i < len(struct_pools) - 1:
                time.sleep(api_delay)

    return stats


# =============================================================================
# Program Enrichment
# =============================================================================

PROGRAM_TYPE_KEYWORDS = {
    'dex': ['swap', 'amm', 'dex', 'raydium', 'orca', 'jupiter', 'serum', 'openbook'],
    'lending': ['lend', 'borrow', 'solend', 'mango', 'port', 'jet', 'marginfi'],
    'nft': ['nft', 'metaplex', 'candy', 'auction'],
    'token': ['token', 'spl-token', 'mint'],
    'system': ['system', 'vote', 'stake', 'config'],
    'compute': ['compute', 'budget'],
    'router': ['router', 'aggregator', 'jupiter'],
}

KNOWN_PROGRAMS = {
    '11111111111111111111111111111111': ('System Program', 'system'),
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA': ('Token Program', 'token'),
    'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL': ('Associated Token Program', 'token'),
    'ComputeBudget111111111111111111111111111111': ('Compute Budget', 'compute'),
    'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK': ('Raydium CPMM', 'dex'),
    'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C': ('Raydium CPMM', 'dex'),
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8': ('Raydium AMM V4', 'dex'),
    'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc': ('Orca Whirlpool', 'dex'),
    'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4': ('Jupiter V6', 'router'),
    'LBUZKhRxPF3XUpBCjp4YzTKgLXWjvfY2LnPkDsHEVBo': ('Meteora DLMM', 'dex'),
    'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB': ('Meteora Pools', 'dex'),
    'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA': ('Pump.fun AMM', 'dex'),
    '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P': ('Pump.fun', 'dex'),
}


def infer_program_type(label):
    text = (label or '').lower()
    for prog_type, keywords in PROGRAM_TYPE_KEYWORDS.items():
        for kw in keywords:
            if kw in text:
                return prog_type
    return 'other'


def enrich_programs(tag, session, cursor, conn, limit, max_attempts, api_delay, api_timeout):
    stats = {'processed': 0, 'updated': 0, 'known': 0, 'failed': 0}

    programs = claim_batch(
        cursor, conn, table='tx_program',
        where_clause="name IS NULL",
        where_params=(),
        max_attempts=max_attempts, limit=limit,
        join_clause='JOIN tx_address a ON a.id = t.program_address_id',
        select_columns='t.id, a.address'
    )

    if not programs:
        log(tag, "[programs] No programs need enrichment")
        return stats

    log(tag, f"[programs] {len(programs)} programs needing metadata")

    for i, prog in enumerate(programs):
        program_id = prog['id']
        address = prog['address']

        # Check known programs first
        if address in KNOWN_PROGRAMS:
            name, prog_type = KNOWN_PROGRAMS[address]
            cursor.execute(
                "UPDATE tx_program SET name = %s, program_type = %s, attempt_cnt = 0 WHERE id = %s",
                (name, prog_type, program_id))
            conn.commit()
            log(tag, f"    {address[:20]}... {name} (known)")
            stats['known'] += 1
            stats['updated'] += 1
            stats['processed'] += 1
            continue

        # Fetch from Solscan API
        try:
            metadata = fetch_account_metadata(session, address, api_timeout)
            if metadata:
                label = metadata.get('account_label') or metadata.get('label')
                if label:
                    prog_type = infer_program_type(label)
                    cursor.execute(
                        "UPDATE tx_program SET name = %s, program_type = %s, attempt_cnt = 0 WHERE id = %s",
                        (label, prog_type, program_id))
                    conn.commit()
                    log(tag, f"    {address[:20]}... {label} ({prog_type})")
                    stats['updated'] += 1
                else:
                    stats['failed'] += 1
            else:
                stats['failed'] += 1
        except Exception as e:
            log(tag, f"    {address[:20]}... error: {e}")
            stats['failed'] += 1

        stats['processed'] += 1
        if api_delay > 0 and i < len(programs) - 1:
            time.sleep(api_delay)

    return stats


# =============================================================================
# Status
# =============================================================================

def get_enrichment_status(cursor):
    cursor.execute("""
        SELECT COUNT(*) as cnt FROM tx_token
        WHERE (token_symbol IS NULL OR token_symbol = ''
           OR token_name IS NULL OR token_name = ''
           OR decimals IS NULL)
          AND COALESCE(attempt_cnt, 0) < 3
    """)
    tokens_pending = cursor.fetchone()['cnt']

    cursor.execute("SELECT COUNT(*) as cnt FROM tx_token WHERE token_symbol IS NOT NULL AND token_symbol != ''")
    tokens_enriched = cursor.fetchone()['cnt']

    cursor.execute("""
        SELECT COUNT(*) as cnt FROM tx_pool
        WHERE (token_account1_id IS NULL OR pool_label IS NULL)
          AND COALESCE(attempt_cnt, 0) < 3
    """)
    pools_pending = cursor.fetchone()['cnt']

    cursor.execute("SELECT COUNT(*) as cnt FROM tx_pool WHERE token_account1_id IS NOT NULL")
    pools_enriched = cursor.fetchone()['cnt']

    cursor.execute("""
        SELECT COUNT(*) as cnt FROM tx_program
        WHERE name IS NULL AND COALESCE(attempt_cnt, 0) < 3
    """)
    programs_pending = cursor.fetchone()['cnt']

    cursor.execute("SELECT COUNT(*) as cnt FROM tx_program WHERE name IS NOT NULL")
    programs_enriched = cursor.fetchone()['cnt']

    return {
        'tokens': {'pending': tokens_pending, 'enriched': tokens_enriched},
        'pools': {'pending': pools_pending, 'enriched': pools_enriched},
        'programs': {'pending': programs_pending, 'enriched': programs_enriched},
    }


# =============================================================================
# Worker thread
# =============================================================================

class WorkerThread(threading.Thread):
    def __init__(self, worker_id, prefetch, dry_run, stop_event,
                 poll_idle_sec, reconnect_sec, api_timeout_sec,
                 api_delay_sec, batch_limit, max_attempts):
        super().__init__(daemon=True)
        self.tag = f"W-{worker_id}"
        self.worker_id = worker_id
        self.prefetch = prefetch
        self.dry_run = dry_run
        self.stop_event = stop_event
        self.poll_idle_sec = poll_idle_sec
        self.reconnect_sec = reconnect_sec
        self.api_timeout_sec = api_timeout_sec
        self.api_delay_sec = api_delay_sec
        self.batch_limit = batch_limit
        self.max_attempts = max_attempts

    def run(self):
        log(self.tag, f"Starting (prefetch={self.prefetch})")
        session = solscan_session()
        db_conn = None
        cursor = None

        while not self.stop_event.is_set():
            try:
                if db_conn is None:
                    db_conn = db_connect()
                    cursor = db_conn.cursor(dictionary=True)
                    cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                    log(self.tag, "DB connected (READ COMMITTED)")

                rmq_conn, ch = rmq_connect()
                ch.basic_qos(prefetch_count=self.prefetch)
                log(self.tag, "RabbitMQ connected, consuming...")

                while not self.stop_event.is_set():
                    method, properties, body = ch.basic_get(queue=REQUEST_QUEUE, auto_ack=False)
                    if method is None:
                        time.sleep(self.poll_idle_sec)
                        rmq_conn.process_data_events(time_limit=0)
                        continue

                    self._handle_message(ch, method, properties, body, cursor, db_conn, session)

                try:
                    rmq_conn.close()
                except Exception:
                    pass

            except MySQLError as e:
                log(self.tag, f"MySQL error: {e}, reconnecting in {self.reconnect_sec}s...")
                db_conn = None
                cursor = None
                time.sleep(self.reconnect_sec)
            except pika.exceptions.AMQPConnectionError as e:
                log(self.tag, f"RabbitMQ lost: {e}, reconnecting in {self.reconnect_sec}s...")
                time.sleep(self.reconnect_sec)
            except Exception as e:
                log(self.tag, f"Unexpected error: {e}, retrying in {self.reconnect_sec}s...")
                time.sleep(self.reconnect_sec)

        if db_conn:
            try:
                db_conn.close()
            except Exception:
                pass
        session.close()
        log(self.tag, "Stopped")

    def _handle_message(self, ch, method, properties, body, cursor, db_conn, session):
        worker_log_id = None
        try:
            msg = json.loads(body.decode('utf-8'))
            request_id     = msg.get('request_id', str(uuid.uuid4()))
            correlation_id = msg.get('correlation_id', request_id)
            api_key_id     = msg.get('api_key_id')
            action         = msg.get('action', 'enrich')
            priority       = msg.get('priority', 5)
            batch          = msg.get('batch', {})

            # ── DB poll: supervisor-scheduled enrichment pass ──
            if action == 'db-poll-enrich':
                self._handle_db_poll(ch, method, cursor, db_conn, session)
                return

            # ── Normal queue message: gateway request ──
            operations = batch.get('operations', ['tokens', 'pools', 'programs'])
            if isinstance(operations, str):
                operations = [op.strip() for op in operations.split(',')]
            limit = batch.get('limit', self.batch_limit)
            max_attempts = batch.get('max_attempts', self.max_attempts)
            delay = batch.get('delay', self.api_delay_sec)
            _batch_cfg = load_db_config(cursor, 'batch')
            prime_enabled = cfg_int(_batch_cfg, 'enricher_prime_enabled', 1) > 0

            log(self.tag, f"Request {request_id[:8]} (action={action}, ops={operations})")

            worker_log_id = log_worker_request(
                cursor, db_conn, request_id, correlation_id,
                limit, priority, api_key_id)

            total_updated = 0
            all_stats = {}

            if 'tokens' in operations:
                run_token_backfill(self.tag, cursor)
                s = enrich_tokens(self.tag, session, cursor, db_conn, limit, max_attempts, delay, self.api_timeout_sec,
                                  rmq_channel=ch, prime_enabled=prime_enabled)
                total_updated += s.get('updated', 0)
                all_stats['tokens'] = s

            if 'pools' in operations:
                s = enrich_pools(self.tag, session, cursor, db_conn, limit, max_attempts, delay, self.api_timeout_sec)
                total_updated += s.get('updated', 0)
                all_stats['pools'] = s

            if 'programs' in operations:
                s = enrich_programs(self.tag, session, cursor, db_conn, limit, max_attempts, delay, self.api_timeout_sec)
                total_updated += s.get('updated', 0)
                all_stats['programs'] = s

            resp = {'processed': total_updated, 'stats': all_stats}
            update_worker_request(cursor, db_conn, worker_log_id, 'completed', resp)
            self._publish_response(ch, request_id, correlation_id, 'completed', resp)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except MySQLError:
            nack_with_retry(ch, method.delivery_tag, properties,
                            log_fn=lambda msg: log(self.tag, f"[DB ERROR] {msg}"),
                            queue_name=REQUEST_QUEUE, body=body)
            raise
        except Exception as e:
            log(self.tag, f"ERROR processing message -> DLQ: {e}")
            import traceback
            traceback.print_exc()
            if worker_log_id:
                try:
                    update_worker_request(cursor, db_conn, worker_log_id, 'failed', {'error': str(e)})
                except Exception:
                    pass
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _handle_db_poll(self, ch, method, cursor, db_conn, session):
        """Handle supervisor-scheduled enrichment pass — single batch per type per poll."""
        grand_total = 0
        all_stats = {}
        _batch_cfg = load_db_config(cursor, 'batch')
        prime_enabled = cfg_int(_batch_cfg, 'enricher_prime_enabled', 1) > 0

        # Token backfill (once per poll)
        bf = run_token_backfill(self.tag, cursor)
        grand_total += bf

        # Tokens — single batch
        s = enrich_tokens(self.tag, session, cursor, db_conn,
                          self.batch_limit, self.max_attempts,
                          self.api_delay_sec, self.api_timeout_sec,
                          rmq_channel=ch, prime_enabled=prime_enabled)
        grand_total += s.get('updated', 0)
        all_stats['tokens'] = s

        # Pools — single batch
        s = enrich_pools(self.tag, session, cursor, db_conn,
                         self.batch_limit, self.max_attempts,
                         self.api_delay_sec, self.api_timeout_sec)
        grand_total += s.get('updated', 0) + s.get('backfill_created', 0) + s.get('backfill_updated', 0)
        all_stats['pools'] = s

        # Programs — single batch
        s = enrich_programs(self.tag, session, cursor, db_conn,
                            self.batch_limit, self.max_attempts,
                            self.api_delay_sec, self.api_timeout_sec)
        grand_total += s.get('updated', 0)
        all_stats['programs'] = s

        if grand_total > 0:
            log(self.tag, f"DB poll: {grand_total} enriched")
            try:
                dl_id = log_daemon_request(cursor, db_conn, 'db-poll-enrich', grand_total)
                update_worker_request(cursor, db_conn, dl_id, 'completed', {
                    'updated': grand_total, 'stats': all_stats
                })
            except Exception as e:
                log(self.tag, f"Failed to log request: {e}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _publish_response(self, ch, request_id, correlation_id, status, result):
        body = json.dumps({
            'request_id':     request_id,
            'correlation_id': correlation_id,
            'worker':         'enricher',
            'status':         status,
            'timestamp':      datetime.now(timezone.utc).isoformat(),
            'result':         result,
        })
        ch.basic_publish(
            exchange='', routing_key=RESPONSE_QUEUE,
            body=body.encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2, content_type='application/json'))


# =============================================================================
# Supervisor
# =============================================================================

def read_config(cursor):
    cfg = load_db_config(cursor, 'queue')
    return {
        'threads':          cfg_int(cfg, 'enricher_wrk_cnt_threads', 0),
        'prefetch':         cfg_int(cfg, 'enricher_wrk_cnt_prefetch', 5),
        'supervisor_poll':  cfg_float(cfg, 'enricher_wrk_supervisor_poll_sec', 5.0),
        'poll_idle':        cfg_float(cfg, 'enricher_wrk_poll_idle_sec', 0.25),
        'reconnect':        cfg_float(cfg, 'enricher_wrk_reconnect_sec', 5.0),
        'shutdown_timeout': cfg_float(cfg, 'enricher_wrk_shutdown_timeout_sec', 10.0),
        'api_timeout':      cfg_float(cfg, 'enricher_wrk_api_timeout_sec', 60.0),
        'api_delay':        cfg_float(cfg, 'enricher_wrk_api_delay_sec', 0.30),
        'batch_limit':      cfg_int(cfg, 'enricher_wrk_batch_limit', 100),
        'max_attempts':     cfg_int(cfg, 'enricher_wrk_max_attempts', 3),
        'db_poll':          cfg_float(cfg, 'enricher_wrk_db_poll_sec', 0),
    }


def run_supervisor(dry_run=False):
    print(f"""
+-----------------------------------------------------------+
|  Guide Enricher - Supervisor                              |
|  vhost: {RABBITMQ_VHOST:<10}  queue: {REQUEST_QUEUE:<24} |
+-----------------------------------------------------------+
""", flush=True)

    workers = {}
    next_id = 1
    svr_conn = None
    svr_cursor = None
    svr_rmq_conn = None
    svr_rmq_ch = None
    last_db_poll = 0.0

    def ensure_svr_db():
        nonlocal svr_conn, svr_cursor
        try:
            if svr_conn is not None:
                svr_conn.ping(reconnect=False, attempts=1, delay=0)
                return True
        except Exception:
            svr_conn = None
        try:
            svr_conn = db_connect()
            svr_cursor = svr_conn.cursor(dictionary=True)
            log('SVR', 'DB connected')
            return True
        except Exception as e:
            log('SVR', f'DB connect failed: {e}')
            return False

    def ensure_svr_rmq():
        nonlocal svr_rmq_conn, svr_rmq_ch
        try:
            if svr_rmq_conn is not None and svr_rmq_conn.is_open:
                return True
        except Exception:
            svr_rmq_conn = None
        try:
            svr_rmq_conn, svr_rmq_ch = rmq_connect()
            log('SVR', 'RabbitMQ connected (publisher)')
            return True
        except Exception as e:
            log('SVR', f'RabbitMQ connect failed: {e}')
            return False

    def publish_db_poll():
        # Skip if queue already has pending messages (workers loop until drained)
        result = svr_rmq_ch.queue_declare(queue=REQUEST_QUEUE, passive=True)
        if result.method.message_count > 0:
            return
        msg = json.dumps({
            'request_id': f"dbpoll-{uuid.uuid4().hex[:12]}",
            'action': 'db-poll-enrich',
        })
        svr_rmq_ch.basic_publish(
            exchange='', routing_key=REQUEST_QUEUE,
            body=msg.encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2, content_type='application/json'))
        log('SVR', 'Published db-poll-enrich to queue')

    try:
        while True:
            if not ensure_svr_db():
                time.sleep(DB_FALLBACK_RETRY_SEC)
                continue

            cfg = read_config(svr_cursor)

            # Prune dead workers
            dead = [wid for wid, (w, _) in workers.items() if not w.is_alive()]
            for wid in dead:
                log('SVR', f'Worker W-{wid} died, removing')
                del workers[wid]

            active = len(workers)
            if active != cfg['threads']:
                log('SVR', f"Config: threads={cfg['threads']} prefetch={cfg['prefetch']} | active={active}")

            # Scale up
            while len(workers) < cfg['threads']:
                stop_evt = threading.Event()
                w = WorkerThread(
                    next_id, cfg['prefetch'], dry_run, stop_evt,
                    cfg['poll_idle'], cfg['reconnect'], cfg['api_timeout'],
                    cfg['api_delay'], cfg['batch_limit'], cfg['max_attempts'])
                w.start()
                workers[next_id] = (w, stop_evt)
                next_id += 1

            # Scale down (stop newest first)
            while len(workers) > cfg['threads']:
                wid = max(workers.keys())
                wthread, stop_evt = workers.pop(wid)
                log('SVR', f'Stopping W-{wid}...')
                stop_evt.set()

            # DB poll scheduler
            now = time.time()
            if cfg['db_poll'] > 0 and active > 0 and (now - last_db_poll) >= cfg['db_poll']:
                if ensure_svr_rmq():
                    try:
                        publish_db_poll()
                        last_db_poll = now
                    except Exception as e:
                        log('SVR', f'Failed to publish db poll: {e}')
                        svr_rmq_conn = None

            time.sleep(cfg['supervisor_poll'])

    except KeyboardInterrupt:
        log('SVR', 'Shutting down...')

    shutdown_timeout = cfg['shutdown_timeout'] if 'cfg' in dir() else 10.0

    for wid, (w, stop_evt) in workers.items():
        stop_evt.set()
    for wid, (w, _) in workers.items():
        w.join(timeout=shutdown_timeout)
        if w.is_alive():
            log('SVR', f'W-{wid} did not stop in time')

    if svr_rmq_conn:
        try:
            svr_rmq_conn.close()
        except Exception:
            pass
    if svr_conn:
        try:
            svr_conn.close()
        except Exception:
            pass

    log('SVR', 'Shutdown complete')


# =============================================================================
# Manual modes
# =============================================================================

def run_manual(args):
    """Run enrichment manually (single run or specific operations)."""
    conn = db_connect()
    cursor = conn.cursor(dictionary=True)
    session = solscan_session()

    try:
        # Status mode
        if args.status:
            status = get_enrichment_status(cursor)
            print(f"\n{'='*60}")
            print(f"  Tokens pending:   {status['tokens']['pending']:,}")
            print(f"  Tokens enriched:  {status['tokens']['enriched']:,}")
            print(f"  Pools pending:    {status['pools']['pending']:,}")
            print(f"  Pools enriched:   {status['pools']['enriched']:,}")
            print(f"  Programs pending: {status['programs']['pending']:,}")
            print(f"  Programs enriched:{status['programs']['enriched']:,}")
            print(f"{'='*60}")
            return 0

        # Single pool mode
        if args.pool:
            # Inline single-pool enrichment
            address = args.pool
            log('MANUAL', f"Enriching pool: {address}")
            cursor.execute("SELECT id FROM tx_address WHERE address = %s", (address,))
            row = cursor.fetchone()
            if not row:
                cursor.execute("INSERT INTO tx_address (address, address_type) VALUES (%s, 'pool')", (address,))
                conn.commit()
                address_id = cursor.lastrowid
            else:
                address_id = row['id']

            cursor.execute("SELECT id FROM tx_pool WHERE pool_address_id = %s", (address_id,))
            row = cursor.fetchone()
            if not row:
                cursor.execute("INSERT INTO tx_pool (pool_address_id) VALUES (%s)", (address_id,))
                conn.commit()
                pool_id = cursor.lastrowid
            else:
                pool_id = row['id']

            api_data = fetch_pool_info(session, address, 30)
            if api_data:
                log('MANUAL', f"  Program: {api_data.get('program_id', 'unknown')}")
            time.sleep(0.3)
            metadata = fetch_account_metadata(session, address, 30)
            label = metadata.get('account_label') if metadata else None
            if label:
                log('MANUAL', f"  Label: {label}")
            if api_data:
                update_pool_from_api(cursor, conn, address_id, pool_id, api_data, label)
                log('MANUAL', "  Pool updated")
            return 0

        # Parse operations
        if args.enrich.lower() == 'all':
            operations = ['tokens', 'pools', 'programs']
        else:
            operations = [op.strip().lower() for op in args.enrich.split(',')]

        limit = args.limit
        max_attempts = args.max_attempts
        delay = args.delay

        log('MANUAL', f"Operations: {', '.join(operations)}, limit={limit}")

        if 'tokens' in operations:
            s = enrich_tokens('MANUAL', session, cursor, conn, limit, max_attempts, delay, 30)
            log('MANUAL', f"Tokens: {s.get('updated', 0)} updated, {s.get('failed', 0)} failed")

        if 'pools' in operations:
            s = enrich_pools('MANUAL', session, cursor, conn, limit, max_attempts, delay, 30)
            log('MANUAL', f"Pools: {s.get('updated', 0)} updated, {s.get('not_found', 0)} not found")

        if 'programs' in operations:
            s = enrich_programs('MANUAL', session, cursor, conn, limit, max_attempts, delay, 30)
            log('MANUAL', f"Programs: {s.get('updated', 0)} updated ({s.get('known', 0)} known)")

    finally:
        session.close()
        conn.close()

    return 0


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Guide Enricher - Token/Pool/Program metadata enrichment')
    parser.add_argument('--dry-run', action='store_true', help='Preview only')
    parser.add_argument('--enrich', '-e', type=str, default='all',
                        help='Operations: tokens,pools,programs,all (default: all)')
    parser.add_argument('--limit', '-l', type=int, default=100,
                        help='Max items per batch (default: 100)')
    parser.add_argument('--max-attempts', type=int, default=3,
                        help='Skip items with more than N failed attempts (default: 3)')
    parser.add_argument('--delay', type=float, default=0.3,
                        help='Delay between API calls (default: 0.3)')
    parser.add_argument('--pool', type=str, help='Enrich single pool address')
    parser.add_argument('--status', action='store_true', help='Show enrichment status')
    args = parser.parse_args()

    if args.status or args.pool or args.enrich != 'all' or args.limit != 100:
        return run_manual(args)
    else:
        return run_supervisor(dry_run=args.dry_run)


if __name__ == '__main__':
    sys.exit(main() or 0)
