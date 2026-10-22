#!/usr/bin/env python3
"""
Guide Shredder - Staging table consumer for forward-only transaction processing
(multi-threaded, config-driven)

Supervisor thread polls config for desired thread count.
Worker threads each own their own DB + RabbitMQ connections.

Config keys (config_type='queue'):
    shredder_wrk_cnt_threads          - desired worker thread count (0 = idle)
    shredder_wrk_batch_size           - staging rows per batch per worker
    shredder_wrk_supervisor_poll_sec  - supervisor config poll interval
    shredder_wrk_poll_idle_sec        - worker sleep when no rows available
    shredder_wrk_reconnect_sec        - delay before reconnecting after errors
    shredder_wrk_shutdown_timeout_sec - max wait for worker thread on shutdown
    shredder_wrk_purge_every_n        - purge every N supervisor polls
    shredder_wrk_purge_min_age_sec    - min age before purging staging rows

Consumes staged transactions from t16o_db_staging.txs and calls the appropriate
stored procedure based on tx_state:
  - tx_state=8 (decoded): CALL sp_tx_parse_staging_decode()
  - tx_state=16 (detailed): CALL sp_tx_parse_staging_detail()

After processing, tx_state is set to 4 (shredded).

Usage:
    python guide-shredder.py --daemon                 # Supervisor + worker threads
    python guide-shredder.py --once                   # Process once and exit
    python guide-shredder.py --daemon --dry-run      # Preview only
"""

import argparse
import json
import os
import sys
import time
import threading
from datetime import datetime

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

try:
    import pika
    HAS_PIKA = True
except ImportError:
    HAS_PIKA = False

# =============================================================================
# Static config (from common.config → guide-config.json)
# =============================================================================

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from t16o_exchange.guide.common.config import (
    get_db_config, get_rabbitmq_config, get_staging_config, get_queue_names,
    load_db_config, cfg_int, cfg_float,
)

_rmq                = get_rabbitmq_config()
_queues             = get_queue_names('shredder')
_staging            = get_staging_config()

DB_CONFIG           = get_db_config()
STAGING_SCHEMA      = _staging['schema']
STAGING_TABLE       = _staging['table']
RABBITMQ_HOST       = _rmq['host']
RABBITMQ_PORT       = _rmq['port']
RABBITMQ_USER       = _rmq['user']
RABBITMQ_PASS       = _rmq['password']
RABBITMQ_VHOST      = _rmq['vhost']
RABBITMQ_RESPONSE_QUEUE = _queues['response']

# Default tx_state values (will be fetched from config table)
TX_STATE_SHREDDED = 4
TX_STATE_DECODED = 8
TX_STATE_DETAILED = 16
TX_STATE_PROCESSING = 1  # Claimed by shredder, in progress

# Max retry attempts for detailed rows before giving up
MAX_DETAILED_ATTEMPTS = 3

# Known Solana program addresses for participant classification
KNOWN_PROGRAMS = {
    '11111111111111111111111111111111',              # System Program
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',  # Token Program
    'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb',  # Token-2022 Program
    'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL', # Associated Token Program
    'ComputeBudget111111111111111111111111111111',   # Compute Budget
    'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s',  # Metaplex Token Metadata
    'BPFLoaderUpgradeab1e11111111111111111111111',   # BPF Upgradeable Loader
    'BPFLoader2111111111111111111111111111111111',   # BPF Loader 2
    'SysvarRent111111111111111111111111111111111',   # Sysvar Rent
    'SysvarC1ock11111111111111111111111111111111',   # Sysvar Clock
    'Sysvar1nstructions1111111111111111111111111',   # Sysvar Instructions
    'Vote111111111111111111111111111111111111111',   # Vote Program
    'Stake11111111111111111111111111111111111111',   # Stake Program
    # DEX Programs
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',  # Raydium AMM V4
    'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',  # Raydium CPMM
    'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C',  # Raydium CLMM
    '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h',  # Raydium Stable
    'srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX',  # Serum DEX V3
    '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin',  # Serum DEX V2
    'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',  # Orca Whirlpools
    'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',  # Jupiter V6
    'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',  # Meteora DLMM
    'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB', # Meteora Pools
    '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P',  # Pump.fun
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
    if not HAS_PIKA:
        return None, None
    creds = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, virtual_host=RABBITMQ_VHOST,
        credentials=creds, heartbeat=600,
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=RABBITMQ_RESPONSE_QUEUE, durable=True,
                     arguments={'x-max-priority': 10})
    return conn, ch


# =============================================================================
# Gateway Response Publishing
# =============================================================================

def check_correlation_complete(cursor, correlation_id, completed_correlations):
    """
    Check if all staging rows for a correlation_id are shredded.
    Returns stats dict if complete, None if not.
    """
    if not correlation_id or correlation_id in completed_correlations:
        return None

    cursor.execute(f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN tx_state = %s THEN 1 ELSE 0 END) as shredded,
            SUM(CASE WHEN tx_state != %s THEN 1 ELSE 0 END) as pending
        FROM {STAGING_SCHEMA}.{STAGING_TABLE}
        WHERE correlation_id = %s
    """, (TX_STATE_SHREDDED, TX_STATE_SHREDDED, correlation_id))

    row = cursor.fetchone()
    if not row or row['total'] == 0:
        return None

    if row['pending'] == 0:
        completed_correlations.add(correlation_id)
        return {
            'total_staging_rows': row['total'],
            'shredded': row['shredded']
        }

    return None


def publish_shredder_response(channel, correlation_id, stats):
    """Publish shredder completion response to gateway"""
    if not channel:
        return

    response = {
        'correlation_id': correlation_id,
        'worker': 'shredder',
        'status': 'completed',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'result': {
            'staging_rows_processed': stats['total_staging_rows'],
            'batch_num': 1
        }
    }

    try:
        channel.basic_publish(
            exchange='', routing_key=RABBITMQ_RESPONSE_QUEUE,
            body=json.dumps(response).encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2, content_type='application/json')
        )
    except Exception:
        pass  # Non-critical, logged at caller level


# =============================================================================
# Core Processing (used by both WorkerThread and run_once)
# =============================================================================

class ShredderProcessor:
    """Processes staged transactions using forward-only stored procedures"""

    def __init__(self, db_conn, dry_run=False, tag='PROC'):
        self.db_conn = db_conn
        self.cursor = db_conn.cursor(dictionary=True)
        self.dry_run = dry_run
        self.tag = tag
        self._tx_states = {}
        self._completed_correlations = set()

    def get_tx_state(self, key):
        """Get tx_state value from config table (cached)"""
        if key not in self._tx_states:
            self.cursor.execute(
                "SELECT CAST(config_value AS UNSIGNED) as val FROM config "
                "WHERE config_type = 'tx_state' AND config_key = %s", (key,)
            )
            row = self.cursor.fetchone()
            defaults = {'shredded': TX_STATE_SHREDDED, 'decoded': TX_STATE_DECODED, 'detailed': TX_STATE_DETAILED}
            self._tx_states[key] = row['val'] if row else defaults.get(key, 0)
        return self._tx_states[key]

    def extract_and_insert_participants(self, staging_id):
        """
        Extract participants from staging row JSON and insert into tx_participant.
        Returns number of participants inserted.
        """
        fresh_cursor = self.db_conn.cursor(dictionary=True)
        try:
            fresh_cursor.execute(f"""
                SELECT txs FROM {STAGING_SCHEMA}.{STAGING_TABLE} WHERE id = %s
            """, (staging_id,))
            row = fresh_cursor.fetchone()
            if not row or not row.get('txs'):
                return 0

            txs_data = row['txs']
            if isinstance(txs_data, str):
                txs_data = json.loads(txs_data)

            data_list = txs_data.get('data', [])
            if not data_list:
                return 0

            total_inserted = 0

            for tx_item in data_list:
                tx_hash = tx_item.get('tx_hash')
                account_keys = tx_item.get('account_keys', [])

                if not tx_hash or not account_keys:
                    continue

                fresh_cursor.execute("SELECT id FROM tx WHERE signature = %s", (tx_hash,))
                tx_row = fresh_cursor.fetchone()
                if not tx_row:
                    continue

                tx_id = tx_row['id']

                for idx, acct in enumerate(account_keys):
                    pubkey = acct.get('pubkey')
                    if not pubkey:
                        continue

                    is_signer = 1 if acct.get('signer') else 0
                    is_fee_payer = 1 if idx == 0 else 0
                    is_writable = 1 if acct.get('writable') else 0
                    is_program = 1 if pubkey in KNOWN_PROGRAMS else 0

                    fresh_cursor.execute("SELECT id FROM tx_address WHERE address = %s", (pubkey,))
                    addr_row = fresh_cursor.fetchone()
                    if addr_row:
                        address_id = addr_row['id']
                    else:
                        addr_type = 'program' if is_program else 'unknown'
                        fresh_cursor.execute(
                            "INSERT INTO tx_address (address, address_type) VALUES (%s, %s)",
                            (pubkey, addr_type)
                        )
                        address_id = fresh_cursor.lastrowid

                    try:
                        fresh_cursor.execute("""
                            INSERT INTO tx_participant
                                (tx_id, address_id, is_signer, is_fee_payer, is_writable, is_program, account_index)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                is_signer = VALUES(is_signer),
                                is_fee_payer = VALUES(is_fee_payer),
                                is_writable = VALUES(is_writable),
                                is_program = VALUES(is_program)
                        """, (tx_id, address_id, is_signer, is_fee_payer, is_writable, is_program, idx))
                        total_inserted += 1
                    except MySQLError:
                        pass

            self.db_conn.commit()
            return total_inserted
        finally:
            fresh_cursor.close()

    def fetch_pending_staging_rows(self, limit=50):
        """
        Claim and fetch staging rows that need processing.
        Uses atomic UPDATE to claim rows, avoiding locks that block INSERTs.

        Priority order:
        1. Complete pairs (decoded + detailed ready, SUM=24) - prevents starvation
        2. Standalone decoded rows
        3. Standalone detailed rows (where tx exists)

        Within each category, ordered by priority DESC.
        Decoded is always processed before detailed for the same sig_hash.
        """
        decoded_state = self.get_tx_state('decoded')
        detailed_state = self.get_tx_state('detailed')

        if self.dry_run:
            self.cursor.execute(f"""
                SELECT id, tx_state, priority, created_utc, correlation_id, sig_hash
                FROM {STAGING_SCHEMA}.{STAGING_TABLE}
                WHERE tx_state IN (%s, %s)
                ORDER BY priority DESC, sig_hash, tx_state, created_utc ASC
                LIMIT %s
            """, (decoded_state, detailed_state, limit))
            return self.cursor.fetchall()

        claimed_count = 0

        # Use READ COMMITTED to avoid gap locks that block INSERTs from decoder/detailer
        self.cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        self.cursor.execute("START TRANSACTION")

        try:
            # Step 1: Find complete pairs (SUM(tx_state) = 24) ordered by priority
            self.cursor.execute(f"""
                SELECT sig_hash, MAX(priority) as max_priority
                FROM {STAGING_SCHEMA}.{STAGING_TABLE}
                WHERE tx_state IN (%s, %s)
                  AND sig_hash IS NOT NULL
                GROUP BY sig_hash
                HAVING SUM(tx_state) = 24
                ORDER BY max_priority DESC
                LIMIT %s
            """, (decoded_state, detailed_state, limit // 2))

            complete_pairs = [row['sig_hash'] for row in self.cursor.fetchall()]

            # Step 2: Claim both decoded and detailed for complete pairs using SKIP LOCKED
            if complete_pairs:
                placeholders = ','.join(['%s'] * len(complete_pairs))

                # SELECT FOR UPDATE SKIP LOCKED to exclusively lock rows, then UPDATE
                self.cursor.execute(f"""
                    SELECT id FROM {STAGING_SCHEMA}.{STAGING_TABLE}
                    WHERE tx_state = %s
                      AND sig_hash IN ({placeholders})
                    FOR UPDATE SKIP LOCKED
                """, (decoded_state, *complete_pairs))
                pair_decoded_ids = [r['id'] for r in self.cursor.fetchall()]

                self.cursor.execute(f"""
                    SELECT id FROM {STAGING_SCHEMA}.{STAGING_TABLE}
                    WHERE tx_state = %s
                      AND sig_hash IN ({placeholders})
                      AND attempt_cnt < {MAX_DETAILED_ATTEMPTS}
                    FOR UPDATE SKIP LOCKED
                """, (detailed_state, *complete_pairs))
                pair_detailed_ids = [r['id'] for r in self.cursor.fetchall()]

                if pair_decoded_ids:
                    id_ph = ','.join(['%s'] * len(pair_decoded_ids))
                    self.cursor.execute(f"""
                        UPDATE {STAGING_SCHEMA}.{STAGING_TABLE}
                        SET priority = priority * 10 + 1,
                            tx_state = {TX_STATE_PROCESSING}
                        WHERE id IN ({id_ph})
                    """, tuple(pair_decoded_ids))

                if pair_detailed_ids:
                    id_ph = ','.join(['%s'] * len(pair_detailed_ids))
                    self.cursor.execute(f"""
                        UPDATE {STAGING_SCHEMA}.{STAGING_TABLE}
                        SET priority = priority * 10 + 2,
                            tx_state = {TX_STATE_PROCESSING}
                        WHERE id IN ({id_ph})
                    """, tuple(pair_detailed_ids))

                claimed_count = len(pair_decoded_ids) + len(pair_detailed_ids)

            # Step 3: Claim standalone decoded rows (remaining slots) using SKIP LOCKED
            remaining_slots = limit - claimed_count
            if remaining_slots > 0:
                self.cursor.execute(f"""
                    SELECT id FROM {STAGING_SCHEMA}.{STAGING_TABLE}
                    WHERE tx_state = %s
                    ORDER BY priority DESC, created_utc ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                """, (decoded_state, remaining_slots))
                standalone_decoded_ids = [r['id'] for r in self.cursor.fetchall()]

                if standalone_decoded_ids:
                    id_ph = ','.join(['%s'] * len(standalone_decoded_ids))
                    self.cursor.execute(f"""
                        UPDATE {STAGING_SCHEMA}.{STAGING_TABLE}
                        SET priority = priority * 10 + 1,
                            tx_state = {TX_STATE_PROCESSING}
                        WHERE id IN ({id_ph})
                    """, tuple(standalone_decoded_ids))
                    claimed_count += len(standalone_decoded_ids)

            # Step 4: Claim standalone detailed rows (remaining slots) using SKIP LOCKED
            remaining_slots = limit - claimed_count
            if remaining_slots > 0:
                self.cursor.execute(f"""
                    SELECT id FROM {STAGING_SCHEMA}.{STAGING_TABLE}
                    WHERE tx_state = %s
                      AND attempt_cnt < {MAX_DETAILED_ATTEMPTS}
                    ORDER BY priority DESC, created_utc ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                """, (detailed_state, remaining_slots))
                standalone_detailed_ids = [r['id'] for r in self.cursor.fetchall()]

                if standalone_detailed_ids:
                    id_ph = ','.join(['%s'] * len(standalone_detailed_ids))
                    self.cursor.execute(f"""
                        UPDATE {STAGING_SCHEMA}.{STAGING_TABLE}
                        SET priority = priority * 10 + 2,
                            tx_state = {TX_STATE_PROCESSING}
                        WHERE id IN ({id_ph})
                    """, tuple(standalone_detailed_ids))
                    claimed_count += len(standalone_detailed_ids)

            self.db_conn.commit()

        except Exception:
            self.db_conn.rollback()
            raise

        if claimed_count == 0:
            return []

        # Step 5: Fetch claimed rows, ordered so decoded comes before detailed per sig_hash
        self.cursor.execute(f"""
            SELECT id,
                   IF(priority MOD 10 = 1, %s, %s) as tx_state,
                   (priority DIV 10) as priority,
                   created_utc,
                   correlation_id,
                   sig_hash
            FROM {STAGING_SCHEMA}.{STAGING_TABLE}
            WHERE tx_state = {TX_STATE_PROCESSING}
            ORDER BY sig_hash, (priority MOD 10) ASC
        """, (decoded_state, detailed_state))

        return self.cursor.fetchall()

    def process_staging_row(self, staging_id, tx_state, priority):
        """
        Process a single staging row based on its tx_state.
        Updates staging row to shredded (4) on success, restores original state on error.
        """
        result = {
            'staging_id': staging_id,
            'tx_state': tx_state,
            'success': False,
            'tx_count': 0,
            'transfer_count': 0,
            'swap_count': 0,
            'activity_count': 0,
            'sol_balance_count': 0,
            'token_balance_count': 0,
            'skipped_count': 0,
            'participant_count': 0,
            'error': None
        }

        decoded_state = self.get_tx_state('decoded')
        detailed_state = self.get_tx_state('detailed')
        shredded_state = self.get_tx_state('shredded')

        try:
            if tx_state == decoded_state:
                self.cursor.execute(
                    "SET @tx=0, @xfer=0, @swap=0, @act=0, @skip=0"
                )
                self.cursor.execute(
                    "CALL sp_tx_parse_staging_decode(%s, @tx, @xfer, @swap, @act, @skip)",
                    (staging_id,)
                )
                self.cursor.execute("SELECT @tx, @xfer, @swap, @act, @skip")
                row = self.cursor.fetchone()
                if row:
                    result['tx_count'] = row['@tx'] or 0
                    result['transfer_count'] = row['@xfer'] or 0
                    result['swap_count'] = row['@swap'] or 0
                    result['activity_count'] = row['@act'] or 0
                    result['skipped_count'] = row['@skip'] or 0
                result['success'] = True

            elif tx_state == detailed_state:
                self.cursor.execute(
                    "SET @tx=0, @sol=0, @tok=0, @skip=0"
                )
                self.cursor.execute(
                    "CALL sp_tx_parse_staging_detail(%s, @tx, @sol, @tok, @skip)",
                    (staging_id,)
                )
                self.cursor.execute("SELECT @tx, @sol, @tok, @skip")
                row = self.cursor.fetchone()
                if row:
                    result['tx_count'] = row['@tx'] or 0
                    result['sol_balance_count'] = row['@sol'] or 0
                    result['token_balance_count'] = row['@tok'] or 0
                    result['skipped_count'] = row['@skip'] or 0
                if result['tx_count'] > 0 or result['sol_balance_count'] > 0 or result['token_balance_count'] > 0:
                    result['success'] = True
                else:
                    self.cursor.execute(f"""
                        SELECT attempt_cnt FROM {STAGING_SCHEMA}.{STAGING_TABLE} WHERE id = %s
                    """, (staging_id,))
                    attempt_row = self.cursor.fetchone()
                    current_attempts = (attempt_row['attempt_cnt'] if attempt_row else 0) + 1
                    if current_attempts >= MAX_DETAILED_ATTEMPTS:
                        result['error'] = f'All txs skipped - giving up after {current_attempts} attempts'
                    else:
                        result['error'] = f'All txs skipped (attempt {current_attempts}/{MAX_DETAILED_ATTEMPTS})'

            else:
                result['error'] = f'Unknown tx_state: {tx_state}'
                self.cursor.execute(f"""
                    UPDATE {STAGING_SCHEMA}.{STAGING_TABLE}
                    SET tx_state = %s, priority = %s
                    WHERE id = %s
                """, (self.get_tx_state('shredded'), priority, staging_id))

            if result['success']:
                self.cursor.execute(f"""
                    UPDATE {STAGING_SCHEMA}.{STAGING_TABLE}
                    SET tx_state = %s, priority = %s
                    WHERE id = %s
                """, (shredded_state, priority, staging_id))
            else:
                self.cursor.execute(f"""
                    UPDATE {STAGING_SCHEMA}.{STAGING_TABLE}
                    SET tx_state = IF(attempt_cnt + 1 >= {MAX_DETAILED_ATTEMPTS}, %s, %s),
                        priority = %s,
                        attempt_cnt = attempt_cnt + 1
                    WHERE id = %s
                """, (shredded_state, tx_state, priority, staging_id))

            self.db_conn.commit()

        except MySQLError as e:
            result['error'] = str(e)
            self.db_conn.rollback()
            try:
                self.cursor.execute(f"""
                    UPDATE {STAGING_SCHEMA}.{STAGING_TABLE}
                    SET tx_state = %s, priority = %s
                    WHERE id = %s
                """, (tx_state, priority, staging_id))
                self.db_conn.commit()
            except:
                pass

        return result

    def process_batch(self, limit=10, mq_channel=None):
        """
        Process a batch of staging rows.
        Returns summary of processing.
        """
        summary = {
            'processed': 0,
            'decoded_count': 0,
            'detailed_count': 0,
            'errors': 0,
            'results': [],
            'correlations_completed': []
        }

        rows = self.fetch_pending_staging_rows(limit)

        if not rows:
            return summary

        decoded_state = self.get_tx_state('decoded')
        detailed_state = self.get_tx_state('detailed')

        processed_correlations = set()

        for row in rows:
            staging_id = row['id']
            tx_state = row['tx_state']
            priority = row['priority']
            correlation_id = row.get('correlation_id')

            if correlation_id:
                processed_correlations.add(correlation_id)

            state_name = 'decoded' if tx_state == decoded_state else (
                'detailed' if tx_state == detailed_state else f'unknown({tx_state})'
            )

            if self.dry_run:
                log(self.tag, f"[DRY] Would process staging.id={staging_id} "
                      f"(state={state_name}, priority={priority})")
                summary['processed'] += 1
                continue

            result = self.process_staging_row(staging_id, tx_state, priority)
            summary['results'].append(result)

            if result['success']:
                summary['processed'] += 1
                if tx_state == decoded_state:
                    summary['decoded_count'] += 1
                    log(self.tag, f"staging.id={staging_id} (decoded): "
                          f"tx={result['tx_count']} xfer={result['transfer_count']} "
                          f"swap={result['swap_count']} act={result['activity_count']} "
                          f"skip={result['skipped_count']}")
                elif tx_state == detailed_state:
                    summary['detailed_count'] += 1
                    participant_info = f" part={result['participant_count']}" if result.get('participant_count') else ""
                    log(self.tag, f"staging.id={staging_id} (detailed): "
                          f"tx={result['tx_count']} sol={result['sol_balance_count']} "
                          f"tok={result['token_balance_count']} skip={result['skipped_count']}{participant_info}")
            else:
                summary['errors'] += 1
                log(self.tag, f"staging.id={staging_id}: ERROR - {result['error']}")

        if not self.dry_run:
            for corr_id in processed_correlations:
                stats = check_correlation_complete(self.cursor, corr_id, self._completed_correlations)
                if stats:
                    summary['correlations_completed'].append(corr_id)
                    publish_shredder_response(mq_channel, corr_id, stats)

        return summary


# =============================================================================
# Staging Cleanup (supervisor only)
# =============================================================================

def purge_completed_staging(cursor, conn, min_age_seconds=60):
    """
    Delete staging rows that have been fully processed (tx_state=shredded).
    Only deletes rows older than min_age_seconds to avoid racing with
    correlation completion checks.
    Returns number of staging rows deleted.
    """
    cursor.execute(f"""
        DELETE FROM {STAGING_SCHEMA}.{STAGING_TABLE}
        WHERE tx_state = %s
          AND created_utc < DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s SECOND)
        LIMIT 1000
    """, (TX_STATE_SHREDDED, min_age_seconds))
    deleted = cursor.rowcount
    conn.commit()
    return deleted


# =============================================================================
# Worker Thread
# =============================================================================

class WorkerThread(threading.Thread):
    def __init__(self, worker_id, batch_size, dry_run, stop_event,
                 poll_idle_sec, reconnect_sec):
        super().__init__(daemon=True)
        self.tag = f"W-{worker_id}"
        self.worker_id = worker_id
        self.batch_size = batch_size
        self.dry_run = dry_run
        self.stop_event = stop_event
        self.poll_idle_sec = poll_idle_sec
        self.reconnect_sec = reconnect_sec

    def run(self):
        log(self.tag, f"Starting (batch_size={self.batch_size})")
        db_conn = None
        mq_channel = None
        mq_conn = None

        while not self.stop_event.is_set():
            try:
                # Connect DB
                if db_conn is None:
                    db_conn = db_connect()
                    log(self.tag, "DB connected")

                # Connect RabbitMQ
                if mq_channel is None:
                    mq_conn, mq_channel = rmq_connect()
                    if mq_channel:
                        log(self.tag, "RabbitMQ connected")

                processor = ShredderProcessor(db_conn, self.dry_run, self.tag)

                # Process loop
                consecutive_empty = 0
                while not self.stop_event.is_set():
                    summary = processor.process_batch(self.batch_size, mq_channel)

                    if summary['processed'] > 0:
                        corr_info = f", corr_done={len(summary['correlations_completed'])}" if summary['correlations_completed'] else ""
                        log(self.tag, f"Batch: {summary['processed']} "
                              f"(dec={summary['decoded_count']}, det={summary['detailed_count']}, "
                              f"err={summary['errors']}{corr_info})")
                        consecutive_empty = 0
                        # Process more immediately if full batch
                        if summary['processed'] >= self.batch_size:
                            continue
                    else:
                        consecutive_empty += 1

                    # Adaptive sleep
                    sleep_time = self.poll_idle_sec if consecutive_empty < 10 else self.poll_idle_sec * 2
                    # Sleep in small increments so we can check stop_event
                    elapsed = 0.0
                    while elapsed < sleep_time and not self.stop_event.is_set():
                        time.sleep(min(0.5, sleep_time - elapsed))
                        elapsed += 0.5

            except MySQLError as e:
                log(self.tag, f"MySQL error: {e}, reconnecting in {self.reconnect_sec}s...")
                db_conn = None
                mq_channel = None
                mq_conn = None
                time.sleep(self.reconnect_sec)
            except Exception as e:
                log(self.tag, f"Unexpected error: {e}, retrying in {self.reconnect_sec}s...")
                import traceback
                traceback.print_exc()
                db_conn = None
                mq_channel = None
                mq_conn = None
                time.sleep(self.reconnect_sec)

        # Cleanup
        if db_conn:
            try:
                db_conn.close()
            except Exception:
                pass
        if mq_conn:
            try:
                mq_conn.close()
            except Exception:
                pass
        log(self.tag, "Stopped")


# =============================================================================
# Supervisor (config-driven daemon)
# =============================================================================

def read_config(cursor):
    cfg = load_db_config(cursor, 'queue')
    return {
        'threads':          cfg_int(cfg, 'shredder_wrk_cnt_threads', 1),
        'batch_size':       cfg_int(cfg, 'shredder_wrk_batch_size', 10),
        'supervisor_poll':  cfg_float(cfg, 'shredder_wrk_supervisor_poll_sec', 5.0),
        'poll_idle':        cfg_float(cfg, 'shredder_wrk_poll_idle_sec', 5.0),
        'reconnect':        cfg_float(cfg, 'shredder_wrk_reconnect_sec', 5.0),
        'shutdown_timeout': cfg_float(cfg, 'shredder_wrk_shutdown_timeout_sec', 10.0),
        'purge_every_n':    cfg_int(cfg, 'shredder_wrk_purge_every_n', 10),
        'purge_min_age':    cfg_int(cfg, 'shredder_wrk_purge_min_age_sec', 60),
    }


def run_supervisor(dry_run=False):
    print(f"""
+-----------------------------------------------------------+
|  Guide Shredder - Supervisor (config-driven)              |
+-----------------------------------------------------------+
""", flush=True)

    workers = {}       # worker_id -> (WorkerThread, stop_event)
    next_id = 1
    svr_conn = None
    svr_cursor = None
    poll_counter = 0

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
            svr_cursor = svr_conn.cursor()
            log('SVR', 'DB connected')
            return True
        except Exception as e:
            log('SVR', f'DB connect failed: {e}')
            return False

    cfg = {}

    try:
        while True:
            if not ensure_svr_db():
                time.sleep(5.0)
                continue

            cfg = read_config(svr_cursor)

            # Prune dead workers
            dead = [wid for wid, (w, _) in workers.items() if not w.is_alive()]
            for wid in dead:
                log('SVR', f'Worker W-{wid} died, removing')
                del workers[wid]

            active = len(workers)

            if active != cfg['threads']:
                log('SVR', f"Config: threads={cfg['threads']} batch_size={cfg['batch_size']} | active={active}")

            # Scale up
            while len(workers) < cfg['threads']:
                stop_evt = threading.Event()
                w = WorkerThread(
                    next_id, cfg['batch_size'], dry_run, stop_evt,
                    cfg['poll_idle'], cfg['reconnect'])
                w.start()
                workers[next_id] = (w, stop_evt)
                next_id += 1

            # Scale down (stop newest first)
            while len(workers) > cfg['threads']:
                wid = max(workers.keys())
                wthread, stop_evt = workers.pop(wid)
                log('SVR', f'Stopping W-{wid}...')
                stop_evt.set()

            # Periodic purge (supervisor handles this, not workers)
            poll_counter += 1
            if cfg['purge_every_n'] > 0 and poll_counter % cfg['purge_every_n'] == 0:
                try:
                    purged = purge_completed_staging(svr_cursor, svr_conn, cfg['purge_min_age'])
                    if purged > 0:
                        log('SVR', f'Purged {purged} completed staging rows')
                except Exception as e:
                    log('SVR', f'Purge error: {e}')
                    svr_conn = None  # Force reconnect

            time.sleep(cfg['supervisor_poll'])

    except KeyboardInterrupt:
        log('SVR', 'Shutting down...')

    shutdown_timeout = cfg.get('shutdown_timeout', 10.0)

    # Signal all workers to stop
    for wid, (w, stop_evt) in workers.items():
        stop_evt.set()

    # Wait for graceful exit
    for wid, (w, _) in workers.items():
        w.join(timeout=shutdown_timeout)
        if w.is_alive():
            log('SVR', f'W-{wid} did not stop in time')

    if svr_conn:
        try:
            svr_conn.close()
        except Exception:
            pass

    log('SVR', 'Shutdown complete')


# =============================================================================
# Single-run mode (unchanged)
# =============================================================================

def run_once(batch_size=100, dry_run=False):
    """Process all pending staging rows once and exit"""
    print(f"""
+-----------------------------------------------------------+
|         Guide Shredder - Single Run Mode                  |
+-----------------------------------------------------------+
""")
    if dry_run:
        print("MODE: DRY RUN\n")

    try:
        conn = db_connect()
        processor = ShredderProcessor(conn, dry_run, 'ONCE')
        log('ONCE', 'Database connected')

        total_processed = 0
        total_decoded = 0
        total_detailed = 0
        total_errors = 0

        while True:
            summary = processor.process_batch(batch_size)

            if summary['processed'] == 0:
                break

            total_processed += summary['processed']
            total_decoded += summary['decoded_count']
            total_detailed += summary['detailed_count']
            total_errors += summary['errors']

        # Purge all completed staging rows
        if not dry_run:
            total_purged = 0
            while True:
                purged = purge_completed_staging(processor.cursor, conn, min_age_seconds=0)
                total_purged += purged
                if purged < 1000:
                    break
            if total_purged > 0:
                log('ONCE', f'Purged {total_purged} completed staging rows')

        log('ONCE', f"Total processed: {total_processed} "
              f"(decoded={total_decoded}, detailed={total_detailed}, errors={total_errors})")

        conn.close()

    except MySQLError as e:
        log('ONCE', f"MySQL error: {e}")
        return 1

    return 0


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Guide Shredder - Forward-only staging table processor'
    )
    parser.add_argument('--daemon', action='store_true',
                        help='Run as daemon (supervisor + worker threads)')
    parser.add_argument('--once', action='store_true',
                        help='Process all pending rows once and exit')
    parser.add_argument('--batch-size', type=int, default=None,
                        help='Override batch size (--once mode only; daemon uses config table)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview only, no DB changes')

    args = parser.parse_args()

    if not HAS_MYSQL:
        print("Error: mysql-connector-python not installed")
        return 1

    if args.once:
        return run_once(
            batch_size=args.batch_size or 100,
            dry_run=args.dry_run
        )
    else:
        # Default to daemon/supervisor mode (--daemon flag optional)
        return run_supervisor(dry_run=args.dry_run)


if __name__ == '__main__':
    exit(main())
