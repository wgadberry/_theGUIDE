#!/usr/bin/env python3
"""
Guide Gateway - Central orchestration service for theGuide Pipeline
Provides REST API and queue-based routing for all pipeline workers.

Features:
- REST API endpoints for external clients
- Queue-based inbound routing for internal cascades
- API key authentication and validation
- Per-API-key rate limiting (configurable requests/minute)
- Request logging to tx_request_log
- Worker request routing with priority support
- REST API defaults to priority 8; CLI/queue defaults to 5

Architecture:
    [External Clients] --REST API--> [Gateway] --> [Worker Queues]
    [Workers] --Cascade Queue--> [Gateway] --> [Downstream Workers]

Usage:
    # Start gateway server
    python guide-gateway.py

    # Start with custom port
    python guide-gateway.py --port 5100

    # Start with queue consumer for cascades
    python guide-gateway.py --with-queue-consumer

    # Start with response consumer for auto-cascade routing
    python guide-gateway.py --with-response-consumer

    # Full production mode (REST + queue + response consumers)
    python guide-gateway.py --with-queue-consumer --with-response-consumer

    # Debug mode
    python guide-gateway.py --debug

API Endpoints:
    POST /api/trigger/<worker>   - Trigger a worker with request payload
    GET  /api/status/<request_id> - Get status of a request
    GET  /api/workers             - List available workers
    GET  /api/health              - Health check
"""

import argparse
import json
import os
import sys
import uuid
import hashlib
import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

# Flask for REST API
try:
    from flask import Flask, request, jsonify
    HAS_FLASK = True
except ImportError:
    HAS_FLASK = False

# RabbitMQ client
try:
    import pika
    HAS_PIKA = True
except ImportError:
    HAS_PIKA = False

# MySQL connector
try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
    from mysql.connector.pooling import MySQLConnectionPool
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

# =============================================================================
# Static config (from common.config → guide-config.json)
# =============================================================================

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from t16o_exchange.guide.common.config import (
    get_db_config, get_rabbitmq_config, get_queue_names,
    nack_with_retry,
)

_rmq = get_rabbitmq_config()
_queues = get_queue_names('gateway')

DB_CONFIG = get_db_config()

# Connection pool — eliminates per-request TCP connect/close overhead.
# Pool size 32 handles ~200 concurrent requests comfortably (most DB calls
# are <10ms so connections recycle fast). Connections return to pool on .close().
DB_POOL = None
DB_POOL_SIZE = 32
if HAS_MYSQL:
    try:
        DB_POOL = MySQLConnectionPool(
            pool_name="gateway",
            pool_size=DB_POOL_SIZE,
            pool_reset_session=False,  # skip SET SESSION on each get — faster
            **DB_CONFIG
        )
        print(f"[POOL] MySQL connection pool created (size={DB_POOL_SIZE})")
    except Exception as e:
        print(f"[POOL] Failed to create connection pool: {e}, falling back to per-request connections")
        DB_POOL = None

RABBITMQ_CONFIG = {
    'host':     _rmq['host'],
    'port':     _rmq['port'],
    'user':     _rmq['user'],
    'password': _rmq['password'],
    'vhost':    _rmq['vhost'],
}

# Gateway
GATEWAY_PORT = 5100
GATEWAY_HOST = '0.0.0.0'

# Queue names
GATEWAY_REQUEST_QUEUE = _queues['request']
GATEWAY_RESPONSE_QUEUE = _queues['response']
GATEWAY_DLQ = _queues['dlq']

# Correlation tracking for pipeline completion detection
# Structure: {correlation_id: {
#     'expected_batches': int,        # Total batches from producer (0 until producer responds)
#     'expected_workers': ['decoder', 'detailer'],  # Workers that should respond per batch
#     'received': {'decoder': set(), 'detailer': set()},  # batch_nums received per worker
#     'started_at': datetime,
#     'producer_done': bool
# }}
_correlation_tracker: Dict[str, Dict] = {}
_tracker_lock = threading.Lock()

# Default downstream workers for producer (decoder + detailer fetch, then shredder processes)
# Shredder is the final stage - pipeline is complete when shredder finishes
DEFAULT_DOWNSTREAM_WORKERS = ['decoder', 'detailer', 'shredder']

# =============================================================================
# Feature Flags (bitmask constants)
# =============================================================================
# These control what data is collected during transaction processing.
# Core features (always collected) have no flag - they're the default.
# These flags ENABLE additional data collection beyond core.

# Billable feature flags (à la carte)
FEATURE_BALANCE_CHANGES   = 0x0001  # All balance changes for all participants
FEATURE_ALL_ADDRESSES     = 0x0002  # All addresses (ATAs, vaults, intermediate)
FEATURE_SWAP_ROUTING      = 0x0004  # Full swap routing paths
FEATURE_ATA_MAPPING       = 0x0008  # Associated token account mappings
FEATURE_FUNDER_DISCOVERY  = 0x0010  # Solscan API funder lookups

# Core features (always enabled, not gated):
# - Token metadata enrichment (background daemon)
# - Address labels/tags (background enrichment)
# - Program details (collected in activities)


def _get_or_create_tracker(correlation_id: str) -> Dict:
    """Get existing tracker or create a new one (must be called with lock held)"""
    if correlation_id not in _correlation_tracker:
        _correlation_tracker[correlation_id] = {
            'expected_batches': 0,  # Unknown until producer responds
            'expected_workers': DEFAULT_DOWNSTREAM_WORKERS,
            'received': {w: set() for w in DEFAULT_DOWNSTREAM_WORKERS},
            'started_at': datetime.now(),
            'producer_done': False
        }
    return _correlation_tracker[correlation_id]

def record_batch_response(correlation_id: str, worker: str, batch_num: int) -> Optional[Dict]:
    """Record a batch response and return completion status if pipeline is done"""
    with _tracker_lock:
        tracker = _get_or_create_tracker(correlation_id)

        if worker in tracker['received']:
            tracker['received'][worker].add(batch_num)

        # Can only check completion if producer has reported expected batch count
        if not tracker['producer_done'] or tracker['expected_batches'] == 0:
            return None

        # Check if all workers have completed
        # - decoder/detailer: must have expected_batches responses (one per batch)
        # - shredder: needs only 1 response (processes all staging as one unit)
        expected = tracker['expected_batches']
        all_complete = True
        for w in tracker['expected_workers']:
            received = len(tracker['received'].get(w, set()))
            if w == 'shredder':
                # Shredder sends ONE response when all staging rows are done
                if received < 1:
                    all_complete = False
                    break
            else:
                # decoder/detailer send one response per batch
                if received < expected:
                    all_complete = False
                    break

        if all_complete:
            # Pipeline complete - remove from tracker and return summary
            elapsed = (datetime.now() - tracker['started_at']).total_seconds()
            total_received = sum(len(tracker['received'].get(w, set())) for w in tracker['expected_workers'])
            summary = {
                'correlation_id': correlation_id,
                'batches': expected,
                'workers': tracker['expected_workers'],
                'total_responses': total_received,
                'elapsed_seconds': round(elapsed, 2)
            }
            del _correlation_tracker[correlation_id]
            return summary

        return None

def mark_producer_done(correlation_id: str, total_batches: int) -> Optional[Dict]:
    """Mark producer as done and check if pipeline is already complete"""
    with _tracker_lock:
        tracker = _get_or_create_tracker(correlation_id)
        tracker['producer_done'] = True
        tracker['expected_batches'] = total_batches

        # Check if downstream workers already finished before producer response arrived
        if total_batches == 0:
            # No batches sent, pipeline is complete
            del _correlation_tracker[correlation_id]
            return {
                'correlation_id': correlation_id,
                'batches': 0,
                'workers': tracker['expected_workers'],
                'total_responses': 0,
                'elapsed_seconds': 0
            }

        # Check if all workers have completed (same logic as record_batch_response)
        expected = tracker['expected_batches']
        all_complete = True
        for w in tracker['expected_workers']:
            received = len(tracker['received'].get(w, set()))
            if w == 'shredder':
                if received < 1:
                    all_complete = False
                    break
            else:
                if received < expected:
                    all_complete = False
                    break

        if all_complete:
            # Already complete! All workers finished before producer response
            elapsed = (datetime.now() - tracker['started_at']).total_seconds()
            total_received = sum(len(tracker['received'].get(w, set())) for w in tracker['expected_workers'])
            summary = {
                'correlation_id': correlation_id,
                'batches': expected,
                'workers': tracker['expected_workers'],
                'total_responses': total_received,
                'elapsed_seconds': round(elapsed, 2)
            }
            del _correlation_tracker[correlation_id]
            return summary

        # Not complete yet, log progress
        received_counts = {w: len(tracker['received'].get(w, set())) for w in tracker['expected_workers']}
        print(f"[TRACK] {correlation_id[:8]}: expecting {total_batches} batches, received so far: {received_counts}")
        return None

# Worker registry with queue mappings (derived from common.config)
def _build_worker_registry():
    workers = {
        'producer':   {'description': 'Fetches transaction signatures from RPC',     'cascade_to': []},
        'decoder':    {'description': 'Fetches decoded transaction actions from Solscan', 'cascade_to': []},
        'detailer':   {'description': 'Enriches transactions with balance change details', 'cascade_to': []},
        'funder':     {'description': 'Discovers funding relationships for addresses', 'cascade_to': []},
        'aggregator': {'description': 'Syncs funding edges and guide data',          'cascade_to': ['enricher']},
        'enricher':   {'description': 'Enriches token and pool metadata',            'cascade_to': []},
        'shredder':      {'description': 'Processes staged data into tx tables',        'cascade_to': []},
        'synchronizer':  {'description': 'Fills gaps in mint transaction history',      'cascade_to': []},
    }
    for name, info in workers.items():
        q = get_queue_names(name)
        info['request_queue']  = q['request']
        info['response_queue'] = q['response']
        info['dlq']            = q['dlq']
    return workers

WORKER_REGISTRY = _build_worker_registry()

# =============================================================================
# Database Functions
# =============================================================================

def get_db_connection():
    """Get a database connection from the pool (or create one if pool unavailable).
    Caller must call conn.close() when done — this returns it to the pool."""
    if DB_POOL:
        return DB_POOL.get_connection()
    return mysql.connector.connect(**DB_CONFIG)


# =============================================================================
# API Key Cache (60-second TTL)
# =============================================================================

API_KEY_CACHE = {}  # {api_key: {'data': {...}, 'timestamp': time.time()}}
API_KEY_CACHE_TTL = 60  # seconds
API_KEY_CACHE_LOCK = threading.Lock()

# =============================================================================
# Rate Limiting (sliding window per API key)
# =============================================================================

RATE_LIMIT_TRACKER = {}  # {api_key_id: {'requests': [(timestamp, count), ...], 'window_start': time}}
RATE_LIMIT_LOCK = threading.Lock()
RATE_LIMIT_WINDOW_SECONDS = 60  # 1-minute sliding window


# Default priority for REST API calls (higher than CLI default of 5)
REST_API_DEFAULT_PRIORITY = 5


def check_rate_limit(api_key_id: int, rate_limit: int) -> Tuple[bool, Optional[int]]:
    """
    Check if an API key has exceeded its rate limit.

    Args:
        api_key_id: The API key's database ID
        rate_limit: Maximum requests allowed per window (0 = unlimited)

    Returns:
        Tuple of (allowed: bool, retry_after_seconds: Optional[int])
        If allowed is False, retry_after_seconds indicates when to retry
    """
    if not rate_limit or rate_limit <= 0:
        return True, None  # No rate limit configured

    current_time = time.time()
    window_start = current_time - RATE_LIMIT_WINDOW_SECONDS

    with RATE_LIMIT_LOCK:
        if api_key_id not in RATE_LIMIT_TRACKER:
            RATE_LIMIT_TRACKER[api_key_id] = {'requests': []}

        tracker = RATE_LIMIT_TRACKER[api_key_id]

        # Remove requests outside the sliding window
        tracker['requests'] = [ts for ts in tracker['requests'] if ts > window_start]

        # Count requests in current window
        request_count = len(tracker['requests'])

        if request_count >= rate_limit:
            # Rate limit exceeded - calculate retry time
            if tracker['requests']:
                oldest_in_window = min(tracker['requests'])
                retry_after = int(oldest_in_window - window_start) + 1
            else:
                retry_after = RATE_LIMIT_WINDOW_SECONDS
            return False, retry_after

        # Allow request and record it
        tracker['requests'].append(current_time)
        return True, None


def get_rate_limit_status(api_key_id: int, rate_limit: int) -> dict:
    """Get current rate limit status for an API key"""
    if not rate_limit or rate_limit <= 0:
        return {'limited': False, 'limit': None, 'remaining': None, 'reset_seconds': None}

    current_time = time.time()
    window_start = current_time - RATE_LIMIT_WINDOW_SECONDS

    with RATE_LIMIT_LOCK:
        if api_key_id not in RATE_LIMIT_TRACKER:
            return {'limited': False, 'limit': rate_limit, 'remaining': rate_limit, 'reset_seconds': 0}

        tracker = RATE_LIMIT_TRACKER[api_key_id]
        requests_in_window = [ts for ts in tracker['requests'] if ts > window_start]
        remaining = max(0, rate_limit - len(requests_in_window))

        if requests_in_window:
            oldest = min(requests_in_window)
            reset_seconds = int((oldest + RATE_LIMIT_WINDOW_SECONDS) - current_time)
        else:
            reset_seconds = 0

        return {
            'limited': remaining == 0,
            'limit': rate_limit,
            'remaining': remaining,
            'reset_seconds': max(0, reset_seconds)
        }


def validate_api_key(api_key: str) -> Optional[Dict]:
    """
    Validate API key and return key details if valid.
    Uses in-memory cache with 60-second TTL to reduce DB hits.
    """
    if not HAS_MYSQL:
        return {'id': 0, 'name': 'No-Auth', 'permissions': {'workers': ['*'], 'actions': ['*']}}

    current_time = time.time()

    # Check cache first
    with API_KEY_CACHE_LOCK:
        if api_key in API_KEY_CACHE:
            cached = API_KEY_CACHE[api_key]
            age = current_time - cached['timestamp']
            if age < API_KEY_CACHE_TTL:
                return cached['data']
            # Cache expired, will re-query below

    # Query database
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT id, name, permissions, rate_limit, active, feature_mask
            FROM tx_api_key
            WHERE api_key = %s AND active = 1
        """, (api_key,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result and result['permissions']:
            result['permissions'] = json.loads(result['permissions']) if isinstance(result['permissions'], str) else result['permissions']

        # Update cache (even if None - caches invalid keys too)
        with API_KEY_CACHE_LOCK:
            API_KEY_CACHE[api_key] = {
                'data': result,
                'timestamp': current_time
            }

        return result
    except Exception as e:
        print(f"[ERROR] API key validation failed: {e}")
        return None


def check_permission(key_info: Dict, worker: str, action: str) -> bool:
    """Check if API key has permission for worker and action"""
    if not key_info or not key_info.get('permissions'):
        return False

    perms = key_info['permissions']
    workers = perms.get('workers', [])
    actions = perms.get('actions', [])

    worker_ok = '*' in workers or worker in workers
    action_ok = '*' in actions or action in actions

    return worker_ok and action_ok


def log_request(
    request_id: str,
    api_key_id: Optional[int],
    source: str,
    target_worker: str,
    action: str,
    priority: int = 5,
    payload: Optional[Dict] = None,
    correlation_id: Optional[str] = None,
    status: str = 'queued',
    error: Optional[str] = None,
    features: int = 0
) -> Optional[int]:
    """Log a request to tx_request_log

    Args:
        correlation_id: Original REST request ID that flows through entire cascade chain.
                       For REST requests, this equals request_id.
                       For cascades, this is the original parent's request_id.
        status: Request status (queued, rejected, failed, completed)
        error: Error message if status is rejected/failed
        features: Bitmask of enabled features for this request

    Returns:
        The inserted tx_request_log.id, or None on failure
    """
    if not HAS_MYSQL:
        return 0  # Return 0 instead of None when MySQL not available

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        payload_hash = None
        payload_summary = None
        if payload:
            payload_str = json.dumps(payload, sort_keys=True)
            payload_hash = hashlib.sha256(payload_str.encode()).hexdigest()
            # Extract summary fields
            payload_summary = {
                'address_sig_cnt': payload.get('batch', {}).get('address_sig_cnt'),
                'filters': payload.get('batch', {}).get('filters'),
                'action': action
            }
            if error:
                payload_summary['error'] = error

        # Ensure correlation_id is set (default to request_id for REST requests)
        effective_correlation_id = correlation_id if correlation_id else request_id

        # Check if request already exists (for idempotent handling of retries)
        if api_key_id is not None:
            cursor.execute("""
                SELECT id, status, result FROM tx_request_log
                WHERE request_id = %s AND target_worker = %s AND api_key_id = %s
            """, (request_id, target_worker, api_key_id))
        else:
            cursor.execute("""
                SELECT id, status, result FROM tx_request_log
                WHERE request_id = %s AND target_worker = %s AND api_key_id IS NULL
            """, (request_id, target_worker))

        existing = cursor.fetchone()
        if existing:
            print(f"[LOG] Request {request_id[:8]} already exists (id: {existing['id']}, status: {existing['status']})", flush=True)
            cursor.close()
            conn.close()
            # Return negative ID to indicate duplicate (caller can check and handle appropriately)
            return -existing['id']

        cursor.execute("""
            INSERT INTO tx_request_log
            (request_id, correlation_id, api_key_id, source, target_worker, action, priority, features, payload_hash, payload_summary, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            request_id,
            effective_correlation_id,
            api_key_id,
            source,
            target_worker,
            action,
            priority,
            features,
            payload_hash,
            json.dumps(payload_summary) if payload_summary else None,
            status
        ))
        inserted_id = cursor.lastrowid
        print(f"[LOG] Request {request_id[:8]} logged as {status} (id: {inserted_id}, correlation: {effective_correlation_id[:8]})", flush=True)
        conn.commit()
        cursor.close()
        conn.close()
        return inserted_id
    except Exception as e:
        print(f"[ERROR] Failed to log request: {e}", flush=True)
        return None


def update_request_status(
    request_id: str,
    status: str,
    error_message: Optional[str] = None,
    result: Optional[Dict] = None
):
    """Update request status in tx_request_log"""
    if not HAS_MYSQL:
        return

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if status == 'processing':
            cursor.execute("""
                UPDATE tx_request_log
                SET status = %s, started_at = CURRENT_TIMESTAMP(3)
                WHERE request_id = %s
            """, (status, request_id))
        elif status in ('completed', 'failed', 'timeout'):
            cursor.execute("""
                UPDATE tx_request_log
                SET status = %s, completed_at = CURRENT_TIMESTAMP(3),
                    error_message = %s, result = %s
                WHERE request_id = %s
            """, (status, error_message, json.dumps(result) if result else None, request_id))
        else:
            cursor.execute("""
                UPDATE tx_request_log SET status = %s WHERE request_id = %s
            """, (status, request_id))

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"[ERROR] Failed to update request status: {e}")


def get_request_status(request_id: str) -> Optional[Dict]:
    """Get request status from tx_request_log, aggregated by worker

    Since a single request_id can have multiple records (one per worker in the cascade),
    this returns an aggregated view with per-worker status.
    """
    if not HAS_MYSQL:
        return None

    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Get all records for this request_id
        cursor.execute("""
            SELECT request_id, correlation_id, source, target_worker, action, priority,
                   status, error_message, created_utc, started_at, completed_at,
                   duration_ms, result, payload_summary
            FROM tx_request_log
            WHERE request_id = %s
            ORDER BY created_utc ASC
        """, (request_id,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        if not rows:
            return None

        # Build aggregated response
        first_row = rows[0]
        workers = {}
        overall_status = 'completed'
        total_pending = 0
        total_failed = 0

        for row in rows:
            worker = row['target_worker']

            # Convert datetime objects
            for key in ['created_utc', 'started_at', 'completed_at']:
                if row.get(key):
                    row[key] = row[key].isoformat()

            # Parse result JSON
            result_data = None
            if row.get('result'):
                result_data = json.loads(row['result']) if isinstance(row['result'], str) else row['result']

            # Parse payload_summary for batch info
            batch_num = None
            if row.get('payload_summary'):
                summary = json.loads(row['payload_summary']) if isinstance(row['payload_summary'], str) else row['payload_summary']
                batch_info = summary.get('filters', {})
                if isinstance(batch_info, dict):
                    batch_num = batch_info.get('batch_num')

            # Track per-worker status
            if worker not in workers:
                workers[worker] = {
                    'status': row['status'],
                    'source': row['source'],
                    'action': row['action'],
                    'records': [],
                    'completed': 0,
                    'failed': 0,
                    'pending': 0
                }

            workers[worker]['records'].append({
                'status': row['status'],
                'batch_num': batch_num,
                'created_utc': row['created_utc'],
                'completed_at': row['completed_at'],
                'duration_ms': row['duration_ms'],
                'error_message': row.get('error_message'),
                'result': result_data
            })

            # Update counts
            if row['status'] == 'completed':
                workers[worker]['completed'] += 1
            elif row['status'] == 'failed':
                workers[worker]['failed'] += 1
                total_failed += 1
            elif row['status'] in ('queued', 'processing'):
                workers[worker]['pending'] += 1
                total_pending += 1

            # Update worker-level status
            if row['status'] == 'failed':
                workers[worker]['status'] = 'failed'
            elif row['status'] in ('queued', 'processing') and workers[worker]['status'] != 'failed':
                workers[worker]['status'] = 'processing'

        # Determine overall status
        if total_pending > 0:
            overall_status = 'processing'
        elif total_failed > 0:
            overall_status = 'partial'
        else:
            overall_status = 'completed'

        return {
            'request_id': request_id,
            'correlation_id': first_row['correlation_id'],
            'priority': first_row['priority'],
            'overall_status': overall_status,
            'created_utc': first_row['created_utc'] if isinstance(first_row['created_utc'], str) else first_row['created_utc'].isoformat() if first_row['created_utc'] else None,
            'workers': workers
        }
    except Exception as e:
        print(f"[ERROR] Failed to get request status: {e}")
        return None


def get_correlation_status(correlation_id: str) -> Optional[Dict]:
    """Get aggregated status for all requests in a correlation chain"""
    if not HAS_MYSQL:
        return None

    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Get all requests for this correlation
        cursor.execute("""
            SELECT target_worker, COUNT(*) as count,
                   SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                   SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                   SUM(CASE WHEN status IN ('queued', 'processing') THEN 1 ELSE 0 END) as pending
            FROM tx_request_log
            WHERE correlation_id = %s
            GROUP BY target_worker
        """, (correlation_id,))
        worker_stats = cursor.fetchall()

        # Get overall progress
        cursor.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                   SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                   SUM(CASE WHEN status IN ('queued', 'processing') THEN 1 ELSE 0 END) as pending,
                   MIN(created_utc) as started_at,
                   MAX(completed_at) as last_completed_at
            FROM tx_request_log
            WHERE correlation_id = %s
        """, (correlation_id,))
        overall = cursor.fetchone()

        cursor.close()
        conn.close()

        if not overall or overall['total'] == 0:
            return None

        # Determine overall status
        if overall['pending'] > 0:
            overall_status = 'processing'
        elif overall['failed'] > 0:
            overall_status = 'partial'
        else:
            overall_status = 'completed'

        # Build progress dict by worker
        progress = {}
        for stat in worker_stats:
            worker = stat['target_worker']
            progress[worker] = {
                'total': stat['count'],
                'completed': stat['completed'],
                'failed': stat['failed'],
                'pending': stat['pending']
            }

        # Convert datetime
        if overall.get('started_at'):
            overall['started_at'] = overall['started_at'].isoformat()
        if overall.get('last_completed_at'):
            overall['last_completed_at'] = overall['last_completed_at'].isoformat()

        return {
            'correlation_id': correlation_id,
            'overall_status': overall_status,
            'total_requests': overall['total'],
            'completed': overall['completed'],
            'failed': overall['failed'],
            'pending': overall['pending'],
            'started_at': overall.get('started_at'),
            'last_completed_at': overall.get('last_completed_at'),
            'progress': progress
        }
    except Exception as e:
        print(f"[ERROR] Failed to get correlation status: {e}")
        return None


# =============================================================================
# RabbitMQ Functions
# =============================================================================

def get_rabbitmq_connection():
    """Create a RabbitMQ connection"""
    credentials = pika.PlainCredentials(RABBITMQ_CONFIG['user'], RABBITMQ_CONFIG['password'])
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_CONFIG['host'],
        port=RABBITMQ_CONFIG['port'],
        virtual_host=RABBITMQ_CONFIG['vhost'],
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    return pika.BlockingConnection(parameters)


def ensure_queues_exist(channel):
    """Ensure all gateway and worker queues exist with dead-letter routing"""
    # Gateway DLQ (must exist before request queue references it)
    channel.queue_declare(
        queue=GATEWAY_DLQ, durable=True,
        arguments={'x-max-priority': 10, 'x-message-ttl': 86400000}
    )

    # Gateway queues — request queue routes rejected messages to DLQ
    channel.queue_declare(
        queue=GATEWAY_REQUEST_QUEUE, durable=True,
        arguments={
            'x-max-priority': 10,
            'x-dead-letter-exchange': '',
            'x-dead-letter-routing-key': GATEWAY_DLQ,
        }
    )
    channel.queue_declare(
        queue=GATEWAY_RESPONSE_QUEUE, durable=True,
        arguments={'x-max-priority': 10}
    )

    # Worker queues
    for worker, config in WORKER_REGISTRY.items():
        # DLQ first (must exist before request queue references it)
        channel.queue_declare(
            queue=config['dlq'], durable=True,
            arguments={'x-max-priority': 10, 'x-message-ttl': 86400000}
        )
        # Request queue with dead-letter routing to DLQ
        channel.queue_declare(
            queue=config['request_queue'], durable=True,
            arguments={
                'x-max-priority': 10,
                'x-dead-letter-exchange': '',
                'x-dead-letter-routing-key': config['dlq'],
            }
        )
        # Response queue (no DLQ — responses are informational)
        channel.queue_declare(
            queue=config['response_queue'], durable=True,
            arguments={'x-max-priority': 10}
        )


# =============================================================================
# Persistent RabbitMQ publish connection (thread-safe)
# =============================================================================
# Single connection + channel reused across all Flask threads.
# pika.BlockingConnection is NOT thread-safe, so all publishes go through a lock.
# This eliminates per-request connection setup (~15 queue_declares + TCP handshake).

_publish_conn = None
_publish_channel = None
_publish_lock = threading.Lock()
_queues_declared = False


def _ensure_publish_channel():
    """Get or create the persistent publish channel. Must be called under _publish_lock."""
    global _publish_conn, _publish_channel, _queues_declared

    needs_reconnect = (
        _publish_conn is None or
        _publish_conn.is_closed or
        _publish_channel is None or
        _publish_channel.is_closed
    )

    if needs_reconnect:
        # Close stale connection if any
        if _publish_conn and not _publish_conn.is_closed:
            try:
                _publish_conn.close()
            except Exception:
                pass

        _publish_conn = get_rabbitmq_connection()
        _publish_channel = _publish_conn.channel()
        _queues_declared = False
        print("[RMQ] Publish connection established", flush=True)

    if not _queues_declared:
        ensure_queues_exist(_publish_channel)
        _queues_declared = True
        print("[RMQ] Queues declared", flush=True)

    return _publish_channel


def _publish_message(routing_key: str, message: Dict, priority: int = 5) -> bool:
    """Thread-safe publish to any queue via the persistent channel."""
    correlation_id = message.get('correlation_id')
    request_id = message.get('request_id')
    body = json.dumps(message).encode('utf-8')
    props = pika.BasicProperties(
        delivery_mode=2,
        content_type='application/json',
        priority=priority,
        correlation_id=correlation_id,
        message_id=request_id
    )

    with _publish_lock:
        try:
            ch = _ensure_publish_channel()
            ch.basic_publish(exchange='', routing_key=routing_key, body=body, properties=props)
            return True
        except Exception as e:
            # Connection dropped — reset and retry once
            global _publish_conn, _publish_channel, _queues_declared
            _publish_conn = None
            _publish_channel = None
            _queues_declared = False
            try:
                ch = _ensure_publish_channel()
                ch.basic_publish(exchange='', routing_key=routing_key, body=body, properties=props)
                return True
            except Exception as e2:
                print(f"[ERROR] RMQ publish failed after retry: {e2}", flush=True)
                return False


def publish_to_worker(worker: str, message: Dict, priority: int = 5) -> bool:
    """Publish a message to a worker's request queue"""
    if worker not in WORKER_REGISTRY:
        return False
    return _publish_message(WORKER_REGISTRY[worker]['request_queue'], message, priority)


def publish_to_gateway(message: Dict, priority: int = 5) -> bool:
    """Publish a cascade message to gateway request queue"""
    return _publish_message(GATEWAY_REQUEST_QUEUE, message, priority)


# =============================================================================
# Request Processing
# =============================================================================

def process_request(
    worker: str,
    action: str,
    payload: Dict,
    api_key: str,
    source: str = 'rest',
    correlation_id: Optional[str] = None,
    request_id: Optional[str] = None,
    request_log_id: Optional[int] = None,
    features: int = 0
) -> Dict:
    """Process a request: validate, log, and route to worker

    Args:
        correlation_id: For REST requests, this is None and will be set to request_id.
                       For cascades, this is passed from parent to track the original request.
        request_id: Optional request ID to use. If not provided, a new UUID is generated.
                   This allows queue messages to preserve their original request_id.
        request_log_id: The tx_request_log.id of the "gateway" record for billing.
                       This is passed through the cascade chain to link tx records.
        features: Bitmask of enabled features for this request.
    """
    if not request_id:
        request_id = str(uuid.uuid4())
    timestamp = datetime.now().isoformat() + 'Z'

    # For REST requests, correlation_id = request_id (start of chain)
    # For cascades, correlation_id is passed from parent
    effective_correlation_id = correlation_id or request_id

    # Validate API key
    key_info = validate_api_key(api_key)
    if not key_info:
        return {
            'success': False,
            'error': 'Invalid or inactive API key',
            'request_id': request_id,
            'correlation_id': effective_correlation_id
        }

    # Check permissions
    if not check_permission(key_info, worker, action):
        return {
            'success': False,
            'error': f'Permission denied for worker={worker}, action={action}',
            'request_id': request_id,
            'correlation_id': effective_correlation_id
        }

    # Build request message
    priority = payload.get('priority', 5)
    message = {
        'request_id': request_id,
        'correlation_id': effective_correlation_id,  # Pass through cascade chain
        'request_log_id': request_log_id,  # For billing - links tx records to gateway request
        'api_key': api_key,
        'api_key_id': key_info.get('id'),  # For billing tracking through cascade
        'features': features,  # Feature flags for data collection
        'priority': priority,
        'timestamp': timestamp,
        'action': action,
        'batch': payload.get('batch', {}),
        'sync': payload.get('sync', {}),
    }

    # Log request for this worker (not the gateway record)
    log_request(
        request_id=request_id,
        api_key_id=key_info.get('id'),
        source=source,
        target_worker=worker,
        action=action,
        priority=priority,
        payload=message,
        correlation_id=effective_correlation_id,
        features=features
    )

    # Route to worker
    if publish_to_worker(worker, message, priority):
        return {
            'success': True,
            'request_id': request_id,
            'correlation_id': effective_correlation_id,
            'request_log_id': request_log_id,
            'worker': worker,
            'action': action,
            'queued_at': timestamp
        }
    else:
        update_request_status(request_id, 'failed', 'Failed to queue message')
        return {
            'success': False,
            'error': 'Failed to queue request',
            'request_id': request_id,
            'correlation_id': effective_correlation_id
        }


def process_cascade(message: Dict) -> List[Dict]:
    """Process a cascade message from a worker, route to downstream workers

    The original request_id from the calling API is preserved throughout the cascade chain.
    The target_worker field differentiates stages, and batch info is stored in payload_summary.
    This allows the caller to query /api/status/{request_id} and see progress across all workers.
    The request_log_id (gateway record) is also passed through for billing linkage.
    Features are also preserved through the cascade chain.
    """
    results = []
    source_worker = message.get('source_worker')
    source_request_id = message.get('source_request_id')
    correlation_id = message.get('correlation_id')  # Preserve original request chain
    request_log_id = message.get('request_log_id')  # Preserve for billing
    features = message.get('features', 31)  # Preserve feature flags
    targets = message.get('targets', [])
    batch = message.get('batch', {})
    api_key = message.get('api_key', 'internal_cascade_key')
    priority = message.get('priority', 5)

    for target in targets:
        if target not in WORKER_REGISTRY:
            results.append({
                'worker': target,
                'success': False,
                'error': 'Unknown worker'
            })
            continue

        result = process_request(
            worker=target,
            action='cascade',
            payload={
                'priority': priority,
                'batch': batch,
                'source_worker': source_worker,
                'source_request_id': source_request_id,
                'features': features
            },
            api_key=api_key,
            source='cascade',
            features=features,
            correlation_id=correlation_id,  # Pass through cascade chain
            request_id=source_request_id,  # Preserve original request_id for status queries
            request_log_id=request_log_id  # Preserve for billing linkage
        )
        results.append({
            'worker': target,
            **result
        })

    return results


# =============================================================================
# Flask Application
# =============================================================================

def create_app():
    """Create Flask application"""
    app = Flask(__name__)

    # Enable CORS for all routes (allows browser-based API testing)
    @app.after_request
    def add_cors_headers(response):
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, X-API-Key, X-Correlation-Id, X-Request-Id, X-Priority'
        response.headers['Access-Control-Expose-Headers'] = 'X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset, Retry-After'
        return response

    # Global gzip compression for all JSON responses > 1KB
    @app.after_request
    def gzip_response(response):
        if (response.status_code < 200 or response.status_code >= 300
                or response.content_length is not None and response.content_length < 1024
                or 'Content-Encoding' in response.headers
                or 'gzip' not in request.headers.get('Accept-Encoding', '')):
            return response
        import gzip as gzip_mod
        data = response.get_data()
        if len(data) < 1024:
            return response
        compressed = gzip_mod.compress(data)
        response.set_data(compressed)
        response.headers['Content-Encoding'] = 'gzip'
        response.headers['Content-Length'] = len(compressed)
        return response

    # Handle preflight OPTIONS requests
    @app.route('/<path:path>', methods=['OPTIONS'])
    @app.route('/', methods=['OPTIONS'])
    def options_handler(path=''):
        return '', 204

    # ---- API Documentation ----

    @app.route('/', methods=['GET'])
    def index():
        """Redirect root to API docs"""
        from flask import redirect
        return redirect('/docs')

    @app.route('/docs', methods=['GET'])
    def docs():
        """Serve API documentation page"""
        docs_path = os.path.join(os.path.dirname(__file__), 'api-docs.html')
        try:
            with open(docs_path, 'r', encoding='utf-8') as f:
                return f.read(), 200, {'Content-Type': 'text/html'}
        except FileNotFoundError:
            return 'API docs not found', 404

    @app.route('/api-guide', methods=['GET'])
    def api_guide():
        """Serve API integration guide"""
        guide_path = os.path.join(os.path.dirname(__file__), 'svcs-t16o-io-api-guide.html')
        try:
            with open(guide_path, 'r', encoding='utf-8') as f:
                return f.read(), 200, {'Content-Type': 'text/html'}
        except FileNotFoundError:
            return 'API guide not found', 404

    @app.route('/api/openapi.json', methods=['GET'])
    @app.route('/api', methods=['GET'])
    def openapi_spec():
        """Serve the raw OpenAPI JSON spec (for .NET service references, Swagger, etc.)"""
        docs_path = os.path.join(os.path.dirname(__file__), 'api-docs.html')
        try:
            with open(docs_path, 'r', encoding='utf-8') as f:
                html = f.read()
            # Extract JSON between <script id="api-reference" ...> and </script>
            start = html.index('{', html.index('id="api-reference"'))
            end = html.rindex('}', 0, html.index('</script>')) + 1
            return html[start:end], 200, {'Content-Type': 'application/json'}
        except Exception as e:
            return jsonify({'error': f'Failed to extract OpenAPI spec: {e}'}), 500

    @app.route('/api/health', methods=['GET'])
    def health():
        """Health check endpoint"""
        checks = {
            'flask': True,
            'mysql': HAS_MYSQL,
            'pika': HAS_PIKA
        }

        # Test DB connection
        if HAS_MYSQL:
            try:
                conn = get_db_connection()
                conn.close()
                checks['db_connection'] = True
            except:
                checks['db_connection'] = False

        # Test RabbitMQ connection
        if HAS_PIKA:
            try:
                conn = get_rabbitmq_connection()
                conn.close()
                checks['rabbitmq_connection'] = True
            except:
                checks['rabbitmq_connection'] = False

        healthy = all(checks.values())
        return jsonify({
            'status': 'healthy' if healthy else 'degraded',
            'checks': checks,
            'timestamp': datetime.now().isoformat() + 'Z'
        }), 200 if healthy else 503

    @app.route('/api/bmap/get', methods=['GET'])
    def bmap_get():
        """Get bmap data for a token/signature via sp_tx_bmap_get.
        Auto-fetches from chain if no data exists."""

        # --- Auth ---
        api_key = request.headers.get('X-API-Key')
        if not api_key:
            return jsonify({'error': 'Missing X-API-Key header'}), 401
        key_info = validate_api_key(api_key)
        if not key_info:
            return jsonify({'error': 'Invalid or inactive API key'}), 401

        request_id = request.headers.get('X-Request-Id') or str(uuid.uuid4())
        correlation_id = request.headers.get('X-Correlation-Id') or request_id

        # Rate limit
        rate_limit = key_info.get('rate_limit', 0)
        allowed, retry_after = check_rate_limit(key_info['id'], rate_limit)
        if not allowed:
            resp = jsonify({'error': 'Rate limit exceeded', 'retry_after_seconds': retry_after})
            resp.headers['Retry-After'] = str(retry_after)
            return resp, 429

        # --- Query params ---
        token_name   = request.args.get('token_name')
        token_symbol = request.args.get('token_symbol')
        mint_address = request.args.get('mint')
        signature    = request.args.get('signature')
        block_time   = request.args.get('block_time', type=int)
        tx_limit     = request.args.get('limit', default=10, type=int)

        # --- Call stored procedure ---
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.callproc('sp_tx_bmap_get', [
                token_name, token_symbol, mint_address,
                signature, block_time, tx_limit
            ])
            data = None
            for result_set in cursor.stored_results():
                row = result_set.fetchone()
                if row:
                    data = json.loads(row[0])
                    break
            cursor.close()
        except Exception as e:
            return jsonify({'error': str(e)}), 500
        finally:
            if conn:
                conn.close()

        if not data:
            return jsonify({'error': 'No result from procedure'}), 500

        # --- Return data or error as-is (no auto-fetch) ---
        return jsonify(data), 200 if 'error' not in data.get('result', {}) else 404

    @app.route('/api/bmap/get-wallet-txs', methods=['GET', 'POST'])
    def bmap_get_wallet_txs():
        """Get a wallet's transaction history via sp_tx_bmap_get_wallet_txs_v2."""

        # --- Auth ---
        api_key = request.headers.get('X-API-Key')
        if not api_key:
            return jsonify({'error': 'Missing X-API-Key header'}), 401
        key_info = validate_api_key(api_key)
        if not key_info:
            return jsonify({'error': 'Invalid or inactive API key'}), 401

        # Rate limit
        rate_limit = key_info.get('rate_limit', 0)
        allowed, retry_after = check_rate_limit(key_info['id'], rate_limit)
        if not allowed:
            resp = jsonify({'error': 'Rate limit exceeded', 'retry_after_seconds': retry_after})
            resp.headers['Retry-After'] = str(retry_after)
            return resp, 429

        # --- Params: query string (GET) or JSON body (POST) ---
        if request.method == 'POST' and request.is_json:
            body = request.get_json(silent=True) or {}
            address = body.get('address')
            mint_address = body.get('mint')
            signature = body.get('signature')
            limit = body.get('limit', 50)
            try:
                limit = int(limit)
            except (TypeError, ValueError):
                limit = 50
        else:
            address = request.args.get('address')
            mint_address = request.args.get('mint')
            signature = request.args.get('signature')
            limit = request.args.get('limit', default=50, type=int)

        if not address:
            return jsonify({'error': 'address is required'}), 400

        # --- Call stored procedure ---
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.callproc('sp_tx_bmap_get_wallet_txs_v2', [address, mint_address, min(limit, 200), signature or None])

            # Check if result is a JSON error or a result set of rows
            rows = []
            for result_set in cursor.stored_results():
                rows = result_set.fetchall()
                break

            cursor.close()

            if not rows:
                return jsonify({'address': address, 'mint': mint_address, 'transactions': []}), 200

            # Check for SP error response (single row with 'result' key containing JSON)
            if len(rows) == 1 and 'result' in rows[0]:
                error_data = json.loads(rows[0]['result']) if isinstance(rows[0]['result'], str) else rows[0]['result']
                return jsonify(error_data), 404

            # Convert rows: datetime/decimal to serializable types
            txs = []
            for row in rows:
                tx = {}
                for k, v in row.items():
                    if hasattr(v, 'isoformat'):
                        tx[k] = v.isoformat()
                    elif isinstance(v, __import__('decimal').Decimal):
                        tx[k] = float(v)
                    else:
                        tx[k] = v
                txs.append(tx)

            return jsonify({
                'address': address,
                'mint': mint_address,
                'count': len(txs),
                'transactions': txs
            }), 200

        except Exception as e:
            return jsonify({'error': str(e)}), 500
        finally:
            if conn:
                conn.close()

    @app.route('/api/bmap/v3/get-wallet-txs', methods=['GET', 'POST'])
    def bmap_get_wallet_txs_v3():
        """Get a wallet's transaction history via sp_tx_bmap_get_wallet_txs_v3 (JSON output)."""

        # --- Auth ---
        api_key = request.headers.get('X-API-Key')
        if not api_key:
            return jsonify({'error': 'Missing X-API-Key header'}), 401
        key_info = validate_api_key(api_key)
        if not key_info:
            return jsonify({'error': 'Invalid or inactive API key'}), 401

        # Rate limit
        rate_limit = key_info.get('rate_limit', 0)
        allowed, retry_after = check_rate_limit(key_info['id'], rate_limit)
        if not allowed:
            resp = jsonify({'error': 'Rate limit exceeded', 'retry_after_seconds': retry_after})
            resp.headers['Retry-After'] = str(retry_after)
            return resp, 429

        # --- Params: query string (GET) or JSON body (POST) ---
        if request.method == 'POST' and request.is_json:
            body = request.get_json(silent=True) or {}
            address = body.get('address')
            mint_address = body.get('mint')
            signature = body.get('signature')
            limit = body.get('limit', 50)
            after = body.get('after')
            before = body.get('before')
            type_mask = body.get('type_mask')
        else:
            address = request.args.get('address')
            mint_address = request.args.get('mint')
            signature = request.args.get('signature')
            limit = request.args.get('limit', default=50, type=int)
            after = request.args.get('after', type=int)
            before = request.args.get('before', type=int)
            type_mask = request.args.get('type_mask', type=int)

        # Coerce types
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            limit = 50
        try:
            after = int(after) if after is not None else None
        except (TypeError, ValueError):
            after = None
        try:
            before = int(before) if before is not None else None
        except (TypeError, ValueError):
            before = None
        try:
            type_mask = int(type_mask) if type_mask is not None else None
        except (TypeError, ValueError):
            type_mask = None

        if not address:
            return jsonify({'error': 'address is required'}), 400

        # --- Call stored procedure ---
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.callproc('sp_tx_bmap_get_wallet_txs_v3', [
                address,
                mint_address or None,
                min(limit, 200),
                signature or None,
                after,
                before,
                type_mask
            ])

            # SP returns a single row with 'result' column containing JSON
            rows = []
            for result_set in cursor.stored_results():
                rows = result_set.fetchall()
                break

            cursor.close()

            if not rows or 'result' not in rows[0]:
                return jsonify({'address': address, 'count': 0, 'mint': mint_address, 'transactions': []}), 200

            result = rows[0]['result']
            data = json.loads(result) if isinstance(result, str) else result

            # Check for SP error response
            if 'error' in data:
                return jsonify(data), 404

            # Reorder keys to tell the story: who, what, when, how much
            TX_KEY_ORDER = [
                'signature', 'mint_address', 'mint_funded_by', 'block_time', 'party',
                'party_funded_by', 'type_code', 'token_symbol', 'direction', 'amount',
                'block_time_utc', 'party_label', 'dex', 'post_balance'
            ]
            ENVELOPE_KEY_ORDER = ['address', 'address_funded_by', 'count', 'edge_count', 'mint', 'transactions']

            if 'transactions' in data and data['transactions']:
                data['transactions'] = [
                    {k: tx[k] for k in TX_KEY_ORDER if k in tx}
                    for tx in data['transactions']
                ]
            data = {k: data[k] for k in ENVELOPE_KEY_ORDER if k in data}

            return app.response_class(
                response=json.dumps(data, ensure_ascii=False),
                status=200,
                mimetype='application/json'
            )

        except Exception as e:
            return jsonify({'error': str(e)}), 500
        finally:
            if conn:
                conn.close()

    @app.route('/api/bmap/v3.1/get-wallet-txs', methods=['GET', 'POST'])
    def bmap_get_wallet_txs_v31():
        """Get wallet tx history — grouped edges + gzip. Same SP as v3, different output shape."""
        import gzip as gzip_mod
        from collections import OrderedDict

        # --- Auth (same as v3) ---
        api_key = request.headers.get('X-API-Key')
        if not api_key:
            return jsonify({'error': 'Missing X-API-Key header'}), 401
        key_info = validate_api_key(api_key)
        if not key_info:
            return jsonify({'error': 'Invalid or inactive API key'}), 401
        rate_limit = key_info.get('rate_limit', 0)
        allowed, retry_after = check_rate_limit(key_info['id'], rate_limit)
        if not allowed:
            resp = jsonify({'error': 'Rate limit exceeded', 'retry_after_seconds': retry_after})
            resp.headers['Retry-After'] = str(retry_after)
            return resp, 429

        # --- Params (same as v3) ---
        if request.method == 'POST' and request.is_json:
            body = request.get_json(silent=True) or {}
            address = body.get('address')
            mint_address = body.get('mint')
            signature = body.get('signature')
            limit = body.get('limit', 50)
            after = body.get('after')
            before = body.get('before')
            type_mask = body.get('type_mask')
        else:
            address = request.args.get('address')
            mint_address = request.args.get('mint')
            signature = request.args.get('signature')
            limit = request.args.get('limit', default=50, type=int)
            after = request.args.get('after', type=int)
            before = request.args.get('before', type=int)
            type_mask = request.args.get('type_mask', type=int)

        try:
            limit = int(limit)
        except (TypeError, ValueError):
            limit = 50
        try:
            after = int(after) if after is not None else None
        except (TypeError, ValueError):
            after = None
        try:
            before = int(before) if before is not None else None
        except (TypeError, ValueError):
            before = None
        try:
            type_mask = int(type_mask) if type_mask is not None else None
        except (TypeError, ValueError):
            type_mask = None

        if not address:
            return jsonify({'error': 'address is required'}), 400

        # --- Call same SP as v3 ---
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.callproc('sp_tx_bmap_get_wallet_txs_v3', [
                address, mint_address or None, min(limit, 10000),
                signature or None, after, before, type_mask
            ])

            rows = []
            for result_set in cursor.stored_results():
                rows = result_set.fetchall()
                break
            cursor.close()

            if not rows or 'result' not in rows[0]:
                return jsonify({'address': address, 'count': 0, 'edge_count': 0, 'mint': mint_address, 'transactions': []}), 200

            result = rows[0]['result']
            data = json.loads(result) if isinstance(result, str) else result

            if 'error' in data:
                return jsonify(data), 404

            # --- Group flat rows into transactions → edges grouped by mint_address ---
            EDGE_KEYS = ['party', 'party_funded_by', 'type_code', 'direction', 'amount',
                         'party_label', 'dex', 'post_balance']
            TX_KEYS = ['signature', 'block_time', 'block_time_utc']

            grouped = OrderedDict()
            edge_count = 0
            for row in (data.get('transactions') or []):
                sig = row.get('signature')
                if sig not in grouped:
                    grouped[sig] = OrderedDict(
                        (k, row.get(k)) for k in TX_KEYS
                    )
                    grouped[sig]['edges'] = OrderedDict()
                mint = row.get('mint_address', '')
                if mint not in grouped[sig]['edges']:
                    grouped[sig]['edges'][mint] = OrderedDict([
                        ('token_symbol', row.get('token_symbol')),
                        ('funded_by', row.get('mint_funded_by')),
                        ('activity', [])
                    ])
                grouped[sig]['edges'][mint]['activity'].append(
                    OrderedDict((k, row.get(k)) for k in EDGE_KEYS)
                )
                edge_count += 1
            output = OrderedDict([
                ('address', data.get('address')),
                ('address_funded_by', data.get('address_funded_by')),
                ('count', len(grouped)),
                ('edge_count', edge_count),
                ('mint', data.get('mint')),
                ('transactions', list(grouped.values()))
            ])

            payload = json.dumps(output, ensure_ascii=False).encode('utf-8')
            compressed = gzip_mod.compress(payload)

            return app.response_class(
                response=compressed,
                status=200,
                mimetype='application/json',
                headers={
                    'Content-Encoding': 'gzip',
                    'X-Uncompressed-Length': str(len(payload)),
                    'X-Compressed-Length': str(len(compressed))
                }
            )

        except Exception as e:
            return jsonify({'error': str(e)}), 500
        finally:
            if conn:
                conn.close()

    @app.route('/api/bmap/stats', methods=['GET'])
    def bmap_stats():
        """Pipeline statistics via sp_tx_stats."""
        api_key = request.headers.get('X-API-Key')
        if not api_key:
            return jsonify({'error': 'Missing X-API-Key header'}), 401
        key_info = validate_api_key(api_key)
        if not key_info:
            return jsonify({'error': 'Invalid or inactive API key'}), 401

        start_utc = request.args.get('start') or None
        end_utc = request.args.get('end') or None

        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.callproc('sp_tx_stats', [start_utc, end_utc])

            result = None
            for result_set in cursor.stored_results():
                row = result_set.fetchone()
                if row:
                    result = row[0]
                break
            cursor.close()

            if result:
                return app.response_class(
                    response=result if isinstance(result, str) else json.dumps(result),
                    status=200,
                    mimetype='application/json'
                )
            return jsonify({'error': 'No stats returned'}), 500
        except Exception as e:
            return jsonify({'error': str(e)}), 500
        finally:
            if conn:
                conn.close()

    @app.route('/api/workers', methods=['GET'])
    def list_workers():
        """List available workers with live consumer/message counts and thread config"""
        # Fetch queue stats from RabbitMQ management API
        queue_stats = {}
        try:
            import requests as http_requests
            rmq_mgmt_url = f"http://{RABBITMQ_CONFIG['host']}:15672/api/queues/{RABBITMQ_CONFIG['vhost']}"
            resp = http_requests.get(
                rmq_mgmt_url,
                auth=(RABBITMQ_CONFIG['user'], RABBITMQ_CONFIG['password']),
                timeout=3
            )
            if resp.status_code == 200:
                for q in resp.json():
                    queue_stats[q['name']] = {
                        'consumers': q.get('consumers', 0),
                        'messages': q.get('messages', 0),
                    }
        except Exception:
            pass  # Fall back to no stats if management API unavailable

        # Fetch configured thread counts from config table
        thread_config = {}
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT config_key, config_value FROM config "
                "WHERE config_key LIKE '%\\_wrk\\_cnt\\_threads' ESCAPE '\\\\'"
            )
            for key, val in cursor.fetchall():
                worker_name = key.replace('_wrk_cnt_threads', '')
                thread_config[worker_name] = int(val) if val else 0
            cursor.close()
            conn.close()
        except Exception:
            pass  # Fall back to no thread info

        workers = {}
        total_active = 0
        for name, config in WORKER_REGISTRY.items():
            req_queue = config['request_queue']
            resp_queue = config['response_queue']
            dlq = config['dlq']
            req_stats = queue_stats.get(req_queue, {})
            resp_stats = queue_stats.get(resp_queue, {})
            dlq_stats = queue_stats.get(dlq, {})
            consumers = req_stats.get('consumers', 0)
            threads = thread_config.get(name, None)
            if consumers > 0:
                total_active += 1
            workers[name] = {
                'description': config['description'],
                'cascade_to': config['cascade_to'],
                'active': consumers > 0,
                'consumers': consumers,
                'threads': threads,
                'queues': {
                    'request':  {**{'name': req_queue},  **req_stats},
                    'response': {**{'name': resp_queue}, **resp_stats},
                    'dlq':      {**{'name': dlq},        **dlq_stats},
                }
            }
        return jsonify({
            'active_workers': total_active,
            'total_workers': len(WORKER_REGISTRY),
            'workers': workers
        })

    # ---- Transaction Query Engine ----
    # All endpoints call sp_tx_get with different defaults/masks.

    def _call_sp_tx_get(p_address=None, p_mint=None, p_symbol=None, p_signature=None,
                        p_limit=None, p_after=None, p_before=None, p_type_mask=None, p_result_mask=None):
        """Shared helper: call sp_tx_get and return gzip-compressed Flask response."""
        import gzip as gzip_mod
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.callproc('sp_tx_get', [
                p_address, p_mint, p_symbol, p_signature,
                p_limit, p_after, p_before, p_type_mask, p_result_mask
            ])
            result = None
            for res in cursor.stored_results():
                row = res.fetchone()
                if row:
                    result = row[0]
                break
            cursor.close()
            if result:
                payload = (result if isinstance(result, str) else json.dumps(result)).encode('utf-8')
                # Gzip if client accepts it and payload is worth compressing
                accept_enc = request.headers.get('Accept-Encoding', '')
                if 'gzip' in accept_enc and len(payload) > 1024:
                    compressed = gzip_mod.compress(payload)
                    return app.response_class(
                        response=compressed, status=200, mimetype='application/json',
                        headers={
                            'Content-Encoding': 'gzip',
                            'X-Uncompressed-Length': str(len(payload)),
                            'X-Compressed-Length': str(len(compressed))
                        })
                return app.response_class(response=payload, status=200, mimetype='application/json')
            return jsonify({'error': 'No result returned'}), 404
        except Exception as e:
            return jsonify({'error': str(e)}), 500
        finally:
            if conn:
                conn.close()

    def _require_auth():
        """Validate API key and rate limit. Returns key_info or a Flask error response."""
        api_key = request.headers.get('X-API-Key')
        if not api_key:
            return None, (jsonify({'error': 'Missing X-API-Key header'}), 401)
        key_info = validate_api_key(api_key)
        if not key_info:
            return None, (jsonify({'error': 'Invalid or inactive API key'}), 401)
        rate_limit = key_info.get('rate_limit', 0)
        allowed, retry_after = check_rate_limit(key_info['id'], rate_limit)
        if not allowed:
            resp = jsonify({'error': 'Rate limit exceeded', 'retry_after_seconds': retry_after})
            resp.headers['Retry-After'] = str(retry_after)
            return None, (resp, 429)
        return key_info, None

    # ---- /api/txs — Full flexible query ----

    @app.route('/api/txs', methods=['GET'])
    def txs():
        """Unified transaction query — all params exposed."""
        key_info, err = _require_auth()
        if err: return err
        return _call_sp_tx_get(
            p_address=request.args.get('address') or None,
            p_mint=request.args.get('mint') or None,
            p_symbol=request.args.get('symbol') or None,
            p_signature=request.args.get('signature') or None,
            p_limit=request.args.get('limit', type=int),
            p_after=request.args.get('after', type=int),
            p_before=request.args.get('before', type=int),
            p_type_mask=request.args.get('type_mask', type=int),
            p_result_mask=request.args.get('result_mask', type=int))

    # ---- Wallet-Centric Endpoints ----

    @app.route('/api/wallet/<address>/history', methods=['GET'])
    def wallet_history(address):
        """Wallet transaction table. mask=113."""
        key_info, err = _require_auth()
        if err: return err
        return _call_sp_tx_get(
            p_address=address,
            p_mint=request.args.get('mint') or None,
            p_signature=request.args.get('signature') or None,
            p_limit=request.args.get('limit', default=50, type=int),
            p_after=request.args.get('after', type=int),
            p_before=request.args.get('before', type=int),
            p_type_mask=request.args.get('type_mask', type=int),
            p_result_mask=113)

    @app.route('/api/wallet/<address>/tokens', methods=['GET'])
    def wallet_tokens(address):
        """All tokens this wallet interacted with. mask=129 (EDGES+RELATED)."""
        key_info, err = _require_auth()
        if err: return err
        return _call_sp_tx_get(
            p_address=address,
            p_limit=request.args.get('limit', default=200, type=int),
            p_result_mask=129)

    @app.route('/api/wallet/<address>/map', methods=['GET'])
    def wallet_map(address):
        """Bubble map from wallet perspective. mask=207."""
        key_info, err = _require_auth()
        if err: return err
        return _call_sp_tx_get(
            p_address=address,
            p_mint=request.args.get('mint') or None,
            p_signature=request.args.get('signature') or None,
            p_limit=request.args.get('limit', default=10, type=int),
            p_result_mask=207)

    # ---- Token-Centric Endpoints ----

    @app.route('/api/token/<mint>/activity', methods=['GET'])
    def token_activity(mint):
        """Token activity graph (bubble map). mask=207."""
        key_info, err = _require_auth()
        if err: return err
        return _call_sp_tx_get(
            p_mint=mint,
            p_signature=request.args.get('signature') or None,
            p_limit=request.args.get('limit', default=10, type=int),
            p_after=request.args.get('after', type=int),
            p_before=request.args.get('before', type=int),
            p_result_mask=207)

    # Background edge cache: file-based so all Gunicorn workers share it
    import tempfile as _tempfile
    _EDGE_CACHE_DIR = _tempfile.gettempdir()
    _EDGE_CACHE_TTL = 300  # 5 minutes

    def _derive_address_type(owner_meta, ata_metas):
        """Derive address type from owner metadata + ATA metadata.

        Check both — ATA tags often have richer classification
        (e.g., 'pump.fun amm', 'market') while owner may just show 'address'.
        """
        all_tags = []
        owner_tags = owner_meta.get('account_tags', [])
        all_tags.extend(owner_tags)

        for ata_meta in ata_metas:
            all_tags.extend(ata_meta.get('account_tags', []))

        all_labels = []
        ol = owner_meta.get('account_label', '')
        if ol:
            all_labels.append(ol.lower())
        for ata_meta in ata_metas:
            al = ata_meta.get('account_label', '')
            if al:
                all_labels.append(al.lower())

        if not all_tags and not all_labels:
            return 'wallet'

        tags_lower = [t.lower() for t in all_tags]
        labels_combined = ' '.join(all_labels)

        if 'dex_wallet' in tags_lower:
            return 'dex_wallet'
        if any('amm' in t or 'market' in t for t in tags_lower):
            return 'pool'
        if any('program' in t for t in tags_lower):
            return 'program'
        if any('vault' in t or 'authority' in t for t in tags_lower):
            return 'vault'
        if any('token' in t for t in tags_lower):
            return 'mint'

        if any(kw in labels_combined for kw in ['raydium', 'orca', 'meteora', 'jupiter', 'pump.fun', 'openbook']):
            if any(kw in labels_combined for kw in ['pool', 'amm', 'market', 'lp']):
                return 'pool'
            if 'vault' in labels_combined or 'authority' in labels_combined:
                return 'vault'
            return 'dex_wallet'
        if any(kw in labels_combined for kw in ['binance', 'coinbase', 'kraken', 'okx', 'bybit']):
            return 'cex'

        return 'wallet'

    def _edge_cache_path(mint):
        return os.path.join(_EDGE_CACHE_DIR, f'holder_edges_{mint[:16]}.json')

    def _edge_cache_read(mint):
        import time as _t
        path = _edge_cache_path(mint)
        try:
            if not os.path.exists(path):
                return None
            age = _t.time() - os.path.getmtime(path)
            if age > _EDGE_CACHE_TTL:
                return None
            with open(path, 'r') as f:
                return json.loads(f.read())
        except Exception:
            return None

    def _edge_cache_write(mint, data):
        path = _edge_cache_path(mint)
        try:
            with open(path, 'w') as f:
                f.write(json.dumps(data))
        except Exception as e:
            print(f"[HOLDERS-RT] Cache write failed: {e}", flush=True)

    def _portfolio_cache_path(mint):
        return os.path.join(_EDGE_CACHE_DIR, f'holder_portfolio_{mint[:16]}.json')

    def _portfolio_cache_read(mint):
        import time as _t
        path = _portfolio_cache_path(mint)
        try:
            if not os.path.exists(path):
                return None
            age = _t.time() - os.path.getmtime(path)
            if age > _EDGE_CACHE_TTL:
                return None
            with open(path, 'r') as f:
                return json.loads(f.read())
        except Exception:
            return None

    def _portfolio_cache_write(mint, data):
        path = _portfolio_cache_path(mint)
        try:
            with open(path, 'w') as f:
                f.write(json.dumps(data))
        except Exception as e:
            print(f"[PORTFOLIO] Cache write failed: {e}", flush=True)

    @app.route('/api/token/<mint>/holders', methods=['GET'])
    def token_holders(mint):
        """
        Bubblemaps-style holder graph: real-time top holders + transfer connections.

        Progressive loading (default mode):
          1st call: returns holders + metadata instantly (~2s), edges fetched in background
          2nd call: returns holders + edges + clusters (from background cache)

        Portfolio mode (?mode=portfolio):
          Synchronous: global token transfers + per-holder portfolio overlap clustering.

        Query params:
            mode=portfolio  — portfolio overlap mode
            refresh=1       — force Solscan re-fetch
        """
        key_info, err = _require_auth()
        if err: return err
        force_refresh = 'refresh' in request.args
        mode = request.args.get('mode', 'default')
        if mode == 'portfolio':
            return _build_portfolio_graph(mint, force_refresh=force_refresh)
        return _build_holder_graph(mint, force_refresh=force_refresh)

    def _build_holder_graph(mint: str, force_refresh: bool = False):
        """100% realtime holder graph — progressive loading.

        1st call (~2s): holders + metadata returned immediately,
                        edge fetch kicked off in background
        2nd call (~0s): holders + edges + clusters from background cache

        All data from Solscan — zero DB dependency.
        """
        import requests as _req
        import time as _time
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from t16o_exchange.guide.common.config import load_config

        t0 = _time.time()

        cfg = load_config()
        api_base = cfg.get('SOLSCAN_API', 'https://pro-api.solscan.io/v2.0')
        api_token = cfg.get('SOLSCAN_TOKEN', '')
        ss_headers = {'token': api_token}
        correlation_id = request.headers.get('X-Correlation-Id') or str(uuid.uuid4())

        try:
            # ── Check edge cache first (file-based, shared across workers) ──
            if not force_refresh:
                cached = _edge_cache_read(mint)
                if cached:
                    holders_out = cached['holders']
                    edges_out = cached['edges']
                    holders_out, clusters_out = _cluster_with_networkx(holders_out, edges_out)

                    elapsed = _time.time() - t0
                    result = {
                        'token': cached['token'],
                        'holders': holders_out,
                        'edges': edges_out,
                        'clusters': clusters_out,
                        'meta': {
                            'holder_count': len(holders_out),
                            'edge_count': len(edges_out),
                            'cluster_count': len(clusters_out),
                            'sig_count': cached.get('sig_count', 0),
                            'correlation_id': correlation_id,
                            'elapsed_ms': round(elapsed * 1000),
                            'progressive': 'complete',
                        },
                    }
                    import gzip as gzip_mod
                    import orjson
                    body = orjson.dumps(result)
                    accept = request.headers.get('Accept-Encoding', '')
                    if 'gzip' in accept:
                        body = gzip_mod.compress(body)
                        return app.response_class(body, mimetype='application/json',
                            headers={'Content-Encoding': 'gzip'})
                    return app.response_class(body, mimetype='application/json')

            # ── Step 1: Fetch token metadata + holders in parallel ────────
            token_info = {}
            all_holders = []

            def fetch_token_meta():
                try:
                    resp = _req.get(f'{api_base}/token/meta',
                                    params={'address': mint},
                                    headers=ss_headers, timeout=10)
                    if resp.status_code == 200:
                        return resp.json().get('data', {})
                except Exception:
                    pass
                return {}

            def fetch_holder_page(page):
                try:
                    resp = _req.get(f'{api_base}/token/holders',
                                    params={'address': mint, 'page_size': 40, 'page': page},
                                    headers=ss_headers, timeout=10)
                    if resp.status_code != 200:
                        return []
                    return resp.json().get('data', {}).get('items', [])
                except Exception:
                    return []

            # Fetch token meta + 2 holder pages concurrently
            with ThreadPoolExecutor(max_workers=3) as pool:
                f_meta = pool.submit(fetch_token_meta)
                f_p1 = pool.submit(fetch_holder_page, 1)
                f_p2 = pool.submit(fetch_holder_page, 2)
                token_info = f_meta.result()
                for f in [f_p1, f_p2]:
                    result = f.result()
                    if result:
                        all_holders.extend(result)

            if not all_holders:
                return jsonify({'error': 'No holders found from Solscan', 'mint': mint}), 404

            # Build lookup maps:
            # Solscan holders: address=ATA, owner=wallet
            ata_to_owner = {}   # ATA address → owner wallet
            owner_to_atas = {}  # owner wallet → [ATA addresses]
            all_addrs_to_fetch = set()  # both ATAs and owners for metadata
            for h in all_holders:
                owner = h.get('owner', '')
                ata = h.get('address', '')
                if not owner:
                    continue
                all_addrs_to_fetch.add(owner)
                if ata:
                    ata_to_owner[ata] = owner
                    owner_to_atas.setdefault(owner, []).append(ata)
                    all_addrs_to_fetch.add(ata)

            # ── Step 2: Fetch metadata for BOTH ATAs and owners (50/call) ────
            # ATA metadata often has richer tags (e.g., "Raydium Pool Token Account")
            # Owner metadata has funded_by and account labels
            addr_metadata = {}  # address → metadata dict

            def fetch_metadata_chunk(chunk):
                try:
                    params = [('address[]', addr) for addr in chunk]
                    resp = _req.get(f'{api_base}/account/metadata/multi',
                                    params=params, headers=ss_headers, timeout=15)
                    if resp.status_code == 200:
                        return resp.json().get('data', [])
                except Exception as e:
                    print(f"[HOLDERS-RT] Metadata fetch error: {e}", flush=True)
                return []

            all_addrs_list = list(all_addrs_to_fetch)
            chunks = [all_addrs_list[i:i + 50] for i in range(0, len(all_addrs_list), 50)]
            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = [pool.submit(fetch_metadata_chunk, c) for c in chunks]
                for f in futures:
                    data = f.result()
                    if isinstance(data, list):
                        for item in data:
                            addr = item.get('account_address', '')
                            if addr:
                                addr_metadata[addr] = item

            # ── Known address overrides (Solscan gaps) ────────────────────
            KNOWN_ADDRESSES = {
                '8N2ssXZGJbvVLszanERuCJpLZd2nuADgpZwLkDDxwNnS': ('Upbit Hot Wallet', 'cex', ['cex']),
                '5tzFkiKscjHK98gSfMQXAiYaXdVjbAETRMEK6bQ3uDiA': ('Binance Hot Wallet', 'cex', ['cex']),
                'AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2': ('Bybit Hot Wallet', 'cex', ['cex']),
                '2AQdvzPBQkpxJMhBbCZbKTFnQMXDg3UZqEbQvJGmtJmk': ('OKX Hot Wallet', 'cex', ['cex']),
                'H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS': ('Coinbase Custody', 'cex', ['cex']),
            }

            # Apply overrides to metadata
            for addr, (label, addr_type, tags) in KNOWN_ADDRESSES.items():
                if addr in addr_metadata:
                    addr_metadata[addr]['account_label'] = label
                    addr_metadata[addr]['account_tags'] = tags
                else:
                    addr_metadata[addr] = {
                        'account_address': addr,
                        'account_label': label,
                        'account_tags': tags,
                    }

            # ── Step 3: Build holders list (Bubblemaps style) ─────────────
            holders_out = []
            holder_address_set = set()  # owner addresses for edge matching
            for rank, h in enumerate(all_holders, 1):
                owner = h.get('owner', '')
                if not owner or owner in holder_address_set:
                    continue  # deduplicate by owner
                holder_address_set.add(owner)

                owner_meta = addr_metadata.get(owner, {})
                ata_addrs = owner_to_atas.get(owner, [])
                ata_metas = [addr_metadata.get(a, {}) for a in ata_addrs if a in addr_metadata]

                addr_type = _derive_address_type(owner_meta, ata_metas)

                # Merge tags from both owner and ATAs
                all_tags = list(set(
                    owner_meta.get('account_tags', []) +
                    [t for am in ata_metas for t in am.get('account_tags', [])]
                ))

                # Best label: prefer owner label, fall back to ATA label
                label = owner_meta.get('account_label')
                if not label:
                    for am in ata_metas:
                        label = am.get('account_label')
                        if label:
                            break

                holders_out.append({
                    'rank': rank,
                    'address': owner,
                    'address_type': addr_type,
                    'label': label,
                    'tags': all_tags,
                    'funded_by': (owner_meta.get('funded_by') or {}).get('funded_by', None),
                    'active_age': owner_meta.get('active_age', None),
                    'percentage': h.get('percentage', 0),
                    'amount': str(h.get('amount', 0)),
                    'decimals': h.get('decimals', 0),
                    'atas': ata_addrs,
                })

            # ── Step 3b: Enrich from DB (address_type, label, pool, funded_by) ──
            if holder_address_set and HAS_MYSQL:
                try:
                    import json as _json
                    db_conn = get_db_connection()
                    db_cursor = db_conn.cursor(dictionary=True)
                    addr_json = _json.dumps(list(holder_address_set))
                    db_cursor.execute("CALL sp_tx_address(%s)", (addr_json,))
                    db_rows = db_cursor.fetchall()
                    try:
                        while db_cursor.nextset():
                            db_cursor.fetchall()
                    except Exception:
                        pass
                    db_cursor.close()
                    db_conn.close()

                    db_data = {r['address']: r for r in db_rows}
                    enriched = 0
                    for h in holders_out:
                        db = db_data.get(h['address'])
                        if not db:
                            continue
                        enriched += 1
                        # Address type: DB wins over Solscan 'wallet' default
                        db_type = db.get('address_type')
                        if db_type and h['address_type'] == 'wallet':
                            h['address_type'] = db_type
                        # Label: DB wins when Solscan has none
                        if not h.get('label'):
                            h['label'] = db.get('label') or db.get('pool_label')
                        # Pool info
                        if db.get('pool_label'):
                            h['pool_label'] = db['pool_label']
                            h['dex'] = db.get('dex')
                            h['token_pair'] = db.get('token_pair')
                        if db.get('program_name'):
                            h['program_name'] = db['program_name']
                        # Funded-by: DB overrides Solscan when available
                        if db.get('funded_by'):
                            h['funded_by'] = db['funded_by']
                        # Active age from DB
                        if db.get('active_age_days') and not h.get('active_age'):
                            h['active_age'] = db['active_age_days']

                    if enriched:
                        print(f"[HOLDERS-RT] DB enrichment: {enriched}/{len(holders_out)} "
                              f"holders matched tx_address", flush=True)
                except Exception as e:
                    print(f"[HOLDERS-RT] DB lookup failed (non-fatal): {e}", flush=True)

            # ── Step 4: Kick off edge fetch in background ─────────────────
            token_data = {
                'mint': mint,
                'symbol': token_info.get('symbol', ''),
                'name': token_info.get('name', ''),
                'decimals': token_info.get('decimals', 0),
            }

            def _bg_fetch_edges(m, holders, addr_set, tok_data, corr_id):
                """Background: fetch THIS TOKEN's transfers between holders.

                Bubblemaps approach: trace transfers of the queried token
                between top holders AND discover 2nd-degree counterparties.
                """
                try:
                    one_year_ago = int(_time.time()) - 365 * 24 * 60 * 60

                    def fetch_holder_xfers(owner_addr):
                        items = []
                        for page in range(1, 6):
                            try:
                                resp = _req.get(
                                    f'{api_base}/account/transfer',
                                    params={
                                        'address': owner_addr,
                                        'token': m,  # THIS token only
                                        'activity_type[]': 'ACTIVITY_SPL_TRANSFER',
                                        'page': page,
                                        'page_size': 100,
                                        'sort_by': 'block_time',
                                        'sort_order': 'desc',
                                    },
                                    headers=ss_headers, timeout=15)
                                if resp.status_code != 200:
                                    break
                                data = resp.json().get('data', [])
                                if isinstance(data, dict):
                                    data = data.get('items', [])
                                if not data:
                                    break
                                hit_cutoff = False
                                for item in data:
                                    if item.get('block_time', 0) < one_year_ago:
                                        hit_cutoff = True
                                        break
                                    items.append(item)
                                if hit_cutoff or len(data) < 100:
                                    break
                            except Exception:
                                break
                        return items

                    all_transfers = []
                    with ThreadPoolExecutor(max_workers=20) as pool:
                        futures = {pool.submit(fetch_holder_xfers, addr): addr
                                   for addr in addr_set}
                        for f in as_completed(futures):
                            result = f.result()
                            if result:
                                all_transfers.extend(result)

                    # Build edges + discover 2nd-degree addresses
                    seen_edges = {}
                    all_sigs = set()
                    discovered = {}  # addr → transfer_count with known holders
                    for tx in all_transfers:
                        if tx.get('activity_type') != 'ACTIVITY_SPL_TRANSFER':
                            continue
                        sig = tx.get('trans_id', '')
                        if sig:
                            all_sigs.add(sig)
                        src = tx.get('from_address', '')
                        dst = tx.get('to_address', '')
                        if not src or not dst or src == dst:
                            continue
                        src_known = src in addr_set
                        dst_known = dst in addr_set
                        # Track unknown counterparties
                        if src_known and not dst_known:
                            discovered[dst] = discovered.get(dst, 0) + 1
                        elif dst_known and not src_known:
                            discovered[src] = discovered.get(src, 0) + 1

                    # Promote top discovered addresses (by transfer count) into the graph
                    MAX_DISCOVERED = 40
                    top_discovered = sorted(discovered.items(), key=lambda x: x[1], reverse=True)[:MAX_DISCOVERED]
                    expanded_set = set(addr_set)
                    for addr, _ in top_discovered:
                        expanded_set.add(addr)

                    # Now build edges using the expanded address set
                    for tx in all_transfers:
                        if tx.get('activity_type') != 'ACTIVITY_SPL_TRANSFER':
                            continue
                        src = tx.get('from_address', '')
                        dst = tx.get('to_address', '')
                        if src in expanded_set and dst in expanded_set and src != dst:
                            key = (src, dst)
                            bt = tx.get('block_time', 0)
                            if key in seen_edges:
                                seen_edges[key]['transfer_count'] += 1
                                seen_edges[key]['amount'] += tx.get('amount', 0)
                                if bt > seen_edges[key]['last_transfer']:
                                    seen_edges[key]['last_transfer'] = bt
                                if bt < seen_edges[key]['first_transfer']:
                                    seen_edges[key]['first_transfer'] = bt
                            else:
                                seen_edges[key] = {
                                    'from': src, 'to': dst,
                                    'transfer_count': 1,
                                    'amount': tx.get('amount', 0),
                                    'decimals': 0,
                                    'edge_types': 'spl_transfer',
                                    'first_transfer': bt,
                                    'last_transfer': bt,
                                    'source': 'solscan',
                                }

                    edges = list(seen_edges.values())
                    for e in edges:
                        e['total_amount'] = str(e.pop('amount', 0))

                    # Enrich discovered addresses with metadata
                    new_addrs = [addr for addr, _ in top_discovered]
                    new_holders = []
                    if new_addrs:
                        # Fetch Solscan metadata for discovered addresses
                        new_metadata = {}
                        chunks = [new_addrs[i:i+50] for i in range(0, len(new_addrs), 50)]
                        for chunk in chunks:
                            try:
                                params = [('address[]', a) for a in chunk]
                                resp = _req.get(f'{api_base}/account/metadata/multi',
                                                params=params, headers=ss_headers, timeout=15)
                                if resp.status_code == 200:
                                    for item in resp.json().get('data', []):
                                        addr = item.get('account_address', '')
                                        if addr:
                                            new_metadata[addr] = item
                            except Exception:
                                pass

                        # DB enrichment for discovered addresses
                        db_data = {}
                        if HAS_MYSQL:
                            try:
                                import json as _json
                                db_conn = get_db_connection()
                                db_cursor = db_conn.cursor(dictionary=True)
                                db_cursor.execute("CALL sp_tx_address(%s)",
                                                  (_json.dumps(new_addrs),))
                                for row in db_cursor.fetchall():
                                    db_data[row['address']] = row
                                try:
                                    while db_cursor.nextset():
                                        db_cursor.fetchall()
                                except Exception:
                                    pass
                                db_cursor.close()
                                db_conn.close()
                            except Exception:
                                pass

                        base_rank = len(holders) + 1
                        for idx, (addr, xfer_cnt) in enumerate(top_discovered):
                            meta = new_metadata.get(addr, {})
                            db = db_data.get(addr, {})
                            tags = meta.get('account_tags', [])
                            label = db.get('label') or meta.get('account_label')
                            addr_type = db.get('address_type') or 'wallet'
                            if addr_type == 'wallet':
                                addr_type = _derive_address_type(meta, [])
                            new_holders.append({
                                'rank': base_rank + idx,
                                'address': addr,
                                'address_type': addr_type,
                                'label': label,
                                'tags': tags,
                                'funded_by': db.get('funded_by') or
                                    (meta.get('funded_by') or {}).get('funded_by'),
                                'percentage': 0,
                                'amount': '0',
                                'decimals': 0,
                                'atas': [],
                                'discovered': True,
                                'transfer_count': xfer_cnt,
                            })
                            if db.get('pool_label'):
                                new_holders[-1]['pool_label'] = db['pool_label']
                                new_holders[-1]['dex'] = db.get('dex')

                        print(f"[HOLDERS-RT] Discovered {len(new_holders)} 2nd-degree "
                              f"addresses from transfers", flush=True)

                    all_holders = holders + new_holders

                    # Cache the result (file-based, shared across workers)
                    _edge_cache_write(m, {
                        'holders': all_holders,
                        'edges': edges,
                        'token': tok_data,
                        'sig_count': len(all_sigs),
                    })

                    print(f"[HOLDERS-RT] Background edges ready: {m[:8]}… — "
                          f"{len(edges)} edges, {len(all_sigs)} sigs", flush=True)

                    # Fire sigs to producer
                    if all_sigs and HAS_PIKA:
                        sig_list = list(all_sigs)
                        for i in range(0, len(sig_list), 200):
                            chunk = sig_list[i:i + 200]
                            msg = {
                                'request_id': str(uuid.uuid4()),
                                'correlation_id': corr_id,
                                'action': 'process',
                                'priority': 1,
                                'batch': {'filters': {'signature': chunk}},
                                'tx_origin': 3,
                            }
                            publish_to_worker('producer', msg, priority=1)

                except Exception as e:
                    import traceback
                    print(f"[HOLDERS-RT] Background edge fetch failed: {e}\n"
                          f"{traceback.format_exc()}", flush=True)

            import threading as _threading
            _threading.Thread(
                target=_bg_fetch_edges,
                args=(mint, holders_out, holder_address_set, token_data, correlation_id),
                daemon=True
            ).start()

            # ── Step 5: Return holders immediately (no edges yet) ─────────
            holders_out_copy, clusters_out = _cluster_with_networkx(holders_out, [])

            elapsed = _time.time() - t0
            print(f"[HOLDERS-RT] {mint[:8]}… — {len(holders_out)} holders returned "
                  f"in {elapsed:.1f}s (edges loading in background)", flush=True)

            result = {
                'token': token_data,
                'holders': holders_out_copy,
                'edges': [],
                'clusters': clusters_out,
                'meta': {
                    'holder_count': len(holders_out_copy),
                    'edge_count': 0,
                    'cluster_count': len(clusters_out),
                    'sig_count': 0,
                    'correlation_id': correlation_id,
                    'elapsed_ms': round(elapsed * 1000),
                    'progressive': 'holders_ready',
                },
            }

            import gzip as gzip_mod
            import orjson
            body = orjson.dumps(result)
            accept = request.headers.get('Accept-Encoding', '')
            if 'gzip' in accept:
                body = gzip_mod.compress(body)
                return app.response_class(body, mimetype='application/json',
                    headers={'Content-Encoding': 'gzip'})
            return app.response_class(body, mimetype='application/json')

        except Exception as e:
            import traceback
            print(f"[HOLDERS-RT] Error: {e}\n{traceback.format_exc()}", flush=True)
            return jsonify({'error': str(e)}), 500

    def _build_portfolio_graph(mint: str, force_refresh: bool = False):
        """Portfolio-overlap holder graph — synchronous.

        1. Fetch top holders from Solscan /token/holders
        2. Fetch metadata + DB enrichment (same as default mode)
        3. Page /token/transfer until every holder has ≥1 SPL transfer
        4. Fetch /account/portfolio for each holder (parallel)
        5. Build transfer edges + portfolio overlap edges
        6. Cluster with NetworkX, return synchronously
        """
        import requests as _req
        import time as _time
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from t16o_exchange.guide.common.config import load_config

        t0 = _time.time()

        cfg = load_config()
        api_base = cfg.get('SOLSCAN_API', 'https://pro-api.solscan.io/v2.0')
        api_token = cfg.get('SOLSCAN_TOKEN', '')
        ss_headers = {'token': api_token}
        correlation_id = request.headers.get('X-Correlation-Id') or str(uuid.uuid4())

        try:
            # ── Cache check ──────────────────────────────────────────────
            if not force_refresh:
                cached = _portfolio_cache_read(mint)
                if cached:
                    holders_out = cached['holders']
                    edges_out = cached['edges']
                    holders_out, clusters_out = _cluster_with_networkx(holders_out, edges_out)

                    elapsed = _time.time() - t0
                    result = {
                        'token': cached['token'],
                        'holders': holders_out,
                        'edges': edges_out,
                        'clusters': clusters_out,
                        'meta': {
                            'holder_count': len(holders_out),
                            'edge_count': len(edges_out),
                            'cluster_count': len(clusters_out),
                            'correlation_id': correlation_id,
                            'elapsed_ms': round(elapsed * 1000),
                            'mode': 'portfolio',
                            'progressive': 'complete',
                        },
                    }
                    import gzip as gzip_mod
                    import orjson
                    body = orjson.dumps(result)
                    accept = request.headers.get('Accept-Encoding', '')
                    if 'gzip' in accept:
                        body = gzip_mod.compress(body)
                        return app.response_class(body, mimetype='application/json',
                            headers={'Content-Encoding': 'gzip'})
                    return app.response_class(body, mimetype='application/json')

            # ── Step 1: Fetch token metadata + holders in parallel ───────
            token_info = {}
            all_holders = []

            def fetch_token_meta():
                try:
                    resp = _req.get(f'{api_base}/token/meta',
                                    params={'address': mint},
                                    headers=ss_headers, timeout=10)
                    if resp.status_code == 200:
                        return resp.json().get('data', {})
                except Exception:
                    pass
                return {}

            def fetch_holder_page(page):
                try:
                    resp = _req.get(f'{api_base}/token/holders',
                                    params={'address': mint, 'page_size': 40, 'page': page},
                                    headers=ss_headers, timeout=10)
                    if resp.status_code != 200:
                        return []
                    return resp.json().get('data', {}).get('items', [])
                except Exception:
                    return []

            with ThreadPoolExecutor(max_workers=3) as pool:
                f_meta = pool.submit(fetch_token_meta)
                f_p1 = pool.submit(fetch_holder_page, 1)
                f_p2 = pool.submit(fetch_holder_page, 2)
                token_info = f_meta.result()
                for f in [f_p1, f_p2]:
                    result = f.result()
                    if result:
                        all_holders.extend(result)

            if not all_holders:
                return jsonify({'error': 'No holders found from Solscan', 'mint': mint}), 404

            # ── Step 2: Build holder lookup maps + metadata ──────────────
            ata_to_owner = {}
            owner_to_atas = {}
            all_addrs_to_fetch = set()
            for h in all_holders:
                owner = h.get('owner', '')
                ata = h.get('address', '')
                if not owner:
                    continue
                all_addrs_to_fetch.add(owner)
                if ata:
                    ata_to_owner[ata] = owner
                    owner_to_atas.setdefault(owner, []).append(ata)
                    all_addrs_to_fetch.add(ata)

            addr_metadata = {}

            def fetch_metadata_chunk(chunk):
                try:
                    params = [('address[]', addr) for addr in chunk]
                    resp = _req.get(f'{api_base}/account/metadata/multi',
                                    params=params, headers=ss_headers, timeout=15)
                    if resp.status_code == 200:
                        return resp.json().get('data', [])
                except Exception as e:
                    print(f"[PORTFOLIO] Metadata fetch error: {e}", flush=True)
                return []

            all_addrs_list = list(all_addrs_to_fetch)
            chunks = [all_addrs_list[i:i + 50] for i in range(0, len(all_addrs_list), 50)]
            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = [pool.submit(fetch_metadata_chunk, c) for c in chunks]
                for f in futures:
                    data = f.result()
                    if isinstance(data, list):
                        for item in data:
                            addr = item.get('account_address', '')
                            if addr:
                                addr_metadata[addr] = item

            KNOWN_ADDRESSES = {
                '8N2ssXZGJbvVLszanERuCJpLZd2nuADgpZwLkDDxwNnS': ('Upbit Hot Wallet', 'cex', ['cex']),
                '5tzFkiKscjHK98gSfMQXAiYaXdVjbAETRMEK6bQ3uDiA': ('Binance Hot Wallet', 'cex', ['cex']),
                'AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2': ('Bybit Hot Wallet', 'cex', ['cex']),
                '2AQdvzPBQkpxJMhBbCZbKTFnQMXDg3UZqEbQvJGmtJmk': ('OKX Hot Wallet', 'cex', ['cex']),
                'H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS': ('Coinbase Custody', 'cex', ['cex']),
            }
            for addr, (label, addr_type, tags) in KNOWN_ADDRESSES.items():
                if addr in addr_metadata:
                    addr_metadata[addr]['account_label'] = label
                    addr_metadata[addr]['account_tags'] = tags
                else:
                    addr_metadata[addr] = {
                        'account_address': addr,
                        'account_label': label,
                        'account_tags': tags,
                    }

            # ── Step 3: Build holders list ───────────────────────────────
            holders_out = []
            holder_address_set = set()
            for rank, h in enumerate(all_holders, 1):
                owner = h.get('owner', '')
                if not owner or owner in holder_address_set:
                    continue
                holder_address_set.add(owner)

                owner_meta = addr_metadata.get(owner, {})
                ata_addrs = owner_to_atas.get(owner, [])
                ata_metas = [addr_metadata.get(a, {}) for a in ata_addrs if a in addr_metadata]

                addr_type = _derive_address_type(owner_meta, ata_metas)

                all_tags = list(set(
                    owner_meta.get('account_tags', []) +
                    [t for am in ata_metas for t in am.get('account_tags', [])]
                ))

                label = owner_meta.get('account_label')
                if not label:
                    for am in ata_metas:
                        label = am.get('account_label')
                        if label:
                            break

                holders_out.append({
                    'rank': rank,
                    'address': owner,
                    'address_type': addr_type,
                    'label': label,
                    'tags': all_tags,
                    'funded_by': (owner_meta.get('funded_by') or {}).get('funded_by', None),
                    'active_age': owner_meta.get('active_age', None),
                    'percentage': h.get('percentage', 0),
                    'amount': str(h.get('amount', 0)),
                    'decimals': h.get('decimals', 0),
                    'atas': ata_addrs,
                })

            # ── Step 3b: DB enrichment ───────────────────────────────────
            if holder_address_set and HAS_MYSQL:
                try:
                    import json as _json
                    db_conn = get_db_connection()
                    db_cursor = db_conn.cursor(dictionary=True)
                    addr_json = _json.dumps(list(holder_address_set))
                    db_cursor.execute("CALL sp_tx_address(%s)", (addr_json,))
                    db_rows = db_cursor.fetchall()
                    try:
                        while db_cursor.nextset():
                            db_cursor.fetchall()
                    except Exception:
                        pass
                    db_cursor.close()
                    db_conn.close()

                    db_data = {r['address']: r for r in db_rows}
                    for h in holders_out:
                        db = db_data.get(h['address'])
                        if not db:
                            continue
                        if db.get('address_type') and h['address_type'] == 'wallet':
                            h['address_type'] = db['address_type']
                        if not h.get('label'):
                            h['label'] = db.get('label') or db.get('pool_label')
                        if db.get('pool_label'):
                            h['pool_label'] = db['pool_label']
                            h['dex'] = db.get('dex')
                            h['token_pair'] = db.get('token_pair')
                        if db.get('program_name'):
                            h['program_name'] = db['program_name']
                        if db.get('funded_by'):
                            h['funded_by'] = db['funded_by']
                        if db.get('active_age_days') and not h.get('active_age'):
                            h['active_age'] = db['active_age_days']
                except Exception as e:
                    print(f"[PORTFOLIO] DB lookup failed (non-fatal): {e}", flush=True)

            print(f"[PORTFOLIO] {mint[:8]}… — {len(holders_out)} holders, "
                  f"fetching transfers + portfolios...", flush=True)

            # ── Step 4: Sequential /token/transfer + /token/defi/activities ──
            # Two separate endpoints: transfers (from→to) and swaps (from only)
            # Page both in parallel threads, stop each when coverage is met
            covered_holders = set()
            all_transfer_items = []   # SPL transfers (have from + to)
            all_swap_items = []       # DeFi swaps (have from only)
            MAX_PAGES = 10

            def _fetch_transfer_pages():
                """Page /token/transfer for SPL transfers."""
                items_out = []
                covered = set()
                pages = 0
                for page in range(1, MAX_PAGES + 1):
                    try:
                        resp = _req.get(f'{api_base}/token/transfer',
                                        params={
                                            'address': mint,
                                            'activity_type[]': 'ACTIVITY_SPL_TRANSFER',
                                            'page_size': 100,
                                            'page': page,
                                        },
                                        headers=ss_headers, timeout=10)
                        if resp.status_code != 200:
                            break
                        items = resp.json().get('data', [])
                        if isinstance(items, dict):
                            items = items.get('items', [])
                        if not items:
                            break
                    except Exception:
                        break
                    items_out.extend(items)
                    pages = page
                    for tx in items:
                        src = tx.get('from_address', '')
                        dst = tx.get('to_address', '')
                        if src in holder_address_set:
                            covered.add(src)
                        if dst in holder_address_set:
                            covered.add(dst)
                    if covered >= holder_address_set:
                        break
                return items_out, covered, pages

            def _fetch_swap_pages():
                """Page /token/defi/activities for swaps."""
                items_out = []
                covered = set()
                pages = 0
                for page in range(1, MAX_PAGES + 1):
                    try:
                        resp = _req.get(f'{api_base}/token/defi/activities',
                                        params={
                                            'address': mint,
                                            'activity_type[]': 'ACTIVITY_TOKEN_SWAP',
                                            'page_size': 100,
                                            'page': page,
                                        },
                                        headers=ss_headers, timeout=10)
                        if resp.status_code != 200:
                            break
                        items = resp.json().get('data', [])
                        if isinstance(items, dict):
                            items = items.get('items', [])
                        if not items:
                            break
                    except Exception:
                        break
                    items_out.extend(items)
                    pages = page
                    for tx in items:
                        src = tx.get('from_address', '')
                        if src in holder_address_set:
                            covered.add(src)
                    if covered >= holder_address_set:
                        break
                return items_out, covered, pages

            # Run both fetches in parallel
            with ThreadPoolExecutor(max_workers=2) as pool:
                f_xfer = pool.submit(_fetch_transfer_pages)
                f_swap = pool.submit(_fetch_swap_pages)
                xfer_items, xfer_covered, xfer_pages = f_xfer.result()
                swap_items, swap_covered, swap_pages = f_swap.result()

            all_transfer_items = xfer_items
            all_swap_items = swap_items
            covered_holders = xfer_covered | swap_covered
            pages_fetched = xfer_pages + swap_pages

            print(f"[PORTFOLIO] Transfers: {len(all_transfer_items)} items in "
                  f"{xfer_pages} pages | Swaps: {len(all_swap_items)} items in "
                  f"{swap_pages} pages | Coverage: "
                  f"{len(covered_holders)}/{len(holder_address_set)} holders",
                  flush=True)

            # ── Step 5: Parallel /account/portfolio ──────────────────────
            holder_portfolios = {}

            def fetch_portfolio(owner_addr):
                try:
                    resp = _req.get(f'{api_base}/account/portfolio',
                                    params={'address': owner_addr},
                                    headers=ss_headers, timeout=15)
                    if resp.status_code == 200:
                        data = resp.json().get('data', {})
                        tokens = data.get('tokens', [])
                        return owner_addr, [
                            {
                                'mint': t.get('token_address', ''),
                                'symbol': t.get('token_symbol', ''),
                                'name': t.get('token_name', ''),
                                'balance': t.get('balance', 0),
                                'value': t.get('value', 0),
                            }
                            for t in tokens if t.get('token_address')
                        ]
                except Exception:
                    pass
                return owner_addr, []

            with ThreadPoolExecutor(max_workers=15) as pool:
                futures = [pool.submit(fetch_portfolio, addr)
                           for addr in holder_address_set]
                for f in as_completed(futures):
                    addr, tokens = f.result()
                    holder_portfolios[addr] = tokens

            portfolio_cnt = sum(1 for v in holder_portfolios.values() if v)
            print(f"[PORTFOLIO] Portfolios fetched: {portfolio_cnt}/{len(holder_address_set)} "
                  f"holders have tokens", flush=True)

            # ── Step 6: Build transfer + swap edges ────────────────────────
            seen_edges = {}
            all_sigs = set()

            # 6a: SPL transfer edges (from→to between holders)
            for tx in all_transfer_items:
                if tx.get('activity_type') != 'ACTIVITY_SPL_TRANSFER':
                    continue
                sig = tx.get('trans_id', '')
                if sig:
                    all_sigs.add(sig)
                src = tx.get('from_address', '')
                dst = tx.get('to_address', '')
                if src in holder_address_set and dst in holder_address_set and src != dst:
                    key = (src, dst)
                    bt = tx.get('block_time', 0)
                    if key in seen_edges:
                        seen_edges[key]['transfer_count'] += 1
                        seen_edges[key]['amount'] += tx.get('amount', 0)
                        if bt > seen_edges[key]['last_transfer']:
                            seen_edges[key]['last_transfer'] = bt
                        if bt < seen_edges[key]['first_transfer']:
                            seen_edges[key]['first_transfer'] = bt
                    else:
                        seen_edges[key] = {
                            'from': src, 'to': dst,
                            'transfer_count': 1,
                            'amount': tx.get('amount', 0),
                            'decimals': 0,
                            'edge_types': 'spl_transfer',
                            'first_transfer': bt,
                            'last_transfer': bt,
                            'source': 'solscan',
                        }

            # 6b: Swap activity edges — swaps only have from_address (the swapper).
            # Group by block_time proximity to find holders who swapped in same block.
            # Also collect all holder swap sigs and create co-swap edges.
            holder_swaps = {}  # holder_addr → [{sig, block_time, platform}]
            for tx in all_swap_items:
                sig = tx.get('trans_id', '')
                if sig:
                    all_sigs.add(sig)
                src = tx.get('from_address', '')
                if src in holder_address_set:
                    holder_swaps.setdefault(src, []).append({
                        'sig': sig,
                        'block_time': tx.get('block_time', 0),
                        'platform': (tx.get('platform', [None]) or [None])[0],
                    })

            # Build swap co-activity edges: holders who both swapped this token
            swap_edges = {}
            swapper_list = list(holder_swaps.keys())
            for i in range(len(swapper_list)):
                for j in range(i + 1, len(swapper_list)):
                    a, b = swapper_list[i], swapper_list[j]
                    # Sort so key is consistent
                    key = (min(a, b), max(a, b))
                    cnt_a = len(holder_swaps[a])
                    cnt_b = len(holder_swaps[b])
                    swap_edges[key] = {
                        'from': key[0], 'to': key[1],
                        'transfer_count': 0,
                        'total_amount': '0',
                        'decimals': 0,
                        'edge_types': 'swap_activity',
                        'weight': min(cnt_a, cnt_b),
                        'swaps_a': cnt_a,
                        'swaps_b': cnt_b,
                        'source': 'defi_activity',
                    }

            transfer_edges = list(seen_edges.values())
            for e in transfer_edges:
                e['total_amount'] = str(e.pop('amount', 0))

            swap_activity_edges = list(swap_edges.values())

            # ── Step 7: Build portfolio overlap edges ────────────────────
            holder_mint_sets = {}
            for addr, tokens in holder_portfolios.items():
                holder_mint_sets[addr] = set(t['mint'] for t in tokens)

            portfolio_edges = []
            holder_list = list(holder_address_set)
            for i in range(len(holder_list)):
                for j in range(i + 1, len(holder_list)):
                    a, b = holder_list[i], holder_list[j]
                    mints_a = holder_mint_sets.get(a, set())
                    mints_b = holder_mint_sets.get(b, set())
                    shared = mints_a & mints_b
                    if len(shared) >= 3:
                        portfolio_edges.append({
                            'from': a,
                            'to': b,
                            'transfer_count': 0,
                            'total_amount': '0',
                            'decimals': 0,
                            'edge_types': 'portfolio_overlap',
                            'weight': len(shared),
                            'shared_mints': list(shared),
                            'source': 'portfolio',
                        })

            all_edges = transfer_edges + swap_activity_edges + portfolio_edges

            print(f"[PORTFOLIO] Edges: {len(transfer_edges)} transfer + "
                  f"{len(swap_activity_edges)} swap + "
                  f"{len(portfolio_edges)} portfolio overlap = {len(all_edges)} total",
                  flush=True)

            # ── Step 8: Attach portfolio + cluster ───────────────────────
            for h in holders_out:
                h['portfolio'] = holder_portfolios.get(h['address'], [])

            token_data = {
                'mint': mint,
                'symbol': token_info.get('symbol', ''),
                'name': token_info.get('name', ''),
                'decimals': token_info.get('decimals', 0),
            }

            holders_out, clusters_out = _cluster_with_networkx(holders_out, all_edges)

            # ── Step 9: Cache + return ───────────────────────────────────
            _portfolio_cache_write(mint, {
                'holders': holders_out,
                'edges': all_edges,
                'token': token_data,
            })

            elapsed = _time.time() - t0
            print(f"[PORTFOLIO] {mint[:8]}… — complete in {elapsed:.1f}s",
                  flush=True)

            result = {
                'token': token_data,
                'holders': holders_out,
                'edges': all_edges,
                'clusters': clusters_out,
                'meta': {
                    'holder_count': len(holders_out),
                    'edge_count': len(all_edges),
                    'transfer_edge_count': len(transfer_edges),
                    'swap_edge_count': len(swap_activity_edges),
                    'portfolio_edge_count': len(portfolio_edges),
                    'cluster_count': len(clusters_out),
                    'sig_count': len(all_sigs),
                    'transfer_pages_fetched': xfer_pages,
                    'swap_pages_fetched': swap_pages,
                    'holder_coverage': len(covered_holders),
                    'correlation_id': correlation_id,
                    'elapsed_ms': round(elapsed * 1000),
                    'mode': 'portfolio',
                },
            }

            import gzip as gzip_mod
            import orjson
            body = orjson.dumps(result)
            accept = request.headers.get('Accept-Encoding', '')
            if 'gzip' in accept:
                body = gzip_mod.compress(body)
                return app.response_class(body, mimetype='application/json',
                    headers={'Content-Encoding': 'gzip'})
            return app.response_class(body, mimetype='application/json')

        except Exception as e:
            import traceback
            print(f"[PORTFOLIO] Error: {e}\n{traceback.format_exc()}", flush=True)
            return jsonify({'error': str(e)}), 500

    # ---- SSE Streaming Holder Endpoint ----

    @app.route('/api/token/<mint>/holders/stream', methods=['GET'])
    def token_holders_stream(mint):
        """
        Server-Sent Events endpoint for progressive holder graph loading.

        Streams events as each worker thread completes:
          event: holders   — immediate (~2s), holders + metadata
          event: edges     — per-thread partial edge batches (up to 20)
          event: complete  — final clusters + stats
        """
        key_info, err = _require_auth()
        if err:
            return err

        force_refresh = 'refresh' in request.args

        import requests as _req
        import time as _time
        import queue
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from t16o_exchange.guide.common.config import load_config
        import orjson

        cfg = load_config()
        api_base = cfg.get('SOLSCAN_API', 'https://pro-api.solscan.io/v2.0')
        api_token = cfg.get('SOLSCAN_TOKEN', '')
        ss_headers = {'token': api_token}
        correlation_id = request.headers.get('X-Correlation-Id') or str(uuid.uuid4())

        def sse_generator():
            t0 = _time.time()

            # ── Check edge cache first ──────────────────────────────────
            if not force_refresh:
                cached = _edge_cache_read(mint)
                if cached:
                    holders_out = cached['holders']
                    edges_out = cached['edges']
                    holders_out, clusters_out = _cluster_with_networkx(holders_out, edges_out)

                    elapsed = _time.time() - t0
                    payload = {
                        'token': cached['token'],
                        'holders': holders_out,
                        'edges': edges_out,
                        'clusters': clusters_out,
                        'meta': {
                            'holder_count': len(holders_out),
                            'edge_count': len(edges_out),
                            'cluster_count': len(clusters_out),
                            'sig_count': cached.get('sig_count', 0),
                            'correlation_id': correlation_id,
                            'elapsed_ms': round(elapsed * 1000),
                            'progressive': 'complete',
                        },
                    }
                    yield f"event: complete\ndata: {orjson.dumps(payload).decode()}\n\n"
                    return

            # ── Step 1: Fetch token metadata + holders ──────────────────
            token_info = {}
            all_holders = []

            def fetch_token_meta():
                try:
                    resp = _req.get(f'{api_base}/token/meta',
                                    params={'address': mint},
                                    headers=ss_headers, timeout=10)
                    if resp.status_code == 200:
                        return resp.json().get('data', {})
                except Exception:
                    pass
                return {}

            def fetch_holder_page(page):
                try:
                    resp = _req.get(f'{api_base}/token/holders',
                                    params={'address': mint, 'page_size': 40, 'page': page},
                                    headers=ss_headers, timeout=10)
                    if resp.status_code != 200:
                        return []
                    return resp.json().get('data', {}).get('items', [])
                except Exception:
                    return []

            with ThreadPoolExecutor(max_workers=3) as pool:
                f_meta = pool.submit(fetch_token_meta)
                f_p1 = pool.submit(fetch_holder_page, 1)
                f_p2 = pool.submit(fetch_holder_page, 2)
                token_info = f_meta.result()
                for f in [f_p1, f_p2]:
                    result = f.result()
                    if result:
                        all_holders.extend(result)

            if not all_holders:
                yield f"event: error\ndata: {orjson.dumps({'error': 'No holders found', 'mint': mint}).decode()}\n\n"
                return

            # Build lookup maps
            ata_to_owner = {}
            owner_to_atas = {}
            all_addrs_to_fetch = set()
            for h in all_holders:
                owner = h.get('owner', '')
                ata = h.get('address', '')
                if not owner:
                    continue
                all_addrs_to_fetch.add(owner)
                if ata:
                    ata_to_owner[ata] = owner
                    owner_to_atas.setdefault(owner, []).append(ata)
                    all_addrs_to_fetch.add(ata)

            # ── Step 2: Metadata enrichment ─────────────────────────────
            addr_metadata = {}

            def fetch_metadata_chunk(chunk):
                try:
                    params = [('address[]', addr) for addr in chunk]
                    resp = _req.get(f'{api_base}/account/metadata/multi',
                                    params=params, headers=ss_headers, timeout=15)
                    if resp.status_code == 200:
                        return resp.json().get('data', [])
                except Exception as e:
                    print(f"[HOLDERS-SSE] Metadata fetch error: {e}", flush=True)
                return []

            all_addrs_list = list(all_addrs_to_fetch)
            chunks = [all_addrs_list[i:i + 50] for i in range(0, len(all_addrs_list), 50)]
            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = [pool.submit(fetch_metadata_chunk, c) for c in chunks]
                for f in futures:
                    data = f.result()
                    if isinstance(data, list):
                        for item in data:
                            addr = item.get('account_address', '')
                            if addr:
                                addr_metadata[addr] = item

            # Known address overrides
            KNOWN_ADDRESSES = {
                '8N2ssXZGJbvVLszanERuCJpLZd2nuADgpZwLkDDxwNnS': ('Upbit Hot Wallet', 'cex', ['cex']),
                '5tzFkiKscjHK98gSfMQXAiYaXdVjbAETRMEK6bQ3uDiA': ('Binance Hot Wallet', 'cex', ['cex']),
                'AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2': ('Bybit Hot Wallet', 'cex', ['cex']),
                '2AQdvzPBQkpxJMhBbCZbKTFnQMXDg3UZqEbQvJGmtJmk': ('OKX Hot Wallet', 'cex', ['cex']),
                'H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS': ('Coinbase Custody', 'cex', ['cex']),
            }
            for addr, (label, addr_type, tags) in KNOWN_ADDRESSES.items():
                if addr in addr_metadata:
                    addr_metadata[addr]['account_label'] = label
                    addr_metadata[addr]['account_tags'] = tags
                else:
                    addr_metadata[addr] = {
                        'account_address': addr,
                        'account_label': label,
                        'account_tags': tags,
                    }

            # ── Step 3: Build holders list ──────────────────────────────
            holders_out = []
            holder_address_set = set()
            for rank, h in enumerate(all_holders, 1):
                owner = h.get('owner', '')
                if not owner or owner in holder_address_set:
                    continue
                holder_address_set.add(owner)

                owner_meta = addr_metadata.get(owner, {})
                ata_addrs = owner_to_atas.get(owner, [])
                ata_metas = [addr_metadata.get(a, {}) for a in ata_addrs if a in addr_metadata]

                addr_type = _derive_address_type(owner_meta, ata_metas)

                all_tags = list(set(
                    owner_meta.get('account_tags', []) +
                    [t for am in ata_metas for t in am.get('account_tags', [])]
                ))

                label = owner_meta.get('account_label')
                if not label:
                    for am in ata_metas:
                        label = am.get('account_label')
                        if label:
                            break

                holders_out.append({
                    'rank': rank,
                    'address': owner,
                    'address_type': addr_type,
                    'label': label,
                    'tags': all_tags,
                    'funded_by': (owner_meta.get('funded_by') or {}).get('funded_by', None),
                    'active_age': owner_meta.get('active_age', None),
                    'percentage': h.get('percentage', 0),
                    'amount': str(h.get('amount', 0)),
                    'decimals': h.get('decimals', 0),
                    'atas': ata_addrs,
                })

            # Step 3b: Enrich from DB (address_type, label, pool, funded_by)
            if holder_address_set and HAS_MYSQL:
                try:
                    import json as _json
                    db_conn = get_db_connection()
                    db_cursor = db_conn.cursor(dictionary=True)
                    addr_json = _json.dumps(list(holder_address_set))
                    db_cursor.execute("CALL sp_tx_address(%s)", (addr_json,))
                    db_rows = db_cursor.fetchall()
                    try:
                        while db_cursor.nextset():
                            db_cursor.fetchall()
                    except Exception:
                        pass
                    db_cursor.close()
                    db_conn.close()

                    db_data = {r['address']: r for r in db_rows}
                    enriched = 0
                    for h in holders_out:
                        db = db_data.get(h['address'])
                        if not db:
                            continue
                        enriched += 1
                        db_type = db.get('address_type')
                        if db_type and h['address_type'] == 'wallet':
                            h['address_type'] = db_type
                        if not h.get('label'):
                            h['label'] = db.get('label') or db.get('pool_label')
                        if db.get('pool_label'):
                            h['pool_label'] = db['pool_label']
                            h['dex'] = db.get('dex')
                            h['token_pair'] = db.get('token_pair')
                        if db.get('program_name'):
                            h['program_name'] = db['program_name']
                        if db.get('funded_by'):
                            h['funded_by'] = db['funded_by']
                        if db.get('active_age_days') and not h.get('active_age'):
                            h['active_age'] = db['active_age_days']

                    if enriched:
                        print(f"[HOLDERS-SSE] DB enrichment: {enriched}/{len(holders_out)} "
                              f"holders matched tx_address", flush=True)
                except Exception as e:
                    print(f"[HOLDERS-SSE] DB lookup failed (non-fatal): {e}", flush=True)

            token_data = {
                'mint': mint,
                'symbol': token_info.get('symbol', ''),
                'name': token_info.get('name', ''),
                'decimals': token_info.get('decimals', 0),
            }

            # ── Emit holders event ──────────────────────────────────────
            elapsed = _time.time() - t0
            holders_payload = {
                'token': token_data,
                'holders': holders_out,
                'meta': {
                    'holder_count': len(holders_out),
                    'correlation_id': correlation_id,
                    'elapsed_ms': round(elapsed * 1000),
                    'progressive': 'holders_ready',
                },
            }
            yield f"event: holders\ndata: {orjson.dumps(holders_payload).decode()}\n\n"

            print(f"[HOLDERS-SSE] {mint[:8]}… — {len(holders_out)} holders streamed "
                  f"in {elapsed:.1f}s, starting edge fetch", flush=True)

            # ── Step 4: Fetch edges with per-thread streaming ───────────
            one_year_ago = int(_time.time()) - 365 * 24 * 60 * 60
            edge_queue = queue.Queue()
            addr_list = list(holder_address_set)

            def fetch_holder_xfers(owner_addr):
                """Fetch transfers for one holder, push raw items to queue."""
                items = []
                for page in range(1, 6):
                    try:
                        resp = _req.get(
                            f'{api_base}/account/transfer',
                            params={
                                'address': owner_addr,
                                'token': mint,
                                'activity_type[]': 'ACTIVITY_SPL_TRANSFER',
                                'page': page,
                                'page_size': 100,
                                'sort_by': 'block_time',
                                'sort_order': 'desc',
                            },
                            headers=ss_headers, timeout=15)
                        if resp.status_code != 200:
                            break
                        data = resp.json().get('data', [])
                        if isinstance(data, dict):
                            data = data.get('items', [])
                        if not data:
                            break
                        hit_cutoff = False
                        for item in data:
                            if item.get('block_time', 0) < one_year_ago:
                                hit_cutoff = True
                                break
                            items.append(item)
                        if hit_cutoff or len(data) < 100:
                            break
                    except Exception:
                        break

                # Build edges against KNOWN holders only (for streaming)
                holder_edges = {}
                holder_sigs = set()
                discovered_addrs = {}  # counterparties not in holder set
                for tx in items:
                    if tx.get('activity_type') != 'ACTIVITY_SPL_TRANSFER':
                        continue
                    sig = tx.get('trans_id', '')
                    if sig:
                        holder_sigs.add(sig)
                    src = tx.get('from_address', '')
                    dst = tx.get('to_address', '')
                    if not src or not dst or src == dst:
                        continue
                    src_known = src in holder_address_set
                    dst_known = dst in holder_address_set
                    # Track unknown counterparties
                    if src_known and not dst_known:
                        discovered_addrs[dst] = discovered_addrs.get(dst, 0) + 1
                    elif dst_known and not src_known:
                        discovered_addrs[src] = discovered_addrs.get(src, 0) + 1
                    # Only build edges between known holders for streaming
                    if src_known and dst_known:
                        key = (src, dst)
                        bt = tx.get('block_time', 0)
                        if key in holder_edges:
                            holder_edges[key]['transfer_count'] += 1
                            holder_edges[key]['amount'] += tx.get('amount', 0)
                            if bt > holder_edges[key]['last_transfer']:
                                holder_edges[key]['last_transfer'] = bt
                            if bt < holder_edges[key]['first_transfer']:
                                holder_edges[key]['first_transfer'] = bt
                        else:
                            holder_edges[key] = {
                                'from': src, 'to': dst,
                                'transfer_count': 1,
                                'amount': tx.get('amount', 0),
                                'decimals': 0,
                                'edge_types': 'spl_transfer',
                                'first_transfer': bt,
                                'last_transfer': bt,
                                'source': 'solscan',
                            }

                edges = list(holder_edges.values())
                for e in edges:
                    e['total_amount'] = str(e.pop('amount', 0))

                edge_queue.put({
                    'edges': edges,
                    'sigs': list(holder_sigs),
                    'holder': owner_addr,
                    'raw_items': items,
                    'discovered': discovered_addrs,
                })

            # Launch 20 workers, collect results as they finish
            import threading as _threading
            total_workers = len(addr_list)
            completed_count = [0]
            all_edges = []
            all_sigs = set()
            all_raw_items = []
            all_discovered = {}
            lock = _threading.Lock()

            def run_edge_workers():
                with ThreadPoolExecutor(max_workers=20) as pool:
                    futures = {pool.submit(fetch_holder_xfers, addr): addr
                               for addr in addr_list}
                    for f in as_completed(futures):
                        try:
                            f.result()
                        except Exception as e:
                            addr = futures[f]
                            print(f"[HOLDERS-SSE] Edge fetch error for {addr[:8]}…: {e}",
                                  flush=True)
                edge_queue.put(None)

            worker_thread = _threading.Thread(target=run_edge_workers, daemon=True)
            worker_thread.start()

            # Stream edge events as they arrive from the queue
            while True:
                try:
                    item = edge_queue.get(timeout=60)
                except queue.Empty:
                    print(f"[HOLDERS-SSE] Edge queue timeout for {mint[:8]}…", flush=True)
                    break

                if item is None:
                    break

                with lock:
                    completed_count[0] += 1
                    all_edges.extend(item['edges'])
                    all_sigs.update(item['sigs'])
                    all_raw_items.extend(item['raw_items'])
                    for addr, cnt in item['discovered'].items():
                        all_discovered[addr] = all_discovered.get(addr, 0) + cnt

                if item['edges']:
                    edge_payload = {
                        'edges': item['edges'],
                        'holder': item['holder'],
                        'meta': {
                            'edge_count': len(item['edges']),
                            'sig_count': len(item['sigs']),
                            'workers_done': completed_count[0],
                            'workers_total': total_workers,
                        },
                    }
                    yield f"event: edges\ndata: {orjson.dumps(edge_payload).decode()}\n\n"

                elif completed_count[0] % 5 == 0 or completed_count[0] == total_workers:
                    progress_payload = {
                        'meta': {
                            'workers_done': completed_count[0],
                            'workers_total': total_workers,
                        },
                    }
                    yield f"event: progress\ndata: {orjson.dumps(progress_payload).decode()}\n\n"

            # ── Step 4b: Promote top discovered addresses ──────────────
            MAX_DISCOVERED = 40
            top_discovered = sorted(all_discovered.items(),
                                    key=lambda x: x[1], reverse=True)[:MAX_DISCOVERED]
            expanded_set = set(holder_address_set)
            for addr, _ in top_discovered:
                expanded_set.add(addr)

            # Rebuild edges using expanded set (includes discovered→discovered)
            if top_discovered:
                expanded_edges = {}
                for tx in all_raw_items:
                    if tx.get('activity_type') != 'ACTIVITY_SPL_TRANSFER':
                        continue
                    src = tx.get('from_address', '')
                    dst = tx.get('to_address', '')
                    if src in expanded_set and dst in expanded_set and src != dst:
                        key = (src, dst)
                        bt = tx.get('block_time', 0)
                        if key in expanded_edges:
                            expanded_edges[key]['transfer_count'] += 1
                            expanded_edges[key]['amount'] += tx.get('amount', 0)
                            if bt > expanded_edges[key]['last_transfer']:
                                expanded_edges[key]['last_transfer'] = bt
                            if bt < expanded_edges[key]['first_transfer']:
                                expanded_edges[key]['first_transfer'] = bt
                        else:
                            expanded_edges[key] = {
                                'from': src, 'to': dst,
                                'transfer_count': 1,
                                'amount': tx.get('amount', 0),
                                'decimals': 0,
                                'edge_types': 'spl_transfer',
                                'first_transfer': bt,
                                'last_transfer': bt,
                                'source': 'solscan',
                            }
                all_edges = list(expanded_edges.values())
                for e in all_edges:
                    if 'amount' in e:
                        e['total_amount'] = str(e.pop('amount', 0))

                # Fetch metadata for discovered addresses
                new_addrs = [addr for addr, _ in top_discovered]
                new_metadata = {}
                chunks = [new_addrs[i:i+50] for i in range(0, len(new_addrs), 50)]
                for chunk in chunks:
                    try:
                        params = [('address[]', a) for a in chunk]
                        resp = _req.get(f'{api_base}/account/metadata/multi',
                                        params=params, headers=ss_headers, timeout=15)
                        if resp.status_code == 200:
                            for it in resp.json().get('data', []):
                                a = it.get('account_address', '')
                                if a:
                                    new_metadata[a] = it
                    except Exception:
                        pass

                # DB enrichment for discovered addresses
                db_data = {}
                if HAS_MYSQL:
                    try:
                        import json as _json
                        db_conn = get_db_connection()
                        db_cursor = db_conn.cursor(dictionary=True)
                        db_cursor.execute("CALL sp_tx_address(%s)",
                                          (_json.dumps(new_addrs),))
                        for row in db_cursor.fetchall():
                            db_data[row['address']] = row
                        try:
                            while db_cursor.nextset():
                                db_cursor.fetchall()
                        except Exception:
                            pass
                        db_cursor.close()
                        db_conn.close()
                    except Exception:
                        pass

                base_rank = len(holders_out) + 1
                for idx, (addr, xfer_cnt) in enumerate(top_discovered):
                    meta = new_metadata.get(addr, {})
                    db = db_data.get(addr, {})
                    tags = meta.get('account_tags', [])
                    label = db.get('label') or meta.get('account_label')
                    addr_type = db.get('address_type') or 'wallet'
                    if addr_type == 'wallet':
                        addr_type = _derive_address_type(meta, [])
                    h = {
                        'rank': base_rank + idx,
                        'address': addr,
                        'address_type': addr_type,
                        'label': label,
                        'tags': tags,
                        'funded_by': db.get('funded_by') or
                            (meta.get('funded_by') or {}).get('funded_by'),
                        'percentage': 0,
                        'amount': '0',
                        'decimals': 0,
                        'atas': [],
                        'discovered': True,
                        'transfer_count': xfer_cnt,
                    }
                    if db.get('pool_label'):
                        h['pool_label'] = db['pool_label']
                        h['dex'] = db.get('dex')
                    holders_out.append(h)

                print(f"[HOLDERS-SSE] Discovered {len(top_discovered)} 2nd-degree "
                      f"addresses from transfers", flush=True)

            # ── Step 5: Final clustering + complete event ───────────────
            holders_clustered, clusters_out = _cluster_with_networkx(
                holders_out, all_edges)

            elapsed = _time.time() - t0
            complete_payload = {
                'token': token_data,
                'holders': holders_clustered,
                'edges': all_edges,
                'clusters': clusters_out,
                'meta': {
                    'holder_count': len(holders_clustered),
                    'edge_count': len(all_edges),
                    'cluster_count': len(clusters_out),
                    'sig_count': len(all_sigs),
                    'correlation_id': correlation_id,
                    'elapsed_ms': round(elapsed * 1000),
                    'progressive': 'complete',
                },
            }
            yield f"event: complete\ndata: {orjson.dumps(complete_payload).decode()}\n\n"

            print(f"[HOLDERS-SSE] {mint[:8]}… — complete: {len(all_edges)} edges, "
                  f"{len(clusters_out)} clusters in {elapsed:.1f}s", flush=True)

            # Write to edge cache (so non-SSE endpoint benefits too)
            _edge_cache_write(mint, {
                'holders': holders_out,
                'edges': all_edges,
                'token': token_data,
                'sig_count': len(all_sigs),
            })

            # Fire sigs to producer (same as original background logic)
            if all_sigs and HAS_PIKA:
                sig_list = list(all_sigs)
                for i in range(0, len(sig_list), 200):
                    chunk = sig_list[i:i + 200]
                    msg = {
                        'request_id': str(uuid.uuid4()),
                        'correlation_id': correlation_id,
                        'action': 'process',
                        'priority': 1,
                        'batch': {'filters': {'signature': chunk}},
                        'tx_origin': 3,
                    }
                    publish_to_worker('producer', msg, priority=1)

        return app.response_class(
            sse_generator(),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'X-Accel-Buffering': 'no',
                'Connection': 'keep-alive',
            },
        )

    def _fetch_and_cache_holders(conn, cursor, mint: str, token_id: int):
        """Fetch top 80 holders from Solscan and bulk upsert via sp_tx_token_holder_upsert."""
        import requests as _req
        import json as _json
        from concurrent.futures import ThreadPoolExecutor
        from t16o_exchange.guide.common.config import load_config

        cfg = load_config()
        api_base = cfg.get('SOLSCAN_API', 'https://pro-api.solscan.io/v2.0')
        api_token = cfg.get('SOLSCAN_TOKEN', '')
        headers = {'token': api_token}

        def fetch_page(page):
            resp = _req.get(
                f'{api_base}/token/holders',
                params={'address': mint, 'page_size': 40, 'page': page},
                headers=headers,
                timeout=10,
            )
            if resp.status_code != 200:
                return []
            return resp.json().get('data', {}).get('items', [])

        # Fetch both holder pages concurrently (~1s instead of ~2s sequential)
        all_holders = []
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(fetch_page, p) for p in range(1, 3)]
            for f in futures:
                result = f.result()
                if result:
                    all_holders.extend(result)

        if not all_holders:
            return

        # Single SP call: address resolution + bulk upsert + stale cleanup
        holders_json = _json.dumps(all_holders)
        cursor.execute("CALL sp_tx_token_holder_upsert(%s, %s)", (token_id, holders_json))
        cursor.fetchall()
        try:
            while cursor.nextset():
                cursor.fetchall()
        except Exception:
            pass
        conn.commit()

    def _fetch_token_transfers_with_sigs(mint: str, holder_addresses: set, max_pages: int = 20) -> tuple:
        """
        Fetch token transfers from Solscan in parallel (5 concurrent).
        Returns (edges_between_holders, all_unique_sigs).
        """
        import requests as _req
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from t16o_exchange.guide.common.config import load_config

        cfg = load_config()
        api_base = cfg.get('SOLSCAN_API', 'https://pro-api.solscan.io/v2.0')
        api_token = cfg.get('SOLSCAN_TOKEN', '')
        headers = {'token': api_token}

        def fetch_page(page):
            try:
                resp = _req.get(
                    f'{api_base}/token/transfer',
                    params={
                        'address': mint,
                        'activity_type[]': 'ACTIVITY_SPL_TRANSFER',
                        'page_size': 40,
                        'page': page,
                    },
                    headers=headers,
                    timeout=10,
                )
                if resp.status_code != 200:
                    return []
                data = resp.json()
                items = data.get('data', [])
                if isinstance(items, dict):
                    items = items.get('items', [])
                return items or []
            except Exception:
                return []

        # Fetch pages in parallel (5 at a time)
        all_items = []
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(fetch_page, p): p for p in range(1, max_pages + 1)}
            for f in as_completed(futures):
                items = f.result()
                if items:
                    all_items.extend(items)

        # Process results
        edges = []
        seen_edges = set()
        all_sigs = set()

        for tx in all_items:
            if tx.get('activity_type') != 'ACTIVITY_SPL_TRANSFER':
                continue
            sig = tx.get('trans_id', '')
            if sig:
                all_sigs.add(sig)

            src = tx.get('from_address', '')
            dst = tx.get('to_address', '')
            if src in holder_addresses and dst in holder_addresses and src != dst:
                key = (src, dst)
                if key not in seen_edges:
                    seen_edges.add(key)
                    edges.append({
                        'from_address': src,
                        'to_address': dst,
                        'amount': tx.get('amount', 0),
                        'block_time': tx.get('block_time', 0),
                        'source': 'solscan',
                    })

        return edges, all_sigs

    def _fetch_holder_transfers_for_graph(mint: str, holder_addresses: set,
                                          max_pages_per_holder: int = 3,
                                          parallel_cnt: int = 5) -> tuple:
        """
        Per-holder transfer fetch via /v2.0/account/transfer for the holder graph.

        For each holder, fetches their SPL transfers for this token (both directions).
        Builds edges between holders and collects all signatures for backfill.
        Much more targeted than the global /token/transfer endpoint.

        Returns (edges_between_holders, all_unique_sigs).
        """
        import requests as _req
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from t16o_exchange.guide.common.config import load_config

        cfg = load_config()
        api_base = cfg.get('SOLSCAN_API', 'https://pro-api.solscan.io/v2.0')
        api_token = cfg.get('SOLSCAN_TOKEN', '')
        headers = {'token': api_token}

        import time as _time
        one_year_ago = int(_time.time()) - 365 * 24 * 60 * 60

        def fetch_holder(owner_addr: str) -> list:
            """Fetch account/transfer for one holder, THIS token only, 1yr cap."""
            items = []
            for page in range(1, max_pages_per_holder + 1):
                try:
                    resp = _req.get(
                        f'{api_base}/account/transfer',
                        params={
                            'address': owner_addr,
                            'token': mint,
                            'activity_type[]': 'ACTIVITY_SPL_TRANSFER',
                            'page': page,
                            'page_size': 100,
                            'sort_by': 'block_time',
                            'sort_order': 'desc',
                        },
                        headers=headers, timeout=15)
                    if resp.status_code != 200:
                        break
                    data = resp.json()
                    page_items = data.get('data', [])
                    if isinstance(page_items, dict):
                        page_items = page_items.get('items', [])
                    if not page_items:
                        break
                    # Stop when we hit transfers older than 1 year
                    hit_cutoff = False
                    for item in page_items:
                        if item.get('block_time', 0) < one_year_ago:
                            hit_cutoff = True
                            break
                        items.append(item)
                    if hit_cutoff or len(page_items) < 100:
                        break
                except Exception:
                    break
            return items

        # Fetch transfers for all holders in parallel
        all_items = []
        with ThreadPoolExecutor(max_workers=parallel_cnt) as pool:
            futures = {pool.submit(fetch_holder, addr): addr
                       for addr in holder_addresses}
            for f in as_completed(futures):
                result = f.result()
                if result:
                    all_items.extend(result)

        # Build edges between holders + collect all sigs
        edges = []
        seen_edges = {}  # (src, dst) → edge dict (accumulate transfer_count)
        all_sigs = set()

        for tx in all_items:
            if tx.get('activity_type') != 'ACTIVITY_SPL_TRANSFER':
                continue
            sig = tx.get('trans_id', '')
            if sig:
                all_sigs.add(sig)

            src = tx.get('from_address', '')
            dst = tx.get('to_address', '')
            if src in holder_addresses and dst in holder_addresses and src != dst:
                key = (src, dst)
                if key in seen_edges:
                    seen_edges[key]['transfer_count'] += 1
                    seen_edges[key]['amount'] += tx.get('amount', 0)
                    bt = tx.get('block_time', 0)
                    if bt > seen_edges[key]['block_time']:
                        seen_edges[key]['block_time'] = bt
                else:
                    seen_edges[key] = {
                        'from_address': src,
                        'to_address': dst,
                        'amount': tx.get('amount', 0),
                        'transfer_count': 1,
                        'block_time': tx.get('block_time', 0),
                        'source': 'solscan',
                    }

        edges = list(seen_edges.values())
        print(f"[HOLDERS] Per-holder fetch: {len(holder_addresses)} holders, "
              f"{len(all_items)} transfers, {len(edges)} holder-to-holder edges, "
              f"{len(all_sigs)} unique sigs", flush=True)

        return edges, all_sigs

    def _backfill_sigs_to_pipeline(sigs: set, existing_cursor=None) -> int:
        """
        Fire transaction signatures to decoder + detailer queues for pipeline backfill.
        Low priority (1) so user requests jump ahead. Returns count published.
        Non-blocking — returns 0 if RabbitMQ is unavailable.
        Uses existing_cursor to avoid opening new DB connections.
        """
        if not sigs:
            return 0

        # Skip if RabbitMQ is not connected
        if not HAS_PIKA:
            return 0

        # Check which sigs already exist in tx table
        existing = set()
        try:
            cur = existing_cursor
            sig_list = list(sigs)
            for i in range(0, len(sig_list), 100):
                chunk = sig_list[i:i+100]
                placeholders = ','.join(['%s'] * len(chunk))
                cur.execute(
                    f"SELECT signature FROM tx WHERE signature IN ({placeholders})",
                    chunk
                )
                existing.update(row[0] if isinstance(row, tuple) else row.get('signature', row) for row in cur.fetchall())
        except Exception as e:
            print(f"[HOLDERS] Backfill sig check failed: {e}", flush=True)

        new_sigs = sigs - existing
        if not new_sigs:
            return 0

        published = 0
        for sig in new_sigs:
            req_id = str(uuid.uuid4())
            msg = {
                'batch': {
                    'signatures': [sig],
                },
                'tx_origin': 3,  # prime/backfill
                'request_id': req_id,
            }
            # Send to both decoder and detailer
            if publish_to_worker('decoder', msg, priority=1):
                published += 1
            publish_to_worker('detailer', msg, priority=1)

        print(f"[HOLDERS] Backfill: {published}/{len(new_sigs)} new sigs published "
              f"({len(existing)} already in pipeline)", flush=True)
        return published

    def _build_holder_edges(holder_rows, edge_rows, solscan_edges):
        """Build holders and edges lists (no clustering — done by RAG service)."""
        holders_out = []
        for r in (holder_rows or []):
            holders_out.append({
                'rank': r['rank'],
                'address': r['address'],
                'address_type': r.get('address_type', 'wallet'),
                'label': r.get('label'),
                'percentage': float(r['percentage']) if r.get('percentage') else 0,
                'amount': str(r['amount']) if r.get('amount') else '0',
                'decimals': r.get('decimals', 0),
            })

        edges_out = []
        if edge_rows:
            for r in edge_rows:
                # Only spl_transfer edges for holder graph
                edge_types = r.get('edge_types', '')
                if 'spl_transfer' not in edge_types:
                    continue
                edges_out.append({
                    'from': r['from_address'],
                    'to': r['to_address'],
                    'transfer_count': r.get('transfer_count', 1),
                    'total_amount': str(r.get('total_amount_raw', 0)),
                    'decimals': r.get('decimals') or 0,
                    'edge_types': 'spl_transfer',
                    'first_transfer': r.get('first_transfer'),
                    'last_transfer': r.get('last_transfer'),
                    'source': 'tx_guide',
                })

        guide_pairs = {(e['from'], e['to']) for e in edges_out}
        for e in solscan_edges:
            pair = (e['from_address'], e['to_address'])
            if pair not in guide_pairs:
                edges_out.append({
                    'from': pair[0],
                    'to': pair[1],
                    'transfer_count': e.get('transfer_count', 1),
                    'total_amount': str(e.get('amount', 0)),
                    'decimals': 0,
                    'edge_types': 'spl_transfer',
                    'first_transfer': e.get('block_time'),
                    'last_transfer': e.get('block_time'),
                    'source': 'solscan',
                })

        return holders_out, edges_out

    def _cluster_with_networkx(holders_out, edges_out):
        """
        Connected-components clustering on the holder graph.

        Drop-in replacement for _cluster_via_rag on the real-time holder map path.
        80 nodes / ~200 edges → <1ms in-process with no HTTP round-trip.

        Algorithm matches bubblemaps visual model exactly: their "clusters" are
        connected components with a force-directed layout, not ML clusters.

        Extra forensic enrichment bundled in at essentially zero cost:
          - central_wallet  : highest degree-centrality node per cluster
          - cycle_count     : simple cycles in directed subgraph (wash trading signal)
          - density         : edge density of subgraph

        The PCA → UMAP → HDBSCAN pipeline (_cluster_via_rag) is preserved for
        oracle context assembly where semantic embedding similarity matters.
        """
        import networkx as nx

        if not holders_out:
            return holders_out, []

        G = nx.Graph()  # Undirected — we want reachability, not direction

        for h in holders_out:
            G.add_node(h['address'],
                       rank=h['rank'],
                       percentage=h['percentage'])

        # Add transfer edges
        for e in edges_out:
            src, dst = e.get('from'), e.get('to')
            if src and dst and G.has_node(src) and G.has_node(dst):
                if G.has_edge(src, dst):
                    G[src][dst]['transfer_count'] += e.get('transfer_count', 1)
                else:
                    G.add_edge(src, dst,
                               transfer_count=e.get('transfer_count', 1),
                               total_amount=e.get('total_amount', '0'),
                               edge_type='transfer')

        # Add funded-by edges: holders sharing the same funder → connected
        funder_groups = {}  # funder_address → [holder_addresses]
        for h in holders_out:
            fb = h.get('funded_by')
            if fb:
                funder_groups.setdefault(fb, []).append(h['address'])

        funded_by_edges = 0
        for funder, members in funder_groups.items():
            if len(members) < 2:
                continue
            # Connect all holders with the same funder (clique)
            for i in range(len(members)):
                for j in range(i + 1, len(members)):
                    if not G.has_edge(members[i], members[j]):
                        G.add_edge(members[i], members[j],
                                   transfer_count=0,
                                   total_amount='0',
                                   edge_type='funded_by',
                                   common_funder=funder)
                        # Add to edges_out so viewer can render them
                        edges_out.append({
                            'from': members[i],
                            'to': members[j],
                            'transfer_count': 0,
                            'total_amount': '0',
                            'decimals': 0,
                            'edge_types': 'funded_by',
                            'common_funder': funder,
                            'source': 'metadata',
                        })
                        funded_by_edges += 1

        if funded_by_edges:
            print(f"[NETWORKX] Added {funded_by_edges} funded-by edges "
                  f"from {len([g for g in funder_groups.values() if len(g) > 1])} "
                  f"common funders", flush=True)

        # ── Connected components = visual clusters ─────────────────────────────
        components = list(nx.connected_components(G))

        # Sort by total % held descending (largest supply cluster first)
        components.sort(
            key=lambda c: sum(G.nodes[n].get('percentage', 0) for n in c),
            reverse=True,
        )

        addr_to_cluster: Dict[str, int] = {}
        clusters_out = []

        for cluster_id, members in enumerate(components):
            total_pct = sum(G.nodes[n].get('percentage', 0) for n in members)
            sub = G.subgraph(members)

            try:
                central = max(nx.degree_centrality(sub).items(), key=lambda x: x[1])
            except Exception:
                central = (None, 0)

            # Build directed subgraph for cycle detection (wash trading signal)
            # Only use transfer/funded_by edges (not swap_activity/portfolio_overlap
            # which create near-complete subgraphs and explode cycle enumeration)
            cycles = []
            try:
                dg = nx.DiGraph()
                for e in edges_out:
                    et = e.get('edge_types', '')
                    if 'portfolio_overlap' in et or 'swap_activity' in et:
                        continue
                    if e.get('from') in members and e.get('to') in members:
                        dg.add_edge(e['from'], e['to'])
                if dg.number_of_edges() <= 500:
                    cycle_gen = nx.simple_cycles(dg)
                    for c in cycle_gen:
                        cycles.append(c)
                        if len(cycles) >= 50:
                            break
            except Exception:
                pass

            for addr in members:
                addr_to_cluster[addr] = cluster_id

            clusters_out.append({
                'id':               cluster_id,
                'size':             len(members),
                'members':          list(members),
                'total_percentage': round(total_pct, 4),
                'central_wallet':   central[0],
                'cycle_count':      len(cycles),   # >0 = potential wash trading
                'density':          round(nx.density(sub), 4),
            })

        # Stamp cluster_id onto each holder record
        for h in holders_out:
            h['cluster_id'] = addr_to_cluster.get(h['address'], -1)

        return holders_out, clusters_out

    def _cluster_via_rag(holders_out, edges_out):
        """Call PCA → UMAP → HDBSCAN on the Qdrant RAG service."""
        import requests as _req

        try:
            resp = _req.post(
                f'{QDRANT_RAG_URL}/api/graph/rag/cluster',
                json={'holders': holders_out, 'edges': edges_out},
                timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get('holders', holders_out), data.get('clusters', [])
            else:
                print(f"[HOLDERS] Cluster service error {resp.status_code}: {resp.text[:200]}", flush=True)
        except Exception as e:
            print(f"[HOLDERS] Cluster service unavailable: {e}", flush=True)

        # Fallback: no clustering
        return holders_out, []

    # ---- Holder Transfer Discovery (POST) ----

    @app.route('/api/token/<mint>/holders', methods=['POST'])
    def token_holders_discover(mint):
        """
        Discover holder transfer activity and submit signatures to the pipeline.

        1. Fetch top N holders from Solscan /v2.0/token/holders
        2. Upsert holders into tx_token_holder
        3. For each holder, fetch account transfers (Solscan /v2.0/account/transfer)
           filtered to this token — parallelized, both in/out directions
        4. Collect distinct transaction signatures
        5. Submit to producer in batches, priority 8, same correlation_id

        Body (all optional — defaults from config table):
            { "max_holders": 40, "max_sigs_per_holder": 500 }
        """
        key_info, err = _require_auth()
        if err:
            return err
        return _discover_holder_transfers(mint, key_info)

    def _discover_holder_transfers(mint: str, key_info: Dict):
        """Orchestrate holder discovery: fetch holders → fetch transfers → submit to producer."""
        import requests as _req
        import time as _time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        t0 = _time.time()
        conn = None

        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)

            # ── Load config defaults ──────────────────────────────────
            cfg_defaults = {}
            cursor.execute(
                "SELECT config_key, config_value FROM config "
                "WHERE config_type = 'batch' AND config_key LIKE 'holders\\_%' ESCAPE '\\\\'")
            for row in cursor.fetchall():
                cfg_defaults[row['config_key']] = int(row['config_value'])

            body = request.get_json() or {}
            max_holders = body.get('max_holders', cfg_defaults.get('holders_max_holders', 40))
            max_sigs_per_holder = body.get('max_sigs_per_holder', cfg_defaults.get('holders_max_sigs_per_holder', 500))
            parallel_cnt = cfg_defaults.get('holders_parallel_cnt', 5)
            producer_batch_size = cfg_defaults.get('holders_producer_batch_size', 200)

            # Correlation ID — single ID for the entire pipeline
            correlation_id = request.headers.get('X-Correlation-Id') or str(uuid.uuid4())
            request_id = request.headers.get('X-Request-Id') or str(uuid.uuid4())

            # ── Step 0: Resolve token ─────────────────────────────────
            cursor.execute(
                "SELECT tk.id, tk.token_symbol, tk.token_name, tk.decimals, "
                "       tk.supply, mint.address AS mint_address "
                "FROM tx_token tk "
                "JOIN tx_address mint ON mint.id = tk.mint_address_id "
                "WHERE mint.address = %s LIMIT 1",
                (mint,))
            token_row = cursor.fetchone()
            if not token_row:
                return jsonify({'error': 'Token not found', 'mint': mint}), 404

            token_id = token_row['id']

            # ── Step 1: Fetch holders from Solscan ────────────────────
            from t16o_exchange.guide.common.config import load_config
            cfg = load_config()
            api_base = cfg.get('SOLSCAN_API', 'https://pro-api.solscan.io/v2.0')
            api_token = cfg.get('SOLSCAN_TOKEN', '')
            ss_headers = {'token': api_token}

            all_holders = []
            page_size = 40
            pages_needed = (max_holders + page_size - 1) // page_size
            for page in range(1, pages_needed + 1):
                resp = _req.get(
                    f'{api_base}/token/holders',
                    params={'address': mint, 'page_size': page_size, 'page': page},
                    headers=ss_headers, timeout=10)
                if resp.status_code != 200:
                    print(f"[HOLDER-DISC] Solscan holders page {page} failed: {resp.status_code}", flush=True)
                    break
                data = resp.json()
                items = data.get('data', {}).get('items', [])
                if not items:
                    break
                all_holders.extend(items)
                if len(all_holders) >= max_holders:
                    all_holders = all_holders[:max_holders]
                    break

            if not all_holders:
                return jsonify({
                    'error': 'No holders found from Solscan',
                    'mint': mint, 'correlation_id': correlation_id
                }), 404

            # ── Step 2: Upsert holders ────────────────────────────────
            import json as _json
            holders_json = _json.dumps(all_holders)
            cursor.execute("CALL sp_tx_token_holder_upsert(%s, %s)", (token_id, holders_json))
            cursor.fetchall()
            try:
                while cursor.nextset():
                    cursor.fetchall()
            except Exception:
                pass
            conn.commit()

            holders_found = len(all_holders)
            # Build owner address set (owner = wallet address)
            owner_addresses = [h.get('owner', '') for h in all_holders if h.get('owner')]
            print(f"[HOLDER-DISC] {mint[:8]}… — {holders_found} holders fetched, "
                  f"scanning transfers for {len(owner_addresses)} owners", flush=True)

            # ── Step 3: Fetch transfers per holder (parallel) ─────────
            page_size_xfer = 100
            max_pages_per_holder = (max_sigs_per_holder + page_size_xfer - 1) // page_size_xfer

            import time as _time
            one_year_ago = int(_time.time()) - 365 * 24 * 60 * 60

            def fetch_holder_transfers(owner_addr: str) -> list:
                """Fetch account/transfer pages for one holder, capped at 1 year."""
                sigs = []
                for page in range(1, max_pages_per_holder + 1):
                    try:
                        resp = _req.get(
                            f'{api_base}/account/transfer',
                            params={
                                'address': owner_addr,
                                'token': mint,
                                'activity_type[]': 'ACTIVITY_SPL_TRANSFER',
                                'page': page,
                                'page_size': page_size_xfer,
                                'sort_by': 'block_time',
                                'sort_order': 'desc',
                            },
                            headers=ss_headers, timeout=15)
                        if resp.status_code != 200:
                            break
                        data = resp.json()
                        items = data.get('data', [])
                        if isinstance(items, dict):
                            items = items.get('items', [])
                        if not items:
                            break
                        hit_cutoff = False
                        for tx in items:
                            if tx.get('block_time', 0) < one_year_ago:
                                hit_cutoff = True
                                break
                            if tx.get('activity_type') != 'ACTIVITY_SPL_TRANSFER':
                                continue
                            sig = tx.get('trans_id', '')
                            if sig:
                                sigs.append(sig)
                        if hit_cutoff or len(items) < page_size_xfer:
                            break
                    except Exception as e:
                        print(f"[HOLDER-DISC] Transfer fetch error for {owner_addr[:8]}… page {page}: {e}", flush=True)
                        break
                return sigs

            all_sigs = set()
            pages_fetched = 0
            with ThreadPoolExecutor(max_workers=parallel_cnt) as pool:
                futures = {pool.submit(fetch_holder_transfers, addr): addr for addr in owner_addresses}
                for f in as_completed(futures):
                    holder_sigs = f.result()
                    all_sigs.update(holder_sigs)
                    if holder_sigs:
                        pages_fetched += (len(holder_sigs) + page_size_xfer - 1) // page_size_xfer

            print(f"[HOLDER-DISC] {mint[:8]}… — {len(all_sigs)} distinct signatures collected "
                  f"from {len(owner_addresses)} holders", flush=True)

            # ── Step 4: Filter out existing signatures via SP ─────────
            import json as _json
            sig_list = list(all_sigs)
            new_sigs = []
            try:
                for i in range(0, len(sig_list), 1000):
                    chunk = sig_list[i:i + 1000]
                    cursor.execute("CALL sp_tx_filter_exists(%s)", (_json.dumps(chunk),))
                    rows = cursor.fetchall()
                    new_sigs.extend(row['signature'] for row in rows)
                    try:
                        while cursor.nextset():
                            cursor.fetchall()
                    except Exception:
                        pass
            except Exception as e:
                print(f"[HOLDER-DISC] sp_tx_filter_exists failed, submitting all: {e}", flush=True)
                new_sigs = sig_list

            existing_cnt = len(all_sigs) - len(new_sigs)
            print(f"[HOLDER-DISC] {mint[:8]}… — {len(new_sigs)} new sigs "
                  f"({existing_cnt} already in pipeline)", flush=True)

            # ── Step 5: Log gateway anchor request ────────────────────
            api_key_id = key_info.get('id')
            features = key_info.get('feature_mask') or 31
            api_key_val = request.headers.get('X-API-Key')

            gateway_log_id = log_request(
                request_id=request_id,
                api_key_id=api_key_id,
                source='rest',
                target_worker='gateway',
                action='holder_discover',
                priority=8,
                payload={'mint': mint, 'holders': holders_found,
                         'signatures_total': len(all_sigs),
                         'signatures_new': len(new_sigs)},
                correlation_id=correlation_id,
                status='processing',
                features=features)

            # ── Step 6: Chunk & submit to producer ────────────────────
            batches_submitted = 0
            sigs_submitted = 0

            for i in range(0, len(new_sigs), producer_batch_size):
                chunk = new_sigs[i:i + producer_batch_size]
                batch_request_id = str(uuid.uuid4())

                # Log each batch submission
                batch_log_id = log_request(
                    request_id=batch_request_id,
                    api_key_id=api_key_id,
                    source='rest',
                    target_worker='producer',
                    action='process',
                    priority=8,
                    payload={'batch': {'filters': {'signature': chunk}}},
                    correlation_id=correlation_id,
                    status='queued',
                    features=features)

                msg = {
                    'request_id': batch_request_id,
                    'correlation_id': correlation_id,
                    'request_log_id': batch_log_id if batch_log_id and batch_log_id > 0 else gateway_log_id,
                    'api_key': api_key_val,
                    'api_key_id': api_key_id,
                    'features': features,
                    'priority': 8,
                    'action': 'process',
                    'batch': {
                        'filters': {
                            'signature': chunk
                        }
                    },
                    'tx_origin': 3,
                }
                if publish_to_worker('producer', msg, priority=8):
                    batches_submitted += 1
                    sigs_submitted += len(chunk)

            elapsed = _time.time() - t0
            print(f"[HOLDER-DISC] {mint[:8]}… — {sigs_submitted} sigs submitted in "
                  f"{batches_submitted} batches, {elapsed:.1f}s", flush=True)

            # ── Step 7: Update gateway anchor log ─────────────────────
            if gateway_log_id and gateway_log_id > 0:
                try:
                    upd_conn = get_db_connection()
                    upd_cursor = upd_conn.cursor()
                    upd_cursor.execute(
                        "UPDATE tx_request_log SET status = 'completed', "
                        "started_at = COALESCE(started_at, created_utc), "
                        "completed_at = NOW(3), "
                        "result = %s WHERE id = %s",
                        (_json.dumps({'batches': batches_submitted,
                                      'sigs_submitted': sigs_submitted,
                                      'sigs_total': len(all_sigs),
                                      'sigs_existing': existing_cnt}),
                         gateway_log_id))
                    upd_conn.commit()
                    upd_cursor.close()
                    upd_conn.close()
                except Exception as e:
                    print(f"[HOLDER-DISC] Failed to update request log: {e}", flush=True)

            return jsonify({
                'success': True,
                'correlation_id': correlation_id,
                'request_id': request_id,
                'mint': mint,
                'token': {
                    'symbol': token_row['token_symbol'],
                    'name': token_row['token_name'],
                },
                'holders_found': holders_found,
                'signatures_total': len(all_sigs),
                'signatures_existing': existing_cnt,
                'signatures_submitted': sigs_submitted,
                'batches_submitted': batches_submitted,
                'pages_fetched': pages_fetched,
                'config': {
                    'max_holders': max_holders,
                    'max_sigs_per_holder': max_sigs_per_holder,
                    'parallel_cnt': parallel_cnt,
                    'producer_batch_size': producer_batch_size,
                },
                'elapsed_ms': round(elapsed * 1000),
            })

        except Exception as e:
            import traceback
            print(f"[HOLDER-DISC] Error: {e}\n{traceback.format_exc()}", flush=True)
            return jsonify({'error': str(e), 'mint': mint}), 500
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    @app.route('/api/token/lookup/<symbol>', methods=['GET'])
    def token_lookup(symbol):
        """Resolve token symbol to mint and metadata. mask=1."""
        key_info, err = _require_auth()
        if err: return err
        return _call_sp_tx_get(
            p_symbol=symbol,
            p_limit=1,
            p_result_mask=1)

    # ---- Qdrant RAG Search Proxy ----

    QDRANT_RAG_URL = os.environ.get('QDRANT_RAG_URL', '')

    @app.route('/api/graph/rag/search', methods=['GET'])
    def rag_search_proxy():
        """Proxy search requests to Qdrant RAG search service."""
        import requests as _req
        if not QDRANT_RAG_URL:
            return jsonify({'error': 'RAG search service not configured'}), 503
        key_info, err = _require_auth()
        if err: return err
        try:
            resp = _req.get(
                f'{QDRANT_RAG_URL}/api/graph/rag/search',
                params=request.args,
                timeout=15
            )
            return app.response_class(
                response=resp.content,
                status=resp.status_code,
                mimetype='application/json'
            )
        except _req.exceptions.Timeout:
            return jsonify({'error': 'RAG search service timeout'}), 504
        except _req.exceptions.ConnectionError:
            return jsonify({'error': 'RAG search service unavailable'}), 503
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    @app.route('/api/graph/rag/cluster', methods=['POST'])
    def rag_cluster_proxy():
        """Proxy cluster requests to Qdrant RAG clustering service."""
        import requests as _req
        if not QDRANT_RAG_URL:
            return jsonify({'error': 'RAG cluster service not configured'}), 503
        key_info, err = _require_auth()
        if err: return err
        try:
            resp = _req.post(
                f'{QDRANT_RAG_URL}/api/graph/rag/cluster',
                json=request.get_json(),
                timeout=30
            )
            return app.response_class(
                response=resp.content,
                status=resp.status_code,
                mimetype='application/json'
            )
        except _req.exceptions.Timeout:
            return jsonify({'error': 'RAG cluster service timeout'}), 504
        except _req.exceptions.ConnectionError:
            return jsonify({'error': 'RAG cluster service unavailable'}), 503
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    # ---- Qdrant RAG Findings Proxy ----

    @app.route('/api/graph/rag/findings/ingest', methods=['POST'])
    def rag_findings_ingest_proxy():
        """Proxy findings ingest requests to Qdrant RAG search service."""
        import requests as _req
        if not QDRANT_RAG_URL:
            return jsonify({'error': 'RAG search service not configured'}), 503
        key_info, err = _require_auth()
        if err: return err
        try:
            resp = _req.post(
                f'{QDRANT_RAG_URL}/api/graph/rag/findings/ingest',
                json=request.get_json(),
                timeout=15
            )
            return app.response_class(
                response=resp.content,
                status=resp.status_code,
                mimetype='application/json'
            )
        except _req.exceptions.Timeout:
            return jsonify({'error': 'RAG findings ingest timeout'}), 504
        except _req.exceptions.ConnectionError:
            return jsonify({'error': 'RAG search service unavailable'}), 503
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    @app.route('/api/graph/rag/findings/search', methods=['GET'])
    def rag_findings_search_proxy():
        """Proxy findings search requests to Qdrant RAG search service."""
        import requests as _req
        if not QDRANT_RAG_URL:
            return jsonify({'error': 'RAG search service not configured'}), 503
        key_info, err = _require_auth()
        if err: return err
        try:
            resp = _req.get(
                f'{QDRANT_RAG_URL}/api/graph/rag/findings/search',
                params=request.args,
                timeout=15
            )
            return app.response_class(
                response=resp.content,
                status=resp.status_code,
                mimetype='application/json'
            )
        except _req.exceptions.Timeout:
            return jsonify({'error': 'RAG findings search timeout'}), 504
        except _req.exceptions.ConnectionError:
            return jsonify({'error': 'RAG search service unavailable'}), 503
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    @app.route('/api/graph/rag/findings/stats', methods=['GET'])
    def rag_findings_stats_proxy():
        """Proxy findings stats requests to Qdrant RAG search service."""
        import requests as _req
        if not QDRANT_RAG_URL:
            return jsonify({'error': 'RAG search service not configured'}), 503
        key_info, err = _require_auth()
        if err: return err
        try:
            resp = _req.get(
                f'{QDRANT_RAG_URL}/api/graph/rag/findings/stats',
                timeout=10
            )
            return app.response_class(
                response=resp.content,
                status=resp.status_code,
                mimetype='application/json'
            )
        except _req.exceptions.Timeout:
            return jsonify({'error': 'RAG findings stats timeout'}), 504
        except _req.exceptions.ConnectionError:
            return jsonify({'error': 'RAG search service unavailable'}), 503
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    # ---- Navigation Endpoints — oldest/newest tx for an address ----
    # IMPORTANT: these must be registered BEFORE /api/tx/<signature> catch-all

    def _call_sp_tx_boundary(address, direction):
        """Call sp_tx_boundary and return JSON response."""
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.callproc('sp_tx_boundary', [address, direction])
            result = None
            for res in cursor.stored_results():
                row = res.fetchone()
                if row:
                    result = row[0]
                break
            cursor.close()
            if result:
                parsed = json.loads(result) if isinstance(result, str) else result
                if 'error' in parsed:
                    return jsonify(parsed), 404
                return jsonify(parsed)
            return jsonify({'error': 'No result returned'}), 404
        except Exception as e:
            return jsonify({'error': str(e)}), 500
        finally:
            if conn:
                conn.close()

    @app.route('/api/tx/<address>/oldest', methods=['GET'])
    def tx_oldest(address):
        """Return the oldest transaction signature for a mint or wallet address."""
        key_info, err = _require_auth()
        if err: return err
        return _call_sp_tx_boundary(address, 'oldest')

    @app.route('/api/tx/<address>/latest', methods=['GET'])
    def tx_latest(address):
        """Return the latest (newest) transaction signature for a mint or wallet address."""
        key_info, err = _require_auth()
        if err: return err
        return _call_sp_tx_boundary(address, 'latest')

    @app.route('/api/tx/<address>/bridge', methods=['GET'])
    def tx_bridge(address):
        """Return the contiguous coverage bridge for an address at a given block_time."""
        key_info, err = _require_auth()
        if err: return err
        block_time = request.args.get('block_time', type=int)
        return _compute_bridge(address, block_time)

    def _compute_bridge(address, block_time=None):
        """Walk tx_boundary rows to find the contiguous coverage bridge containing block_time."""
        TOLERANCE = 60  # seconds — merge sessions within 60s of each other
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)

            # Resolve address_id
            cursor.execute("SELECT id FROM tx_address WHERE address = %s", (address,))
            addr_row = cursor.fetchone()
            if not addr_row:
                return jsonify({'error': 'Address not found'}), 404
            addr_id = addr_row['id']

            # Resolve token_id for sig lookups (if this is a mint address)
            cursor.execute("SELECT id FROM tx_token WHERE mint_address_id = %s LIMIT 1", (addr_id,))
            token_row = cursor.fetchone()
            token_id = token_row['id'] if token_row else None

            # Fetch all boundary rows with effective coverage ranges
            cursor.execute("""
                SELECT
                    COALESCE(request_until_block_time, oldest_fetched_block_time) AS eff_oldest,
                    COALESCE(request_before_block_time, latest_fetched_block_time) AS eff_newest
                FROM tx_boundary
                WHERE address_id = %s
                  AND oldest_fetched_block_time IS NOT NULL
                ORDER BY COALESCE(request_until_block_time, oldest_fetched_block_time) ASC
            """, (addr_id,))
            rows = cursor.fetchall()

            # Filter out rows with NULL effective ranges
            rows = [r for r in rows if r['eff_oldest'] is not None and r['eff_newest'] is not None]

            if not rows:
                return jsonify({'bridge': None, 'total_bridges': 0, 'address': address})

            # Interval merge — build bridges from overlapping/adjacent sessions
            bridges = []
            cur_oldest = rows[0]['eff_oldest']
            cur_newest = rows[0]['eff_newest']

            for row in rows[1:]:
                if row['eff_oldest'] <= cur_newest + TOLERANCE:
                    cur_newest = max(cur_newest, row['eff_newest'])
                else:
                    bridges.append((cur_oldest, cur_newest))
                    cur_oldest = row['eff_oldest']
                    cur_newest = row['eff_newest']
            bridges.append((cur_oldest, cur_newest))

            # Find which bridge contains the requested block_time
            selected = None
            selected_idx = None
            if block_time:
                for i, (oldest, newest) in enumerate(bridges):
                    if oldest - TOLERANCE <= block_time <= newest + TOLERANCE:
                        selected = (oldest, newest)
                        selected_idx = i
                        break
            # Fallback: latest bridge (most recent data)
            if selected is None:
                selected = bridges[-1]
                selected_idx = len(bridges) - 1

            bridge_oldest, bridge_newest = selected

            # Find actual sigs at bridge edges
            oldest_sig_row = None
            newest_sig_row = None

            if token_id:
                # Mint-based: query tx_guide by token_id
                cursor.execute("""
                    SELECT t.signature, t.block_time
                    FROM tx_guide g JOIN tx t ON t.id = g.tx_id
                    WHERE g.token_id = %s AND t.block_time >= %s AND t.block_time <= %s
                    ORDER BY t.block_time ASC LIMIT 1
                """, (token_id, bridge_oldest, bridge_newest))
                oldest_sig_row = cursor.fetchone()

                cursor.execute("""
                    SELECT t.signature, t.block_time
                    FROM tx_guide g JOIN tx t ON t.id = g.tx_id
                    WHERE g.token_id = %s AND t.block_time >= %s AND t.block_time <= %s
                    ORDER BY t.block_time DESC LIMIT 1
                """, (token_id, bridge_oldest, bridge_newest))
                newest_sig_row = cursor.fetchone()
            else:
                # Wallet-based: query tx_guide by address
                cursor.execute("""
                    SELECT t.signature, t.block_time
                    FROM tx_guide g JOIN tx t ON t.id = g.tx_id
                    WHERE (g.from_address_id = %s OR g.to_address_id = %s)
                      AND t.block_time >= %s AND t.block_time <= %s
                    ORDER BY t.block_time ASC LIMIT 1
                """, (addr_id, addr_id, bridge_oldest, bridge_newest))
                oldest_sig_row = cursor.fetchone()

                cursor.execute("""
                    SELECT t.signature, t.block_time
                    FROM tx_guide g JOIN tx t ON t.id = g.tx_id
                    WHERE (g.from_address_id = %s OR g.to_address_id = %s)
                      AND t.block_time >= %s AND t.block_time <= %s
                    ORDER BY t.block_time DESC LIMIT 1
                """, (addr_id, addr_id, bridge_oldest, bridge_newest))
                newest_sig_row = cursor.fetchone()

            # Build gap list relative to current bridge
            gaps = []
            if selected_idx > 0:
                prev_bridge = bridges[selected_idx - 1]
                gaps.append({
                    'direction': 'older',
                    'gap_start': prev_bridge[1],   # where previous bridge ends
                    'gap_end': bridge_oldest,        # where current bridge starts
                    'gap_seconds': bridge_oldest - prev_bridge[1],
                    'fetch_before_bt': bridge_oldest + 1  # use this as "before" param to fill
                })
            if selected_idx < len(bridges) - 1:
                next_bridge = bridges[selected_idx + 1]
                gaps.append({
                    'direction': 'newer',
                    'gap_start': bridge_newest,
                    'gap_end': next_bridge[0],
                    'gap_seconds': next_bridge[0] - bridge_newest,
                    'fetch_before_bt': next_bridge[0] + 1
                })

            from datetime import datetime, timezone
            def fmt_utc(ts):
                return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S') if ts else None

            cursor.close()
            return jsonify({
                'bridge': {
                    'oldest_time': bridge_oldest,
                    'newest_time': bridge_newest,
                    'oldest_time_utc': fmt_utc(bridge_oldest),
                    'newest_time_utc': fmt_utc(bridge_newest),
                    'oldest_sig': oldest_sig_row['signature'] if oldest_sig_row else None,
                    'newest_sig': newest_sig_row['signature'] if newest_sig_row else None,
                },
                'gaps': gaps,
                'total_bridges': len(bridges),
                'bridge_index': selected_idx,
                'has_older_bridge': selected_idx > 0,
                'has_newer_bridge': selected_idx < len(bridges) - 1,
                'canyon_before': True,   # Always assume more history exists before the oldest bridge edge
                'canyon_after': selected_idx < len(bridges) - 1,
                'address': address
            })

        except Exception as e:
            return jsonify({'error': str(e)}), 500
        finally:
            if conn:
                conn.close()

    # ---- Transaction-Centric Endpoints ----

    @app.route('/api/tx/<signature>', methods=['GET'])
    def tx_detail(signature):
        """Single transaction detail with full graph. mask=207, limit=1."""
        key_info, err = _require_auth()
        if err: return err
        return _call_sp_tx_get(
            p_signature=signature,
            p_limit=1,
            p_result_mask=207)

    @app.route('/api/tx/<signature>/context', methods=['GET'])
    def tx_context(signature):
        """Transaction in context — half before, half after. mask=207."""
        key_info, err = _require_auth()
        if err: return err
        return _call_sp_tx_get(
            p_signature=signature,
            p_mint=request.args.get('mint') or None,
            p_limit=request.args.get('limit', default=10, type=int),
            p_result_mask=207)

    # ---- Filtered Endpoints ----

    @app.route('/api/swaps', methods=['GET'])
    def swaps():
        """Swap activity only. Filters to swap edge types."""
        key_info, err = _require_auth()
        if err: return err
        # type_mask for swap_in + swap_out indicators
        swap_mask = 137438953472 | 274877906944  # swap_in | swap_out
        return _call_sp_tx_get(
            p_address=request.args.get('address') or None,
            p_mint=request.args.get('mint') or None,
            p_symbol=request.args.get('symbol') or None,
            p_limit=request.args.get('limit', default=50, type=int),
            p_after=request.args.get('after', type=int),
            p_before=request.args.get('before', type=int),
            p_type_mask=swap_mask,
            p_result_mask=request.args.get('result_mask', default=113, type=int))

    @app.route('/api/transfers', methods=['GET'])
    def transfers():
        """Transfer activity only. Filters to spl_transfer + sol_transfer edge types."""
        key_info, err = _require_auth()
        if err: return err
        # type_mask for spl_transfer + sol_transfer indicators
        transfer_mask = 8589934592 | 17179869184  # sol_transfer | spl_transfer
        return _call_sp_tx_get(
            p_address=request.args.get('address') or None,
            p_mint=request.args.get('mint') or None,
            p_symbol=request.args.get('symbol') or None,
            p_limit=request.args.get('limit', default=50, type=int),
            p_after=request.args.get('after', type=int),
            p_before=request.args.get('before', type=int),
            p_type_mask=transfer_mask,
            p_result_mask=request.args.get('result_mask', default=113, type=int))

    # ---- Worker Triggers ----

    @app.route('/api/trigger/<worker>', methods=['POST'])
    def trigger_worker(worker):
        """Trigger a worker with request payload"""
        if worker not in WORKER_REGISTRY:
            return jsonify({
                'success': False,
                'error': f'Unknown worker: {worker}',
                'available_workers': list(WORKER_REGISTRY.keys())
            }), 404

        # Get API key from header
        api_key = request.headers.get('X-API-Key')
        if not api_key:
            return jsonify({
                'success': False,
                'error': 'Missing X-API-Key header'
            }), 401

        # Validate API key first (needed for rate limit check)
        key_info = validate_api_key(api_key)
        if not key_info:
            return jsonify({
                'success': False,
                'error': 'Invalid or inactive API key'
            }), 401

        # Get request ID and correlation ID from headers (auto-generate if not provided)
        request_id = request.headers.get('X-Request-Id') or str(uuid.uuid4())
        correlation_id = request.headers.get('X-Correlation-Id') or request_id

        # Check rate limit
        rate_limit = key_info.get('rate_limit', 0)
        allowed, retry_after = check_rate_limit(key_info['id'], rate_limit)
        if not allowed:
            # Log rate-limited request
            rl_priority = request.headers.get('X-Priority', type=int) or REST_API_DEFAULT_PRIORITY
            log_request(
                request_id=request_id,
                api_key_id=key_info.get('id'),
                source='rest',
                target_worker=worker,
                action='process',
                priority=rl_priority,
                payload={},
                status='rejected',
                error=f'Rate limit exceeded ({rate_limit}/min)',
                correlation_id=correlation_id
            )
            print(f"[RATE LIMIT] API key {key_info.get('name', 'unknown')} exceeded {rate_limit}/min for {worker}", flush=True)

            response = jsonify({
                'success': False,
                'error': 'Rate limit exceeded',
                'request_id': request_id,
                'correlation_id': correlation_id,
                'retry_after_seconds': retry_after,
                'rate_limit': rate_limit,
                'window_seconds': RATE_LIMIT_WINDOW_SECONDS
            })
            response.headers['Retry-After'] = str(retry_after)
            return response, 429

        # Get request payload
        payload = request.get_json() or {}
        action = payload.get('action', 'process')

        # Priority from header (default 5), injected into payload for RabbitMQ message body
        priority = request.headers.get('X-Priority', type=int) or REST_API_DEFAULT_PRIORITY
        payload['priority'] = priority
        payload['request_id'] = request_id
        payload['correlation_id'] = correlation_id

        # Get features from API key entitlement (no request parameter needed)
        features = key_info.get('feature_mask') or 31
        payload['features'] = features

        # Check feature requirements for specific workers
        if worker == 'funder' and not (features & FEATURE_FUNDER_DISCOVERY):
            return jsonify({
                'success': False,
                'error': 'Funder discovery feature not enabled for this API key',
                'request_id': request_id,
                'correlation_id': correlation_id
            }), 403

        # Create "gateway" record as the billing anchor
        # This is the canonical record that tx records will link back to
        request_log_id = log_request(
            request_id=request_id,
            api_key_id=key_info.get('id'),
            source='rest',
            target_worker='gateway',
            action=action,
            priority=priority,
            payload=payload,
            correlation_id=correlation_id,
            status='queued',
            features=features
        )

        if not request_log_id:
            return jsonify({
                'success': False,
                'error': 'Request Already Exists',
                'request_id': request_id,
                'correlation_id': correlation_id
            }), 500

        # Handle duplicate request_id (negative value indicates existing record)
        if request_log_id < 0:
            existing_id = -request_log_id
            return jsonify({
                'success': False,
                'error': f'Request ID already exists (use a unique request_id)',
                'request_id': request_id,
                'correlation_id': correlation_id,
                'existing_log_id': existing_id
            }), 409  # 409 Conflict

        result = process_request(
            worker=worker,
            action=action,
            payload=payload,
            api_key=api_key,
            source='rest',
            correlation_id=correlation_id,
            request_id=request_id,
            request_log_id=request_log_id,  # Pass gateway record ID for billing
            features=features
        )

        # Add rate limit headers to successful responses
        rate_status = get_rate_limit_status(key_info['id'], rate_limit)
        response = jsonify(result)
        if rate_limit:
            response.headers['X-RateLimit-Limit'] = str(rate_limit)
            response.headers['X-RateLimit-Remaining'] = str(rate_status['remaining'])
            response.headers['X-RateLimit-Reset'] = str(rate_status['reset_seconds'])

        status_code = 200 if result['success'] else 400
        return response, status_code

    @app.route('/api/status/<request_id>', methods=['GET'])
    def get_status(request_id):
        """Get status of a single request"""
        status = get_request_status(request_id)
        if status:
            return jsonify(status)
        else:
            return jsonify({'error': 'Request not found'}), 404

    @app.route('/api/pipeline/<correlation_id>', methods=['GET'])
    def get_pipeline_status(correlation_id):
        """Get aggregated status of entire pipeline by correlation_id

        Returns progress across all workers for the original REST request.
        Example response:
        {
            "correlation_id": "abc123",
            "overall_status": "processing",
            "total_requests": 25,
            "completed": 20,
            "failed": 0,
            "pending": 5,
            "progress": {
                "producer": {"total": 1, "completed": 1, "failed": 0, "pending": 0},
                "shredder": {"total": 10, "completed": 10, "failed": 0, "pending": 0},
                "funder": {"total": 8, "completed": 5, "failed": 0, "pending": 3},
                ...
            }
        }
        """
        status = get_correlation_status(correlation_id)
        if status:
            return jsonify(status)
        else:
            return jsonify({'error': 'Correlation not found'}), 404

    @app.route('/api/cascade', methods=['POST'])
    def cascade():
        """Handle cascade from worker (internal use)"""
        api_key = request.headers.get('X-API-Key', 'internal_cascade_key')

        # Validate internal key
        key_info = validate_api_key(api_key)
        if not key_info or not check_permission(key_info, '*', 'cascade'):
            return jsonify({'error': 'Cascade permission denied'}), 403

        payload = request.get_json()
        if not payload:
            return jsonify({'error': 'No payload provided'}), 400

        results = process_cascade(payload)
        return jsonify({'results': results})

    # --- GraphQL (optional — only if strawberry is installed) ---
    try:
        from guide_graphql import mount_graphql
        mount_graphql(app, get_db_connection, validate_api_key, check_rate_limit)
    except ImportError:
        print("[GraphQL] strawberry-graphql not installed — /api/graphql disabled")
    except Exception as e:
        print(f"[GraphQL] Failed to mount: {e}")

    return app


# =============================================================================
# Queue Consumer (for cascade messages)
# =============================================================================

def run_queue_consumer(ready_event: threading.Event = None):
    """Run a consumer for gateway request queue (cascade messages)"""
    print(f"[INFO] Starting queue consumer for {GATEWAY_REQUEST_QUEUE}")

    while True:
        try:
            conn = get_rabbitmq_connection()
            channel = conn.channel()
            ensure_queues_exist(channel)
            channel.basic_qos(prefetch_count=10)

            def callback(ch, method, properties, body):
                try:
                    message = json.loads(body.decode('utf-8'))
                    action = message.get('action')

                    if action == 'cascade':
                        # Cascade from worker to downstream workers
                        results = process_cascade(message)
                        print(f"[CASCADE] Processed cascade from {message.get('source_worker')}: {len(results)} downstream")
                    else:
                        # Regular request - validate API key and route to worker
                        api_key = message.get('api_key')
                        target_worker = message.get('target_worker')
                        source = message.get('source', 'queue')
                        request_id = message.get('request_id', str(uuid.uuid4()))
                        correlation_id = message.get('correlation_id')  # May be None for initial requests
                        msg_features = message.get('features', 31)  # Feature flags from message

                        if not api_key:
                            print(f"[REJECT] Missing api_key in request {request_id}", flush=True)
                            log_request(
                                request_id=request_id,
                                api_key_id=None,
                                source=source,
                                target_worker=target_worker or 'unknown',
                                action=action or 'process',
                                payload=message,
                                status='rejected',
                                error='Missing api_key'
                            )
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                            return

                        if not target_worker:
                            print(f"[REJECT] Missing target_worker in request {request_id}", flush=True)
                            log_request(
                                request_id=request_id,
                                api_key_id=None,
                                source=source,
                                target_worker='unknown',
                                action=action or 'process',
                                payload=message,
                                status='rejected',
                                error='Missing target_worker'
                            )
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                            return

                        # Validate API key
                        key_info = validate_api_key(api_key)
                        if not key_info:
                            print(f"[REJECT] Invalid API key: {api_key[:8]}... for request {request_id}", flush=True)
                            log_request(
                                request_id=request_id,
                                api_key_id=None,
                                source=source,
                                target_worker=target_worker,
                                action=action or 'process',
                                payload=message,
                                status='rejected',
                                error=f'Invalid API key: {api_key[:8]}...'
                            )
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                            return

                        # Check permissions
                        if not check_permission(key_info, target_worker, action or 'process'):
                            print(f"[REJECT] Permission denied for {target_worker}/{action} request {request_id}", flush=True)
                            log_request(
                                request_id=request_id,
                                api_key_id=key_info.get('id'),
                                source=source,
                                target_worker=target_worker,
                                action=action or 'process',
                                payload=message,
                                status='rejected',
                                error=f'Permission denied for {target_worker}/{action}'
                            )
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                            return

                        # Process the request, preserving the original request_id and correlation_id
                        result = process_request(
                            worker=target_worker,
                            action=action or 'process',
                            payload=message,
                            api_key=api_key,
                            source=source,
                            correlation_id=correlation_id,
                            request_id=request_id,
                            features=msg_features
                        )
                        print(f"[REQUEST] {target_worker}/{action}: {result.get('request_id', 'unknown')}")

                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except MySQLError as e:
                    print(f"[DB ERROR] {e}")
                    nack_with_retry(ch, method.delivery_tag, properties,
                                    log_fn=lambda msg: print(f"[DB ERROR] {msg}"),
                                    queue_name=GATEWAY_REQUEST_QUEUE, body=body)
                except json.JSONDecodeError as e:
                    print(f"[ERROR] Invalid JSON -> DLQ: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                except Exception as e:
                    print(f"[ERROR] Failed to process message -> DLQ: {e}")
                    import traceback
                    traceback.print_exc()
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_consume(queue=GATEWAY_REQUEST_QUEUE, on_message_callback=callback)
            print(f"[OK] Queue consumer ready - consuming from {GATEWAY_REQUEST_QUEUE}")

            # Signal that we're ready
            if ready_event:
                ready_event.set()

            channel.start_consuming()

        except Exception as e:
            print(f"[ERROR] Queue consumer error: {e}")
            time.sleep(5)


# =============================================================================
# Response Queue Consumer (for worker responses and auto-cascade)
# =============================================================================

def run_response_consumer(ready_event: threading.Event = None):
    """
    Run a consumer for all worker response queues.
    When a worker completes, this triggers cascades to downstream workers.
    """
    print(f"[INFO] Starting response queue consumer")

    # Collect all response queues
    response_queues = [config['response_queue'] for config in WORKER_REGISTRY.values()]
    print(f"[INFO] Monitoring response queues: {', '.join(response_queues)}")

    while True:
        try:
            conn = get_rabbitmq_connection()
            channel = conn.channel()
            ensure_queues_exist(channel)
            channel.basic_qos(prefetch_count=10)

            def callback(ch, method, properties, body):
                try:
                    response = json.loads(body.decode('utf-8'))
                    worker = response.get('worker')
                    request_id = response.get('request_id', '')
                    correlation_id = response.get('correlation_id', request_id)
                    status = response.get('status')
                    result = response.get('result', {})

                    # Extract batch info for tracking
                    batch_num = result.get('batch_num', 0)
                    total_batches = result.get('batches', 0)

                    # Update request status in DB
                    if status in ('completed', 'partial'):
                        update_request_status(request_id, 'completed', result=result)
                    elif status == 'failed':
                        update_request_status(request_id, 'failed',
                                            error_message=result.get('error', 'Unknown error'),
                                            result=result)
                        print(f"[FAILED] {worker} request {request_id[:8]} failed: {result.get('error', 'Unknown')}")
                    else:
                        update_request_status(request_id, 'completed', result=result)

                    # Pipeline completion tracking
                    if worker == 'producer' and status in ('completed', 'partial'):
                        # Producer done - mark complete and check if downstream already finished
                        print(f"[PRODUCER] {correlation_id[:8]}: {result.get('processed', 0)} sigs → {total_batches} batches")
                        completion = mark_producer_done(correlation_id, total_batches)
                        if completion:
                            # Downstream workers already finished before producer response arrived!
                            print(f"[PIPELINE COMPLETE] {completion['correlation_id'][:8]}: "
                                  f"{completion['batches']} batches × {len(completion['workers'])} workers "
                                  f"({completion['total_responses']} responses) in {completion['elapsed_seconds']}s")

                    elif worker in ('decoder', 'detailer') and status in ('completed', 'partial'):
                        # Track batch completion
                        completion = record_batch_response(correlation_id, worker, batch_num)
                        if completion:
                            # Pipeline complete!
                            print(f"[PIPELINE COMPLETE] {completion['correlation_id'][:8]}: "
                                  f"{completion['batches']} batches × {len(completion['workers'])} workers "
                                  f"({completion['total_responses']} responses) in {completion['elapsed_seconds']}s")

                    elif worker == 'shredder' and status in ('completed', 'partial'):
                        # Shredder sends ONE response when all staging rows for a correlation are done
                        # Mark all batches as complete for shredder (it processes them as one unit)
                        completion = record_batch_response(correlation_id, worker, 1)  # Always batch 1
                        staging_rows = result.get('staging_rows_processed', 0)
                        print(f"[SHREDDER] {correlation_id[:8]}: processed {staging_rows} staging rows")
                        if completion:
                            # Pipeline complete!
                            print(f"[PIPELINE COMPLETE] {completion['correlation_id'][:8]}: "
                                  f"{completion['batches']} batches × {len(completion['workers'])} workers "
                                  f"({completion['total_responses']} responses) in {completion['elapsed_seconds']}s")

                    # Check for auto-cascade (on success or partial)
                    if status in ('completed', 'partial') and worker in WORKER_REGISTRY:
                        cascade_to = WORKER_REGISTRY[worker].get('cascade_to', [])

                        if cascade_to and result.get('cascade_to'):
                            # Worker specified which targets to cascade to
                            targets = [t for t in result['cascade_to'] if t in cascade_to]
                        elif cascade_to:
                            # Use default cascade targets
                            targets = cascade_to
                        else:
                            targets = []

                        if targets:
                            # Build cascade batch from worker result
                            cascade_batch = {}

                            # Transfer tx_ids if present
                            if result.get('tx_ids'):
                                cascade_batch['tx_ids'] = result['tx_ids']

                            # Transfer address_ids if present
                            if result.get('address_ids'):
                                cascade_batch['address_ids'] = result['address_ids']

                            # Transfer token_ids if present
                            if result.get('token_ids'):
                                cascade_batch['token_ids'] = result['token_ids']

                            # Transfer signatures if present
                            if result.get('signatures'):
                                cascade_batch['signatures'] = result['signatures']

                            # Include processed counts
                            cascade_batch['source_processed'] = result.get('processed', 0)

                            if cascade_batch:
                                cascade_msg = {
                                    'action': 'cascade',
                                    'source_worker': worker,
                                    'source_request_id': request_id,
                                    'correlation_id': correlation_id,
                                    'request_log_id': response.get('request_log_id'),  # For billing linkage
                                    'targets': targets,
                                    'batch': cascade_batch,
                                    'api_key': response.get('api_key') or 'internal_cascade_key',
                                    'priority': response.get('priority', 5)
                                }

                                # Route directly to downstream workers
                                cascade_results = process_cascade(cascade_msg)
                                print(f"[AUTO-CASCADE] {worker} -> {targets}: "
                                      f"{len([r for r in cascade_results if r.get('success')])} queued")

                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except MySQLError as e:
                    print(f"[DB ERROR] {e}")
                    nack_with_retry(ch, method.delivery_tag, properties,
                                    log_fn=lambda msg: print(f"[DB ERROR] {msg}"),
                                    queue_name=method.routing_key, body=body)
                except Exception as e:
                    # Other errors - send to DLQ
                    print(f"[ERROR] Failed to process response: {e}")
                    import traceback
                    traceback.print_exc()
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            # Subscribe to all response queues
            for queue in response_queues:
                channel.basic_consume(queue=queue, on_message_callback=callback)

            print(f"[OK] Response consumer ready - monitoring {len(response_queues)} queues")

            # Signal that we're ready
            if ready_event:
                ready_event.set()

            channel.start_consuming()

        except Exception as e:
            print(f"[ERROR] Response consumer error: {e}")
            time.sleep(5)


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Guide Gateway - Pipeline orchestration service')
    parser.add_argument('--port', type=int, default=GATEWAY_PORT, help='Server port')
    parser.add_argument('--host', default=GATEWAY_HOST, help='Server host')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--with-queue-consumer', action='store_true',
                        help='Also start queue consumer for cascade messages')
    parser.add_argument('--queue-consumer-only', action='store_true',
                        help='Only run queue consumer, no REST API')
    parser.add_argument('--with-response-consumer', action='store_true',
                        help='Also start response consumer for auto-cascade routing')
    parser.add_argument('--response-consumer-only', action='store_true',
                        help='Only run response consumer for auto-cascade')
    parser.add_argument('--init-queues', action='store_true',
                        help='Initialize all queues and exit')

    args = parser.parse_args()

    # Check dependencies
    if not HAS_FLASK and not args.queue_consumer_only:
        print("[ERROR] Flask not installed. Run: pip install flask")
        return 1

    if not HAS_PIKA:
        print("[ERROR] pika not installed. Run: pip install pika")
        return 1

    # Initialize queues
    if args.init_queues:
        print("[INFO] Initializing queues...")
        try:
            conn = get_rabbitmq_connection()
            channel = conn.channel()
            ensure_queues_exist(channel)
            conn.close()
            print("[OK] All queues created successfully")
            return 0
        except Exception as e:
            print(f"[ERROR] Failed to initialize queues: {e}")
            return 1

    # Queue consumer only mode
    if args.queue_consumer_only:
        run_queue_consumer()
        return 0

    # Response consumer only mode
    if args.response_consumer_only:
        run_response_consumer()
        return 0

    # Track consumer ready events
    queue_consumer_ready = threading.Event()
    response_consumer_ready = threading.Event()

    # Start queue consumer in background thread
    if args.with_queue_consumer:
        consumer_thread = threading.Thread(target=run_queue_consumer, args=(queue_consumer_ready,), daemon=True)
        consumer_thread.start()
        print("[INFO] Queue consumer starting...")

    # Start response consumer in background thread
    if args.with_response_consumer:
        response_thread = threading.Thread(target=run_response_consumer, args=(response_consumer_ready,), daemon=True)
        response_thread.start()
        print("[INFO] Response consumer starting...")

    # Wait for consumers to be ready before accepting REST requests
    if args.with_queue_consumer:
        if queue_consumer_ready.wait(timeout=10):
            print("[OK] Queue consumer ready")
        else:
            print("[WARN] Queue consumer startup timeout - continuing anyway")

    if args.with_response_consumer:
        if response_consumer_ready.wait(timeout=10):
            print("[OK] Response consumer ready")
        else:
            print("[WARN] Response consumer startup timeout - continuing anyway")

    # Start Flask server
    print(f"""
+-----------------------------------------------------------+
|               theGuide Gateway                            |
|                                                           |
|  REST API:  http://{args.host}:{args.port}                         |
|  vhost:     {RABBITMQ_CONFIG['vhost']}                                     |
|  Workers:   {len(WORKER_REGISTRY)}                                            |
+-----------------------------------------------------------+
""")

    app = create_app()
    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)


if __name__ == '__main__':
    exit(main() or 0)
