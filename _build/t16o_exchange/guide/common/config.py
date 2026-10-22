"""
Shared configuration for T16O Exchange Guide Services

Loads configuration from guide-config.json and provides service-specific settings.
All workers should import config from here instead of loading guide-config.json directly.
"""

import json
import os
from typing import Dict, Any, Optional

# Default paths
DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
    '_wrk', 'guide-config.json'
)

# Cached config
_config_cache: Optional[Dict[str, Any]] = None


def _require(cfg: Dict[str, Any], key: str) -> Any:
    """Get a required config key, raise if missing."""
    if key not in cfg:
        raise KeyError(f"Missing required config key '{key}' in guide-config.json")
    return cfg[key]


def load_config(config_path: str = None) -> Dict[str, Any]:
    """
    Load configuration from guide-config.json.
    Raises FileNotFoundError if config file is missing.
    Raises json.JSONDecodeError if config file is malformed.
    """
    global _config_cache

    if _config_cache is not None and config_path is None:
        return _config_cache

    path = config_path or DEFAULT_CONFIG_PATH

    with open(path, 'r') as f:
        config = json.load(f)

    if config_path is None:
        _config_cache = config

    return config


def get_db_config(autocommit: bool = True) -> Dict[str, Any]:
    """Get MySQL database configuration."""
    cfg = load_config()
    return {
        'host':               _require(cfg, 'DB_HOST'),
        'port':               _require(cfg, 'DB_PORT'),
        'user':               _require(cfg, 'DB_USER'),
        'password':           _require(cfg, 'DB_PASSWORD'),
        'database':           _require(cfg, 'DB_NAME'),
        'ssl_disabled':       True,
        'use_pure':           True,
        'ssl_verify_cert':    False,
        'ssl_verify_identity': False,
        'autocommit':         autocommit,
    }


def get_rabbitmq_config() -> Dict[str, Any]:
    """Get RabbitMQ configuration including heartbeat/timeout settings."""
    cfg = load_config()
    return {
        'host':             _require(cfg, 'RABBITMQ_HOST'),
        'port':             _require(cfg, 'RABBITMQ_PORT'),
        'user':             _require(cfg, 'RABBITMQ_USER'),
        'password':         _require(cfg, 'RABBITMQ_PASSWORD'),
        'vhost':            _require(cfg, 'RABBITMQ_VHOST'),
        'heartbeat':        cfg.get('RABBITMQ_HEARTBEAT', 600),
        'blocked_timeout':  cfg.get('RABBITMQ_BLOCKED_TIMEOUT', 300),
    }


def get_queue_names(worker: str) -> Dict[str, str]:
    """Get RabbitMQ queue names for a worker (request/response/dlq)."""
    base = f'mq.guide.{worker}'
    return {
        'request':  f'{base}.request',
        'response': f'{base}.response',
        'dlq':      f'{base}.dlq',
    }


def get_rpc_config() -> Dict[str, Any]:
    """Get Solana RPC configuration."""
    cfg = load_config()
    return {
        'url': _require(cfg, 'RPC_URL'),
        'urls': cfg.get('RPC_URLs', [_require(cfg, 'RPC_URL')]),
    }


def get_solscan_config() -> Dict[str, Any]:
    """Get Solscan API configuration."""
    cfg = load_config()
    return {
        'api_base': _require(cfg, 'SOLSCAN_API'),
        'token':    _require(cfg, 'SOLSCAN_TOKEN'),
    }


def get_staging_config() -> Dict[str, str]:
    """Get staging database/table names."""
    return {
        'schema': 't16o_db_staging',
        'table':  'txs',
    }


def get_retry_config() -> Dict[str, Any]:
    """Get retry/fallback configuration."""
    cfg = load_config()
    return {
        'db_fallback_retry_sec': cfg.get('DB_FALLBACK_RETRY_SEC', 5),
        'max_attempts':          cfg.get('RETRY_MAX_ATTEMPTS', 3),
        'base_delay':            cfg.get('RETRY_BASE_DELAY', 1.0),
        'max_delay':             cfg.get('RETRY_MAX_DELAY', 30.0),
    }


# Default max redelivery count before sending to DLQ
DEFAULT_MAX_REDELIVERY = 3


def _get_retry_count(properties) -> int:
    """Get current retry count from custom header."""
    headers = getattr(properties, 'headers', None) or {}
    return headers.get('x-retry-count', 0)


def nack_with_retry(ch, delivery_tag, properties, log_fn=None,
                    max_retries: int = DEFAULT_MAX_REDELIVERY,
                    queue_name: str = None, body: bytes = None):
    """Handle a failed message with retry tracking.

    Classic queues don't support x-delivery-count, so we track retries
    via a custom x-retry-count header. On retry: ack + republish with
    incremented header. On max exceeded: nack with requeue=False (-> DLQ).

    Args:
        ch: pika channel
        delivery_tag: message delivery tag
        properties: pika message properties
        log_fn: optional callable(msg) for logging
        max_retries: max retry attempts before DLQ
        queue_name: queue to republish to (required for retry to work)
        body: original message body (required for retry to work)
    """
    import pika
    retry_count = _get_retry_count(properties)

    if retry_count < max_retries and queue_name and body is not None:
        # Ack original, republish with incremented retry count
        retry_count += 1
        if log_fn:
            log_fn(f"Requeuing (attempt {retry_count}/{max_retries})")

        headers = dict(getattr(properties, 'headers', None) or {})
        headers['x-retry-count'] = retry_count

        ch.basic_ack(delivery_tag=delivery_tag)
        ch.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=properties.delivery_mode,
                content_type=properties.content_type,
                priority=properties.priority,
                headers=headers,
            ))
    else:
        # Max retries exceeded or missing queue/body — send to DLQ
        if log_fn:
            log_fn(f"Max retries ({max_retries}) exceeded -> DLQ")
        ch.basic_nack(delivery_tag=delivery_tag, requeue=False)

    return retry_count < max_retries


def get_service_config(service_name: str) -> Dict[str, Any]:
    """Get service-specific configuration (for Windows service wrappers)."""
    mq = get_rabbitmq_config()
    queues = get_queue_names(service_name)

    return {
        'service_name':  f'T16OExchange.Guide.{service_name.capitalize()}',
        'display_name':  f'T16O Exchange - Guide {service_name.capitalize()}',
        'description':   f'T16O Exchange Guide {service_name.capitalize()} Service',
        'request_queue': queues['request'],
        'response_queue': queues['response'],
        'dlq_queue':     queues['dlq'],
        'rabbitmq':      mq,
        'database':      get_db_config(),
    }


# Service registry - maps service names to worker modules
SERVICE_REGISTRY = {
    'gateway': {
        'worker_module': 'guide-gateway',
        'worker_entry': 'main',
        'has_queue': True,
        'auto_args': False,  # gateway uses its own CLI flags
    },
    'producer': {
        'worker_module': 'guide-producer',
        'worker_entry': 'run_queue_consumer',
        'has_queue': True,
    },
    'decoder': {
        'worker_module': 'guide-decoder',
        'worker_entry': 'run_supervisor',
        'has_queue': True,
        'auto_args': False,  # supervisor pattern — no CLI args needed
    },
    'detailer': {
        'worker_module': 'guide-detailer',
        'worker_entry': 'run_supervisor',
        'has_queue': True,
        'auto_args': False,  # supervisor pattern — no CLI args needed
    },
    'funder': {
        'worker_module': 'guide-funder',
        'worker_entry': 'run_supervisor',
        'has_queue': True,
        'auto_args': False,  # supervisor pattern — no CLI args needed
    },
    'aggregator': {
        'worker_module': 'guide-aggregator',
        'worker_entry': 'run_supervisor',
        'has_queue': True,
        'auto_args': False,  # supervisor pattern — no CLI args needed
    },
    'enricher': {
        'worker_module': 'guide-enricher',
        'worker_entry': 'run_supervisor',
        'has_queue': True,
        'auto_args': False,  # supervisor pattern — no CLI args needed
    },
    'shredder': {
        'worker_module': 'guide-shredder',
        'worker_entry': 'run_supervisor',
        'has_queue': False,  # Polls DB staging table
        'auto_args': False,
    },
    'synchronizer': {
        'worker_module': 'guide-synchronizer',
        'worker_entry': 'run_supervisor',
        'has_queue': True,
        'auto_args': False,
    },
}


# =============================================================================
# Runtime config from DB — single-call replacement for N x sp_config_get()
# =============================================================================

def load_db_config(cursor, config_type: str) -> Dict[str, str]:
    """
    Load all config keys for a type in one DB call via sp_config_get_by_type.
    Returns dict of {key: value} strings. Workers parse with cfg_int/cfg_float.

    Usage:
        cfg = load_db_config(cursor, 'queue')
        threads = cfg_int(cfg, 'decoder_wrk_cnt_threads', 4)
    """
    try:
        # Drain any unread results from prior queries (avoids "Commands out of sync")
        try:
            cursor.fetchall()
        except Exception:
            pass
        try:
            while cursor.nextset():
                cursor.fetchall()
        except Exception:
            pass

        cursor.execute("CALL sp_config_get_by_type(%s)", (config_type,))
        row = cursor.fetchone()
        result = {}
        if row:
            # Handle both tuple cursors (row[0]) and dictionary cursors (row['config_json'])
            val = row[0] if isinstance(row, tuple) else row.get('config_json')
            if val:
                result = json.loads(val) if isinstance(val, str) else val
        # Drain SP result sets
        try:
            while cursor.nextset():
                cursor.fetchall()
        except Exception:
            pass
        return result
    except Exception as e:
        print(f"[CONFIG] Failed to load config type '{config_type}': {e}", flush=True)
    return {}


def cfg_int(cfg: Dict[str, str], key: str, default: int = 0) -> int:
    """Get an int config value from a loaded config dict."""
    val = cfg.get(key)
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def cfg_float(cfg: Dict[str, str], key: str, default: float = 0.0) -> float:
    """Get a float config value from a loaded config dict."""
    val = cfg.get(key)
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def cfg_str(cfg: Dict[str, str], key: str, default: str = '') -> str:
    """Get a string config value from a loaded config dict."""
    return cfg.get(key, default) or default
