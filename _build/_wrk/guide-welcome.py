#!/usr/bin/env python3
"""
theGuide Welcome Launcher

Interactive launcher that prompts for a mint address and starts all core
pipeline workers in separate PowerShell windows.

Usage:
    python guide-welcome.py                     # Interactive mode (legacy)
    python guide-welcome.py --gateway --launch  # Start workers in gateway mode
    python guide-welcome.py --gateway <mint>    # Submit mint via gateway REST API
    python guide-welcome.py --gateway           # Interactive gateway prompt

Gateway Mode:
    1. First, launch workers: python guide-welcome.py --gateway --launch
    2. Wait for workers to initialize (~5 seconds)
    3. Submit requests: python guide-welcome.py --gateway <mint_address>
    4. Or use interactive: python guide-welcome.py --gateway

Workers (Gateway Mode):
    - guide-gateway.py --with-response-consumer  (REST API + auto-cascade)
    - guide-producer.py --queue-consumer
    - guide-shredder.py --queue-consumer
    - guide-detailer.py --queue-consumer
    - guide-funder.py --queue-consumer
"""

import subprocess
import sys
import os
import time
import argparse
import json
from datetime import datetime

# =============================================================================
# Configuration
# =============================================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# =============================================================================
# Worker Registry - All pipeline workers with metadata
# =============================================================================

WORKER_REGISTRY = {
    # === CORE PIPELINE (required for basic processing) ===
    "producer": {
        "name": "Producer",
        "script": "guide-producer.py",
        "category": "core",
        "needs_mint": True,
        "color": "Green",
        "tx_state_bits": [],
        "tx_state_names": [],
        "queues_out": ["tx.guide.signatures"],
        "queues_in": [],
        "description": "Fetches transaction signatures from RPC",
    },
    "shredder": {
        "name": "Shredder",
        "script": "guide-shredder.py",
        "category": "core",
        "needs_mint": False,
        "color": "Cyan",
        "tx_state_bits": [0, 1, 2, 3, 4, 5],
        "tx_state_names": ["SHREDDED", "DECODED", "GUIDE_EDGES", "ADDRESSES_QUEUED", "SWAPS_PARSED", "TRANSFERS_PARSED"],
        "queues_out": ["tx.guide.addresses", "tx.detail.transactions"],
        "queues_in": ["tx.guide.signatures"],
        "description": "Decodes transactions, creates edges, queues addresses",
    },
    "detailer": {
        "name": "Detailer",
        "script": "guide-detailer.py",
        "category": "core",
        "needs_mint": False,
        "color": "Blue",
        "tx_state_bits": [6],
        "tx_state_names": ["DETAILED"],
        "queues_out": [],
        "queues_in": ["tx.detail.transactions"],
        "description": "Fetches balance change details from Solscan",
    },
    "funder": {
        "name": "Funder",
        "script": "guide-funder.py",
        "category": "core",
        "needs_mint": False,
        "color": "Yellow",
        "tx_state_bits": [9],
        "tx_state_names": ["FUNDING_COMPLETE"],
        "queues_out": [],
        "queues_in": ["tx.guide.addresses"],
        "description": "Discovers wallet funding relationships",
    },
    "sync-funding": {
        "name": "Sync-Funding",
        "script": "guide-sync-funding.py",
        "category": "core",
        "needs_mint": False,
        "args": "--daemon --interval 60",
        "color": "Magenta",
        "tx_state_bits": [],
        "tx_state_names": [],
        "queues_out": [],
        "queues_in": [],
        "description": "Syncs tx_funding_edge and tx_token_participant (use aggregator instead)",
        "deprecated": True,
        "replaced_by": "aggregator",
    },
    "aggregator": {
        "name": "Aggregator",
        "script": "guide-aggregator.py",
        "category": "core",
        "needs_mint": False,
        "args": "--sync guide,funding,tokens --daemon --interval 60",
        "color": "Magenta",
        "tx_state_bits": [],
        "tx_state_names": [],
        "queues_out": [],
        "queues_in": [],
        "description": "Consolidated: guide edges, funding, token participants (bmap on-demand)",
    },

    # === ENRICHMENT (optional, improves data quality) ===
    "loader": {
        "name": "Loader",
        "script": "guide-loader.py",
        "category": "enrichment",
        "needs_mint": False,
        "color": "White",
        "tx_state_bits": [],
        "tx_state_names": [],
        "queues_out": [],
        "queues_in": [],
        "description": "Orchestrates sp_tx_guide_batch (use aggregator instead)",
        "deprecated": True,
        "replaced_by": "aggregator",
    },
    "enricher": {
        "name": "Enricher",
        "script": "guide-enricher.py",
        "category": "enrichment",
        "needs_mint": False,
        "args": "--daemon --interval 60",
        "color": "DarkYellow",
        "tx_state_bits": [7, 8],
        "tx_state_names": ["TOKENS_ENRICHED", "POOLS_ENRICHED"],
        "queues_out": [],
        "queues_in": [],
        "description": "Consolidated: token metadata, pool data from Solscan",
    },
    "backfill-tokens": {
        "name": "Backfill-Tokens",
        "script": "guide-backfill-tokens.py",
        "category": "enrichment",
        "needs_mint": False,
        "args": "--daemon --interval 60",
        "color": "DarkYellow",
        "tx_state_bits": [7],
        "tx_state_names": ["TOKENS_ENRICHED"],
        "queues_out": [],
        "queues_in": [],
        "description": "Fetches token metadata from Solscan (use enricher instead)",
        "deprecated": True,
        "replaced_by": "enricher",
    },
    "pool-enricher": {
        "name": "Pool-Enricher",
        "script": "guide-pool-enricher.py",
        "category": "enrichment",
        "needs_mint": False,
        "color": "DarkCyan",
        "tx_state_bits": [8],
        "tx_state_names": ["POOLS_ENRICHED"],
        "queues_out": [],
        "queues_in": [],
        "description": "Fetches pool/AMM data from Solscan (use enricher instead)",
        "deprecated": True,
        "replaced_by": "enricher",
    },
    "address-classifier": {
        "name": "Address-Classifier",
        "script": "guide-address-classifier.py",
        "category": "enrichment",
        "needs_mint": False,
        "color": "DarkMagenta",
        "tx_state_bits": [10],
        "tx_state_names": ["CLASSIFIED"],
        "queues_out": [],
        "queues_in": [],
        "description": "Classifies address types via RPC/Solscan",
    },
    "price-loader": {
        "name": "Price-Loader",
        "script": "guide-price-loader.py",
        "category": "enrichment",
        "needs_mint": False,
        "color": "Gray",
        "tx_state_bits": [],
        "tx_state_names": [],
        "queues_out": [],
        "queues_in": [],
        "description": "Loads historical token prices",
    },

    # === UTILITIES (run on-demand) ===
    "integrity-check": {
        "name": "Integrity-Check",
        "script": "guide-integrity-check.py",
        "category": "utility",
        "needs_mint": False,
        "color": "Gray",
        "tx_state_bits": [],
        "tx_state_names": [],
        "queues_out": [],
        "queues_in": [],
        "description": "Data quality validation and healing",
    },
}

# tx_state bitmask phase definitions
TX_STATE_PHASES = {
    0: {"name": "SHREDDED", "bit": 1, "worker": "shredder"},
    1: {"name": "DECODED", "bit": 2, "worker": "shredder"},
    2: {"name": "GUIDE_EDGES", "bit": 4, "worker": "shredder"},
    3: {"name": "ADDRESSES_QUEUED", "bit": 8, "worker": "shredder"},
    4: {"name": "SWAPS_PARSED", "bit": 16, "worker": "shredder"},
    5: {"name": "TRANSFERS_PARSED", "bit": 32, "worker": "shredder"},
    6: {"name": "DETAILED", "bit": 64, "worker": "detailer"},
    7: {"name": "TOKENS_ENRICHED", "bit": 128, "worker": "enricher"},
    8: {"name": "POOLS_ENRICHED", "bit": 256, "worker": "enricher"},
    9: {"name": "FUNDING_COMPLETE", "bit": 512, "worker": "funder"},
    10: {"name": "CLASSIFIED", "bit": 1024, "worker": "address-classifier"},
}

# Synthetic addresses that must exist for special edge types
SYNTHETIC_ADDRESSES = {
    "BURN111111111111111111111111111111111111111": {"id": 3, "type": "sink", "purpose": "burn"},
    "MINT111111111111111111111111111111111111111": {"id": 4, "type": "source", "purpose": "mint"},
    "CLOSE11111111111111111111111111111111111111": {"id": 5, "type": "sink", "purpose": "close"},
    "CREATE1111111111111111111111111111111111111": {"id": 6, "type": "source", "purpose": "create"},
}

# Default workers launched by interactive mode
# Uses consolidated workers: aggregator replaces sync-funding + loader
DEFAULT_WORKERS = ["producer", "shredder", "detailer", "funder", "aggregator"]

# Optional enrichment workers (use --workers enrichment or --workers all)
ENRICHMENT_WORKERS = ["enricher"]

# Database connection config
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3396,
    "user": "root",
    "password": "rootpassword",
    "database": "t16o_db",
}

# RabbitMQ connection config
RABBITMQ_CONFIG = {
    "host": "localhost",
    "port": 5692,
    "user": "admin",
    "password": "admin123",
}

# Gateway REST API config
GATEWAY_CONFIG = {
    "host": "localhost",
    "port": 5100,
    "api_key": "admin_master_key",
}

# =============================================================================
# Argument Parser
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='theGuide Pipeline Launcher - Solana Transaction Flow Analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python guide-welcome.py                    # Interactive mode (default)
    python guide-welcome.py --audit            # Full pipeline audit
    python guide-welcome.py --status           # Current DB statistics
    python guide-welcome.py --preflight        # Run pre-flight checks only
    python guide-welcome.py --workers all      # Launch all workers
    python guide-welcome.py <mint_address>     # Launch with mint address (skip prompt)

Gateway Mode (new):
    python guide-welcome.py --gateway          # Check gateway status
    python guide-welcome.py --gateway --launch # Launch workers in gateway mode
    python guide-welcome.py --gateway <mint>   # Submit mint to gateway (workers must be running)
    python guide-welcome.py --gateway <mint> --limit 500  # With custom signature limit
        """
    )
    parser.add_argument('mint', nargs='?', help='Mint address (skip interactive prompt)')
    parser.add_argument('--audit', action='store_true',
                        help='Full pipeline audit: workers, phases, gaps')
    parser.add_argument('--status', action='store_true',
                        help='Show current database statistics')
    parser.add_argument('--preflight', action='store_true',
                        help='Run pre-flight checks without launching')
    parser.add_argument('--workers', choices=['default', 'core', 'all', 'enrichment'],
                        default='default',
                        help='Worker set to launch (default: %(default)s)')
    parser.add_argument('--no-banner', action='store_true',
                        help='Skip ASCII banner')
    parser.add_argument('--json', action='store_true',
                        help='Output in JSON format (for --audit, --status)')
    # Gateway mode options
    parser.add_argument('--gateway', action='store_true',
                        help='Use gateway REST API mode')
    parser.add_argument('--launch', action='store_true',
                        help='Launch workers in gateway mode (--queue-consumer)')
    parser.add_argument('--limit', type=int, default=100,
                        help='Max signatures to fetch (default: 100)')
    parser.add_argument('--check-status', metavar='REQUEST_ID',
                        help='Check status of a gateway request')
    return parser.parse_args()


def get_workers_to_launch(mode: str) -> list:
    """Get list of worker configs based on launch mode.

    Modes:
        default    - Core pipeline workers (consolidated)
        core       - Only core category workers (non-deprecated)
        enrichment - Default + enrichment workers (consolidated)
        all        - All non-utility, non-deprecated workers
    """
    if mode == 'default':
        worker_keys = DEFAULT_WORKERS
    elif mode == 'core':
        # Core workers, excluding deprecated
        worker_keys = [k for k, v in WORKER_REGISTRY.items()
                       if v["category"] == "core" and not v.get("deprecated")]
    elif mode == 'enrichment':
        # Default + enrichment workers (using consolidated workers)
        worker_keys = DEFAULT_WORKERS + ENRICHMENT_WORKERS
    elif mode == 'all':
        # All non-utility, non-deprecated workers
        worker_keys = [k for k, v in WORKER_REGISTRY.items()
                       if v["category"] != "utility" and not v.get("deprecated")]
    else:
        worker_keys = DEFAULT_WORKERS

    # Convert to list of worker configs
    return [WORKER_REGISTRY[k] for k in worker_keys if k in WORKER_REGISTRY]


# =============================================================================
# Display Functions
# =============================================================================

def clear_screen():
    """Clear the console"""
    os.system('cls' if os.name == 'nt' else 'clear')


def print_banner(show_pipeline: bool = False):
    """Print welcome banner with optional pipeline diagram."""
    banner = """
    +===============================================================+
    |                                                               |
    |         _____ _          ____       _     _                   |
    |        |_   _| |__   ___|  _ \\ _   _(_) __| | ___             |
    |          | | | '_ \\ / _ \\ | | | | | | |/ _` |/ _ \\            |
    |          | | | | | |  __/ |_| | |_| | | (_| |  __/            |
    |          |_| |_| |_|\\___|____/ \\__,_|_|\\__,_|\\___|            |
    |                                                               |
    |           Solana Transaction Flow Analysis Pipeline           |
    |                                                               |
    +===============================================================+
    """
    print(banner)

    if show_pipeline:
        pipeline = """
    Pipeline Flow:
    ==============
    [Producer] --(signatures)--> [Shredder] --(addresses)--> [Funder]
                                     |
                                     +---(transactions)--> [Detailer]
                                     |
                                     +---> [Aggregator] --> tx_guide, tx_funding_edge, tx_bmap_state

    tx_state Phases (11 bits):
    ==========================
    Bits 0-5: SHREDDED|DECODED|GUIDE_EDGES|ADDRESSES_QUEUED|SWAPS|TRANSFERS [Shredder]
    Bit 6:    DETAILED [Detailer]
    Bits 7-8: TOKENS_ENRICHED|POOLS_ENRICHED [Enricher] *
    Bit 9:    FUNDING_COMPLETE [Funder]
    Bit 10:   CLASSIFIED [Address-Classifier] *

    * = Optional enrichment workers (use --workers enrichment to include)
    """
        print(pipeline)


def print_status(message: str, status: str = "info"):
    """Print status message with indicator"""
    icons = {
        "info": "[*]",
        "ok": "[+]",
        "error": "[!]",
        "wait": "[~]",
    }
    print(f"    {icons.get(status, '[*]')} {message}")


# =============================================================================
# Validation
# =============================================================================

def validate_mint_address(address: str) -> bool:
    """Basic validation for Solana mint address"""
    if not address:
        return False
    # Solana addresses are base58 encoded, 32-44 characters
    if len(address) < 32 or len(address) > 44:
        return False
    # Check for valid base58 characters
    valid_chars = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    return all(c in valid_chars for c in address)


# =============================================================================
# Pre-flight Checks
# =============================================================================

def run_preflight_checks() -> dict:
    """
    Run pre-flight checks for pipeline readiness.
    Returns dict with check results.
    """
    results = {
        "timestamp": datetime.now().isoformat(),
        "mysql": {"status": "unknown", "detail": ""},
        "rabbitmq": {"status": "unknown", "detail": ""},
        "synthetic_addresses": {"status": "unknown", "detail": "", "missing": []},
        "queues": {"status": "unknown", "detail": "", "missing": []},
    }

    # MySQL check
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            connection_timeout=5
        )
        cursor = conn.cursor(dictionary=True)

        # Check synthetic addresses
        addr_list = list(SYNTHETIC_ADDRESSES.keys())
        placeholders = ", ".join(["%s"] * len(addr_list))
        cursor.execute(f"""
            SELECT address, id FROM tx_address
            WHERE address IN ({placeholders})
        """, tuple(addr_list))
        found = {row['address']: row['id'] for row in cursor.fetchall()}

        missing = []
        for addr, expected in SYNTHETIC_ADDRESSES.items():
            if addr not in found:
                missing.append(f"{expected['purpose'].upper()} ({addr[:8]}...)")
            elif found[addr] != expected["id"]:
                missing.append(f"{expected['purpose'].upper()} (wrong ID: {found[addr]} != {expected['id']})")

        results["synthetic_addresses"]["missing"] = missing
        results["synthetic_addresses"]["status"] = "ok" if not missing else "error"
        results["synthetic_addresses"]["detail"] = f"{4 - len(missing)}/4 present"

        cursor.close()
        conn.close()
        results["mysql"]["status"] = "ok"
        results["mysql"]["detail"] = f"Connected to {DB_CONFIG['database']}"
    except ImportError:
        results["mysql"]["status"] = "skip"
        results["mysql"]["detail"] = "mysql.connector not installed"
    except Exception as e:
        results["mysql"]["status"] = "error"
        results["mysql"]["detail"] = str(e)[:50]

    # RabbitMQ check
    try:
        import pika
        credentials = pika.PlainCredentials(RABBITMQ_CONFIG["user"], RABBITMQ_CONFIG["password"])
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_CONFIG["host"],
                port=RABBITMQ_CONFIG["port"],
                credentials=credentials,
                connection_attempts=1,
                socket_timeout=5
            )
        )
        channel = connection.channel()

        expected_queues = ["tx.guide.signatures", "tx.guide.addresses", "tx.detail.transactions"]
        missing = []
        for queue in expected_queues:
            try:
                channel.queue_declare(queue=queue, passive=True)
            except Exception:
                missing.append(queue)
                # Reconnect after failed passive declare
                try:
                    connection = pika.BlockingConnection(
                        pika.ConnectionParameters(
                            host=RABBITMQ_CONFIG["host"],
                            port=RABBITMQ_CONFIG["port"],
                            credentials=credentials,
                            connection_attempts=1,
                            socket_timeout=5
                        )
                    )
                    channel = connection.channel()
                except Exception:
                    pass

        try:
            connection.close()
        except Exception:
            pass

        results["rabbitmq"]["status"] = "ok"
        results["rabbitmq"]["detail"] = "Connected"
        results["queues"]["missing"] = missing
        results["queues"]["status"] = "ok" if not missing else "warning"
        results["queues"]["detail"] = f"{len(expected_queues) - len(missing)}/{len(expected_queues)} queues"
    except ImportError:
        results["rabbitmq"]["status"] = "skip"
        results["rabbitmq"]["detail"] = "pika not installed"
    except Exception as e:
        results["rabbitmq"]["status"] = "error"
        results["rabbitmq"]["detail"] = str(e)[:50]

    return results


def print_preflight_results(results: dict):
    """Print pre-flight check results in human-readable format."""
    print("\n    PRE-FLIGHT CHECKS")
    print("    " + "=" * 50)

    status_icons = {"ok": "[OK]", "error": "[!!]", "warning": "[!?]", "skip": "[--]", "unknown": "[??]"}

    # MySQL
    mysql = results["mysql"]
    icon = status_icons.get(mysql["status"], "[??]")
    print(f"    {icon} MySQL connection        {mysql['detail']}")

    # Synthetic addresses
    synth = results["synthetic_addresses"]
    icon = status_icons.get(synth["status"], "[??]")
    print(f"    {icon} Synthetic addresses     {synth['detail']}")
    if synth["missing"]:
        for m in synth["missing"]:
            print(f"        - Missing: {m}")

    # RabbitMQ
    rmq = results["rabbitmq"]
    icon = status_icons.get(rmq["status"], "[??]")
    print(f"    {icon} RabbitMQ connection     {rmq['detail']}")

    # Queues
    queues = results["queues"]
    icon = status_icons.get(queues["status"], "[??]")
    print(f"    {icon} Queue availability      {queues['detail']}")
    if queues["missing"]:
        for m in queues["missing"]:
            print(f"        - Missing: {m}")

    print()


# =============================================================================
# Gateway REST API Functions
# =============================================================================

def get_gateway_url(endpoint: str = "") -> str:
    """Build gateway URL"""
    return f"http://{GATEWAY_CONFIG['host']}:{GATEWAY_CONFIG['port']}{endpoint}"


def check_gateway_health() -> dict:
    """Check if gateway is running and healthy"""
    try:
        import requests
        response = requests.get(get_gateway_url("/api/health"), timeout=5)
        return response.json()
    except ImportError:
        return {"error": "requests library not installed"}
    except Exception as e:
        return {"error": str(e)}


def submit_gateway_request(worker: str, payload: dict) -> dict:
    """
    Submit a request to the gateway REST API.

    Args:
        worker: Target worker name (producer, shredder, etc.)
        payload: Request payload with action and batch

    Returns:
        Response dict with success status and request_id
    """
    try:
        import requests
        url = get_gateway_url(f"/api/trigger/{worker}")
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": GATEWAY_CONFIG["api_key"]
        }
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        return response.json()
    except ImportError:
        return {"success": False, "error": "requests library not installed"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_request_status(request_id: str) -> dict:
    """Get status of a gateway request"""
    try:
        import requests
        url = get_gateway_url(f"/api/status/{request_id}")
        response = requests.get(url, timeout=10)
        return response.json()
    except ImportError:
        return {"error": "requests library not installed"}
    except Exception as e:
        return {"error": str(e)}


def list_gateway_workers() -> dict:
    """List available workers from gateway"""
    try:
        import requests
        url = get_gateway_url("/api/workers")
        response = requests.get(url, timeout=10)
        return response.json()
    except ImportError:
        return {"error": "requests library not installed"}
    except Exception as e:
        return {"error": str(e)}


def print_gateway_status():
    """Print gateway health and worker status"""
    print("\n    GATEWAY STATUS")
    print("    " + "=" * 50)

    health = check_gateway_health()
    if "error" in health:
        print(f"    [!!] Gateway: {health['error']}")
        print(f"    [*]  Make sure gateway is running:")
        print(f"         python guide-gateway.py --with-response-consumer")
        return False

    print(f"    [OK] Gateway: {health.get('status', 'unknown')}")
    checks = health.get('checks', {})
    for check, status in checks.items():
        icon = "[OK]" if status else "[!!]"
        print(f"         {icon} {check}")

    # List workers
    workers = list_gateway_workers()
    if "workers" in workers:
        print(f"\n    Available workers: {', '.join(workers['workers'].keys())}")

    print()
    return True


def submit_pipeline_request(mint_address: str, limit: int = 100) -> dict:
    """
    Submit a full pipeline request starting with producer.

    The gateway will handle cascading to downstream workers.
    """
    payload = {
        "action": "process",
        "priority": 5,
        "batch": {
            "filters": {
                "mint_address": mint_address
            },
            "limit": limit
        }
    }

    print(f"\n    Submitting request to gateway...")
    print(f"    Target: producer")
    print(f"    Mint: {mint_address}")
    print(f"    Limit: {limit} signatures")
    print()

    result = submit_gateway_request("producer", payload)

    if result.get("success"):
        print(f"    [OK] Request queued successfully!")
        print(f"         Request ID: {result.get('request_id')}")
        print(f"         Worker: {result.get('worker')}")
        print(f"         Queued at: {result.get('queued_at')}")
        print()
        print(f"    [*]  Check status with:")
        print(f"         curl http://localhost:5100/api/status/{result.get('request_id')}")
    else:
        print(f"    [!!] Request failed: {result.get('error')}")

    return result


def launch_gateway_workers():
    """Launch workers in queue-consumer mode for gateway integration"""
    print("\n    Launching workers in gateway mode (--queue-consumer)...\n")

    # Workers to launch with --queue-consumer flag
    gateway_workers = [
        {"name": "Gateway", "script": "guide-gateway.py", "args": "--with-response-consumer", "color": "White"},
        {"name": "Producer", "script": "guide-producer.py", "args": "--queue-consumer", "color": "Green"},
        {"name": "Shredder", "script": "guide-shredder.py", "args": "--queue-consumer", "color": "Cyan"},
        {"name": "Detailer", "script": "guide-detailer.py", "args": "--queue-consumer", "color": "Blue"},
        {"name": "Funder", "script": "guide-funder.py", "args": "--queue-consumer", "color": "Yellow"},
    ]

    launched = 0
    for worker in gateway_workers:
        script_path = os.path.join(SCRIPT_DIR, worker["script"])
        if not os.path.exists(script_path):
            print_status(f"{worker['name']}: Script not found", "error")
            continue

        args = worker.get("args", "")
        py_cmd = f"python '{script_path}' {args}".strip()
        title = f"theGuide - {worker['name']} (Gateway)"
        cmd_str = f'start "{title}" powershell -NoExit -Command "{py_cmd}"'

        try:
            subprocess.Popen(cmd_str, shell=True)
            print_status(f"{worker['name']}: Launched ({args})", "ok")
            launched += 1
            time.sleep(0.5)
        except Exception as e:
            print_status(f"{worker['name']}: Failed - {e}", "error")

    print(f"\n    Launched {launched}/{len(gateway_workers)} workers in gateway mode")
    return launched == len(gateway_workers)


# =============================================================================
# Database Status
# =============================================================================

def get_db_status() -> dict:
    """Get current database statistics."""
    status = {
        "timestamp": datetime.now().isoformat(),
        "tx_count": 0,
        "tx_by_state": {},
        "address_count": 0,
        "guide_edge_count": 0,
        "funding_edge_count": 0,
        "token_count": 0,
        "error": None,
    }

    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
        )
        cursor = conn.cursor(dictionary=True)

        # Transaction count
        cursor.execute("SELECT COUNT(*) as cnt FROM tx")
        status["tx_count"] = cursor.fetchone()["cnt"]

        # Count by tx_state completeness
        cursor.execute("""
            SELECT
                CASE
                    WHEN tx_state IS NULL OR tx_state = 0 THEN 'unprocessed'
                    WHEN (tx_state & 63) = 63 AND (tx_state & 64) = 0 THEN 'shredded'
                    WHEN (tx_state & 127) = 127 THEN 'detailed'
                    WHEN tx_state = 2047 THEN 'fully_processed'
                    ELSE 'partial'
                END as state_group,
                COUNT(*) as cnt
            FROM tx
            GROUP BY state_group
        """)
        status["tx_by_state"] = {row["state_group"]: row["cnt"] for row in cursor.fetchall()}

        cursor.execute("SELECT COUNT(*) as cnt FROM tx_address")
        status["address_count"] = cursor.fetchone()["cnt"]

        cursor.execute("SELECT COUNT(*) as cnt FROM tx_guide")
        status["guide_edge_count"] = cursor.fetchone()["cnt"]

        cursor.execute("SELECT COUNT(*) as cnt FROM tx_funding_edge")
        status["funding_edge_count"] = cursor.fetchone()["cnt"]

        cursor.execute("SELECT COUNT(*) as cnt FROM tx_token")
        status["token_count"] = cursor.fetchone()["cnt"]

        cursor.close()
        conn.close()
    except ImportError:
        status["error"] = "mysql.connector not installed"
    except Exception as e:
        status["error"] = str(e)

    return status


def print_db_status(status: dict):
    """Print database status in human-readable format."""
    print("\n    DATABASE STATUS")
    print("    " + "=" * 50)

    if status.get("error"):
        print(f"    [!!] Error: {status['error']}")
        return

    print(f"\n    TRANSACTION STATISTICS")
    print(f"    -----------------------")
    print(f"    Total transactions:      {status['tx_count']:,}")

    if status["tx_by_state"]:
        print(f"\n    By state:")
        total = status["tx_count"] or 1
        for state, cnt in sorted(status["tx_by_state"].items()):
            pct = (cnt / total) * 100
            print(f"      {state:20s} {cnt:>10,}  ({pct:5.1f}%)")

    print(f"\n    OTHER STATISTICS")
    print(f"    -----------------------")
    print(f"    Addresses:               {status['address_count']:,}")
    print(f"    Tokens:                  {status['token_count']:,}")
    print(f"    Guide edges:             {status['guide_edge_count']:,}")
    print(f"    Funding edges:           {status['funding_edge_count']:,}")
    print()


# =============================================================================
# Pipeline Audit
# =============================================================================

def run_audit(workers_mode: str = "default") -> dict:
    """
    Full pipeline audit:
    1. Worker availability (scripts exist?)
    2. tx_state phase coverage
    3. Pre-flight checks
    4. Gaps analysis
    """
    audit = {
        "timestamp": datetime.now().isoformat(),
        "workers": {},
        "phase_coverage": {},
        "preflight": {},
        "gaps": [],
        "recommendations": [],
    }

    configured_workers = [k for k in DEFAULT_WORKERS]

    # 1. Worker availability
    for key, worker in WORKER_REGISTRY.items():
        script_path = os.path.join(SCRIPT_DIR, worker["script"])
        exists = os.path.exists(script_path)
        is_configured = key in configured_workers
        audit["workers"][key] = {
            "name": worker["name"],
            "exists": exists,
            "configured": is_configured,
            "category": worker["category"],
            "tx_state_bits": worker["tx_state_bits"],
            "description": worker["description"],
        }
        if not exists:
            audit["gaps"].append(f"Worker script missing: {worker['script']}")

    # 2. Phase coverage by configured workers
    covered_bits = set()
    for key in configured_workers:
        if key in WORKER_REGISTRY:
            covered_bits.update(WORKER_REGISTRY[key]["tx_state_bits"])

    for bit, phase in TX_STATE_PHASES.items():
        worker_key = phase["worker"]
        audit["phase_coverage"][phase["name"]] = {
            "bit": bit,
            "value": phase["bit"],
            "covered": bit in covered_bits,
            "worker": worker_key,
            "worker_configured": worker_key in configured_workers,
        }
        if bit not in covered_bits:
            audit["gaps"].append(f"Phase {phase['name']} (bit {bit}) not covered by configured workers")
            audit["recommendations"].append(f"Add '{worker_key}' to launch with --workers all")

    # 3. Pre-flight checks
    audit["preflight"] = run_preflight_checks()

    return audit


def print_audit(audit: dict):
    """Print audit results in human-readable format."""
    print("\n" + "=" * 70)
    print("                        theGuide Pipeline Audit")
    print("=" * 70)

    # Worker availability
    print("\n    WORKER AVAILABILITY")
    print("    " + "-" * 50)

    for key, info in audit["workers"].items():
        exists_icon = "[OK]" if info["exists"] else "[!!]"
        config_str = "(configured)" if info["configured"] else "(available)"
        print(f"    {exists_icon} {info['name']:20s} {info['category']:12s} {config_str}")

    # Phase coverage
    print("\n    TX_STATE PHASE COVERAGE")
    print("    " + "-" * 50)

    for name, info in audit["phase_coverage"].items():
        icon = "[OK]" if info["covered"] else "[!!]"
        status = "(configured)" if info["worker_configured"] else "(NOT configured)"
        print(f"    {icon} Bit {info['bit']:2d}  {name:20s} -> {info['worker']} {status}")

    # Pre-flight summary
    print("\n    PRE-FLIGHT CHECKS")
    print("    " + "-" * 50)
    pf = audit["preflight"]
    status_icons = {"ok": "[OK]", "error": "[!!]", "warning": "[!?]", "skip": "[--]"}
    print(f"    {status_icons.get(pf['mysql']['status'], '[??]')} MySQL: {pf['mysql']['detail']}")
    print(f"    {status_icons.get(pf['rabbitmq']['status'], '[??]')} RabbitMQ: {pf['rabbitmq']['detail']}")
    print(f"    {status_icons.get(pf['synthetic_addresses']['status'], '[??]')} Synthetic addresses: {pf['synthetic_addresses']['detail']}")
    print(f"    {status_icons.get(pf['queues']['status'], '[??]')} Queues: {pf['queues']['detail']}")

    # Gaps & Recommendations
    if audit["gaps"]:
        print("\n    GAPS & RECOMMENDATIONS")
        print("    " + "-" * 50)
        for gap in audit["gaps"]:
            print(f"    [!] {gap}")
        for rec in audit["recommendations"]:
            print(f"    [>] {rec}")

    print("\n" + "=" * 70 + "\n")


# =============================================================================
# Worker Launch
# =============================================================================

def launch_worker(worker: dict, mint_address: str = None) -> bool:
    """Launch a worker in a new PowerShell window"""
    script_path = os.path.join(SCRIPT_DIR, worker["script"])

    if not os.path.exists(script_path):
        print_status(f"{worker['name']}: Script not found - {worker['script']}", "error")
        return False

    # Build the command - use single quotes for paths to avoid escaping issues
    extra_args = worker.get("args", "")
    if worker["needs_mint"] and mint_address:
        # Limit signature fetch to 100 for requested mint address
        py_cmd = f"python '{script_path}' {mint_address} --max-signatures 100 {extra_args}".strip()
    else:
        py_cmd = f"python '{script_path}' {extra_args}".strip()

    # Use cmd /c start to open new window with title
    title = f"theGuide - {worker['name']}"

    # Build command string for shell execution
    # PowerShell -Command uses the py_cmd with single-quoted path
    cmd_str = f'start "{title}" powershell -NoExit -Command "{py_cmd}"'

    try:
        subprocess.Popen(cmd_str, shell=True)
        print_status(f"{worker['name']}: Launched", "ok")
        return True
    except Exception as e:
        print_status(f"{worker['name']}: Failed - {e}", "error")
        return False


def launch_all_workers(mint_address: str, workers: list = None):
    """Launch pipeline workers from provided list."""
    if workers is None:
        workers = get_workers_to_launch("default")

    print("\n    Launching pipeline workers...\n")

    launched = 0
    for worker in workers:
        if launch_worker(worker, mint_address):
            launched += 1
        time.sleep(0.5)  # Small delay between launches

    print(f"\n    Launched {launched}/{len(workers)} workers")
    return launched == len(workers)


# =============================================================================
# Main
# =============================================================================

def main():
    args = parse_args()

    # Handle non-interactive modes first
    if args.audit:
        if not args.no_banner:
            print_banner(show_pipeline=True)
        audit = run_audit(args.workers)
        if args.json:
            print(json.dumps(audit, indent=2))
        else:
            print_audit(audit)
        return 0

    if args.status:
        if not args.no_banner:
            print_banner()
        status = get_db_status()
        if args.json:
            print(json.dumps(status, indent=2))
        else:
            print_db_status(status)
        return 0

    if args.preflight:
        if not args.no_banner:
            print_banner()
        results = run_preflight_checks()
        if args.json:
            print(json.dumps(results, indent=2))
        else:
            print_preflight_results(results)
        return 0

    # Check request status
    if args.check_status:
        status = get_request_status(args.check_status)
        if args.json:
            print(json.dumps(status, indent=2))
        else:
            print(f"\n    Request Status: {args.check_status}")
            print("    " + "=" * 50)
            if "error" in status:
                print(f"    [!!] {status['error']}")
            else:
                print(f"    Status: {status.get('status', 'unknown')}")
                print(f"    Worker: {status.get('target_worker', 'unknown')}")
                print(f"    Action: {status.get('action', 'unknown')}")
                if status.get('created_utc'):
                    print(f"    Created: {status['created_utc']}")
                if status.get('completed_at'):
                    print(f"    Completed: {status['completed_at']}")
                if status.get('duration_ms'):
                    print(f"    Duration: {status['duration_ms']}ms")
                if status.get('result'):
                    print(f"    Result: {json.dumps(status['result'], indent=2)}")
            print()
        return 0

    # Gateway mode
    if args.gateway:
        if not args.no_banner:
            print_banner()

        # Launch workers in gateway mode
        if args.launch:
            print("\n    GATEWAY MODE - Launch Workers")
            print("    " + "=" * 50)
            success = launch_gateway_workers()
            print()
            if success:
                print("    [*]  Workers launched in gateway mode.")
                print("    [*]  Wait a few seconds for workers to initialize.")
                print("    [*]  Then submit requests with:")
                print("         python guide-welcome.py --gateway <mint_address>")
            return 0 if success else 1

        # Check gateway status first
        gateway_ok = print_gateway_status()

        # If mint provided, submit request
        if args.mint:
            if not gateway_ok:
                print("    [!!] Gateway not available. Start it with:")
                print("         python guide-welcome.py --gateway --launch")
                return 1

            if not validate_mint_address(args.mint):
                print_status(f"Invalid mint address: {args.mint}", "error")
                return 1

            result = submit_pipeline_request(args.mint, args.limit)
            return 0 if result.get("success") else 1

        # Interactive gateway mode
        if gateway_ok:
            print("    Enter a mint address to submit, or:")
            print("    - 'workers' to list available workers")
            print("    - 'status <request_id>' to check status")
            print("    - 'q' to quit")
            print()

            while True:
                try:
                    user_input = input("    gateway> ").strip()
                except (KeyboardInterrupt, EOFError):
                    print("\n\n    Goodbye!")
                    return 0

                if not user_input:
                    continue

                if user_input.lower() in ('q', 'quit', 'exit'):
                    print("\n    Goodbye!")
                    return 0

                if user_input.lower() == 'workers':
                    workers = list_gateway_workers()
                    if "workers" in workers:
                        for name, info in workers["workers"].items():
                            print(f"      {name}: {info.get('description', '')}")
                    continue

                if user_input.lower().startswith('status '):
                    req_id = user_input[7:].strip()
                    status = get_request_status(req_id)
                    print(f"      Status: {status.get('status', status.get('error', 'unknown'))}")
                    if status.get('result'):
                        print(f"      Result: {status['result']}")
                    continue

                # Assume it's a mint address
                if validate_mint_address(user_input):
                    submit_pipeline_request(user_input, args.limit)
                else:
                    print("      Invalid mint address or command")

        return 0

    # Interactive mode
    clear_screen()
    print_banner()

    # Get workers to launch based on --workers flag
    workers = get_workers_to_launch(args.workers)

    print("\n    Welcome to theGuide Pipeline Launcher")
    print("    " + "=" * 45)
    print()
    print_status(f"Worker set: {args.workers} ({len(workers)} workers)")
    print()
    for worker in workers:
        suffixes = []
        if worker["needs_mint"]:
            suffixes.append("receives mint")
        if worker.get("args"):
            suffixes.append(worker["args"])
        suffix = f" ({', '.join(suffixes)})" if suffixes else ""
        print(f"        - {worker['name']}: {worker['script']}{suffix}")
    print()
    print("    " + "-" * 45)
    print()

    # Get mint address (from args or prompt)
    mint_input = args.mint
    if not mint_input:
        while True:
            try:
                mint_input = input("    Enter mint address (or 'q' to quit): ").strip()
            except (KeyboardInterrupt, EOFError):
                print("\n\n    Cancelled.")
                return 0

            if mint_input.lower() in ('q', 'quit', 'exit'):
                print("\n    Goodbye!")
                return 0

            if not mint_input:
                print_status("Please enter a mint address", "error")
                continue

            if not validate_mint_address(mint_input):
                print_status("Invalid mint address format (expected 32-44 base58 chars)", "error")
                continue

            break
    else:
        # Validate mint from command line
        if not validate_mint_address(mint_input):
            print_status(f"Invalid mint address: {mint_input}", "error")
            return 1

    print()
    print_status(f"Mint: {mint_input}", "info")
    print()

    # Confirm launch (skip if mint was provided on command line)
    if not args.mint:
        try:
            confirm = input("    Press Enter to launch pipeline (or 'q' to quit): ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n\n    Cancelled.")
            return 0

        if confirm.lower() in ('q', 'quit', 'exit'):
            print("\n    Goodbye!")
            return 0

    # Launch workers
    success = launch_all_workers(mint_input, workers)

    print()
    if success:
        print_status("Pipeline started successfully!", "ok")
        print()
        print("    Workers are running in separate windows.")
        print("    Close this window or press Ctrl+C to exit launcher.")
    else:
        print_status("Some workers failed to start", "error")

    print()

    # Keep window open
    try:
        input("    Press Enter to exit launcher...")
    except (KeyboardInterrupt, EOFError):
        pass

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
