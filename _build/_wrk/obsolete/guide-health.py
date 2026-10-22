#!/usr/bin/env python3
"""
theGuide Health Check Script

Verifies all services and components are running correctly.

Usage:
    python guide-health.py              # Full health check
    python guide-health.py --quick      # Quick check (services only)
    python guide-health.py --watch      # Continuous monitoring
    python guide-health.py --json       # Output as JSON
"""

import argparse
import sys
import os
import time
import socket
from datetime import datetime
from typing import Dict, Tuple, Optional

# =============================================================================
# Configuration
# =============================================================================

# Service endpoints
SERVICES = {
    "mysql": {"host": "localhost", "port": 3396, "type": "tcp"},
    "rabbitmq": {"host": "localhost", "port": 5692, "type": "tcp"},
    "rabbitmq_mgmt": {"host": "localhost", "port": 15692, "type": "http"},
}

# Expected database objects
EXPECTED_COUNTS = {
    "tables": 23,
    "functions": 8,
    "procedures": 14,
    "views": 13,
}

# Expected queues
EXPECTED_QUEUES = [
    "tx.guide.signatures",
    "tx.guide.addresses",
    "tx.funding.addresses",
    "tx.enriched",
]

# Worker patterns
WORKER_PATTERNS = [
    "guide-producer.py",
    "guide-shredder.py",
    "guide-funder.py",
    "guide-sync-funding.py",
]


# =============================================================================
# Helper Functions
# =============================================================================

def print_header(text: str):
    """Print a section header"""
    print(f"\n{'='*60}")
    print(f"  {text}")
    print(f"{'='*60}")


def print_status(name: str, status: bool, details: str = ""):
    """Print a status line"""
    icon = "[OK]" if status else "[X]"
    detail_str = f" ({details})" if details else ""
    print(f"  {icon} {name}{detail_str}")


def check_port(host: str, port: int, timeout: float = 2.0) -> bool:
    """Check if a TCP port is open"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except:
        return False


def check_http(host: str, port: int, timeout: float = 5.0) -> bool:
    """Check if HTTP endpoint responds"""
    try:
        import requests
        response = requests.get(f"http://{host}:{port}/", timeout=timeout)
        return response.status_code < 500
    except:
        return False


# =============================================================================
# Check Functions
# =============================================================================

def check_services() -> Tuple[int, int]:
    """Check all service endpoints"""
    print_header("Services")
    ok = 0
    fail = 0

    for name, cfg in SERVICES.items():
        if cfg["type"] == "tcp":
            status = check_port(cfg["host"], cfg["port"])
        else:
            status = check_http(cfg["host"], cfg["port"])

        print_status(f"{name} ({cfg['host']}:{cfg['port']})", status,
                    "listening" if status else "not responding")
        if status:
            ok += 1
        else:
            fail += 1

    return ok, fail


def check_mysql_connection() -> Tuple[bool, str]:
    """Check MySQL database connection and schema"""
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host="localhost",
            port=3396,
            user="root",
            password="rootpassword",
            database="t16o_db",
            connection_timeout=5
        )
        cursor = conn.cursor()

        # Get counts
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 't16o_db' AND table_type = 'BASE TABLE'
        """)
        tables = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.routines
            WHERE routine_schema = 't16o_db' AND routine_type = 'FUNCTION'
        """)
        functions = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.routines
            WHERE routine_schema = 't16o_db' AND routine_type = 'PROCEDURE'
        """)
        procedures = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.views
            WHERE table_schema = 't16o_db'
        """)
        views = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return True, f"T:{tables} F:{functions} P:{procedures} V:{views}"
    except Exception as e:
        return False, str(e)[:40]


def check_rabbitmq_connection() -> Tuple[bool, str]:
    """Check RabbitMQ connection and queues"""
    try:
        import pika
        credentials = pika.PlainCredentials('admin', 'admin123')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='localhost',
                port=5692,
                credentials=credentials,
                connection_attempts=1,
                socket_timeout=5
            )
        )
        channel = connection.channel()

        # Check queues exist
        queues_ok = 0
        for queue in EXPECTED_QUEUES:
            try:
                channel.queue_declare(queue=queue, passive=True)
                queues_ok += 1
            except:
                pass

        connection.close()
        return True, f"{queues_ok}/{len(EXPECTED_QUEUES)} queues"
    except Exception as e:
        return False, str(e)[:40]


def check_database() -> Tuple[int, int]:
    """Check database connectivity and schema"""
    print_header("Database")
    ok = 0
    fail = 0

    # Check connection
    status, detail = check_mysql_connection()
    print_status("MySQL connection", status, detail)
    if status:
        ok += 1
    else:
        fail += 1
        return ok, fail

    # Check seed data
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host="localhost", port=3396, user="root",
            password="rootpassword", database="t16o_db"
        )
        cursor = conn.cursor()

        seed_tables = [
            ("tx_guide_type", 40),
            ("tx_guide_source", 4),
            ("config", 20),
        ]

        for table, min_rows in seed_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            status = count >= min_rows
            print_status(f"Seed data: {table}", status, f"{count} rows")
            if status:
                ok += 1
            else:
                fail += 1

        cursor.close()
        conn.close()
    except Exception as e:
        print_status("Seed data check", False, str(e)[:40])
        fail += 1

    return ok, fail


def check_message_queue() -> Tuple[int, int]:
    """Check RabbitMQ connectivity and queues"""
    print_header("Message Queue")
    ok = 0
    fail = 0

    # Check connection
    status, detail = check_rabbitmq_connection()
    print_status("RabbitMQ connection", status, detail)
    if status:
        ok += 1
    else:
        fail += 1
        return ok, fail

    # Check queue stats via management API
    try:
        import requests
        response = requests.get(
            "http://localhost:15692/api/queues",
            auth=("admin", "admin123"),
            timeout=5
        )
        if response.status_code == 200:
            queues = {q["name"]: q for q in response.json()}

            for queue_name in EXPECTED_QUEUES:
                if queue_name in queues:
                    q = queues[queue_name]
                    msgs = q.get("messages", 0)
                    consumers = q.get("consumers", 0)
                    print_status(queue_name, True, f"msgs:{msgs} consumers:{consumers}")
                    ok += 1
                else:
                    print_status(queue_name, False, "not found")
                    fail += 1
    except Exception as e:
        print_status("Queue stats", False, str(e)[:40])
        fail += 1

    return ok, fail


def check_workers() -> Tuple[int, int]:
    """Check running worker processes"""
    print_header("Workers")
    ok = 0
    fail = 0

    try:
        import subprocess
        result = subprocess.run(
            ["wmic", "process", "where", "name='python.exe'", "get", "commandline"],
            capture_output=True, text=True, timeout=10
        )

        running = set()
        for line in result.stdout.split('\n'):
            for pattern in WORKER_PATTERNS:
                if pattern in line:
                    running.add(pattern)
                    break

        for worker in WORKER_PATTERNS:
            status = worker in running
            print_status(worker, status, "running" if status else "not running")
            if status:
                ok += 1
            else:
                fail += 1

    except Exception as e:
        print_status("Worker check", False, str(e)[:40])
        fail += len(WORKER_PATTERNS)

    return ok, fail


def check_docker() -> Tuple[int, int]:
    """Check Docker containers"""
    print_header("Docker Containers")
    ok = 0
    fail = 0

    try:
        import subprocess
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
            capture_output=True, text=True, timeout=10
        )

        containers = {}
        for line in result.stdout.strip().split('\n'):
            if line and '\t' in line:
                name, status = line.split('\t', 1)
                containers[name] = status

        expected = ["t16o_v1_mysql", "t16o_v1_rabbitmq"]
        for name in expected:
            if name in containers:
                status = containers[name]
                is_up = "Up" in status
                print_status(name, is_up, status[:30])
                if is_up:
                    ok += 1
                else:
                    fail += 1
            else:
                print_status(name, False, "not found")
                fail += 1

    except Exception as e:
        print_status("Docker check", False, str(e)[:40])
        fail += 2

    return ok, fail


# =============================================================================
# Main
# =============================================================================

def run_health_check(quick: bool = False) -> Dict:
    """Run full health check and return results"""
    results = {
        "timestamp": datetime.now().isoformat(),
        "checks": {},
        "total_ok": 0,
        "total_fail": 0,
    }

    # Services
    ok, fail = check_services()
    results["checks"]["services"] = {"ok": ok, "fail": fail}
    results["total_ok"] += ok
    results["total_fail"] += fail

    if quick:
        return results

    # Docker
    ok, fail = check_docker()
    results["checks"]["docker"] = {"ok": ok, "fail": fail}
    results["total_ok"] += ok
    results["total_fail"] += fail

    # Database
    ok, fail = check_database()
    results["checks"]["database"] = {"ok": ok, "fail": fail}
    results["total_ok"] += ok
    results["total_fail"] += fail

    # Message Queue
    ok, fail = check_message_queue()
    results["checks"]["mq"] = {"ok": ok, "fail": fail}
    results["total_ok"] += ok
    results["total_fail"] += fail

    # Workers
    ok, fail = check_workers()
    results["checks"]["workers"] = {"ok": ok, "fail": fail}
    results["total_ok"] += ok
    results["total_fail"] += fail

    return results


def main():
    parser = argparse.ArgumentParser(
        description='theGuide Health Check Script'
    )
    parser.add_argument('--quick', '-q', action='store_true',
                        help='Quick check (services only)')
    parser.add_argument('--watch', '-w', action='store_true',
                        help='Continuous monitoring mode')
    parser.add_argument('--interval', '-i', type=int, default=30,
                        help='Watch interval in seconds (default: 30)')
    parser.add_argument('--json', '-j', action='store_true',
                        help='Output as JSON')

    args = parser.parse_args()

    if args.watch:
        print("\n  theGuide Health Monitor (Ctrl+C to stop)")
        print("  " + "="*50)

        try:
            while True:
                results = run_health_check(args.quick)

                if args.json:
                    import json
                    print(json.dumps(results))
                else:
                    # Summary
                    print_header("Summary")
                    total = results["total_ok"] + results["total_fail"]
                    print(f"  Checks passed: {results['total_ok']}/{total}")
                    if results["total_fail"] == 0:
                        print("\n  [OK] All systems healthy!")
                    else:
                        print(f"\n  [X] {results['total_fail']} check(s) failed")
                    print(f"\n  Next check in {args.interval}s...")

                time.sleep(args.interval)
                if not args.json:
                    os.system('cls' if os.name == 'nt' else 'clear')

        except KeyboardInterrupt:
            print("\n\n  Monitoring stopped.")
            return 0
    else:
        results = run_health_check(args.quick)

        if args.json:
            import json
            print(json.dumps(results, indent=2))
        else:
            # Summary
            print_header("Summary")
            total = results["total_ok"] + results["total_fail"]
            print(f"  Checks passed: {results['total_ok']}/{total}")

            if results["total_fail"] == 0:
                print("\n  [OK] All systems healthy!")
            else:
                print(f"\n  [X] {results['total_fail']} check(s) failed")

        print()
        return 0 if results["total_fail"] == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
