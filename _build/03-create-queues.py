#!/usr/bin/env python3
"""
theGuide Environment Setup - Step 3: Create RabbitMQ Queues

Creates all message queues required for theGuide pipeline.
Uses pika to declare queues with proper settings.

Usage:
    python 03-create-queues.py                # Create queues
    python 03-create-queues.py --verify       # Verify queues exist
    python 03-create-queues.py --purge        # Purge all messages (caution!)
    python 03-create-queues.py --delete       # Delete queues (caution!)
    python 03-create-queues.py --status       # Show queue status
"""

import argparse
import sys
import json
import os
from typing import Dict, List, Tuple, Optional

try:
    import pika
    HAS_PIKA = True
except ImportError:
    HAS_PIKA = False

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# =============================================================================
# Configuration
# =============================================================================

MQ_CONFIG = {
    "host": "localhost",
    "port": 5692,
    "user": "admin",
    "password": "admin123",
    "vhost": "/",
    "mgmt_port": 15692,
}

# theGuide Queue Definitions
QUEUE_DEFINITIONS = [
    {
        "name": "tx.guide.signatures",
        "durable": True,
        "auto_delete": False,
        "arguments": {"x-max-priority": 10},
        "description": "Transaction signatures to shred",
    },
    {
        "name": "tx.guide.addresses",
        "durable": True,
        "auto_delete": False,
        "arguments": {"x-max-priority": 10},
        "description": "Wallet addresses for funding analysis",
    },
    {
        "name": "tx.funding.addresses",
        "durable": True,
        "auto_delete": False,
        "arguments": {"x-max-priority": 10},
        "description": "Addresses needing funder lookup",
    },
    {
        "name": "tx.enriched",
        "durable": True,
        "auto_delete": False,
        "arguments": {},
        "description": "Enriched transaction output",
    },
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


def get_connection() -> Optional[pika.BlockingConnection]:
    """Create RabbitMQ connection"""
    try:
        credentials = pika.PlainCredentials(MQ_CONFIG["user"], MQ_CONFIG["password"])
        parameters = pika.ConnectionParameters(
            host=MQ_CONFIG["host"],
            port=MQ_CONFIG["port"],
            virtual_host=MQ_CONFIG["vhost"],
            credentials=credentials,
            connection_attempts=3,
            retry_delay=2,
            socket_timeout=10,
        )
        return pika.BlockingConnection(parameters)
    except Exception as e:
        print(f"  [X] Connection failed: {e}")
        return None


def get_queue_info_via_api(queue_name: str) -> Optional[Dict]:
    """Get queue info via management API"""
    if not HAS_REQUESTS:
        return None

    try:
        url = f"http://{MQ_CONFIG['host']}:{MQ_CONFIG['mgmt_port']}/api/queues/%2F/{queue_name}"
        response = requests.get(
            url,
            auth=(MQ_CONFIG["user"], MQ_CONFIG["password"]),
            timeout=10
        )
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None


# =============================================================================
# Queue Operations
# =============================================================================

def create_queues(channel, dry_run: bool = False) -> Tuple[int, int]:
    """Create all queues"""
    print_header("Creating Queues")
    success = 0
    failed = 0

    for queue_def in QUEUE_DEFINITIONS:
        name = queue_def["name"]
        desc = queue_def["description"]

        if dry_run:
            args_str = json.dumps(queue_def["arguments"]) if queue_def["arguments"] else "{}"
            print(f"  [DRY] {name}")
            print(f"        durable={queue_def['durable']}, args={args_str}")
            success += 1
            continue

        try:
            channel.queue_declare(
                queue=name,
                durable=queue_def["durable"],
                auto_delete=queue_def["auto_delete"],
                arguments=queue_def["arguments"] or None,
            )
            print_status(f"{name} - {desc}", True, "created")
            success += 1
        except Exception as e:
            print_status(f"{name} - {desc}", False, str(e)[:40])
            failed += 1

    return success, failed


def verify_queues(channel) -> Tuple[int, int]:
    """Verify all queues exist with correct settings"""
    print_header("Verifying Queues")
    success = 0
    failed = 0

    for queue_def in QUEUE_DEFINITIONS:
        name = queue_def["name"]

        try:
            # Passive declare - fails if queue doesn't exist
            result = channel.queue_declare(queue=name, passive=True)
            msg_count = result.method.message_count
            consumer_count = result.method.consumer_count

            # Get detailed info via API if available
            info = get_queue_info_via_api(name)
            if info:
                durable = info.get("durable", False)
                args = info.get("arguments", {})
                expected_priority = queue_def["arguments"].get("x-max-priority")
                actual_priority = args.get("x-max-priority")

                details = f"msgs:{msg_count}, consumers:{consumer_count}"
                if expected_priority and actual_priority != expected_priority:
                    print_status(name, False, f"priority mismatch: {actual_priority} vs {expected_priority}")
                    failed += 1
                else:
                    print_status(name, True, details)
                    success += 1
            else:
                print_status(name, True, f"msgs:{msg_count}, consumers:{consumer_count}")
                success += 1

        except pika.exceptions.ChannelClosedByBroker:
            print_status(name, False, "queue does not exist")
            failed += 1
            # Reopen channel after closure
            channel = channel.connection.channel()
        except Exception as e:
            print_status(name, False, str(e)[:40])
            failed += 1

    return success, failed


def purge_queues(channel) -> Tuple[int, int]:
    """Purge all messages from queues"""
    print_header("Purging Queues")
    success = 0
    failed = 0

    for queue_def in QUEUE_DEFINITIONS:
        name = queue_def["name"]

        try:
            result = channel.queue_purge(queue=name)
            msg_count = result.method.message_count
            print_status(name, True, f"purged {msg_count} messages")
            success += 1
        except Exception as e:
            print_status(name, False, str(e)[:40])
            failed += 1

    return success, failed


def delete_queues(channel) -> Tuple[int, int]:
    """Delete all queues"""
    print_header("Deleting Queues")
    success = 0
    failed = 0

    for queue_def in QUEUE_DEFINITIONS:
        name = queue_def["name"]

        try:
            channel.queue_delete(queue=name)
            print_status(name, True, "deleted")
            success += 1
        except Exception as e:
            print_status(name, False, str(e)[:40])
            failed += 1

    return success, failed


def show_status() -> int:
    """Show detailed queue status via management API"""
    print_header("Queue Status")

    if not HAS_REQUESTS:
        print("  [X] requests library not installed - cannot query management API")
        return 1

    try:
        url = f"http://{MQ_CONFIG['host']}:{MQ_CONFIG['mgmt_port']}/api/queues"
        response = requests.get(
            url,
            auth=(MQ_CONFIG["user"], MQ_CONFIG["password"]),
            timeout=10
        )
        response.raise_for_status()
        all_queues = response.json()

        # Filter to theGuide queues
        guide_queue_names = {q["name"] for q in QUEUE_DEFINITIONS}
        guide_queues = [q for q in all_queues if q["name"] in guide_queue_names]

        print(f"\n  {'Queue':<30} {'Messages':>10} {'Consumers':>10} {'State':<10}")
        print(f"  {'-'*30} {'-'*10} {'-'*10} {'-'*10}")

        for q in sorted(guide_queues, key=lambda x: x["name"]):
            name = q["name"]
            msgs = q.get("messages", 0)
            consumers = q.get("consumers", 0)
            state = q.get("state", "unknown")
            print(f"  {name:<30} {msgs:>10} {consumers:>10} {state:<10}")

        # Check for missing queues
        found_names = {q["name"] for q in guide_queues}
        missing = guide_queue_names - found_names
        if missing:
            print(f"\n  Missing queues: {', '.join(missing)}")
            return 1

        return 0

    except Exception as e:
        print(f"  [X] Error querying API: {e}")
        return 1


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='theGuide Environment Setup - Create RabbitMQ Queues'
    )
    parser.add_argument('--verify', action='store_true',
                        help='Verify queues exist with correct settings')
    parser.add_argument('--purge', action='store_true',
                        help='Purge all messages from queues (caution!)')
    parser.add_argument('--delete', action='store_true',
                        help='Delete all queues (caution!)')
    parser.add_argument('--status', action='store_true',
                        help='Show queue status via management API')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be created without making changes')
    parser.add_argument('--host', default=MQ_CONFIG['host'],
                        help='RabbitMQ host')
    parser.add_argument('--port', type=int, default=MQ_CONFIG['port'],
                        help='RabbitMQ AMQP port')
    parser.add_argument('--user', default=MQ_CONFIG['user'],
                        help='RabbitMQ user')
    parser.add_argument('--password', default=MQ_CONFIG['password'],
                        help='RabbitMQ password')

    args = parser.parse_args()

    # Update config from args
    MQ_CONFIG["host"] = args.host
    MQ_CONFIG["port"] = args.port
    MQ_CONFIG["user"] = args.user
    MQ_CONFIG["password"] = args.password

    if not HAS_PIKA:
        print("Error: pika library not installed")
        print("Run: pip install pika")
        return 1

    print("\n" + "="*60)
    print("  theGuide RabbitMQ Queue Setup")
    print("="*60)
    print(f"\nServer: {args.host}:{args.port}")
    print(f"Queues: {len(QUEUE_DEFINITIONS)}")

    # Status mode doesn't need AMQP connection
    if args.status:
        return show_status()

    # Connect via AMQP
    print_header("Connecting")
    connection = get_connection()
    if not connection:
        return 1
    print_status("RabbitMQ connection", True)

    channel = connection.channel()

    total_success = 0
    total_failed = 0

    try:
        if args.delete:
            # Confirm deletion
            print("\n  WARNING: This will delete all theGuide queues!")
            confirm = input("  Type 'DELETE' to confirm: ")
            if confirm != "DELETE":
                print("  Aborted.")
                return 1
            s, f = delete_queues(channel)
            total_success += s
            total_failed += f

        elif args.purge:
            # Confirm purge
            print("\n  WARNING: This will purge all messages from theGuide queues!")
            confirm = input("  Type 'PURGE' to confirm: ")
            if confirm != "PURGE":
                print("  Aborted.")
                return 1
            s, f = purge_queues(channel)
            total_success += s
            total_failed += f

        elif args.verify:
            s, f = verify_queues(channel)
            total_success += s
            total_failed += f

        else:
            # Default: create queues
            s, f = create_queues(channel, args.dry_run)
            total_success += s
            total_failed += f

        # Summary
        print_header("Summary")
        print(f"  Success: {total_success}")
        print(f"  Failed:  {total_failed}")

        if total_failed == 0:
            print("\n  [OK] All queue operations completed successfully!")
        else:
            print(f"\n  [X] {total_failed} operations failed")

    finally:
        connection.close()

    print()
    return 0 if total_failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
