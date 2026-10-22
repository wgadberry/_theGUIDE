#!/usr/bin/env python3
"""
CLI tool to publish messages to theGuide Gateway.

All requests are routed through the gateway for API key validation.

Usage:
  python guide-publish.py --api-key <key> producer --addresses addr1,addr2 --limit 100
  python guide-publish.py --api-key <key> decoder --signatures sig1,sig2,sig3
  python guide-publish.py --api-key <key> detailer --signatures sig1,sig2,sig3
  python guide-publish.py --api-key <key> funder --addresses addr1,addr2,addr3
  python guide-publish.py --api-key <key> aggregator --sync guide,funding,tokens
  python guide-publish.py --api-key <key> enricher --enrich tokens,pools

Note: shredder is not available via queue - it polls the staging table directly.
"""

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone

try:
    import pika
except ImportError:
    print("Error: pika not installed. Run: pip install pika")
    sys.exit(1)

# Load config
CONFIG_PATH = "guide-config.json"
try:
    with open(CONFIG_PATH) as f:
        config = json.load(f)
except FileNotFoundError:
    # Fallback defaults
    config = {
        "RABBITMQ_HOST": "localhost",
        "RABBITMQ_PORT": 5692,
        "RABBITMQ_USER": "admin",
        "RABBITMQ_PASSWORD": "admin123",
        "RABBITMQ_VHOST": "t16o_mq"
    }

# All requests go through gateway
GATEWAY_QUEUE = "mq.guide.gateway.request"

# Valid target workers (shredder excluded - it polls staging table, not queue)
VALID_WORKERS = ["producer", "decoder", "detailer", "funder", "aggregator", "enricher"]


def get_connection():
    credentials = pika.PlainCredentials(
        config["RABBITMQ_USER"],
        config["RABBITMQ_PASSWORD"]
    )
    params = pika.ConnectionParameters(
        host=config["RABBITMQ_HOST"],
        port=config["RABBITMQ_PORT"],
        virtual_host=config["RABBITMQ_VHOST"],
        credentials=credentials
    )
    return pika.BlockingConnection(params)


def publish_to_gateway(api_key: str, target_worker: str, action: str, batch: dict, priority: int = 5):
    """Publish request to gateway queue for validation and routing"""
    request_id = str(uuid.uuid4())

    payload = {
        "request_id": request_id,
        "api_key": api_key,
        "target_worker": target_worker,
        "action": action,
        "priority": priority,
        "batch": batch,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "queue"
    }

    connection = get_connection()
    channel = connection.channel()

    # Ensure gateway queue exists
    channel.queue_declare(queue=GATEWAY_QUEUE, durable=True, arguments={'x-max-priority': 10})

    # Publish to gateway
    channel.basic_publish(
        exchange='',
        routing_key=GATEWAY_QUEUE,
        body=json.dumps(payload),
        properties=pika.BasicProperties(
            delivery_mode=2,  # persistent
            priority=priority,
            content_type="application/json"
        )
    )
    connection.close()

    print(f"Published to gateway -> {target_worker}")
    print(f"Request ID: {request_id}")
    print(f"Payload: {json.dumps(payload, indent=2)}")

    return request_id


def main():
    parser = argparse.ArgumentParser(
        description="Publish messages to theGuide Gateway (API key required)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch 100 signatures for token mints
  python guide-publish.py --api-key KEY producer --addresses EPjF...,So11... --limit 100

  # Decode specific signatures via Solscan
  python guide-publish.py --api-key KEY decoder --signatures 5abc...,6def...

  # Get balance details for signatures
  python guide-publish.py --api-key KEY detailer --signatures 5abc...,6def...

  # Trace funding for addresses
  python guide-publish.py --api-key KEY funder --addresses addr1,addr2

  # Run aggregator sync
  python guide-publish.py --api-key KEY aggregator --sync guide,funding,tokens

  # Enrich tokens and pools
  python guide-publish.py --api-key KEY enricher --enrich tokens,pools
        """
    )

    # Required API key
    parser.add_argument("--api-key", "-k", required=True,
                        help="API key for authentication (required)")

    # Target worker
    parser.add_argument("worker", choices=VALID_WORKERS,
                        help="Target worker")

    # Standardized arguments
    parser.add_argument("--addresses", "-a", help="Comma-separated addresses (mints or wallets)")
    parser.add_argument("--signatures", "-s", help="Comma-separated transaction signatures")

    # Producer-specific
    parser.add_argument("--limit", "-l", type=int, default=100,
                        help="Limit for producer (default: 100)")
    parser.add_argument("--before", help="Fetch signatures before this one")
    parser.add_argument("--until", help="Fetch signatures until this one")

    # Aggregator/Enricher-specific
    parser.add_argument("--sync", help="Aggregator sync targets: guide,funding,tokens,bmap,all")
    parser.add_argument("--enrich", help="Enricher targets: tokens,pools,all")

    # Common options
    parser.add_argument("--priority", "-p", type=int, default=5,
                        help="Message priority 0-10 (default: 5)")
    parser.add_argument("--action", default="process",
                        help="Action to perform (default: process)")

    args = parser.parse_args()

    # Build batch based on worker type
    batch = {}
    action = args.action

    if args.worker == "producer":
        if not args.addresses:
            parser.error("producer requires --addresses")
        addresses = [a.strip() for a in args.addresses.split(",")]
        batch = {
            "filters": {
                "addresses": addresses
            },
            "size": args.limit
        }
        if args.before:
            batch["filters"]["before"] = args.before
        if args.until:
            batch["filters"]["until"] = args.until

    elif args.worker in ("decoder", "detailer"):
        if not args.signatures:
            parser.error(f"{args.worker} requires --signatures")
        batch = {
            "signatures": [s.strip() for s in args.signatures.split(",")]
        }

    elif args.worker == "funder":
        if not args.addresses:
            parser.error("funder requires --addresses")
        batch = {
            "addresses": [a.strip() for a in args.addresses.split(",")]
        }

    elif args.worker == "aggregator":
        sync_targets = args.sync or "all"
        batch = {
            "sync": [t.strip() for t in sync_targets.split(",")]
        }
        action = "sync"

    elif args.worker == "enricher":
        enrich_targets = args.enrich or "all"
        batch = {
            "enrich": [t.strip() for t in enrich_targets.split(",")]
        }
        action = "enrich"

    publish_to_gateway(
        api_key=args.api_key,
        target_worker=args.worker,
        action=action,
        batch=batch,
        priority=args.priority
    )


if __name__ == "__main__":
    main()
