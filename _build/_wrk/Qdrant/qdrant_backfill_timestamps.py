"""
qdrant_backfill_timestamps.py
─────────────────────────────────────────────────────────────────────────────
One-time backfill: add created_time/updated_time (unix epoch) to existing
Qdrant points from tx_guide.created_utc/updated_utc in MySQL.

No re-embedding needed — only updates payload fields on existing points.

Usage:
    python3 qdrant_backfill_timestamps.py              # full backfill
    python3 qdrant_backfill_timestamps.py --dry-run    # show counts only

Author : theGuide / T16O
─────────────────────────────────────────────────────────────────────────────
"""

import argparse
import json
import logging
import os
import sys
import time

import pymysql

from qdrant_client_t16o import get_client
from qdrant_client.models import PointStruct

# ─── Logging ─────────────────────────────────────────────────────────────────

log = logging.getLogger("t16o.backfill")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ─── Config ──────────────────────────────────────────────────────────────────

_config_candidates = [
    os.path.join(os.path.dirname(__file__), '..', 'guide-config.json'),
    os.path.join(os.path.dirname(__file__), 'guide-config.json'),
    os.path.join(os.getcwd(), 'guide-config.json'),
]
_cfg = {}
for _cp in _config_candidates:
    if os.path.exists(_cp):
        with open(_cp) as f:
            _cfg = json.load(f)
        break

MYSQL_HOST     = _cfg.get('DB_HOST', '34.196.15.116')
MYSQL_PORT     = _cfg.get('DB_PORT', 3397)
MYSQL_USER     = _cfg.get('DB_USER', 'root')
MYSQL_PASSWORD = _cfg.get('DB_PASSWORD', 'rootpassword')
MYSQL_DB       = _cfg.get('DB_NAME', 't16o_db')

BATCH_SIZE = 500  # points per Qdrant set_payload call


def get_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=30,
        read_timeout=120,
    )


def main():
    parser = argparse.ArgumentParser(description="Backfill created_time/updated_time into Qdrant")
    parser.add_argument("--dry-run", action="store_true", help="Show counts only, don't update")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Points per update batch")
    args = parser.parse_args()

    log.info("Connecting to Qdrant...")
    vc = get_client()
    info = vc.collection_info()
    points_count = info.get("points_count", 0)
    log.info("Collection has %d points", points_count)

    if points_count == 0:
        log.info("Nothing to backfill")
        return

    log.info("Connecting to MySQL %s:%d/%s...", MYSQL_HOST, MYSQL_PORT, MYSQL_DB)
    conn = get_connection()

    # Get all guide_ids and their timestamps from MySQL
    log.info("Fetching timestamps from tx_guide...")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id AS guide_id, "
            "       UNIX_TIMESTAMP(created_utc) AS created_time, "
            "       UNIX_TIMESTAMP(updated_utc) AS updated_time "
            "FROM tx_guide "
            "ORDER BY id"
        )
        rows = cur.fetchall()

    log.info("Found %d tx_guide rows in MySQL", len(rows))

    if args.dry_run:
        log.info("[DRY RUN] Would update up to %d points in Qdrant", min(len(rows), points_count))
        conn.close()
        return

    # Batch update Qdrant points
    updated = 0
    skipped = 0
    t0 = time.time()

    for i in range(0, len(rows), args.batch_size):
        batch = rows[i:i + args.batch_size]

        # Build point ID → payload map
        points_payload = []
        for row in batch:
            gid = int(row["guide_id"])
            payload = {}
            if row["created_time"]:
                payload["created_time"] = int(row["created_time"])
            if row["updated_time"]:
                payload["updated_time"] = int(row["updated_time"])
            if not payload:
                skipped += 1
                continue
            points_payload.append((gid, payload))

        if not points_payload:
            continue

        # Use set_payload with point IDs
        try:
            for gid, payload in points_payload:
                vc.client.set_payload(
                    collection_name="t16o_guide_edges",
                    payload=payload,
                    points=[gid],
                )
            updated += len(points_payload)
        except Exception as e:
            log.error("Batch %d–%d failed: %s", i, i + len(batch), e)
            # Try individual updates for this batch
            for gid, payload in points_payload:
                try:
                    vc.client.set_payload(
                        collection_name="t16o_guide_edges",
                        payload=payload,
                        points=[gid],
                    )
                    updated += 1
                except Exception as e2:
                    skipped += 1
                    log.debug("Point %d skipped: %s", gid, e2)

        if (i + args.batch_size) % 5000 < args.batch_size:
            elapsed = time.time() - t0
            rate = updated / elapsed if elapsed > 0 else 0
            log.info("Progress: %d/%d updated (%.0f/sec)", updated, len(rows), rate)

    elapsed = time.time() - t0
    log.info(
        "Backfill complete: %d updated, %d skipped | %.1f sec (%.0f/sec)",
        updated, skipped, elapsed,
        updated / elapsed if elapsed > 0 else 0,
    )

    conn.close()


if __name__ == "__main__":
    main()
