#!/usr/bin/env python3
"""
Guide Loader - Populates tx_guide, tx_funding_edge, tx_token_participant

Processes tx_activity records that haven't been loaded into the guide tables yet.
Derives edges from tx_swap and tx_transfer, aggregates funding relationships,
and tracks token participation metrics.

Pipeline position:
    guide-shredder.py → tx_activity, tx_swap, tx_transfer
                             ↓
                    guide-loader.py (this script)
                             ↓
              sp_tx_guide_batch → tx_guide, tx_funding_edge, tx_token_participant

Usage:
    python guide-loader.py                      # Process all pending (single run)
    python guide-loader.py --daemon             # Continuous processing mode
    python guide-loader.py --batch-size 500     # Custom batch size
    python guide-loader.py --sync-funding       # Re-sync funding edges only
    python guide-loader.py --max-batches 10     # Limit number of batches
"""

import argparse
import random
import sys
import time
from typing import Tuple, Optional

try:
    import mysql.connector
    from mysql.connector import errorcode
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

# Deadlock retry config
MAX_RETRIES = 5
BASE_BACKOFF = 0.5  # seconds
MAX_BACKOFF = 10.0  # seconds


# =============================================================================
# Guide Loader
# =============================================================================

class GuideLoader:
    """Processes tx_activity into tx_guide, tx_funding_edge, tx_token_participant"""

    def __init__(self, db_conn, batch_size: int = 1000):
        self.db_conn = db_conn
        self.cursor = db_conn.cursor()
        self.batch_size = batch_size

        # Stats
        self.total_batches = 0
        self.total_guide = 0
        self.total_funding = 0
        self.total_participants = 0
        self.deadlock_count = 0

    def get_pending_count(self) -> int:
        """Get count of unprocessed activities"""
        self.cursor.execute("SELECT COUNT(*) FROM tx_activity WHERE guide_loaded = 0")
        return self.cursor.fetchone()[0]

    def run_batch(self) -> Tuple[int, int, int]:
        """
        Execute single batch of guide loading with deadlock retry.
        Returns (guide_count, funding_count, participant_count)
        """
        for attempt in range(MAX_RETRIES):
            try:
                self.cursor.execute("SET @g = 0, @f = 0, @p = 0")
                self.cursor.execute("CALL sp_tx_guide_batch(%s, @g, @f, @p)", (self.batch_size,))
                self.cursor.execute("SELECT @g, @f, @p")
                result = self.cursor.fetchone()
                self.db_conn.commit()

                guide_count = result[0] or 0
                funding_count = result[1] or 0
                participant_count = result[2] or 0

                self.total_batches += 1
                self.total_guide += guide_count
                self.total_funding += funding_count
                self.total_participants += participant_count

                return guide_count, funding_count, participant_count

            except mysql.connector.Error as err:
                if err.errno == 1213:  # ER_LOCK_DEADLOCK
                    self.deadlock_count += 1
                    # Exponential backoff with jitter
                    backoff = min(BASE_BACKOFF * (2 ** attempt) + random.uniform(0, 0.5), MAX_BACKOFF)
                    print(f"    [!] Deadlock detected (attempt {attempt + 1}/{MAX_RETRIES}), retrying in {backoff:.1f}s...")
                    self.db_conn.rollback()
                    time.sleep(backoff)
                else:
                    # Non-deadlock error, re-raise
                    raise

        # Exhausted retries
        raise Exception(f"Failed after {MAX_RETRIES} deadlock retries")

    def run_all(self, max_batches: int = 0) -> Tuple[int, int, int]:
        """
        Process all pending activities.
        max_batches=0 means unlimited.
        Returns totals (guide, funding, participants)
        """
        batch_num = 0
        while True:
            guide, funding, participants = self.run_batch()

            if guide == 0 and funding == 0 and participants == 0:
                # No more to process
                break

            batch_num += 1
            print(f"  Batch {batch_num}: guide={guide}, funding={funding}, participants={participants}")

            if max_batches > 0 and batch_num >= max_batches:
                print(f"  Reached max batches ({max_batches})")
                break

        return self.total_guide, self.total_funding, self.total_participants

    def reconnect(self):
        """Reconnect to database (connections go stale during sleep)"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.db_conn:
                self.db_conn.close()
        except:
            pass

        # Get connection params from existing connection
        config = {
            'host': self.db_conn.server_host if self.db_conn else 'localhost',
            'port': self.db_conn.server_port if self.db_conn else 3396,
            'user': self.db_conn.user if self.db_conn else 'root',
            'database': self.db_conn.database if self.db_conn else 't16o_db',
        }
        # Note: password not accessible, will use from args in main()
        return config

    def set_connection(self, db_conn):
        """Set a new database connection"""
        self.db_conn = db_conn
        self.cursor = db_conn.cursor()

    def run_daemon(self, interval: int = 5, max_batches: int = 0, db_args=None):
        """
        Continuous processing mode.
        Sleeps when no work available.
        db_args: argparse args with db connection params for reconnection
        """
        batch_num = 0
        idle_count = 0

        print(f"Running in daemon mode (interval={interval}s)...")
        print("Press Ctrl+C to stop\n")

        try:
            while True:
                guide, funding, participants = self.run_batch()

                if guide == 0 and funding == 0 and participants == 0:
                    idle_count += 1
                    if idle_count == 1:
                        print(f"  [idle] No pending activities, sleeping {interval}s...")

                    # Close connection before sleeping, reconnect after
                    if db_args:
                        try:
                            self.cursor.close()
                            self.db_conn.close()
                        except:
                            pass

                    time.sleep(interval)

                    # Reconnect after sleep
                    if db_args:
                        self.db_conn = mysql.connector.connect(
                            host=db_args.db_host,
                            port=db_args.db_port,
                            user=db_args.db_user,
                            password=db_args.db_pass,
                            database=db_args.db_name
                        )
                        self.cursor = self.db_conn.cursor()

                    continue

                idle_count = 0
                batch_num += 1
                print(f"  Batch {batch_num}: guide={guide}, funding={funding}, participants={participants}")

                if max_batches > 0 and batch_num >= max_batches:
                    print(f"  Reached max batches ({max_batches})")
                    break

        except KeyboardInterrupt:
            print("\n\nStopping daemon...")

    def sync_funding_edges(self) -> int:
        """
        Sync funding edges for addresses that have funded_by_address_id
        but no corresponding tx_funding_edge entry.
        Returns count of new funding edges created.
        """
        for attempt in range(MAX_RETRIES):
            try:
                # Find addresses with funder but no funding edge
                # Set transfer_count=1 (we know at least 1 transfer happened)
                self.cursor.execute("""
                    INSERT INTO tx_funding_edge (from_address_id, to_address_id,
                                                  total_sol, transfer_count,
                                                  first_transfer_time, last_transfer_time)
                    SELECT a.funded_by_address_id, a.id, 0, 1, NULL, NULL
                    FROM tx_address a
                    WHERE a.funded_by_address_id IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM tx_funding_edge fe
                          WHERE fe.from_address_id = a.funded_by_address_id
                            AND fe.to_address_id = a.id
                      )
                    ON DUPLICATE KEY UPDATE from_address_id = from_address_id
                """)
                count = self.cursor.rowcount
                self.db_conn.commit()
                return count

            except mysql.connector.Error as err:
                if err.errno == 1213:  # ER_LOCK_DEADLOCK
                    self.deadlock_count += 1
                    backoff = min(BASE_BACKOFF * (2 ** attempt) + random.uniform(0, 0.5), MAX_BACKOFF)
                    print(f"    [!] Deadlock detected (attempt {attempt + 1}/{MAX_RETRIES}), retrying in {backoff:.1f}s...")
                    self.db_conn.rollback()
                    time.sleep(backoff)
                else:
                    raise

        raise Exception(f"sync_funding_edges failed after {MAX_RETRIES} deadlock retries")

    def close(self):
        """Close database cursor"""
        if self.cursor:
            self.cursor.close()


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Guide Loader - Populate tx_guide, tx_funding_edge, tx_token_participant'
    )
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of activities to process per batch (default: 1000)')
    parser.add_argument('--max-batches', type=int, default=0,
                        help='Maximum batches to process (0 = unlimited)')
    parser.add_argument('--daemon', action='store_true',
                        help='Run in continuous daemon mode')
    parser.add_argument('--interval', type=int, default=5,
                        help='Sleep interval in daemon mode when idle (seconds)')
    parser.add_argument('--sync-funding', action='store_true',
                        help='Only sync missing funding edges, then exit')
    parser.add_argument('--db-host', default='localhost', help='MySQL host')
    parser.add_argument('--db-port', type=int, default=3396, help='MySQL port')
    parser.add_argument('--db-user', default='root', help='MySQL user')
    parser.add_argument('--db-pass', default='rootpassword', help='MySQL password')
    parser.add_argument('--db-name', default='t16o_db', help='MySQL database')

    args = parser.parse_args()

    if not HAS_MYSQL:
        print("Error: mysql-connector-python not installed")
        print("Install with: pip install mysql-connector-python")
        return 1

    print(f"Guide Loader")
    print(f"{'='*60}")
    print(f"Database: {args.db_host}:{args.db_port}/{args.db_name}")
    print(f"Batch size: {args.batch_size}")
    if args.daemon:
        print(f"Mode: DAEMON (interval={args.interval}s)")
    elif args.sync_funding:
        print(f"Mode: SYNC FUNDING EDGES")
    else:
        print(f"Mode: SINGLE RUN")
    print()

    # Connect to MySQL
    print(f"Connecting to MySQL...")
    db_conn = mysql.connector.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_pass,
        database=args.db_name
    )

    loader = GuideLoader(db_conn, args.batch_size)

    try:
        if args.sync_funding:
            # Just sync funding edges
            print("Syncing missing funding edges...")
            count = loader.sync_funding_edges()
            print(f"  Created {count} new funding edges")

        elif args.daemon:
            # Check pending first
            pending = loader.get_pending_count()
            print(f"Pending activities: {pending:,}")
            print()

            loader.run_daemon(args.interval, args.max_batches, db_args=args)

        else:
            # Single run - process all pending
            pending = loader.get_pending_count()
            print(f"Pending activities: {pending:,}")

            if pending == 0:
                print("Nothing to process.")
            else:
                print(f"Processing in batches of {args.batch_size}...")
                print()

                start_time = time.time()
                guide, funding, participants = loader.run_all(args.max_batches)
                elapsed = time.time() - start_time

                print()
                print(f"{'='*60}")
                print(f"Completed in {elapsed:.1f}s")
                print(f"  Batches processed:  {loader.total_batches}")
                print(f"  Guide edges:        {guide:,}")
                print(f"  Funding edges:      {funding:,}")
                print(f"  Token participants: {participants:,}")
                if loader.deadlock_count > 0:
                    print(f"  Deadlock retries:   {loader.deadlock_count}")
                print(f"{'='*60}")

    finally:
        loader.close()
        db_conn.close()

    return 0


if __name__ == '__main__':
    sys.exit(main())
