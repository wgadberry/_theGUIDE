#!/usr/bin/env python3
"""
Guide Integrity Check - Data quality validation for tx_guide pipeline

Checks data flow from source tables (tx_activity, tx_swap, tx_transfer)
into derived tables (tx_guide, tx_funding_edge, tx_token_participant)

Usage:
    python guide-integrity-check.py                # Run all checks
    python guide-integrity-check.py --diagnose    # Detailed diagnosis of missing coverage
    python guide-integrity-check.py --fix         # Fix duplicates and add unique index
    python guide-integrity-check.py --backfill    # Backfill missing transfer/swap edges
    python guide-integrity-check.py --reset       # Reset stuck activities for reprocessing
    python guide-integrity-check.py --link-orphans  # Link orphaned transfers to activities
    python guide-integrity-check.py --enrich-missing  # Re-fetch missing owner/token data via Solscan
    python guide-integrity-check.py --purge-empty-orphans  # Delete empty orphan transfer records
"""

import argparse
import sys
import os
import json
import time
import functools
import random
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any, Callable

try:
    import mysql.connector
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


# =============================================================================
# Config Loading
# =============================================================================

def load_config() -> dict:
    """Load configuration from JSON file with environment variable fallback."""
    config = {}
    config_paths = [
        Path('./guide-config.json'),
        Path(__file__).parent / 'guide-config.json',
    ]

    for config_path in config_paths:
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                break
            except Exception:
                pass

    # Environment variable fallback/override
    env_mappings = {
        'SOLSCAN_API': 'SOLSCAN_API_URL',
        'SOLSCAN_TOKEN': 'SOLSCAN_API_TOKEN',
        'DB_HOST': 'MYSQL_HOST',
        'DB_PORT': 'MYSQL_PORT',
        'DB_USER': 'MYSQL_USER',
        'DB_PASSWORD': 'MYSQL_PASSWORD',
        'DB_NAME': 'MYSQL_DATABASE',
    }

    for config_key, env_var in env_mappings.items():
        if os.environ.get(env_var):
            config[config_key] = os.environ[env_var]
            if config_key == 'DB_PORT':
                config[config_key] = int(config[config_key])

    return config


_CONFIG = load_config()

# Solscan API configuration
SOLSCAN_API = _CONFIG.get('SOLSCAN_API', 'https://pro-api.solscan.io/v2.0')
SOLSCAN_TOKEN = _CONFIG.get('SOLSCAN_TOKEN', '')
SOLSCAN_DELAY = _CONFIG.get('SOLSCAN_DELAY', 0.25)


# =============================================================================
# Retry Logic
# =============================================================================

def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    retryable_exceptions: tuple = ()
) -> Callable:
    """Decorator for retrying functions with exponential backoff."""
    if not retryable_exceptions and HAS_REQUESTS:
        retryable_exceptions = (requests.RequestException, ConnectionError, TimeoutError)
    elif not retryable_exceptions:
        retryable_exceptions = (ConnectionError, TimeoutError)

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        break
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    delay = delay * (1 + random.random() * 0.5)
                    print(f"    Retry {attempt + 1}/{max_retries} after {delay:.1f}s: {e}")
                    time.sleep(delay)
            raise last_exception
        return wrapper
    return decorator


class IntegrityChecker:
    """Runs data integrity checks on tx_guide pipeline"""

    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.cursor = db_conn.cursor(dictionary=True)
        self.issues = []
        self.stats = {}

    def run_query(self, query: str) -> List[Dict]:
        """Execute query and return results"""
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def run_scalar(self, query: str) -> int:
        """Execute query and return single value"""
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        return list(row.values())[0] if row else 0

    def check_row_counts(self) -> Dict[str, int]:
        """Get baseline row counts"""
        print("\n=== 1. ROW COUNTS ===")
        tables = ['tx_activity', 'tx_swap', 'tx_transfer', 'tx_guide',
                  'tx_funding_edge', 'tx_token_participant']
        counts = {}
        for tbl in tables:
            cnt = self.run_scalar(f"SELECT COUNT(*) FROM {tbl}")
            counts[tbl] = cnt
            print(f"  {tbl}: {cnt:,}")
        self.stats['row_counts'] = counts
        return counts

    def check_source_coverage(self) -> Dict[str, Dict]:
        """Check tx_transfer and tx_swap coverage in tx_guide"""
        print("\n=== 2. SOURCE -> tx_guide COVERAGE ===")

        # tx_transfer - total and covered
        transfer_total = self.run_scalar("SELECT COUNT(*) FROM tx_transfer")
        transfer_covered = self.run_scalar("""
            SELECT COUNT(DISTINCT t.id) FROM tx_transfer t
            JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
        """)

        # Exclude expected missing: burns, mints, create/close account (no valid from->to)
        transfer_expected_missing = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            WHERE g.id IS NULL
              AND t.transfer_type IN ('ACTIVITY_SPL_BURN', 'ACTIVITY_SPL_MINT',
                                      'ACTIVITY_SPL_CREATE_ACCOUNT', 'ACTIVITY_SPL_CLOSE_ACCOUNT')
        """)

        # Also count equivalent edges (edge exists but not source-linked)
        transfer_equiv = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            WHERE g.id IS NULL
              AND t.transfer_type = 'ACTIVITY_SPL_TRANSFER'
              AND t.source_owner_address_id IS NOT NULL
              AND t.destination_owner_address_id IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM tx_guide g2
                  WHERE g2.tx_id = t.tx_id
                    AND g2.from_address_id = t.source_owner_address_id
                    AND g2.to_address_id = t.destination_owner_address_id
                    AND g2.token_id = t.token_id
                    AND g2.amount = t.amount
              )
        """)

        transfer_missing = transfer_total - transfer_covered
        transfer_actionable = transfer_missing - transfer_expected_missing - transfer_equiv
        transfer_pct = (transfer_covered / transfer_total * 100) if transfer_total > 0 else 0

        print(f"  tx_transfer: {transfer_covered:,} / {transfer_total:,} ({transfer_pct:.1f}%)")
        print(f"    Missing: {transfer_missing:,}")
        print(f"      - Expected (burn/mint/account ops): {transfer_expected_missing:,}")
        print(f"      - Has equivalent edge: {transfer_equiv:,}")
        print(f"      - Actionable: {transfer_actionable:,}")

        # tx_swap
        swap_total = self.run_scalar("SELECT COUNT(*) FROM tx_swap")
        swap_covered = self.run_scalar("""
            SELECT COUNT(DISTINCT s.id) FROM tx_swap s
            JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id
        """)

        # Count equivalent edges for swaps
        swap_equiv = self.run_scalar("""
            SELECT COUNT(*) FROM tx_swap s
            LEFT JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id
            WHERE g.id IS NULL
              AND s.account_address_id IS NOT NULL
              AND s.token_1_id IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM tx_guide g2
                  WHERE g2.tx_id = s.tx_id
                    AND g2.from_address_id = s.account_address_id
                    AND g2.token_id = s.token_1_id
              )
        """)

        swap_missing = swap_total - swap_covered
        swap_actionable = swap_missing - swap_equiv
        swap_pct = (swap_covered / swap_total * 100) if swap_total > 0 else 0

        print(f"  tx_swap: {swap_covered:,} / {swap_total:,} ({swap_pct:.1f}%)")
        print(f"    Missing: {swap_missing:,}")
        print(f"      - Has equivalent edge: {swap_equiv:,}")
        print(f"      - Actionable: {swap_actionable:,}")

        # Pending activities
        pending = self.run_scalar("""
            SELECT COUNT(*) FROM tx_activity WHERE guide_loaded = 0 OR guide_loaded IS NULL
        """)
        print(f"  Pending activities (guide_loaded=0): {pending:,}")

        if pending > 0:
            self.issues.append(f"{pending:,} activities pending processing")

        self.stats['coverage'] = {
            'transfer': {
                'total': transfer_total, 'covered': transfer_covered,
                'missing': transfer_missing, 'expected_missing': transfer_expected_missing,
                'equiv': transfer_equiv, 'actionable': transfer_actionable
            },
            'swap': {
                'total': swap_total, 'covered': swap_covered,
                'missing': swap_missing, 'equiv': swap_equiv, 'actionable': swap_actionable
            },
            'pending': pending
        }
        return self.stats['coverage']

    def check_orphans(self) -> Dict[str, int]:
        """Check for orphan records"""
        print("\n=== 3. ORPHAN DETECTION ===")

        checks = [
            ("tx_swap without tx_activity", """
                SELECT COUNT(*) FROM tx_swap s
                LEFT JOIN tx_activity a ON a.id = s.activity_id
                WHERE s.activity_id IS NOT NULL AND a.id IS NULL
            """),
            ("tx_transfer without tx_activity", """
                SELECT COUNT(*) FROM tx_transfer t
                LEFT JOIN tx_activity a ON a.id = t.activity_id
                WHERE t.activity_id IS NOT NULL AND a.id IS NULL
            """),
            ("tx_guide invalid source (transfer)", """
                SELECT COUNT(*) FROM tx_guide g
                LEFT JOIN tx_transfer t ON t.id = g.source_row_id
                WHERE g.source_id = 1 AND t.id IS NULL
            """),
            ("tx_guide invalid source (swap)", """
                SELECT COUNT(*) FROM tx_guide g
                LEFT JOIN tx_swap s ON s.id = g.source_row_id
                WHERE g.source_id = 2 AND s.id IS NULL
            """),
        ]

        orphans = {}
        for name, query in checks:
            cnt = self.run_scalar(query)
            orphans[name] = cnt
            status = "OK" if cnt == 0 else "ISSUE"
            print(f"  [{status}] {name}: {cnt:,}")
            if cnt > 0:
                self.issues.append(f"{cnt:,} {name}")

        self.stats['orphans'] = orphans
        return orphans

    def check_referential_integrity(self) -> Dict[str, int]:
        """Check foreign key integrity"""
        print("\n=== 4. REFERENTIAL INTEGRITY ===")

        checks = [
            ("tx_guide.token_id -> tx_token", """
                SELECT COUNT(*) FROM tx_guide g
                LEFT JOIN tx_token t ON t.id = g.token_id
                WHERE g.token_id IS NOT NULL AND t.id IS NULL
            """),
            ("tx_guide.from_address_id -> tx_address", """
                SELECT COUNT(*) FROM tx_guide g
                LEFT JOIN tx_address a ON a.id = g.from_address_id
                WHERE g.from_address_id IS NOT NULL AND a.id IS NULL
            """),
            ("tx_guide.to_address_id -> tx_address", """
                SELECT COUNT(*) FROM tx_guide g
                LEFT JOIN tx_address a ON a.id = g.to_address_id
                WHERE g.to_address_id IS NOT NULL AND a.id IS NULL
            """),
        ]

        results = {}
        for name, query in checks:
            cnt = self.run_scalar(query)
            results[name] = cnt
            status = "OK" if cnt == 0 else "ISSUE"
            print(f"  [{status}] {name}: {cnt:,}")
            if cnt > 0:
                self.issues.append(f"{cnt:,} broken {name}")

        self.stats['referential'] = results
        return results

    def check_duplicates(self) -> Tuple[int, int]:
        """Check for duplicate edges"""
        print("\n=== 5. DUPLICATE DETECTION ===")

        result = self.run_query("""
            SELECT COUNT(*) as dup_groups, SUM(cnt - 1) as extra FROM (
                SELECT tx_id, from_address_id, to_address_id, token_id, amount, edge_type_id, COUNT(*) as cnt
                FROM tx_guide
                GROUP BY tx_id, from_address_id, to_address_id, token_id, amount, edge_type_id
                HAVING COUNT(*) > 1
            ) d
        """)

        dup_groups = result[0]['dup_groups'] or 0
        extra = result[0]['extra'] or 0

        status = "OK" if dup_groups == 0 else "ISSUE"
        print(f"  [{status}] Duplicate groups: {dup_groups:,}")
        print(f"  [{status}] Extra rows: {extra:,}")

        if dup_groups > 0:
            self.issues.append(f"{extra:,} duplicate tx_guide rows")

        self.stats['duplicates'] = {'groups': dup_groups, 'extra': extra}
        return dup_groups, extra

    def check_stuck_records(self) -> Dict[str, int]:
        """Check for records marked done but missing from tx_guide"""
        print("\n=== 6. STUCK RECORDS (guide_loaded=1 but no edge) ===")

        # Stuck transfers
        stuck_transfers = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            JOIN tx_activity a ON a.id = t.activity_id
            LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            WHERE a.guide_loaded = 1
              AND g.id IS NULL
              AND t.transfer_type = 'ACTIVITY_SPL_TRANSFER'
              AND t.source_owner_address_id IS NOT NULL
              AND t.destination_owner_address_id IS NOT NULL
        """)

        # Check if covered by equivalent edge
        stuck_transfers_real = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            JOIN tx_activity a ON a.id = t.activity_id
            LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            WHERE a.guide_loaded = 1
              AND g.id IS NULL
              AND t.transfer_type = 'ACTIVITY_SPL_TRANSFER'
              AND t.source_owner_address_id IS NOT NULL
              AND t.destination_owner_address_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM tx_guide g2
                  WHERE g2.tx_id = t.tx_id
                    AND g2.from_address_id = t.source_owner_address_id
                    AND g2.to_address_id = t.destination_owner_address_id
                    AND g2.token_id = t.token_id
                    AND g2.amount = t.amount
              )
        """)

        # Stuck swaps
        stuck_swaps = self.run_scalar("""
            SELECT COUNT(*) FROM tx_swap s
            JOIN tx_activity a ON a.id = s.activity_id
            LEFT JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id
            WHERE a.guide_loaded = 1
              AND g.id IS NULL
              AND s.account_address_id IS NOT NULL
              AND s.token_1_id IS NOT NULL
              AND s.token_2_id IS NOT NULL
        """)

        print(f"  Transfers (guide_loaded=1, no edge): {stuck_transfers:,}")
        print(f"    - Covered by equivalent edge: {stuck_transfers - stuck_transfers_real:,}")
        print(f"    - Real gaps: {stuck_transfers_real:,}")
        print(f"  Swaps (guide_loaded=1, no edge): {stuck_swaps:,}")

        if stuck_transfers_real > 0:
            self.issues.append(f"{stuck_transfers_real:,} stuck transfers (real gaps)")

        self.stats['stuck'] = {
            'transfers': stuck_transfers,
            'transfers_real': stuck_transfers_real,
            'swaps': stuck_swaps
        }
        return self.stats['stuck']

    def check_unique_index(self) -> bool:
        """Check if unique index exists on tx_guide"""
        print("\n=== 7. UNIQUE INDEX CHECK ===")

        result = self.run_query("""
            SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'tx_guide'
              AND INDEX_NAME = 'uq_guide_edge'
            LIMIT 1
        """)

        exists = len(result) > 0
        status = "OK" if exists else "MISSING"
        print(f"  [{status}] uq_guide_edge index: {'exists' if exists else 'NOT FOUND'}")

        if not exists:
            self.issues.append("uq_guide_edge index missing - duplicates possible")

        self.stats['unique_index'] = exists
        return exists

    def check_synthetic_addresses(self) -> Dict[str, int]:
        """Check synthetic sink/source addresses exist for burns, mints, etc."""
        print("\n=== 8. SYNTHETIC ADDRESSES ===")

        expected = {
            'BURN111111111111111111111111111111111111111': 'sink',
            'MINT111111111111111111111111111111111111111': 'source',
            'CLOSE11111111111111111111111111111111111111': 'sink',
            'CREATE1111111111111111111111111111111111111': 'source',
        }

        results = {}
        all_ok = True

        for addr, addr_type in expected.items():
            self.cursor.execute("SELECT id FROM tx_address WHERE address = %s", (addr,))
            row = self.cursor.fetchone()
            exists = row is not None
            results[addr] = row['id'] if row else None
            status = "OK" if exists else "MISSING"
            if not exists:
                all_ok = False
            print(f"  [{status}] {addr[:20]}... ({addr_type}): {results[addr] or 'NOT FOUND'}")

        if not all_ok:
            self.issues.append("Missing synthetic sink/source addresses")

        self.stats['synthetic_addresses'] = results
        return results

    def check_burn_mint_coverage(self) -> Dict[str, Dict]:
        """Check burn/mint/create_account activities have guide edges"""
        print("\n=== 9. BURN/MINT/CREATE_ACCOUNT COVERAGE ===")

        # Get synthetic address IDs
        self.cursor.execute("""
            SELECT address, id FROM tx_address
            WHERE address IN ('BURN111111111111111111111111111111111111111',
                              'MINT111111111111111111111111111111111111111',
                              'CREATE1111111111111111111111111111111111111')
        """)
        addr_map = {row['address']: row['id'] for row in self.cursor.fetchall()}

        results = {}

        # Burns: should have edge to BURN sink (id=3)
        burn_total = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer
            WHERE transfer_type = 'ACTIVITY_SPL_BURN'
        """)
        burn_with_edge = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            WHERE t.transfer_type = 'ACTIVITY_SPL_BURN'
        """)
        burn_missing = burn_total - burn_with_edge
        burn_pct = (burn_with_edge / burn_total * 100) if burn_total > 0 else 100

        status = "OK" if burn_missing == 0 else "ISSUE"
        print(f"  [{status}] Burns: {burn_with_edge:,} / {burn_total:,} ({burn_pct:.1f}%) - missing: {burn_missing:,}")
        results['burn'] = {'total': burn_total, 'covered': burn_with_edge, 'missing': burn_missing}

        if burn_missing > 0:
            self.issues.append(f"{burn_missing:,} burns without guide edges")

        # Mints: should have edge from MINT source
        mint_total = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer
            WHERE transfer_type = 'ACTIVITY_SPL_MINT'
        """)
        mint_with_edge = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            WHERE t.transfer_type = 'ACTIVITY_SPL_MINT'
        """)
        mint_missing = mint_total - mint_with_edge
        mint_pct = (mint_with_edge / mint_total * 100) if mint_total > 0 else 100

        status = "OK" if mint_missing == 0 else "ISSUE"
        print(f"  [{status}] Mints: {mint_with_edge:,} / {mint_total:,} ({mint_pct:.1f}%) - missing: {mint_missing:,}")
        results['mint'] = {'total': mint_total, 'covered': mint_with_edge, 'missing': mint_missing}

        if mint_missing > 0:
            self.issues.append(f"{mint_missing:,} mints without guide edges")

        # Create account: edge cases with NULL destination
        create_total = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer
            WHERE transfer_type = 'ACTIVITY_SPL_CREATE_ACCOUNT'
              AND destination_owner_address_id IS NULL
        """)
        create_with_edge = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            WHERE t.transfer_type = 'ACTIVITY_SPL_CREATE_ACCOUNT'
              AND t.destination_owner_address_id IS NULL
        """)
        create_missing = create_total - create_with_edge

        status = "OK" if create_missing == 0 else "WARN"
        print(f"  [{status}] Create Account (NULL dest): {create_with_edge:,} / {create_total:,} - missing: {create_missing:,}")
        results['create_account'] = {'total': create_total, 'covered': create_with_edge, 'missing': create_missing}

        self.stats['burn_mint_coverage'] = results
        return results

    def check_fee_coverage(self) -> Dict[str, int]:
        """Check tx_guide has fee data populated"""
        print("\n=== 10. FEE DATA COVERAGE ===")

        total_guide = self.run_scalar("SELECT COUNT(*) FROM tx_guide")
        has_fee = self.run_scalar("SELECT COUNT(*) FROM tx_guide WHERE fee IS NOT NULL")
        has_priority = self.run_scalar("SELECT COUNT(*) FROM tx_guide WHERE priority_fee IS NOT NULL AND priority_fee > 0")
        missing_fee = total_guide - has_fee

        fee_pct = (has_fee / total_guide * 100) if total_guide > 0 else 0
        priority_pct = (has_priority / total_guide * 100) if total_guide > 0 else 0

        status = "OK" if missing_fee == 0 else "WARN"
        print(f"  [{status}] Guide edges with fee: {has_fee:,} / {total_guide:,} ({fee_pct:.1f}%)")
        print(f"  [INFO] Guide edges with priority_fee > 0: {has_priority:,} ({priority_pct:.1f}%)")

        if missing_fee > 0:
            print(f"  [WARN] Missing fee data: {missing_fee:,}")

        # Fee stats
        result = self.run_query("""
            SELECT
                ROUND(SUM(fee) / 1e9, 4) as total_fee_sol,
                ROUND(SUM(priority_fee) / 1e9, 4) as total_priority_sol,
                ROUND(AVG(fee) / 1e9, 8) as avg_fee_sol,
                ROUND(AVG(priority_fee) / 1e9, 8) as avg_priority_sol
            FROM tx_guide
            WHERE fee IS NOT NULL
        """)
        if result:
            stats = result[0]
            print(f"  [INFO] Total fees: {stats['total_fee_sol'] or 0} SOL + {stats['total_priority_sol'] or 0} SOL priority")
            print(f"  [INFO] Avg fee: {stats['avg_fee_sol'] or 0} SOL, Avg priority: {stats['avg_priority_sol'] or 0} SOL")

        self.stats['fee_coverage'] = {
            'total': total_guide,
            'has_fee': has_fee,
            'has_priority': has_priority,
            'missing': missing_fee
        }
        return self.stats['fee_coverage']

    def check_activity_type_mapping(self) -> Dict[str, Dict]:
        """Check all activity types have proper mapping configuration"""
        print("\n=== 11. ACTIVITY TYPE MAPPING ===")

        # Get all distinct activity types from tx_activity
        self.cursor.execute("""
            SELECT activity_type, COUNT(*) as cnt
            FROM tx_activity
            WHERE activity_type IS NOT NULL
            GROUP BY activity_type
            ORDER BY cnt DESC
        """)
        activity_types = self.cursor.fetchall()

        # Get all mapped types
        self.cursor.execute("SELECT activity_type, creates_edge, guide_type_id FROM tx_activity_type_map")
        mapped = {row['activity_type']: row for row in self.cursor.fetchall()}

        results = {'mapped': 0, 'unmapped': [], 'edge_creators': 0}
        unmapped_with_count = []

        for row in activity_types:
            atype = row['activity_type']
            cnt = row['cnt']
            if atype in mapped:
                results['mapped'] += 1
                if mapped[atype]['creates_edge']:
                    results['edge_creators'] += 1
            else:
                results['unmapped'].append(atype)
                unmapped_with_count.append((atype, cnt))

        status = "OK" if not results['unmapped'] else "WARN"
        print(f"  [{status}] Mapped activity types: {results['mapped']}")
        print(f"  [INFO] Edge-creating types: {results['edge_creators']}")

        if results['unmapped']:
            print(f"  [WARN] Unmapped activity types: {len(results['unmapped'])}")
            for atype, cnt in unmapped_with_count[:10]:
                print(f"         - {atype}: {cnt:,} occurrences")
            if len(unmapped_with_count) > 10:
                print(f"         ... and {len(unmapped_with_count) - 10} more")

        self.stats['activity_mapping'] = results
        return results

    def check_activities_without_guide(self) -> Dict[str, int]:
        """Check activities that should have guide edges but don't"""
        print("\n=== 12. ACTIVITIES WITHOUT GUIDE EDGES ===")

        # Activity types that SHOULD create edges but might be missing
        edge_types = [
            'ACTIVITY_TOKEN_SWAP', 'ACTIVITY_AGG_TOKEN_SWAP',
            'ACTIVITY_SPL_TRANSFER', 'ACTIVITY_SPL_BURN', 'ACTIVITY_SPL_MINT',
            'ACTIVITY_SPL_CREATE_ACCOUNT', 'ACTIVITY_SPL_CLOSE_ACCOUNT',
            'ACTIVITY_TOKEN_ADD_LIQ', 'ACTIVITY_TOKEN_REMOVE_LIQ',
        ]

        results = {}

        self.cursor.execute("""
            SELECT a.activity_type, COUNT(*) as cnt
            FROM tx_activity a
            WHERE NOT EXISTS (SELECT 1 FROM tx_guide g WHERE g.tx_id = a.tx_id)
              AND a.activity_type IS NOT NULL
            GROUP BY a.activity_type
            ORDER BY cnt DESC
        """)

        rows = self.cursor.fetchall()
        total_missing = 0
        actionable_missing = 0

        for row in rows:
            atype = row['activity_type']
            cnt = row['cnt']
            results[atype] = cnt
            total_missing += cnt

            # Check if this is an edge-creating type
            if atype in edge_types:
                actionable_missing += cnt
                print(f"  [ISSUE] {atype}: {cnt:,} (should have edges)")
            elif 'COMPUTE' not in atype:  # Skip compute budget, expected
                print(f"  [INFO] {atype}: {cnt:,}")

        if actionable_missing > 0:
            self.issues.append(f"{actionable_missing:,} edge-creating activities without guide edges")

        print(f"\n  Summary: {total_missing:,} total activities without guide edges")
        print(f"           {actionable_missing:,} are edge-creating types (actionable)")

        self.stats['activities_without_guide'] = results
        return results

    def fix_duplicates(self) -> int:
        """Remove duplicate edges (keep lowest id)"""
        print("\n=== FIX: Removing duplicate edges ===")

        self.cursor.execute("""
            DELETE g1 FROM tx_guide g1
            INNER JOIN tx_guide g2
              ON g1.tx_id = g2.tx_id
              AND g1.from_address_id = g2.from_address_id
              AND g1.to_address_id = g2.to_address_id
              AND COALESCE(g1.token_id, 0) = COALESCE(g2.token_id, 0)
              AND COALESCE(g1.amount, 0) = COALESCE(g2.amount, 0)
              AND g1.edge_type_id = g2.edge_type_id
              AND g1.id > g2.id
        """)
        deleted = self.cursor.rowcount
        self.db_conn.commit()

        print(f"  Deleted {deleted:,} duplicate rows")
        return deleted

    def fix_add_unique_index(self) -> bool:
        """Add unique index if missing"""
        print("\n=== FIX: Adding unique index ===")

        try:
            self.cursor.execute("""
                ALTER TABLE tx_guide ADD UNIQUE INDEX uq_guide_edge
                  (tx_id, from_address_id, to_address_id, token_id, amount, edge_type_id)
            """)
            self.db_conn.commit()
            print("  Index added successfully")
            return True
        except mysql.connector.Error as e:
            if e.errno == 1061:  # Duplicate key name
                print("  Index already exists")
                return True
            raise

    def fix_reset_stuck_swaps(self) -> int:
        """Reset stuck swap activities"""
        print("\n=== FIX: Resetting stuck swap activities ===")

        self.cursor.execute("""
            UPDATE tx_activity a
            JOIN tx_swap s ON s.activity_id = a.id
            LEFT JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id
            SET a.guide_loaded = 0
            WHERE g.id IS NULL
              AND a.guide_loaded = 1
              AND s.account_address_id IS NOT NULL
              AND s.token_1_id IS NOT NULL
              AND s.token_2_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM tx_guide g2
                  JOIN tx_token tk ON tk.id = s.token_1_id
                  WHERE g2.tx_id = s.tx_id
                    AND g2.from_address_id = s.account_address_id
                    AND g2.to_address_id = tk.mint_address_id
                    AND g2.token_id = s.token_1_id
                    AND g2.amount = s.amount_1
              )
        """)
        reset = self.cursor.rowcount
        self.db_conn.commit()

        print(f"  Reset {reset:,} swap activities")
        return reset

    def diagnose_missing_coverage(self) -> Dict[str, Dict]:
        """
        Break down missing tx_transfer and tx_swap records by reason.
        Helps identify WHY records aren't making it into tx_guide.
        """
        print("\n=== 8. MISSING COVERAGE DIAGNOSIS ===")

        results = {'transfer': {}, 'swap': {}}

        # =====================================================================
        # tx_transfer breakdown
        # =====================================================================
        print("\n  --- tx_transfer breakdown ---")

        # Total missing
        transfer_missing = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            WHERE g.id IS NULL
        """)

        # Missing: no activity link
        transfer_no_activity = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            WHERE g.id IS NULL AND t.activity_id IS NULL
        """)

        # Missing: activity guide_loaded = 0
        transfer_not_loaded = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            JOIN tx_activity a ON a.id = t.activity_id
            LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            WHERE g.id IS NULL AND a.guide_loaded = 0
        """)

        # Missing: activity guide_loaded = 1 (stuck)
        transfer_stuck = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            JOIN tx_activity a ON a.id = t.activity_id
            LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            WHERE g.id IS NULL AND a.guide_loaded = 1
        """)

        # Missing: null required fields (source_owner or dest_owner)
        transfer_null_fields = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            WHERE g.id IS NULL
              AND (t.source_owner_address_id IS NULL OR t.destination_owner_address_id IS NULL)
        """)

        # Missing: same source/dest (self-transfer)
        transfer_self = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            WHERE g.id IS NULL
              AND t.source_owner_address_id = t.destination_owner_address_id
        """)

        results['transfer'] = {
            'total_missing': transfer_missing,
            'no_activity': transfer_no_activity,
            'not_loaded': transfer_not_loaded,
            'stuck': transfer_stuck,
            'null_fields': transfer_null_fields,
            'self_transfer': transfer_self
        }

        print(f"    Total missing: {transfer_missing:,}")
        print(f"      - No activity link: {transfer_no_activity:,}")
        print(f"      - Activity not loaded (guide_loaded=0): {transfer_not_loaded:,}")
        print(f"      - Activity stuck (guide_loaded=1, no edge): {transfer_stuck:,}")
        print(f"      - Null source/dest owner: {transfer_null_fields:,}")
        print(f"      - Self-transfer (src=dest): {transfer_self:,}")

        # =====================================================================
        # tx_swap breakdown
        # =====================================================================
        print("\n  --- tx_swap breakdown ---")

        # Total missing
        swap_missing = self.run_scalar("""
            SELECT COUNT(*) FROM tx_swap s
            LEFT JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id
            WHERE g.id IS NULL
        """)

        # Missing: no activity link
        swap_no_activity = self.run_scalar("""
            SELECT COUNT(*) FROM tx_swap s
            LEFT JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id
            WHERE g.id IS NULL AND s.activity_id IS NULL
        """)

        # Missing: activity guide_loaded = 0
        swap_not_loaded = self.run_scalar("""
            SELECT COUNT(*) FROM tx_swap s
            JOIN tx_activity a ON a.id = s.activity_id
            LEFT JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id
            WHERE g.id IS NULL AND a.guide_loaded = 0
        """)

        # Missing: activity guide_loaded = 1 (stuck)
        swap_stuck = self.run_scalar("""
            SELECT COUNT(*) FROM tx_swap s
            JOIN tx_activity a ON a.id = s.activity_id
            LEFT JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id
            WHERE g.id IS NULL AND a.guide_loaded = 1
        """)

        # Missing: null required fields
        swap_null_fields = self.run_scalar("""
            SELECT COUNT(*) FROM tx_swap s
            LEFT JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id
            WHERE g.id IS NULL
              AND (s.account_address_id IS NULL OR s.token_1_id IS NULL OR s.token_2_id IS NULL)
        """)

        results['swap'] = {
            'total_missing': swap_missing,
            'no_activity': swap_no_activity,
            'not_loaded': swap_not_loaded,
            'stuck': swap_stuck,
            'null_fields': swap_null_fields
        }

        print(f"    Total missing: {swap_missing:,}")
        print(f"      - No activity link: {swap_no_activity:,}")
        print(f"      - Activity not loaded (guide_loaded=0): {swap_not_loaded:,}")
        print(f"      - Activity stuck (guide_loaded=1, no edge): {swap_stuck:,}")
        print(f"      - Null account/token fields: {swap_null_fields:,}")

        self.stats['missing_diagnosis'] = results
        return results

    def fix_backfill_transfers(self, batch_size: int = 10000, limit: int = 0) -> int:
        """
        Backfill missing tx_transfer -> tx_guide edges.
        Targets records where activity.guide_loaded=1 but no edge exists.
        """
        print("\n=== FIX: Backfilling missing transfer edges ===")

        # Get edge_type_id for spl_transfer
        self.cursor.execute("SELECT id FROM tx_guide_type WHERE type_code = 'spl_transfer'")
        row = self.cursor.fetchone()
        if not row:
            print("  ERROR: tx_guide_type 'spl_transfer' not found")
            return 0
        spl_transfer_type = row['id']

        # Count candidates
        limit_clause = f"LIMIT {limit}" if limit > 0 else ""
        count_query = f"""
            SELECT COUNT(*) FROM tx_transfer t
            JOIN tx_activity a ON a.id = t.activity_id
            LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            WHERE g.id IS NULL
              AND a.guide_loaded = 1
              AND t.source_owner_address_id IS NOT NULL
              AND t.destination_owner_address_id IS NOT NULL
              AND t.source_owner_address_id != t.destination_owner_address_id
              AND t.token_id IS NOT NULL
            {limit_clause}
        """
        candidates = self.run_scalar(count_query)
        print(f"  Candidates for backfill: {candidates:,}")

        if candidates == 0:
            return 0

        # Backfill in batches
        total_inserted = 0
        offset = 0

        while True:
            batch_limit = min(batch_size, limit - total_inserted) if limit > 0 else batch_size

            insert_query = f"""
                INSERT IGNORE INTO tx_guide (
                    tx_id, from_address_id, to_address_id, token_id,
                    amount, decimals, edge_type_id, block_time,
                    source_id, source_row_id
                )
                SELECT
                    t.tx_id,
                    t.source_owner_address_id,
                    t.destination_owner_address_id,
                    t.token_id,
                    t.amount,
                    t.decimals,
                    {spl_transfer_type},
                    tx.block_time,
                    1,  -- source_id for tx_transfer
                    t.id
                FROM tx_transfer t
                JOIN tx_activity a ON a.id = t.activity_id
                JOIN tx ON tx.id = t.tx_id
                LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
                WHERE g.id IS NULL
                  AND a.guide_loaded = 1
                  AND t.source_owner_address_id IS NOT NULL
                  AND t.destination_owner_address_id IS NOT NULL
                  AND t.source_owner_address_id != t.destination_owner_address_id
                  AND t.token_id IS NOT NULL
                ORDER BY t.id
                LIMIT {batch_limit}
            """

            self.cursor.execute(insert_query)
            inserted = self.cursor.rowcount
            self.db_conn.commit()

            if inserted == 0:
                break

            total_inserted += inserted
            print(f"    Inserted batch: {inserted:,} (total: {total_inserted:,})")

            if limit > 0 and total_inserted >= limit:
                break

        print(f"  Total backfilled: {total_inserted:,}")
        return total_inserted

    def fix_backfill_swaps(self, batch_size: int = 5000, limit: int = 0) -> int:
        """
        Backfill missing tx_swap -> tx_guide edges.
        Creates swap_in and swap_out edges for each swap.
        """
        print("\n=== FIX: Backfilling missing swap edges ===")

        # Get edge_type_ids
        self.cursor.execute("SELECT id, type_code FROM tx_guide_type WHERE type_code IN ('swap_in', 'swap_out')")
        types = {row['type_code']: row['id'] for row in self.cursor.fetchall()}

        if 'swap_in' not in types or 'swap_out' not in types:
            print("  ERROR: tx_guide_type 'swap_in' or 'swap_out' not found")
            return 0

        swap_in_type = types['swap_in']
        swap_out_type = types['swap_out']

        # Count candidates
        limit_clause = f"LIMIT {limit}" if limit > 0 else ""
        count_query = f"""
            SELECT COUNT(*) FROM tx_swap s
            JOIN tx_activity a ON a.id = s.activity_id
            LEFT JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id
            WHERE g.id IS NULL
              AND a.guide_loaded = 1
              AND s.account_address_id IS NOT NULL
              AND s.token_1_id IS NOT NULL
              AND s.token_2_id IS NOT NULL
            {limit_clause}
        """
        candidates = self.run_scalar(count_query)
        print(f"  Candidates for backfill: {candidates:,}")

        if candidates == 0:
            return 0

        total_inserted = 0

        # Backfill swap_out edges (wallet sends token_1)
        print("  Backfilling swap_out edges...")
        insert_out = f"""
            INSERT IGNORE INTO tx_guide (
                tx_id, from_address_id, to_address_id, token_id,
                amount, decimals, edge_type_id, block_time,
                source_id, source_row_id
            )
            SELECT
                s.tx_id,
                s.account_address_id,        -- from wallet
                s.amm_id,                    -- to pool (if known)
                s.token_1_id,                -- token sold
                s.amount_1,
                COALESCE(s.decimals_1, 9),
                {swap_out_type},
                tx.block_time,
                2,  -- source_id for tx_swap
                s.id
            FROM tx_swap s
            JOIN tx_activity a ON a.id = s.activity_id
            JOIN tx ON tx.id = s.tx_id
            LEFT JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id AND g.edge_type_id = {swap_out_type}
            WHERE g.id IS NULL
              AND a.guide_loaded = 1
              AND s.account_address_id IS NOT NULL
              AND s.token_1_id IS NOT NULL
              AND s.token_2_id IS NOT NULL
            ORDER BY s.id
            LIMIT {batch_size if limit == 0 else min(batch_size, limit)}
        """
        self.cursor.execute(insert_out)
        out_inserted = self.cursor.rowcount
        self.db_conn.commit()
        total_inserted += out_inserted
        print(f"    swap_out: {out_inserted:,}")

        # Backfill swap_in edges (wallet receives token_2)
        print("  Backfilling swap_in edges...")
        insert_in = f"""
            INSERT IGNORE INTO tx_guide (
                tx_id, from_address_id, to_address_id, token_id,
                amount, decimals, edge_type_id, block_time,
                source_id, source_row_id
            )
            SELECT
                s.tx_id,
                s.amm_id,                    -- from pool (if known)
                s.account_address_id,        -- to wallet
                s.token_2_id,                -- token received
                s.amount_2,
                COALESCE(s.decimals_2, 9),
                {swap_in_type},
                tx.block_time,
                2,  -- source_id for tx_swap
                s.id
            FROM tx_swap s
            JOIN tx_activity a ON a.id = s.activity_id
            JOIN tx ON tx.id = s.tx_id
            LEFT JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id AND g.edge_type_id = {swap_in_type}
            WHERE g.id IS NULL
              AND a.guide_loaded = 1
              AND s.account_address_id IS NOT NULL
              AND s.token_1_id IS NOT NULL
              AND s.token_2_id IS NOT NULL
            ORDER BY s.id
            LIMIT {batch_size if limit == 0 else min(batch_size, limit)}
        """
        self.cursor.execute(insert_in)
        in_inserted = self.cursor.rowcount
        self.db_conn.commit()
        total_inserted += in_inserted
        print(f"    swap_in: {in_inserted:,}")

        print(f"  Total backfilled: {total_inserted:,}")
        return total_inserted

    def fix_reset_unprocessed(self) -> Dict[str, int]:
        """
        Reset guide_loaded=0 for activities that have source records
        without tx_guide edges. Allows re-processing by the loader.
        """
        print("\n=== FIX: Resetting unprocessed activities ===")

        # Reset transfers
        self.cursor.execute("""
            UPDATE tx_activity a
            JOIN tx_transfer t ON t.activity_id = a.id
            LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            SET a.guide_loaded = 0
            WHERE g.id IS NULL
              AND a.guide_loaded = 1
              AND t.source_owner_address_id IS NOT NULL
              AND t.destination_owner_address_id IS NOT NULL
              AND t.source_owner_address_id != t.destination_owner_address_id
        """)
        transfer_reset = self.cursor.rowcount
        self.db_conn.commit()

        # Reset swaps
        self.cursor.execute("""
            UPDATE tx_activity a
            JOIN tx_swap s ON s.activity_id = a.id
            LEFT JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id
            SET a.guide_loaded = 0
            WHERE g.id IS NULL
              AND a.guide_loaded = 1
              AND s.account_address_id IS NOT NULL
              AND s.token_1_id IS NOT NULL
              AND s.token_2_id IS NOT NULL
        """)
        swap_reset = self.cursor.rowcount
        self.db_conn.commit()

        print(f"  Reset transfers: {transfer_reset:,}")
        print(f"  Reset swaps: {swap_reset:,}")

        return {'transfers': transfer_reset, 'swaps': swap_reset}

    def fix_link_orphaned_transfers(self) -> int:
        """
        Link orphaned tx_transfer records (no activity_id) to activities by tx_id.
        """
        print("\n=== FIX: Linking orphaned transfers to activities ===")

        # First, count orphans
        orphan_count = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            WHERE t.activity_id IS NULL
        """)
        print(f"  Orphaned transfers (no activity_id): {orphan_count:,}")

        if orphan_count == 0:
            return 0

        # Count how many can be linked
        linkable = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            JOIN tx_activity a ON a.tx_id = t.tx_id
            WHERE t.activity_id IS NULL
        """)
        print(f"  Linkable (activity exists for tx_id): {linkable:,}")

        if linkable == 0:
            print("  No linkable transfers found")
            return 0

        # Link them
        self.cursor.execute("""
            UPDATE tx_transfer t
            JOIN tx_activity a ON a.tx_id = t.tx_id
            SET t.activity_id = a.id
            WHERE t.activity_id IS NULL
        """)
        linked = self.cursor.rowcount
        self.db_conn.commit()

        print(f"  Linked: {linked:,}")
        return linked

    def purge_empty_orphan_transfers(self, dry_run: bool = False) -> int:
        """
        Delete empty orphan transfer records.
        These are records with activity_id IS NULL and no actual data
        (no transfer_type, source, destination, amount, or token).
        """
        print("\n=== FIX: Purging empty orphan transfers ===")

        # Count empty orphans
        empty_count = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer
            WHERE activity_id IS NULL
              AND transfer_type IS NULL
              AND source_address_id IS NULL
              AND destination_address_id IS NULL
              AND amount IS NULL
              AND token_id IS NULL
        """)
        print(f"  Empty orphan transfers found: {empty_count:,}")

        if empty_count == 0:
            return 0

        if dry_run:
            print(f"  Dry run: would delete {empty_count:,} records")
            return empty_count

        # Delete in batches to avoid long locks
        total_deleted = 0
        batch_size = 10000

        while True:
            self.cursor.execute(f"""
                DELETE FROM tx_transfer
                WHERE activity_id IS NULL
                  AND transfer_type IS NULL
                  AND source_address_id IS NULL
                  AND destination_address_id IS NULL
                  AND amount IS NULL
                  AND token_id IS NULL
                LIMIT {batch_size}
            """)
            deleted = self.cursor.rowcount
            self.db_conn.commit()

            if deleted == 0:
                break

            total_deleted += deleted
            print(f"    Deleted batch: {deleted:,} (total: {total_deleted:,})")

        print(f"  Total purged: {total_deleted:,}")
        return total_deleted

    def fix_enrich_missing_owners(self, limit: int = 1000, dry_run: bool = False) -> Dict[str, int]:
        """
        Use Solscan API to fetch missing owner addresses for SPL_TRANSFER records.
        Fetches the owner of the source/destination token accounts.
        """
        print("\n=== FIX: Enriching missing owner addresses via Solscan ===")

        if not HAS_REQUESTS:
            print("  ERROR: requests library not installed")
            return {'fetched': 0, 'updated': 0, 'errors': 0}

        if not SOLSCAN_TOKEN:
            print("  ERROR: SOLSCAN_TOKEN not configured")
            return {'fetched': 0, 'updated': 0, 'errors': 0}

        stats = {'fetched': 0, 'updated': 0, 'errors': 0, 'skipped': 0}

        # Get transfers needing enrichment
        self.cursor.execute("""
            SELECT t.id, t.source_address_id, t.destination_address_id,
                   sa.address as source_address, da.address as dest_address
            FROM tx_transfer t
            LEFT JOIN tx_address sa ON sa.id = t.source_address_id
            LEFT JOIN tx_address da ON da.id = t.destination_address_id
            WHERE t.transfer_type = 'ACTIVITY_SPL_TRANSFER'
              AND (t.source_owner_address_id IS NULL OR t.destination_owner_address_id IS NULL)
              AND (t.source_address_id IS NOT NULL OR t.destination_address_id IS NOT NULL)
            LIMIT %s
        """, (limit,))
        transfers = self.cursor.fetchall()

        if not transfers:
            print("  No transfers needing enrichment")
            return stats

        print(f"  Found {len(transfers)} transfers to enrich")
        print(f"  Dry run: {dry_run}")

        session = requests.Session()
        owner_cache = {}  # Cache lookups to avoid duplicate API calls

        for i, row in enumerate(transfers):
            if (i + 1) % 50 == 0:
                print(f"    Progress: {i + 1}/{len(transfers)} "
                      f"(updated: {stats['updated']}, errors: {stats['errors']})")

            transfer_id = row['id']
            source_addr = row['source_address']
            dest_addr = row['dest_address']

            updates = {}

            # Lookup source owner if missing
            if source_addr and row['source_address_id']:
                if source_addr in owner_cache:
                    owner = owner_cache[source_addr]
                else:
                    owner = self._fetch_token_account_owner(session, source_addr)
                    owner_cache[source_addr] = owner
                    stats['fetched'] += 1

                if owner:
                    owner_id = self._ensure_address(owner)
                    if owner_id:
                        updates['source_owner_address_id'] = owner_id

            # Lookup dest owner if missing
            if dest_addr and row['destination_address_id']:
                if dest_addr in owner_cache:
                    owner = owner_cache[dest_addr]
                else:
                    owner = self._fetch_token_account_owner(session, dest_addr)
                    owner_cache[dest_addr] = owner
                    stats['fetched'] += 1

                if owner:
                    owner_id = self._ensure_address(owner)
                    if owner_id:
                        updates['destination_owner_address_id'] = owner_id

            # Apply updates
            if updates and not dry_run:
                set_clauses = ', '.join(f"{k} = %s" for k in updates.keys())
                values = list(updates.values()) + [transfer_id]
                self.cursor.execute(
                    f"UPDATE tx_transfer SET {set_clauses} WHERE id = %s",
                    values
                )
                self.db_conn.commit()
                stats['updated'] += 1
            elif updates:
                stats['updated'] += 1  # Would update in dry run

            time.sleep(SOLSCAN_DELAY)

        print(f"\n  Summary:")
        print(f"    API calls: {stats['fetched']:,}")
        print(f"    Updated: {stats['updated']:,}")
        print(f"    Errors: {stats['errors']:,}")

        return stats

    def _fetch_token_account_owner(self, session, token_account: str) -> Optional[str]:
        """Fetch the owner of a token account via Solscan API."""
        url = f"{SOLSCAN_API}/account/{token_account}"
        headers = {"token": SOLSCAN_TOKEN}

        try:
            response = session.get(url, headers=headers, timeout=30)
            if response.status_code >= 400 and response.status_code < 500:
                return None
            response.raise_for_status()
            result = response.json()

            if result.get('success') and result.get('data'):
                return result['data'].get('owner')
            return None
        except Exception:
            return None

    def _ensure_address(self, address: str) -> Optional[int]:
        """Ensure address exists in tx_address and return its ID."""
        self.cursor.execute("SELECT id FROM tx_address WHERE address = %s", (address,))
        row = self.cursor.fetchone()
        if row:
            return row['id']

        self.cursor.execute(
            "INSERT INTO tx_address (address, address_type) VALUES (%s, 'unknown')",
            (address,)
        )
        self.db_conn.commit()
        return self.cursor.lastrowid

    def fix_enrich_missing_swap_fields(self, limit: int = 1000, dry_run: bool = False) -> Dict[str, int]:
        """
        Use Solscan API to fetch missing fields for swaps.
        """
        print("\n=== FIX: Enriching missing swap fields via Solscan ===")

        if not HAS_REQUESTS:
            print("  ERROR: requests library not installed")
            return {'fetched': 0, 'updated': 0, 'errors': 0}

        if not SOLSCAN_TOKEN:
            print("  ERROR: SOLSCAN_TOKEN not configured")
            return {'fetched': 0, 'updated': 0, 'errors': 0}

        stats = {'fetched': 0, 'updated': 0, 'errors': 0}

        # Get swaps needing enrichment
        self.cursor.execute("""
            SELECT s.id, s.tx_id, tx.signature
            FROM tx_swap s
            JOIN tx ON tx.id = s.tx_id
            WHERE (s.account_address_id IS NULL OR s.token_1_id IS NULL OR s.token_2_id IS NULL)
            LIMIT %s
        """, (limit,))
        swaps = self.cursor.fetchall()

        if not swaps:
            print("  No swaps needing enrichment")
            return stats

        print(f"  Found {len(swaps)} swaps to enrich")
        print(f"  Dry run: {dry_run}")
        print("  Note: Swap enrichment requires re-parsing transaction - complex operation")
        print("  Consider re-ingesting these transactions instead")

        # For now, just report - swap enrichment is complex
        return stats

    def heal(self, dry_run: bool = False) -> Dict[str, int]:
        """
        Automatically fix recoverable data issues using existing data.
        Only performs safe fixes where we have the data to support it.
        """
        print("\n" + "=" * 60)
        print("HEAL: Auto-fixing recoverable issues")
        print("=" * 60)
        if dry_run:
            print("  [DRY RUN MODE - no changes will be made]")

        healed = {
            'duplicates_removed': 0,
            'burns_reset': 0,
            'mints_reset': 0,
            'create_accounts_reset': 0,
            'fees_backfilled': 0,
            'guide_edges_created': 0,
        }

        # 1. Remove duplicate tx_guide rows
        print("\n  [1/5] Checking for duplicate tx_guide rows...")
        dup_count = self.run_scalar("""
            SELECT COUNT(*) FROM (
                SELECT tx_id, from_address_id, to_address_id, token_id, amount, edge_type_id
                FROM tx_guide
                GROUP BY tx_id, from_address_id, to_address_id, token_id, amount, edge_type_id
                HAVING COUNT(*) > 1
            ) d
        """)
        if dup_count > 0:
            if not dry_run:
                self.cursor.execute("""
                    DELETE g1 FROM tx_guide g1
                    INNER JOIN tx_guide g2
                      ON g1.tx_id = g2.tx_id
                      AND g1.from_address_id = g2.from_address_id
                      AND g1.to_address_id = g2.to_address_id
                      AND COALESCE(g1.token_id, 0) = COALESCE(g2.token_id, 0)
                      AND COALESCE(g1.amount, 0) = COALESCE(g2.amount, 0)
                      AND g1.edge_type_id = g2.edge_type_id
                      AND g1.id > g2.id
                """)
                healed['duplicates_removed'] = self.cursor.rowcount
                self.db_conn.commit()
            print(f"       {'Would remove' if dry_run else 'Removed'} {dup_count} duplicate groups")
        else:
            print("       No duplicates found")

        # 2. Reset burns without edges for reprocessing
        print("\n  [2/5] Checking burns without guide edges...")
        burns_missing = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            JOIN tx_activity a ON a.id = t.activity_id
            LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            WHERE t.transfer_type = 'ACTIVITY_SPL_BURN'
              AND g.id IS NULL
              AND t.source_owner_address_id IS NOT NULL
              AND a.guide_loaded = 1
        """)
        if burns_missing > 0:
            if not dry_run:
                self.cursor.execute("""
                    UPDATE tx_activity a
                    JOIN tx_transfer t ON t.activity_id = a.id
                    LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
                    SET a.guide_loaded = 0
                    WHERE t.transfer_type = 'ACTIVITY_SPL_BURN'
                      AND g.id IS NULL
                      AND t.source_owner_address_id IS NOT NULL
                      AND a.guide_loaded = 1
                """)
                healed['burns_reset'] = self.cursor.rowcount
                self.db_conn.commit()
            print(f"       {'Would reset' if dry_run else 'Reset'} {burns_missing} burns for reprocessing")
        else:
            print("       All burns have edges")

        # 3. Reset mints without edges for reprocessing
        print("\n  [3/5] Checking mints without guide edges...")
        mints_missing = self.run_scalar("""
            SELECT COUNT(*) FROM tx_transfer t
            JOIN tx_activity a ON a.id = t.activity_id
            LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
            WHERE t.transfer_type = 'ACTIVITY_SPL_MINT'
              AND g.id IS NULL
              AND t.destination_owner_address_id IS NOT NULL
              AND a.guide_loaded = 1
        """)
        if mints_missing > 0:
            if not dry_run:
                self.cursor.execute("""
                    UPDATE tx_activity a
                    JOIN tx_transfer t ON t.activity_id = a.id
                    LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
                    SET a.guide_loaded = 0
                    WHERE t.transfer_type = 'ACTIVITY_SPL_MINT'
                      AND g.id IS NULL
                      AND t.destination_owner_address_id IS NOT NULL
                      AND a.guide_loaded = 1
                """)
                healed['mints_reset'] = self.cursor.rowcount
                self.db_conn.commit()
            print(f"       {'Would reset' if dry_run else 'Reset'} {mints_missing} mints for reprocessing")
        else:
            print("       All mints have edges")

        # 4. Backfill missing fee data from tx table
        print("\n  [4/5] Checking for missing fee data...")
        fees_missing = self.run_scalar("""
            SELECT COUNT(*) FROM tx_guide g
            JOIN tx t ON t.id = g.tx_id
            WHERE g.fee IS NULL AND t.fee IS NOT NULL
        """)
        if fees_missing > 0:
            if not dry_run:
                self.cursor.execute("""
                    UPDATE tx_guide g
                    JOIN tx t ON t.id = g.tx_id
                    SET g.fee = t.fee, g.priority_fee = t.priority_fee
                    WHERE g.fee IS NULL AND t.fee IS NOT NULL
                """)
                healed['fees_backfilled'] = self.cursor.rowcount
                self.db_conn.commit()
            print(f"       {'Would backfill' if dry_run else 'Backfilled'} {fees_missing} fee records")
        else:
            print("       All edges have fee data")

        # 5. Process any pending activities
        print("\n  [5/5] Processing pending activities...")
        pending = self.run_scalar("SELECT COUNT(*) FROM tx_activity WHERE guide_loaded = 0")
        if pending > 0 and not dry_run:
            total_edges = 0
            batches = 0
            while batches < 100:  # Max 100 batches to avoid infinite loop
                self.cursor.execute("CALL sp_tx_guide_batch(5000, @g, @f, @p)")
                self.cursor.execute("SELECT @g")
                result = self.cursor.fetchone()
                edges = list(result.values())[0] if result else 0
                if edges == 0:
                    break
                total_edges += edges
                batches += 1
                self.db_conn.commit()
            healed['guide_edges_created'] = total_edges
            print(f"       Created {total_edges} guide edges in {batches} batches")
        elif pending > 0:
            print(f"       Would process {pending} pending activities")
        else:
            print("       No pending activities")

        # Summary
        print("\n  " + "-" * 50)
        print("  HEAL SUMMARY:")
        total_fixed = sum(healed.values())
        if total_fixed > 0 or dry_run:
            for key, val in healed.items():
                if val > 0 or (dry_run and key in ['duplicates_removed', 'burns_reset', 'mints_reset']):
                    print(f"    - {key.replace('_', ' ').title()}: {val}")
        else:
            print("    No issues to heal")

        return healed

    def print_summary(self):
        """Print summary of all issues"""
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)

        if not self.issues:
            print("  All checks passed - no issues found")
        else:
            print(f"  Found {len(self.issues)} issue(s):")
            for issue in self.issues:
                print(f"    - {issue}")

    def run_all(self, fix: bool = False, diagnose: bool = False,
                 backfill: bool = False, reset: bool = False,
                 link_orphans: bool = False, enrich_missing: bool = False,
                 purge_empty_orphans: bool = False, heal: bool = False,
                 limit: int = 0, batch_size: int = 10000, dry_run: bool = False):
        """Run all checks and optionally fix issues"""
        self.check_row_counts()
        self.check_source_coverage()
        self.check_orphans()
        self.check_referential_integrity()
        groups, extra = self.check_duplicates()
        self.check_stuck_records()
        has_index = self.check_unique_index()

        # New checks from session discoveries
        self.check_synthetic_addresses()
        self.check_burn_mint_coverage()
        self.check_fee_coverage()
        self.check_activity_type_mapping()
        self.check_activities_without_guide()

        # Detailed diagnosis of missing coverage
        if diagnose:
            self.diagnose_missing_coverage()

        # Heal: auto-fix recoverable issues
        if heal:
            self.heal(dry_run=dry_run)

        # Fix operations
        if fix:
            if groups > 0:
                self.fix_duplicates()
            if not has_index:
                self.fix_add_unique_index()

        if link_orphans:
            self.fix_link_orphaned_transfers()

        if purge_empty_orphans:
            self.purge_empty_orphan_transfers(dry_run=dry_run)

        if reset:
            self.fix_reset_unprocessed()

        if backfill:
            self.fix_backfill_transfers(batch_size=batch_size, limit=limit)
            self.fix_backfill_swaps(batch_size=batch_size // 2, limit=limit)

        if enrich_missing:
            enrich_limit = limit if limit > 0 else 1000
            self.fix_enrich_missing_owners(limit=enrich_limit, dry_run=dry_run)
            self.fix_enrich_missing_swap_fields(limit=enrich_limit, dry_run=dry_run)

        self.print_summary()


def main():
    parser = argparse.ArgumentParser(
        description='Guide Integrity Check',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python guide-integrity-check.py                    # Run all checks
  python guide-integrity-check.py --heal            # Auto-fix all recoverable issues
  python guide-integrity-check.py --heal --dry-run  # Preview what would be fixed
  python guide-integrity-check.py --diagnose        # Detailed diagnosis of missing coverage
  python guide-integrity-check.py --fix             # Fix duplicates and add unique index
  python guide-integrity-check.py --backfill        # Backfill missing transfer/swap edges
  python guide-integrity-check.py --reset           # Reset stuck activities for reprocessing
  python guide-integrity-check.py --link-orphans    # Link orphaned transfers to activities
  python guide-integrity-check.py --purge-empty-orphans  # Delete empty orphan transfers
  python guide-integrity-check.py --enrich-missing  # Fetch missing owners via Solscan API
        """
    )
    parser.add_argument('--heal', action='store_true',
                        help='Auto-fix all recoverable issues (duplicates, burns, mints, fees)')
    parser.add_argument('--fix', action='store_true',
                        help='Fix duplicates and add unique index')
    parser.add_argument('--diagnose', action='store_true',
                        help='Detailed diagnosis of missing source coverage')
    parser.add_argument('--backfill', action='store_true',
                        help='Backfill missing tx_transfer and tx_swap edges to tx_guide')
    parser.add_argument('--reset', action='store_true',
                        help='Reset stuck activities (guide_loaded=1 but no edge) for reprocessing')
    parser.add_argument('--link-orphans', action='store_true',
                        help='Link orphaned transfers (no activity_id) to activities by tx_id')
    parser.add_argument('--enrich-missing', action='store_true',
                        help='Fetch missing owner/token data via Solscan API')
    parser.add_argument('--purge-empty-orphans', action='store_true',
                        help='Delete empty orphan transfer records (no data, just tx_id)')
    parser.add_argument('--limit', type=int, default=0,
                        help='Limit records to process (0 = unlimited for backfill, 1000 for enrich)')
    parser.add_argument('--batch-size', type=int, default=10000,
                        help='Batch size for backfill operations (default: 10000)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview changes without updating database (for --enrich-missing)')
    parser.add_argument('--summary', action='store_true', help='Summary only (faster)')
    parser.add_argument('--db-host', default='localhost', help='MySQL host')
    parser.add_argument('--db-port', type=int, default=3396, help='MySQL port')
    parser.add_argument('--db-user', default='root', help='MySQL user')
    parser.add_argument('--db-pass', default='rootpassword', help='MySQL password')
    parser.add_argument('--db-name', default='t16o_db', help='MySQL database')

    args = parser.parse_args()

    if not HAS_MYSQL:
        print("Error: mysql-connector-python not installed")
        return 1

    print("Guide Integrity Check")
    print("=" * 60)
    print(f"Database: {args.db_host}:{args.db_port}/{args.db_name}")

    modes = []
    if args.heal:
        modes.append("HEAL")
    if args.fix:
        modes.append("FIX")
    if args.link_orphans:
        modes.append("LINK-ORPHANS")
    if args.purge_empty_orphans:
        modes.append("PURGE-EMPTY")
    if args.enrich_missing:
        modes.append("ENRICH")
    if args.backfill:
        modes.append("BACKFILL")
    if args.reset:
        modes.append("RESET")

    if modes:
        print(f"Mode: CHECK + {' + '.join(modes)}")
    else:
        print("Mode: CHECK ONLY")

    db_conn = mysql.connector.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_pass,
        database=args.db_name
    )

    checker = IntegrityChecker(db_conn)

    try:
        checker.run_all(
            fix=args.fix,
            diagnose=args.diagnose,
            backfill=args.backfill,
            reset=args.reset,
            link_orphans=args.link_orphans,
            enrich_missing=args.enrich_missing,
            purge_empty_orphans=args.purge_empty_orphans,
            heal=args.heal,
            limit=args.limit,
            batch_size=args.batch_size,
            dry_run=args.dry_run
        )
    finally:
        db_conn.close()

    return 0 if not checker.issues else 1


if __name__ == '__main__':
    sys.exit(main())
