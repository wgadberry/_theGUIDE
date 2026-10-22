#!/usr/bin/env python3
"""
theGuide Environment Setup - Step 2: Create Database Schema

Creates all database objects (tables, functions, procedures, views) from the
_db directory scripts. Executes in dependency order.

Usage:
    python 02-create-schema.py                    # Create all objects
    python 02-create-schema.py --drop-first       # Drop database and recreate
    python 02-create-schema.py --tables-only      # Create tables only
    python 02-create-schema.py --seed             # Load seed/lookup data only
    python 02-create-schema.py --with-seed        # Create schema + load seed data
    python 02-create-schema.py --dry-run          # Show what would be executed
    python 02-create-schema.py --verify           # Verify existing schema
"""

import argparse
import os
import sys
import re
from typing import List, Tuple, Optional

try:
    import mysql.connector
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

# =============================================================================
# Configuration
# =============================================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 3396,
    "user": "root",
    "password": "rootpassword",
    "database": "t16o_db",
}

# Script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(SCRIPT_DIR, "_db")

# Object execution order (dependency-aware)
TABLE_ORDER = [
    # Base tables (no foreign keys)
    "config",
    "tx_address",
    "tx_guide_type",
    "tx_guide_source",
    # Tables with single FK dependency
    "tx_token",
    "tx_program",
    "tx_pool",
    # Main transaction table
    "tx",
    # Transaction detail tables
    "tx_account",
    "tx_activity",
    "tx_funding_edge",
    "tx_guide",
    "tx_instruction",
    "tx_party",
    "tx_signer",
    "tx_sol_balance_change",
    "tx_swap",
    "tx_token_balance_change",
    "tx_token_holder",
    "tx_token_market",
    "tx_token_participant",
    "tx_token_price",
    "tx_transfer",
]

FUNCTION_ORDER = [
    "fn_tx_ensure_address",
    "fn_tx_ensure_token",
    "fn_tx_ensure_program",
    "fn_tx_ensure_pool",
    "fn_tx_extract_addresses",
    "fn_tx_get_token_name",
    "fn_get_guide_by_wallet",
    "fn_get_guide_by_token",
]

PROCEDURE_ORDER = [
    "sp_config_get",
    "sp_config_get_by_type",
    "sp_config_get_changes",
    "sp_config_set",
    "sp_detect_chart_clipping",
    "sp_tx_backfill_funding",
    "sp_tx_clear_tables",
    "sp_tx_detect_chart_clipping",
    "sp_tx_funding_chain",
    "sp_tx_hound_indexes",
    "sp_tx_prime",
    "sp_tx_prime_batch",
    "sp_tx_release_hound",
    "sp_tx_shred_batch",
]

VIEW_ORDER = [
    "vw_tx_token_info",
    "vw_tx_funding_tree",
    "vw_tx_funding_chain",
    "vw_tx_common_funders",
    "vw_tx_flow_concentration",
    "vw_tx_address_risk_score",
    "vw_tx_high_freq_pairs",
    "vw_tx_high_freq_pairs2",
    "vw_tx_high_freq_pairs3",
    "vw_tx_rapid_fire",
    "vw_tx_sybil_clusters",
    "vw_tx_wash_roundtrip",
    "vw_tx_wash_triangle",
]

# Seed data files (order matters for dependencies)
SEED_ORDER = [
    ("seed_tx_guide_type.sql", "tx_guide_type", "Transaction type classifications"),
    ("seed_tx_guide_source.sql", "tx_guide_source", "Edge source types"),
    ("seed_config.sql", "config", "Configuration defaults"),
    ("seed_tx_program.sql", "tx_program", "Known Solana programs"),
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


def read_sql_file(filepath: str) -> Optional[str]:
    """Read SQL file contents"""
    if not os.path.exists(filepath):
        return None
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()


def parse_sql_statements(sql_content: str, object_type: str) -> List[str]:
    """
    Parse SQL file into executable statements.
    Handles DELIMITER changes for functions/procedures.
    """
    statements = []

    if object_type in ('function', 'procedure'):
        # For functions/procedures, extract the CREATE statement
        # Remove DELIMITER lines and ;; terminators
        content = sql_content
        content = re.sub(r'DELIMITER\s+;;\s*\n?', '', content)
        content = re.sub(r'DELIMITER\s+;\s*\n?', '', content)
        content = re.sub(r';;\s*$', '', content, flags=re.MULTILINE)

        # Find DROP and CREATE statements
        drop_match = re.search(r'DROP\s+(FUNCTION|PROCEDURE)\s+IF\s+EXISTS\s+[`\w]+', content, re.IGNORECASE)
        if drop_match:
            statements.append(drop_match.group(0))

        # Find CREATE FUNCTION/PROCEDURE
        create_match = re.search(r'CREATE\s+(DEFINER\s*=\s*[^\s]+\s+)?(FUNCTION|PROCEDURE).*', content, re.IGNORECASE | re.DOTALL)
        if create_match:
            create_stmt = create_match.group(0).strip()
            # Remove trailing semicolons
            while create_stmt.endswith(';'):
                create_stmt = create_stmt[:-1]
            statements.append(create_stmt)
    else:
        # For tables and views, split on semicolons
        for stmt in sql_content.split(';'):
            stmt = stmt.strip()
            if stmt and not stmt.startswith('--'):
                statements.append(stmt)

    return statements


def execute_sql_file(cursor, conn, filepath: str, object_type: str, dry_run: bool = False) -> Tuple[bool, str]:
    """Execute SQL file and return success status"""
    sql = read_sql_file(filepath)
    if sql is None:
        return False, "file not found"

    statements = parse_sql_statements(sql, object_type)

    if dry_run:
        for stmt in statements:
            preview = stmt[:80].replace('\n', ' ')
            print(f"    [DRY] {preview}...")
        return True, "dry run"

    try:
        for stmt in statements:
            if stmt.strip():
                cursor.execute(stmt)
        conn.commit()
        return True, "created"
    except mysql.connector.Error as e:
        conn.rollback()
        return False, str(e)[:60]


# =============================================================================
# Main Build Functions
# =============================================================================

def drop_database(cursor, conn, db_name: str) -> bool:
    """Drop and recreate database"""
    try:
        cursor.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
        cursor.execute(f"CREATE DATABASE `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
        cursor.execute(f"USE `{db_name}`")
        conn.commit()
        return True
    except mysql.connector.Error as e:
        print(f"  [X] Error: {e}")
        return False


def build_tables(cursor, conn, dry_run: bool = False) -> Tuple[int, int]:
    """Build all tables in dependency order"""
    print_header("Tables")
    success = 0
    failed = 0

    for table in TABLE_ORDER:
        filepath = os.path.join(DB_DIR, "tables", f"{table}.sql")
        ok, detail = execute_sql_file(cursor, conn, filepath, 'table', dry_run)
        print_status(table, ok, detail)
        if ok:
            success += 1
        else:
            failed += 1

    return success, failed


def build_functions(cursor, conn, dry_run: bool = False) -> Tuple[int, int]:
    """Build all functions"""
    print_header("Functions")
    success = 0
    failed = 0

    for func in FUNCTION_ORDER:
        filepath = os.path.join(DB_DIR, "functions", f"{func}.sql")
        ok, detail = execute_sql_file(cursor, conn, filepath, 'function', dry_run)
        print_status(func, ok, detail)
        if ok:
            success += 1
        else:
            failed += 1

    return success, failed


def build_procedures(cursor, conn, dry_run: bool = False) -> Tuple[int, int]:
    """Build all stored procedures"""
    print_header("Stored Procedures")
    success = 0
    failed = 0

    for proc in PROCEDURE_ORDER:
        filepath = os.path.join(DB_DIR, "procedures", f"{proc}.sql")
        ok, detail = execute_sql_file(cursor, conn, filepath, 'procedure', dry_run)
        print_status(proc, ok, detail)
        if ok:
            success += 1
        else:
            failed += 1

    return success, failed


def build_views(cursor, conn, dry_run: bool = False) -> Tuple[int, int]:
    """Build all views"""
    print_header("Views")
    success = 0
    failed = 0

    for view in VIEW_ORDER:
        filepath = os.path.join(DB_DIR, "views", f"{view}.sql")
        ok, detail = execute_sql_file(cursor, conn, filepath, 'view', dry_run)
        print_status(view, ok, detail)
        if ok:
            success += 1
        else:
            failed += 1

    return success, failed


def load_seed_data(cursor, conn, dry_run: bool = False) -> Tuple[int, int]:
    """Load seed/lookup data"""
    print_header("Seed Data")
    success = 0
    failed = 0

    data_dir = os.path.join(DB_DIR, "data")

    for filename, table_name, description in SEED_ORDER:
        filepath = os.path.join(data_dir, filename)

        if not os.path.exists(filepath):
            print_status(f"{table_name} - {description}", False, "file not found")
            failed += 1
            continue

        if dry_run:
            print_status(f"{table_name} - {description}", True, "dry run")
            success += 1
            continue

        try:
            sql = read_sql_file(filepath)
            # Execute each statement separately
            for stmt in sql.split(';'):
                stmt = stmt.strip()
                if stmt and not stmt.startswith('--'):
                    cursor.execute(stmt)
            conn.commit()

            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print_status(f"{table_name} - {description}", True, f"{count} rows")
            success += 1
        except mysql.connector.Error as e:
            conn.rollback()
            print_status(f"{table_name} - {description}", False, str(e)[:50])
            failed += 1

    return success, failed


def verify_seed_data(cursor):
    """Verify seed data is loaded"""
    print_header("Seed Data Verification")

    for _, table_name, description in SEED_ORDER:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print_status(f"{table_name}", count > 0, f"{count} rows")
        except:
            print_status(f"{table_name}", False, "error")


def verify_schema(cursor) -> Tuple[int, int, int, int]:
    """Verify existing schema objects"""
    print_header("Schema Verification")

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

    print(f"  Tables:     {tables:3d} (expected: {len(TABLE_ORDER)})")
    print(f"  Functions:  {functions:3d} (expected: {len(FUNCTION_ORDER)})")
    print(f"  Procedures: {procedures:3d} (expected: {len(PROCEDURE_ORDER)})")
    print(f"  Views:      {views:3d} (expected: {len(VIEW_ORDER)})")

    return tables, functions, procedures, views


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='theGuide Environment Setup - Create Database Schema'
    )
    parser.add_argument('--drop-first', action='store_true',
                        help='Drop database and recreate from scratch')
    parser.add_argument('--tables-only', action='store_true',
                        help='Create tables only (skip functions, procedures, views)')
    parser.add_argument('--seed', action='store_true',
                        help='Load seed/lookup data only (skip schema creation)')
    parser.add_argument('--with-seed', action='store_true',
                        help='Create schema and load seed data')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be executed without making changes')
    parser.add_argument('--verify', action='store_true',
                        help='Verify existing schema only')
    parser.add_argument('--db-host', default=DB_CONFIG['host'],
                        help='MySQL host')
    parser.add_argument('--db-port', type=int, default=DB_CONFIG['port'],
                        help='MySQL port')
    parser.add_argument('--db-user', default=DB_CONFIG['user'],
                        help='MySQL user')
    parser.add_argument('--db-pass', default=DB_CONFIG['password'],
                        help='MySQL password')
    parser.add_argument('--db-name', default=DB_CONFIG['database'],
                        help='MySQL database')

    args = parser.parse_args()

    if not HAS_MYSQL:
        print("Error: mysql-connector-python not installed")
        print("Run: pip install mysql-connector-python")
        return 1

    # Check _db directory exists
    if not os.path.exists(DB_DIR):
        print(f"Error: _db directory not found at {DB_DIR}")
        return 1

    print("\n" + "="*60)
    print("  theGuide Database Schema Builder")
    print("="*60)
    print(f"\nDatabase: {args.db_host}:{args.db_port}/{args.db_name}")
    print(f"SQL Dir:  {DB_DIR}")
    if args.dry_run:
        print("Mode:     DRY RUN")
    if args.drop_first:
        print("Mode:     DROP AND RECREATE")
    if args.seed:
        print("Mode:     SEED DATA ONLY")
    if args.with_seed:
        print("Mode:     SCHEMA + SEED DATA")

    # Connect to MySQL
    try:
        # Connect without database if dropping
        connect_config = {
            "host": args.db_host,
            "port": args.db_port,
            "user": args.db_user,
            "password": args.db_pass,
        }
        if not args.drop_first:
            connect_config["database"] = args.db_name

        conn = mysql.connector.connect(**connect_config)
        cursor = conn.cursor()
        print_status("MySQL connection", True)
    except mysql.connector.Error as e:
        print_status("MySQL connection", False, str(e))
        return 1

    # Stats
    total_success = 0
    total_failed = 0

    try:
        # Verify only?
        if args.verify:
            cursor.execute(f"USE `{args.db_name}`")
            verify_schema(cursor)
            verify_seed_data(cursor)
            return 0

        # Seed only mode?
        if args.seed:
            s, f = load_seed_data(cursor, conn, args.dry_run)
            total_success += s
            total_failed += f

            if not args.dry_run:
                verify_seed_data(cursor)

            print_header("Summary")
            print(f"  Success: {total_success}")
            print(f"  Failed:  {total_failed}")

            if total_failed == 0:
                print("\n  [OK] Seed data loaded successfully!")
            else:
                print(f"\n  [X] {total_failed} seed files failed to load")

            return 0 if total_failed == 0 else 1

        # Drop and recreate?
        if args.drop_first and not args.dry_run:
            print_header("Dropping Database")
            if drop_database(cursor, conn, args.db_name):
                print_status(f"Database {args.db_name}", True, "recreated")
            else:
                return 1
        elif args.drop_first and args.dry_run:
            print_header("Dropping Database")
            print(f"  [DRY] Would drop and recreate {args.db_name}")

        # Build tables
        s, f = build_tables(cursor, conn, args.dry_run)
        total_success += s
        total_failed += f

        if not args.tables_only:
            # Build functions
            s, f = build_functions(cursor, conn, args.dry_run)
            total_success += s
            total_failed += f

            # Build procedures
            s, f = build_procedures(cursor, conn, args.dry_run)
            total_success += s
            total_failed += f

            # Build views
            s, f = build_views(cursor, conn, args.dry_run)
            total_success += s
            total_failed += f

        # Load seed data if requested
        if args.with_seed:
            s, f = load_seed_data(cursor, conn, args.dry_run)
            total_success += s
            total_failed += f

        # Verify
        if not args.dry_run:
            verify_schema(cursor)
            if args.with_seed:
                verify_seed_data(cursor)

        # Summary
        print_header("Summary")
        print(f"  Success: {total_success}")
        print(f"  Failed:  {total_failed}")

        if total_failed == 0:
            print("\n  [OK] Database schema created successfully!")
        else:
            print(f"\n  [X] {total_failed} objects failed to create")

    finally:
        cursor.close()
        conn.close()

    print()
    return 0 if total_failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
