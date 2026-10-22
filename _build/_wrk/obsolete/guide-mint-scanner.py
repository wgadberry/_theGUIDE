#!/usr/bin/env python3
"""
Guide Mint Scanner - Scan tx_guide graph for addresses missing funder info

Scans from a mint address to find addresses that need funding wallet lookup.

Usage:
    python guide-mint-scanner.py <mint_address> [--depth N] [--output filename.txt]

Examples:
    python guide-mint-scanner.py GbLeL5XcQAhZALcFA8RMVnpuCLTt5Bh6tKNZDPdyrGz4 --depth 2
    python guide-mint-scanner.py GbLeL5XcQAhZALcFA8RMVnpuCLTt5Bh6tKNZDPdyrGz4 --depth 3 --output scan-funders.txt
"""

import argparse
import sys
import mysql.connector
from mysql.connector import Error

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 3396,
    'user': 'root',
    'password': 'rootpassword',
    'database': 't16o_db'
}


def get_db_connection():
    """Create database connection."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)


def get_mint_info(cursor, mint_address):
    """Get mint address_id and token_id."""
    cursor.execute("""
        SELECT a.id as address_id, t.id as token_id
        FROM tx_address a
        LEFT JOIN tx_token t ON t.mint_address_id = a.id
        WHERE a.address = %s
    """, (mint_address,))

    row = cursor.fetchone()
    if not row:
        print(f"Mint address not found: {mint_address}")
        return None, None

    return row[0], row[1]


def get_addresses_at_depth(cursor, token_id, current_addresses, visited, depth_num):
    """
    Get connected addresses at current depth level.
    Returns set of new address_ids found.
    """
    if not current_addresses:
        return set()

    # Find addresses connected via tx_guide edges for this token
    # Look for edges where current addresses are either sender or receiver
    placeholders = ','.join(['%s'] * len(current_addresses))

    query = f"""
        SELECT DISTINCT
            CASE
                WHEN from_address_id IN ({placeholders}) THEN to_address_id
                ELSE from_address_id
            END as connected_address_id
        FROM tx_guide
        WHERE token_id = %s
          AND (from_address_id IN ({placeholders}) OR to_address_id IN ({placeholders}))
    """

    params = list(current_addresses) + [token_id] + list(current_addresses) + list(current_addresses)
    cursor.execute(query, params)

    new_addresses = set()
    for row in cursor.fetchall():
        addr_id = row[0]
        if addr_id not in visited:
            new_addresses.add(addr_id)
            visited.add(addr_id)

    print(f"  Depth {depth_num}: Found {len(new_addresses)} new addresses")
    return new_addresses


def get_initial_addresses(cursor, token_id):
    """Get all addresses involved in transfers of this token."""
    query = """
        SELECT DISTINCT address_id
        FROM (
            SELECT from_address_id as address_id FROM tx_guide WHERE token_id = %s
            UNION
            SELECT to_address_id as address_id FROM tx_guide WHERE token_id = %s
        ) combined
    """
    cursor.execute(query, (token_id, token_id))

    addresses = set(row[0] for row in cursor.fetchall())
    print(f"  Depth 1: Found {len(addresses)} addresses directly involved with token")
    return addresses


def filter_missing_funders(cursor, address_ids):
    """Filter to only addresses missing funder info, ordered non-wallets first."""
    if not address_ids:
        return [], {}

    placeholders = ','.join(['%s'] * len(address_ids))

    # Order by: non-wallets first (pool, unknown, vault, ata), then wallets
    # This ensures funding worker processes higher-value targets first
    query = f"""
        SELECT address, address_type
        FROM tx_address
        WHERE id IN ({placeholders})
          AND funded_by_address_id IS NULL
          AND address_type IN ('wallet', 'unknown', 'pool', 'vault', 'ata')
          AND address != '11111111111111111111111111111111'
        ORDER BY
            CASE address_type
                WHEN 'pool' THEN 1
                WHEN 'vault' THEN 2
                WHEN 'unknown' THEN 3
                WHEN 'ata' THEN 4
                WHEN 'wallet' THEN 5
                ELSE 6
            END,
            address
    """

    cursor.execute(query, list(address_ids))
    results = cursor.fetchall()

    # Return addresses and a type breakdown
    addresses = [row[0] for row in results]
    type_counts = {}
    for row in results:
        addr_type = row[1]
        type_counts[addr_type] = type_counts.get(addr_type, 0) + 1

    return addresses, type_counts


def scan_mint_connections(mint_address, depth, output_file=None):
    """
    Main function to scan connections from a mint address.

    Args:
        mint_address: The token mint address to start from
        depth: How many hops to traverse in tx_guide
        output_file: Optional filename to write results
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        print(f"\nScanning mint: {mint_address}")
        print(f"Depth: {depth}")
        print("-" * 60)

        # Get mint and token IDs
        mint_address_id, token_id = get_mint_info(cursor, mint_address)

        if token_id is None:
            print("Warning: No token record found for this mint (may not have any trades)")
            print("Searching by address only...")

            # Fallback: search tx_guide for any edges involving this address
            cursor.execute("""
                SELECT COUNT(*) FROM tx_guide
                WHERE from_address_id = %s OR to_address_id = %s
            """, (mint_address_id, mint_address_id))

            edge_count = cursor.fetchone()[0]
            if edge_count == 0:
                print("No tx_guide edges found for this address. Nothing to scan.")
                return

            print(f"Found {edge_count} edges involving this address directly")

        # Track visited addresses to avoid cycles
        visited = set()
        visited.add(mint_address_id)

        # Depth 1: Get addresses directly involved with this token
        if token_id:
            current_level = get_initial_addresses(cursor, token_id)
        else:
            # Fallback: get addresses connected to the mint address directly
            cursor.execute("""
                SELECT DISTINCT address_id
                FROM (
                    SELECT from_address_id as address_id FROM tx_guide
                    WHERE to_address_id = %s
                    UNION
                    SELECT to_address_id as address_id FROM tx_guide
                    WHERE from_address_id = %s
                ) combined
            """, (mint_address_id, mint_address_id))
            current_level = set(row[0] for row in cursor.fetchall())
            print(f"  Depth 1: Found {len(current_level)} addresses connected to mint")

        visited.update(current_level)
        all_addresses = set(current_level)

        # Traverse additional depths
        for d in range(2, depth + 1):
            if not current_level:
                print(f"  Depth {d}: No more addresses to expand")
                break

            # At deeper levels, we look for ANY tx_guide connections (not just same token)
            placeholders = ','.join(['%s'] * len(current_level))

            query = f"""
                SELECT DISTINCT
                    CASE
                        WHEN from_address_id IN ({placeholders}) THEN to_address_id
                        ELSE from_address_id
                    END as connected_address_id
                FROM tx_guide
                WHERE from_address_id IN ({placeholders}) OR to_address_id IN ({placeholders})
            """
            params = list(current_level) * 3
            cursor.execute(query, params)

            new_level = set()
            for row in cursor.fetchall():
                addr_id = row[0]
                if addr_id not in visited:
                    new_level.add(addr_id)
                    visited.add(addr_id)

            print(f"  Depth {d}: Found {len(new_level)} new addresses")
            all_addresses.update(new_level)
            current_level = new_level

        print("-" * 60)
        print(f"Total unique addresses found: {len(all_addresses)}")

        # Filter to addresses missing funder info (ordered: non-wallets first)
        missing_funders, type_counts = filter_missing_funders(cursor, all_addresses)
        print(f"Addresses missing funder info: {len(missing_funders)}")

        # Show type breakdown in processing order
        if type_counts:
            print("\nProcessing order (non-wallets first):")
            order = ['pool', 'vault', 'unknown', 'ata', 'wallet']
            for addr_type in order:
                if addr_type in type_counts:
                    print(f"  {addr_type}: {type_counts[addr_type]}")

        if missing_funders:
            if output_file:
                with open(output_file, 'w') as f:
                    for addr in missing_funders:
                        f.write(f"{addr}\n")
                print(f"\nWritten to: {output_file}")
            else:
                # Print to stdout if no file specified
                print("\nAddresses missing funders (first 20):")
                for addr in missing_funders[:20]:
                    print(f"  {addr}")
                if len(missing_funders) > 20:
                    print(f"  ... and {len(missing_funders) - 20} more")
                print("\nUse --output <filename> to save all addresses to file")
        else:
            print("\nNo addresses missing funder info found!")

        # Summary stats
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)

        # Get breakdown by address type
        if all_addresses:
            placeholders = ','.join(['%s'] * len(all_addresses))
            cursor.execute(f"""
                SELECT address_type, COUNT(*) as cnt,
                       SUM(CASE WHEN funded_by_address_id IS NULL THEN 1 ELSE 0 END) as missing_funder
                FROM tx_address
                WHERE id IN ({placeholders})
                GROUP BY address_type
                ORDER BY cnt DESC
            """, list(all_addresses))

            print(f"{'Type':<12} {'Total':>10} {'Missing Funder':>15}")
            print("-" * 40)
            for row in cursor.fetchall():
                print(f"{row[0]:<12} {row[1]:>10} {row[2]:>15}")

    finally:
        cursor.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='Scan tx_guide connections from a mint to find addresses missing funder info',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python mint-funder-scanner.py GbLeL5XcQA... --depth 2
  python mint-funder-scanner.py GbLeL5XcQA... --depth 3 --output funders-needed.txt

Depth explanation:
  1 = Only addresses that directly traded this token
  2 = Above + addresses those traders transacted with
  3 = Above + one more hop out
        """
    )

    parser.add_argument('mint_address', help='Token mint address to start from')
    parser.add_argument('--depth', '-d', type=int, default=2,
                        help='Traversal depth (default: 2)')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='Output filename for addresses needing RPC scan')

    args = parser.parse_args()

    if args.depth < 1:
        print("Error: depth must be at least 1")
        sys.exit(1)

    if args.depth > 5:
        print("Warning: depth > 5 may result in very large result sets")
        response = input("Continue? (y/n): ")
        if response.lower() != 'y':
            sys.exit(0)

    scan_mint_connections(args.mint_address, args.depth, args.output)


if __name__ == '__main__':
    main()
