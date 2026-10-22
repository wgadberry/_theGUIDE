#!/usr/bin/env python3
"""
Guide Circular Flow - Detect circular fund flow patterns for a token

Detects where funding wallets later receive funds back from wallets they
funded (profit consolidation).

This catches the classic pump-and-dump pattern:
  1. Wallet A distributes SOL to many bot wallets
  2. Bots trade/dump tokens for profit
  3. Profits flow back to Wallet A

Uses pre-computed tx_funding_edge and tx_token_participant tables for fast queries.

Usage:
    python guide-circular-flow.py <mint_address>
    python circular-flow-detector.py <mint_address> --depth 3
    python circular-flow-detector.py <mint_address> --output results.json
"""

import argparse
import sys
import json
from datetime import datetime, timezone
from collections import defaultdict
from dataclasses import dataclass, asdict
from typing import Dict, List, Set, Tuple, Optional
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

# Analysis parameters
DEFAULT_FUNDING_DEPTH = 2
MIN_RETURN_AMOUNT_SOL = 0.001


@dataclass
class CircularFlowResult:
    """Results for a single funder showing circular flow."""
    funder_address: str
    wallets_funded: List[str]
    total_distributed: float
    total_returned: float
    return_flows: List[Dict]
    net_extraction: float
    circulation_ratio: float
    severity: str


def get_db_connection():
    """Create database connection."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)


def get_token_info(cursor, mint_address: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    """Get token ID and info from mint address."""
    cursor.execute("""
        SELECT t.id, a.id as mint_address_id, t.token_symbol
        FROM tx_address a
        LEFT JOIN tx_token t ON t.mint_address_id = a.id
        WHERE a.address = %s
    """, (mint_address,))

    row = cursor.fetchone()
    if not row:
        return None, None, None
    return row[0], row[1], row[2]


def get_wallets_for_token(cursor, token_id: int) -> Set[int]:
    """Get all wallet address_ids that participated in this token (from pre-computed table)."""
    cursor.execute("""
        SELECT address_id FROM tx_token_participant WHERE token_id = %s
    """, (token_id,))
    return {row[0] for row in cursor.fetchall()}


def get_funding_edges_for_wallets(cursor, wallet_ids: Set[int]) -> Dict[int, Dict[int, float]]:
    """
    Get funding relationships from tx_funding_edge.
    Returns: {funder_id: {funded_id: total_sol}}
    """
    if not wallet_ids:
        return {}

    wallet_list = list(wallet_ids)
    funder_to_funded = defaultdict(dict)

    batch_size = 1000
    for i in range(0, len(wallet_list), batch_size):
        batch = wallet_list[i:i+batch_size]
        placeholders = ','.join(['%s'] * len(batch))

        cursor.execute(f"""
            SELECT from_address_id, to_address_id, total_sol
            FROM tx_funding_edge
            WHERE to_address_id IN ({placeholders})
        """, batch)

        for row in cursor.fetchall():
            funder_id, funded_id, total_sol = row
            funder_to_funded[funder_id][funded_id] = float(total_sol) if total_sol else 0

    return dict(funder_to_funded)


def build_descendant_map(funder_to_funded: Dict[int, Dict[int, float]], max_depth: int = 2) -> Dict[int, Dict[int, int]]:
    """
    For each funder, build map of all descendants.
    Returns: {funder_id: {descendant_id: depth}}
    """
    funder_descendants = {}

    for funder_id, direct_funded in funder_to_funded.items():
        descendants = {}

        # Depth 1: directly funded
        current_level = set(direct_funded.keys())
        for w in current_level:
            descendants[w] = 1

        # Depth 2+: wallets funded by those wallets
        for d in range(2, max_depth + 1):
            next_level = set()
            for w in current_level:
                if w in funder_to_funded:
                    for funded in funder_to_funded[w]:
                        if funded not in descendants:
                            descendants[funded] = d
                            next_level.add(funded)
            current_level = next_level
            if not current_level:
                break

        if descendants:
            funder_descendants[funder_id] = descendants

    return funder_descendants


def get_return_flows(cursor, funder_descendants: Dict[int, Dict[int, int]],
                     min_return: float) -> Dict[int, List[Dict]]:
    """
    Get return flows using tx_funding_edge (flows from descendants back to funders).
    """
    all_funders = set(funder_descendants.keys())
    all_descendants = set()
    for descendants in funder_descendants.values():
        all_descendants.update(descendants.keys())

    if not all_funders or not all_descendants:
        return {}

    # Query edges where from=descendant and to=funder
    return_flows_by_funder = defaultdict(list)

    funder_list = list(all_funders)
    desc_list = list(all_descendants)

    batch_size = 1000
    for i in range(0, len(desc_list), batch_size):
        desc_batch = desc_list[i:i+batch_size]
        desc_placeholders = ','.join(['%s'] * len(desc_batch))
        funder_placeholders = ','.join(['%s'] * len(funder_list))

        cursor.execute(f"""
            SELECT e.from_address_id, e.to_address_id, e.total_sol, e.last_transfer_time,
                   fa.address as from_address
            FROM tx_funding_edge e
            JOIN tx_address fa ON fa.id = e.from_address_id
            WHERE e.from_address_id IN ({desc_placeholders})
              AND e.to_address_id IN ({funder_placeholders})
              AND e.total_sol >= %s
        """, desc_batch + funder_list + [min_return])

        for row in cursor.fetchall():
            from_id, to_id, total_sol, last_time, from_address = row

            # Verify from_id is actually a descendant of to_id
            if to_id in funder_descendants and from_id in funder_descendants[to_id]:
                depth = funder_descendants[to_id][from_id]
                return_flows_by_funder[to_id].append({
                    'from_id': from_id,
                    'from': from_address,
                    'amount': float(total_sol) if total_sol else 0,
                    'token': 'SOL',
                    'timestamp': last_time,
                    'path_length': depth
                })

    return dict(return_flows_by_funder)


def get_addresses_for_ids(cursor, address_ids: Set[int]) -> Dict[int, str]:
    """Get address strings for address IDs."""
    if not address_ids:
        return {}

    id_list = list(address_ids)
    id_to_address = {}

    batch_size = 1000
    for i in range(0, len(id_list), batch_size):
        batch = id_list[i:i+batch_size]
        placeholders = ','.join(['%s'] * len(batch))

        cursor.execute(f"""
            SELECT id, address FROM tx_address WHERE id IN ({placeholders})
        """, batch)

        for row in cursor.fetchall():
            id_to_address[row[0]] = row[1]

    return id_to_address


def analyze_circular_flows(cursor, mint_address: str, depth: int = DEFAULT_FUNDING_DEPTH,
                           min_return: float = MIN_RETURN_AMOUNT_SOL) -> List[CircularFlowResult]:
    """
    Main analysis function - detect circular fund flows for a token.
    Uses pre-computed tables for fast queries.
    """
    print(f"\n{'='*70}")
    print(f"CIRCULAR FLOW ANALYSIS (Optimized)")
    print(f"Token: {mint_address}")
    print(f"{'='*70}\n")

    # Step 1: Get token info
    token_id, mint_address_id, token_symbol = get_token_info(cursor, mint_address)
    if mint_address_id is None:
        print(f"ERROR: Mint address not found in database: {mint_address}")
        return []

    if token_id is None:
        print(f"ERROR: No token record found for this mint")
        return []

    print(f"Token: {token_symbol or 'Unknown'}")
    print(f"Token ID: {token_id}")

    # Step 2: Get wallets from tx_token_participant (instant)
    print("\n[1] Finding wallets involved with token...")
    wallet_ids = get_wallets_for_token(cursor, token_id)
    print(f"    Found {len(wallet_ids)} wallets (from tx_token_participant)")

    if not wallet_ids:
        print("    No wallets found - nothing to analyze")
        return []

    # Step 3: Get funding edges for these wallets (fast indexed query)
    print("\n[2] Querying funding relationships...")
    funder_to_funded = get_funding_edges_for_wallets(cursor, wallet_ids)
    print(f"    Found {len(funder_to_funded)} unique funders")

    if not funder_to_funded:
        print("    No funding relationships found")
        return []

    # Step 4: Build descendant map (in-memory)
    print(f"\n[3] Building funder networks (depth={depth})...")
    funder_descendants = build_descendant_map(funder_to_funded, max_depth=depth)
    print(f"    Built networks for {len(funder_descendants)} funders")

    # Step 5: Get return flows (fast indexed query)
    print("\n[4] Checking for return flows...")
    return_flows_by_funder = get_return_flows(cursor, funder_descendants, min_return)
    print(f"    Found return flows for {len(return_flows_by_funder)} funders")

    # Step 6: Build results
    print("\n[5] Analyzing results...")

    # Get address strings for funders and descendants
    all_ids = set(funder_descendants.keys())
    for descendants in funder_descendants.values():
        all_ids.update(descendants.keys())
    id_to_address = get_addresses_for_ids(cursor, all_ids)

    results = []

    for funder_id, return_flows in return_flows_by_funder.items():
        if not return_flows:
            continue

        funder_address = id_to_address.get(funder_id, str(funder_id))
        descendants = funder_descendants.get(funder_id, {})

        # Get wallet addresses
        wallets_funded = [id_to_address.get(w, str(w)) for w in descendants.keys()]

        # Calculate total distributed (depth=1 only)
        total_distributed = sum(
            funder_to_funded[funder_id].get(w, 0)
            for w, d in descendants.items() if d == 1
        )

        total_returned = sum(rf['amount'] for rf in return_flows)

        # Circulation ratio
        circulation_ratio = total_returned / total_distributed if total_distributed > 0 else 0

        # Severity
        if len(return_flows) >= 10 or circulation_ratio >= 0.8:
            severity = 'critical'
        elif len(return_flows) >= 5 or circulation_ratio >= 0.5:
            severity = 'high'
        elif len(return_flows) >= 2 or circulation_ratio >= 0.2:
            severity = 'medium'
        else:
            severity = 'low'

        results.append(CircularFlowResult(
            funder_address=funder_address,
            wallets_funded=wallets_funded,
            total_distributed=round(total_distributed, 4),
            total_returned=round(total_returned, 4),
            return_flows=return_flows,
            net_extraction=round(total_returned - total_distributed, 4),
            circulation_ratio=round(circulation_ratio, 2),
            severity=severity
        ))

    # Sort by severity and return amount
    severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
    results.sort(key=lambda x: (severity_order[x.severity], -x.total_returned))

    print(f"    Found {len(results)} funders with circular flows")

    return results


def print_results(results: List[CircularFlowResult]):
    """Print results in a readable format."""
    if not results:
        print("\n" + "="*70)
        print("NO CIRCULAR FLOWS DETECTED")
        print("="*70)
        print("No funders were found receiving funds back from wallets they funded.")
        return

    print("\n" + "="*70)
    print("CIRCULAR FLOW RESULTS")
    print("="*70)

    # Summary
    critical = [r for r in results if r.severity == 'critical']
    high = [r for r in results if r.severity == 'high']
    medium = [r for r in results if r.severity == 'medium']

    print(f"\nSummary:")
    print(f"  Total funders with return flows: {len(results)}")
    if critical:
        print(f"  CRITICAL: {len(critical)}")
    if high:
        print(f"  HIGH: {len(high)}")
    if medium:
        print(f"  MEDIUM: {len(medium)}")

    total_circulated = sum(r.total_returned for r in results)
    print(f"  Total funds circulated back: {total_circulated:.4f} SOL")

    # Details
    print("\n" + "-"*70)
    print("DETAILED FINDINGS")
    print("-"*70)

    for i, r in enumerate(results[:10], 1):
        severity_icon = {
            'critical': '[CRIT]',
            'high': '[HIGH]',
            'medium': '[MED]',
            'low': '[LOW]'
        }.get(r.severity, '[?]')

        print(f"\n{severity_icon} [{i}] FUNDER: {r.funder_address}")
        print(f"    Severity: {r.severity.upper()}")
        print(f"    Wallets funded: {len(r.wallets_funded)}")
        print(f"    Total distributed: {r.total_distributed:.4f} SOL")
        print(f"    Total returned: {r.total_returned:.4f} SOL")
        print(f"    Circulation ratio: {r.circulation_ratio:.0%}")
        print(f"    Return flows: {len(r.return_flows)}")

        if r.return_flows:
            print(f"    Top returns:")
            for rf in sorted(r.return_flows, key=lambda x: -x['amount'])[:5]:
                ts = datetime.fromtimestamp(rf['timestamp'], timezone.utc).strftime('%Y-%m-%d %H:%M') if rf['timestamp'] else 'Unknown'
                print(f"      - {rf['from'][:16]}... sent {rf['amount']:.4f} {rf['token']} ({ts})")

    if len(results) > 10:
        print(f"\n... and {len(results) - 10} more funders with return flows")


def save_results(results: List[CircularFlowResult], output_file: str):
    """Save results to JSON file."""
    output = {
        'analysis_time': datetime.now(timezone.utc).isoformat(),
        'total_funders_with_returns': len(results),
        'summary': {
            'critical': len([r for r in results if r.severity == 'critical']),
            'high': len([r for r in results if r.severity == 'high']),
            'medium': len([r for r in results if r.severity == 'medium']),
            'low': len([r for r in results if r.severity == 'low']),
        },
        'results': [asdict(r) for r in results]
    }

    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nResults saved to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Detect circular fund flows for a token (profit consolidation patterns)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python circular-flow-detector.py GbLeL5XcQA...
  python circular-flow-detector.py GbLeL5XcQA... --depth 3
  python circular-flow-detector.py GbLeL5XcQA... --output results.json

What this detects:
  Wallet A funds Wallets B, C, D...
  B, C, D trade tokens and dump
  Profits flow back to Wallet A

  This circular pattern indicates coordinated pump-and-dump operations.

Note: Requires tx_funding_edge and tx_token_participant tables.
      Run populate-funding-tables.py first to build these tables.
        """
    )

    parser.add_argument('mint_address', help='Token mint address to analyze')
    parser.add_argument('--depth', '-d', type=int, default=DEFAULT_FUNDING_DEPTH,
                        help=f'Funding depth to trace (default: {DEFAULT_FUNDING_DEPTH})')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='Output JSON file for results')
    parser.add_argument('--min-return', type=float, default=MIN_RETURN_AMOUNT_SOL,
                        help=f'Minimum return amount in SOL (default: {MIN_RETURN_AMOUNT_SOL})')

    args = parser.parse_args()

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        results = analyze_circular_flows(cursor, args.mint_address, args.depth, args.min_return)
        print_results(results)

        if args.output:
            save_results(results, args.output)

    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
