#!/usr/bin/env python3
"""
SOLTIT Coordinated Wallet Group Analysis
Identifies clusters of wallets that may be operating together
"""

import mysql.connector
from datetime import datetime, timedelta
from collections import defaultdict
import csv
import json

def main():
    conn = mysql.connector.connect(
        host='localhost', port=3396, user='root',
        password='rootpassword', database='t16o_db'
    )
    cursor = conn.cursor(dictionary=True)

    # Get infrastructure addresses to exclude
    cursor.execute('''
        SELECT address FROM tx_address
        WHERE address_type IN ('program', 'pool', 'vault', 'mint')
    ''')
    infrastructure = {r['address'] for r in cursor.fetchall()}
    infrastructure.update({
        'ARu4n5mFdZogZAravu7CcizaojWnS6oqka37gdLT5SZn',
        'HV1KXxWFaSeriyFvXyx48FqG9BoFbfinB8njCJonqP7K',
        '11111111111111111111111111111111',
        'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
        'GpMZbSM2GgvTKHJirzeGfMFoaZ8UR2X7F4v8vHTvxFbL',  # SOLTIT bonding curve
    })

    print("=" * 70)
    print("SOLTIT COORDINATED WALLET ANALYSIS")
    print("=" * 70)

    # 1. Find wallets that received SOLTIT from same distributor
    print("\n[1] WALLETS RECEIVING FROM SAME DISTRIBUTOR")
    print("-" * 70)

    cursor.execute('''
        SELECT
            fa.address as distributor,
            ta.address as receiver,
            COUNT(*) as transfer_count,
            SUM(g.amount) / 1e9 as total_soltit,
            MIN(FROM_UNIXTIME(g.block_time)) as first_transfer,
            MAX(FROM_UNIXTIME(g.block_time)) as last_transfer
        FROM tx_guide g
        JOIN tx_address fa ON g.from_address_id = fa.id
        JOIN tx_address ta ON g.to_address_id = ta.id
        WHERE g.token_id = 2
        AND g.edge_type_id = 2  -- SPL Transfer (not swap)
        GROUP BY fa.address, ta.address
        HAVING total_soltit > 1000000  -- Significant transfers only
        ORDER BY fa.address, total_soltit DESC
    ''')

    distributor_groups = defaultdict(list)
    for row in cursor.fetchall():
        if row['distributor'] not in infrastructure and row['receiver'] not in infrastructure:
            distributor_groups[row['distributor']].append({
                'receiver': row['receiver'],
                'amount': row['total_soltit'],
                'first': row['first_transfer'],
                'last': row['last_transfer']
            })

    # Show distributors with multiple recipients
    for dist, receivers in sorted(distributor_groups.items(), key=lambda x: -len(x[1])):
        if len(receivers) >= 3:  # Distributor sent to 3+ wallets
            total_distributed = sum(r['amount'] for r in receivers)
            print(f"\nDistributor: {dist[:20]}...")
            print(f"  Recipients: {len(receivers)}, Total: {total_distributed:,.0f} SOLTIT")
            for r in receivers[:5]:
                print(f"    -> {r['receiver'][:20]}... : {r['amount']:,.0f}")
            if len(receivers) > 5:
                print(f"    ... and {len(receivers)-5} more")

    # 2. Find wallets that bought/sold within same time windows
    print("\n\n[2] COORDINATED TRADING PATTERNS (Same 5-min Windows)")
    print("-" * 70)

    cursor.execute('''
        SELECT
            FLOOR(g.block_time / 300) * 300 as time_bucket,
            g.edge_type_id,
            fa.address as from_addr,
            ta.address as to_addr,
            g.amount / 1e9 as soltit_amount,
            FROM_UNIXTIME(g.block_time) as tx_time
        FROM tx_guide g
        JOIN tx_address fa ON g.from_address_id = fa.id
        JOIN tx_address ta ON g.to_address_id = ta.id
        WHERE g.token_id = 2
        AND g.edge_type_id IN (3, 4)  -- swaps only
        ORDER BY g.block_time
    ''')

    time_buckets = defaultdict(lambda: {'buyers': set(), 'sellers': set(), 'txns': []})

    for row in cursor.fetchall():
        bucket = row['time_bucket']
        if row['edge_type_id'] == 3:  # swap_in (selling)
            addr = row['from_addr']
            if addr not in infrastructure:
                time_buckets[bucket]['sellers'].add(addr)
        elif row['edge_type_id'] == 4:  # swap_out (buying)
            addr = row['to_addr']
            if addr not in infrastructure:
                time_buckets[bucket]['buyers'].add(addr)
        time_buckets[bucket]['txns'].append(row)

    # Find suspicious time buckets with coordinated activity
    coordinated_events = []
    for bucket, data in time_buckets.items():
        # Multiple different wallets buying or selling in same 5-min window
        if len(data['buyers']) >= 3 or len(data['sellers']) >= 3:
            coordinated_events.append({
                'time': datetime.fromtimestamp(int(bucket)),
                'buyers': data['buyers'],
                'sellers': data['sellers'],
                'txn_count': len(data['txns'])
            })

    print(f"Found {len(coordinated_events)} time windows with 3+ coordinated traders")

    # Show top coordinated events
    for event in sorted(coordinated_events, key=lambda x: -max(len(x['buyers']), len(x['sellers'])))[:10]:
        print(f"\n  {event['time']} - {event['txn_count']} txns")
        if event['buyers']:
            print(f"    Buyers ({len(event['buyers'])}): {', '.join(list(event['buyers'])[:3])}...")
        if event['sellers']:
            print(f"    Sellers ({len(event['sellers'])}): {', '.join(list(event['sellers'])[:3])}...")

    # 3. Find wallets with similar first-seen times (created together?)
    print("\n\n[3] WALLETS APPEARING IN SAME LAUNCH WINDOW (First Hour)")
    print("-" * 70)

    cursor.execute('''
        SELECT
            MIN(g.block_time) as first_seen,
            CASE
                WHEN fa.address NOT IN (SELECT address FROM tx_address WHERE address_type IN ('program', 'pool', 'vault', 'mint'))
                THEN fa.address ELSE NULL END as wallet_from,
            CASE
                WHEN ta.address NOT IN (SELECT address FROM tx_address WHERE address_type IN ('program', 'pool', 'vault', 'mint'))
                THEN ta.address ELSE NULL END as wallet_to
        FROM tx_guide g
        JOIN tx_address fa ON g.from_address_id = fa.id
        JOIN tx_address ta ON g.to_address_id = ta.id
        WHERE g.token_id = 2
        GROUP BY fa.address, ta.address
    ''')

    wallet_first_seen = {}
    for row in cursor.fetchall():
        ts = row['first_seen']
        if row['wallet_from'] and row['wallet_from'] not in infrastructure:
            if row['wallet_from'] not in wallet_first_seen or ts < wallet_first_seen[row['wallet_from']]:
                wallet_first_seen[row['wallet_from']] = ts
        if row['wallet_to'] and row['wallet_to'] not in infrastructure:
            if row['wallet_to'] not in wallet_first_seen or ts < wallet_first_seen[row['wallet_to']]:
                wallet_first_seen[row['wallet_to']] = ts

    # Find min timestamp (token launch)
    if wallet_first_seen:
        launch_time = int(min(wallet_first_seen.values()))
        first_hour_wallets = [w for w, ts in wallet_first_seen.items() if int(ts) - launch_time <= 3600]

        print(f"Token launch: {datetime.fromtimestamp(launch_time)}")
        print(f"Wallets active in first hour: {len(first_hour_wallets)}")

        # Show first 20
        first_hour_sorted = sorted([(w, wallet_first_seen[w]) for w in first_hour_wallets], key=lambda x: x[1])
        print("\nFirst 20 wallets to interact:")
        for w, ts in first_hour_sorted[:20]:
            elapsed = int(ts) - launch_time
            print(f"  +{elapsed:5d}s: {w[:30]}...")

    # 4. Check funding relationships
    print("\n\n[4] FUNDING RELATIONSHIPS (SOL flows between SOLTIT traders)")
    print("-" * 70)

    # Get all SOLTIT traders
    soltit_traders = set()
    cursor.execute('''
        SELECT DISTINCT fa.address as addr FROM tx_guide g
        JOIN tx_address fa ON g.from_address_id = fa.id
        WHERE g.token_id = 2 AND g.edge_type_id IN (3, 4)
        UNION
        SELECT DISTINCT ta.address as addr FROM tx_guide g
        JOIN tx_address ta ON g.to_address_id = ta.id
        WHERE g.token_id = 2 AND g.edge_type_id IN (3, 4)
    ''')
    soltit_traders = {r['addr'] for r in cursor.fetchall()} - infrastructure

    # Find SOL transfers between SOLTIT traders
    cursor.execute('''
        SELECT
            fa.address as funder,
            ta.address as funded,
            COUNT(*) as transfers,
            SUM(g.amount) / 1e9 as total_sol
        FROM tx_guide g
        JOIN tx_address fa ON g.from_address_id = fa.id
        JOIN tx_address ta ON g.to_address_id = ta.id
        WHERE g.token_id = 1  -- SOL
        AND g.edge_type_id IN (1, 2, 42)  -- transfers
        GROUP BY fa.address, ta.address
        HAVING total_sol >= 0.1  -- At least 0.1 SOL
    ''')

    funding_links = []
    for row in cursor.fetchall():
        if row['funder'] in soltit_traders and row['funded'] in soltit_traders:
            if row['funder'] != row['funded']:
                funding_links.append(row)

    # Build funding graph
    funded_by = defaultdict(set)
    funds_to = defaultdict(set)

    for link in funding_links:
        funded_by[link['funded']].add(link['funder'])
        funds_to[link['funder']].add(link['funded'])

    # Find clusters (wallets funded by same source)
    funder_clusters = defaultdict(set)
    for funded, funders in funded_by.items():
        for funder in funders:
            funder_clusters[funder].add(funded)

    print(f"Found {len(funding_links)} funding relationships between SOLTIT traders")
    print(f"Unique funders: {len(funder_clusters)}")

    # Show funders that funded multiple SOLTIT traders
    multi_funders = [(f, recipients) for f, recipients in funder_clusters.items() if len(recipients) >= 2]
    print(f"\nFunders that funded 2+ SOLTIT traders: {len(multi_funders)}")

    for funder, recipients in sorted(multi_funders, key=lambda x: -len(x[1]))[:10]:
        print(f"\n  Funder: {funder[:20]}... -> {len(recipients)} traders")
        for r in list(recipients)[:5]:
            print(f"    -> {r[:30]}...")

    # 5. Summary - identify likely coordinated groups
    print("\n\n[5] SUSPECTED COORDINATED WALLET GROUPS")
    print("=" * 70)

    # Combine signals
    suspected_groups = []

    # Group 1: Wallets funded by same source
    for funder, recipients in multi_funders:
        if len(recipients) >= 3:
            suspected_groups.append({
                'type': 'same_funder',
                'hub': funder,
                'members': list(recipients),
                'size': len(recipients)
            })

    # Group 2: Wallets receiving from same distributor
    for dist, receivers in distributor_groups.items():
        if len(receivers) >= 3:
            suspected_groups.append({
                'type': 'same_distributor',
                'hub': dist,
                'members': [r['receiver'] for r in receivers],
                'size': len(receivers)
            })

    # Deduplicate and rank
    suspected_groups.sort(key=lambda x: -x['size'])

    print(f"\nIdentified {len(suspected_groups)} suspected coordinated groups")

    for i, group in enumerate(suspected_groups[:10], 1):
        print(f"\n{i}. {group['type'].upper()} - {group['size']} wallets")
        print(f"   Hub: {group['hub'][:40]}...")
        print(f"   Members:")
        for m in group['members'][:5]:
            print(f"     - {m[:40]}...")
        if len(group['members']) > 5:
            print(f"     ... and {len(group['members'])-5} more")

    # Write results to JSON
    output = {
        'distributor_groups': {k: v for k, v in distributor_groups.items() if len(v) >= 2},
        'coordinated_events': [
            {'time': str(e['time']), 'buyers': list(e['buyers']), 'sellers': list(e['sellers'])}
            for e in coordinated_events[:50]
        ],
        'funding_clusters': {k: list(v) for k, v in funder_clusters.items() if len(v) >= 2},
        'suspected_groups': suspected_groups[:20]
    }

    with open('soltit---coordination_analysis.json', 'w') as f:
        json.dump(output, f, indent=2, default=str)

    print("\n" + "=" * 70)
    print("Results saved to: soltit---coordination_analysis.json")

    conn.close()

if __name__ == '__main__':
    main()
