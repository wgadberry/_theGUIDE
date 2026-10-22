#!/usr/bin/env python3
"""
SOLTIT Wallet P&L Analysis
Analyzes gains/losses for each wallet over time
"""

import mysql.connector
from datetime import datetime, timedelta
from collections import defaultdict
import csv

def main():
    conn = mysql.connector.connect(
        host='localhost', port=3396, user='root',
        password='rootpassword', database='t16o_db'
    )
    cursor = conn.cursor(dictionary=True)

    # Get infrastructure addresses (programs, pools, vaults)
    cursor.execute('''
        SELECT address FROM tx_address
        WHERE address_type IN ('program', 'pool', 'vault', 'mint')
    ''')
    infrastructure = {r['address'] for r in cursor.fetchall()}

    # Add known infrastructure
    infrastructure.update({
        'ARu4n5mFdZogZAravu7CcizaojWnS6oqka37gdLT5SZn',  # OKX Router
        'HV1KXxWFaSeriyFvXyx48FqG9BoFbfinB8njCJonqP7K',  # OKX Dex
        '11111111111111111111111111111111',
        'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
    })

    # Get all SOLTIT transactions
    cursor.execute('''
        SELECT
            g.tx_id, g.block_time,
            fa.address as from_addr, ta.address as to_addr,
            g.amount, g.decimals, g.edge_type_id
        FROM tx_guide g
        JOIN tx_address fa ON g.from_address_id = fa.id
        JOIN tx_address ta ON g.to_address_id = ta.id
        WHERE g.token_id = 2
        ORDER BY g.block_time
    ''')
    soltit_txns = cursor.fetchall()

    # Get SOL/WSOL flows for swaps
    cursor.execute('''
        SELECT
            g.tx_id, fa.address as from_addr, ta.address as to_addr,
            g.amount, g.decimals, g.edge_type_id
        FROM tx_guide g
        JOIN tx_address fa ON g.from_address_id = fa.id
        JOIN tx_address ta ON g.to_address_id = ta.id
        WHERE g.token_id IN (1, 3)
        AND g.tx_id IN (SELECT DISTINCT tx_id FROM tx_guide WHERE token_id = 2 AND edge_type_id IN (3, 4))
    ''')
    sol_flows = cursor.fetchall()
    conn.close()

    sol_by_tx = defaultdict(list)
    for sf in sol_flows:
        sol_by_tx[sf['tx_id']].append(sf)

    # Track wallet data with time series
    wallet_data = defaultdict(lambda: {
        'soltit_in': 0, 'soltit_out': 0,
        'sol_in': 0, 'sol_out': 0,
        'first_seen': None, 'last_seen': None,
        'txn_count': 0, 'buy_count': 0, 'sell_count': 0,
        'timeline': []
    })

    for txn in soltit_txns:
        from_addr = txn['from_addr']
        to_addr = txn['to_addr']
        amount = txn['amount'] / (10 ** txn['decimals'])
        block_time = txn['block_time']
        edge_type = txn['edge_type_id']
        tx_id = txn['tx_id']
        dt = datetime.fromtimestamp(block_time)
        date_str = dt.strftime('%Y-%m-%d')

        # Skip infrastructure-to-infrastructure
        if from_addr in infrastructure and to_addr in infrastructure:
            continue

        # Update sender (if not infrastructure)
        if from_addr not in infrastructure:
            wallet_data[from_addr]['soltit_out'] += amount
            wallet_data[from_addr]['txn_count'] += 1
            if wallet_data[from_addr]['first_seen'] is None:
                wallet_data[from_addr]['first_seen'] = dt
            wallet_data[from_addr]['last_seen'] = dt

            if edge_type == 3:  # swap_in (selling)
                wallet_data[from_addr]['sell_count'] += 1
                sol_received = 0
                for sf in sol_by_tx.get(tx_id, []):
                    if sf['to_addr'] == from_addr and sf['edge_type_id'] == 4:
                        sol_received += sf['amount'] / (10 ** sf['decimals'])
                wallet_data[from_addr]['sol_in'] += sol_received
                wallet_data[from_addr]['timeline'].append((date_str, 'sell', -amount, sol_received))
            else:
                wallet_data[from_addr]['timeline'].append((date_str, 'send', -amount, 0))

        # Update receiver (if not infrastructure)
        if to_addr not in infrastructure:
            wallet_data[to_addr]['soltit_in'] += amount
            wallet_data[to_addr]['txn_count'] += 1
            if wallet_data[to_addr]['first_seen'] is None:
                wallet_data[to_addr]['first_seen'] = dt
            wallet_data[to_addr]['last_seen'] = dt

            if edge_type == 4:  # swap_out (buying)
                wallet_data[to_addr]['buy_count'] += 1
                sol_spent = 0
                for sf in sol_by_tx.get(tx_id, []):
                    if sf['from_addr'] == to_addr and sf['edge_type_id'] == 3:
                        sol_spent += sf['amount'] / (10 ** sf['decimals'])
                wallet_data[to_addr]['sol_out'] += sol_spent
                wallet_data[to_addr]['timeline'].append((date_str, 'buy', amount, -sol_spent))
            else:
                wallet_data[to_addr]['timeline'].append((date_str, 'recv', amount, 0))

    # Calculate results
    results = []
    for addr, data in wallet_data.items():
        net_soltit = data['soltit_in'] - data['soltit_out']
        net_sol = data['sol_in'] - data['sol_out']

        results.append({
            'wallet': addr,
            'soltit_in': data['soltit_in'],
            'soltit_out': data['soltit_out'],
            'net_soltit': net_soltit,
            'sol_spent': data['sol_out'],
            'sol_received': data['sol_in'],
            'net_sol_pnl': net_sol,
            'buy_count': data['buy_count'],
            'sell_count': data['sell_count'],
            'txn_count': data['txn_count'],
            'first_seen': data['first_seen'],
            'last_seen': data['last_seen'],
            'timeline': data['timeline']
        })

    results.sort(key=lambda x: -x['net_sol_pnl'])

    # Write main CSV
    with open('soltit---wallet_pnl.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['wallet', 'net_sol_pnl', 'sol_spent', 'sol_received', 'net_soltit', 'buys', 'sells', 'first_seen', 'last_seen'])
        for r in results:
            writer.writerow([
                r['wallet'],
                f"{r['net_sol_pnl']:.4f}",
                f"{r['sol_spent']:.4f}",
                f"{r['sol_received']:.4f}",
                f"{r['net_soltit']:.2f}",
                r['buy_count'],
                r['sell_count'],
                r['first_seen'].strftime('%Y-%m-%d') if r['first_seen'] else '',
                r['last_seen'].strftime('%Y-%m-%d') if r['last_seen'] else ''
            ])

    # Summary
    total = len(results)
    profitable = [r for r in results if r['net_sol_pnl'] > 0.01]
    losing = [r for r in results if r['net_sol_pnl'] < -0.01]

    print('=' * 65)
    print('SOLTIT WALLET P&L ANALYSIS (Infrastructure Excluded)')
    print('=' * 65)
    print(f'Total wallets: {total}')
    print(f'Profitable:    {len(profitable)} ({100*len(profitable)/total:.1f}%)')
    print(f'Losing:        {len(losing)} ({100*len(losing)/total:.1f}%)')
    print()
    print(f"Total profits:  {sum(r['net_sol_pnl'] for r in profitable):,.2f} SOL")
    print(f"Total losses:   {sum(r['net_sol_pnl'] for r in losing):,.2f} SOL")
    print()
    print('TOP 10 WINNERS:')
    for r in results[:10]:
        days_active = (r['last_seen'] - r['first_seen']).days + 1 if r['first_seen'] and r['last_seen'] else 0
        print(f"  {r['wallet'][:16]}... | +{r['net_sol_pnl']:7.2f} SOL | {r['buy_count']:2} buys {r['sell_count']:3} sells | {days_active:3}d active")
    print()
    print('TOP 10 LOSERS:')
    for r in results[-10:][::-1]:
        days_active = (r['last_seen'] - r['first_seen']).days + 1 if r['first_seen'] and r['last_seen'] else 0
        print(f"  {r['wallet'][:16]}... | {r['net_sol_pnl']:8.2f} SOL | {r['buy_count']:2} buys {r['sell_count']:3} sells | {days_active:3}d active")
    print()
    print('CSV saved: soltit---wallet_pnl.csv')

    # Time series analysis - aggregate by week
    weekly_data = defaultdict(lambda: {
        'buys': 0, 'sells': 0, 'transfers': 0,
        'sol_spent': 0, 'sol_received': 0,
        'unique_buyers': set(), 'unique_sellers': set()
    })

    for r in results:
        for date_str, event_type, soltit_amt, sol_amt in r['timeline']:
            # Get week start (Monday)
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            week_start = dt - timedelta(days=dt.weekday())
            week_key = week_start.strftime('%Y-%m-%d')

            if event_type == 'buy':
                weekly_data[week_key]['buys'] += 1
                weekly_data[week_key]['sol_spent'] += abs(sol_amt)
                weekly_data[week_key]['unique_buyers'].add(r['wallet'])
            elif event_type == 'sell':
                weekly_data[week_key]['sells'] += 1
                weekly_data[week_key]['sol_received'] += sol_amt
                weekly_data[week_key]['unique_sellers'].add(r['wallet'])
            else:
                weekly_data[week_key]['transfers'] += 1

    print()
    print('=' * 65)
    print('WEEKLY ACTIVITY BREAKDOWN')
    print('=' * 65)
    print(f"{'Week':<12} {'Buys':>6} {'Sells':>6} {'Xfers':>6} {'SOL In':>10} {'SOL Out':>10} {'Buyers':>7} {'Sellers':>7}")
    print('-' * 65)

    for week in sorted(weekly_data.keys()):
        w = weekly_data[week]
        print(f"{week:<12} {w['buys']:>6} {w['sells']:>6} {w['transfers']:>6} "
              f"{w['sol_spent']:>10.2f} {w['sol_received']:>10.2f} "
              f"{len(w['unique_buyers']):>7} {len(w['unique_sellers']):>7}")

    # Write weekly CSV
    with open('soltit---weekly_activity.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['week', 'buys', 'sells', 'transfers', 'sol_spent', 'sol_received', 'unique_buyers', 'unique_sellers'])
        for week in sorted(weekly_data.keys()):
            w = weekly_data[week]
            writer.writerow([
                week, w['buys'], w['sells'], w['transfers'],
                f"{w['sol_spent']:.4f}", f"{w['sol_received']:.4f}",
                len(w['unique_buyers']), len(w['unique_sellers'])
            ])

    print()
    print('CSV saved: soltit---weekly_activity.csv')

if __name__ == '__main__':
    main()
