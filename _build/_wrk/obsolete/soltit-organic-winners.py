#!/usr/bin/env python3
"""
SOLTIT Organic Winners Analysis
Finds profitable traders excluding insiders, early wallets, and large recipients
Categorizes by how they acquired tokens (bought vs received)
"""

import mysql.connector
from datetime import datetime
from collections import defaultdict
import csv

def main():
    conn = mysql.connector.connect(
        host='localhost', port=3396, user='root',
        password='rootpassword', database='t16o_db'
    )
    cursor = conn.cursor(dictionary=True)

    # Get infrastructure
    cursor.execute('SELECT address FROM tx_address WHERE address_type IN ("program","pool","vault","mint")')
    infrastructure = {r['address'] for r in cursor.fetchall()}
    infrastructure.add('GpMZbSM2GgvTKHJirzeGfMFoaZ8UR2X7F4v8vHTvxFbL')

    # Known insiders
    strict_insiders = {
        'BTmqi8V5WvMBiXhEDf7zhncFoxFczQREyQNcKLkhzEZS',
        'yudkpbXU7Ufvumi6fpJ69HMwyfoQQ8UeSFZc4be2hiW',
        '6qZRYpsJbx9WDtDfUkbCwopMGK6eh2yEP25Nzb2J4RxM',
    }

    # Large recipients (>5M tokens via transfer)
    cursor.execute('''
        SELECT ta.address, SUM(g.amount)/1e9 as total
        FROM tx_guide g JOIN tx_address ta ON g.to_address_id = ta.id
        WHERE g.token_id = 2 AND g.edge_type_id = 2
        GROUP BY ta.address HAVING total > 5000000
    ''')
    large_recipients = {r['address'] for r in cursor.fetchall()}

    # Early wallets (first 10 minutes)
    cursor.execute('SELECT MIN(block_time) as launch FROM tx_guide WHERE token_id = 2')
    launch_time = int(cursor.fetchone()['launch'])

    cursor.execute('''
        SELECT DISTINCT fa.address as addr FROM tx_guide g
        JOIN tx_address fa ON g.from_address_id = fa.id
        WHERE g.token_id = 2 AND g.block_time <= %s
        UNION
        SELECT DISTINCT ta.address as addr FROM tx_guide g
        JOIN tx_address ta ON g.to_address_id = ta.id
        WHERE g.token_id = 2 AND g.block_time <= %s
    ''', (launch_time + 600, launch_time + 600))
    early_wallets = {r['addr'] for r in cursor.fetchall()}

    suspect = strict_insiders | large_recipients | early_wallets | infrastructure

    # Get all transactions
    cursor.execute('''
        SELECT g.tx_id, g.block_time, fa.address as from_addr, ta.address as to_addr,
               g.amount, g.decimals, g.edge_type_id
        FROM tx_guide g
        JOIN tx_address fa ON g.from_address_id = fa.id
        JOIN tx_address ta ON g.to_address_id = ta.id
        WHERE g.token_id = 2 ORDER BY g.block_time
    ''')
    txns = cursor.fetchall()

    cursor.execute('''
        SELECT g.tx_id, fa.address as from_addr, ta.address as to_addr,
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

    wallet_data = defaultdict(lambda: {
        'tokens_bought': 0, 'tokens_received': 0, 'tokens_sold': 0,
        'sol_in': 0, 'sol_out': 0,
        'first_seen': None, 'last_seen': None, 'buy_count': 0, 'sell_count': 0
    })

    for txn in txns:
        from_addr, to_addr = txn['from_addr'], txn['to_addr']
        amount = txn['amount'] / (10 ** txn['decimals'])
        dt = datetime.fromtimestamp(int(txn['block_time']))
        edge_type, tx_id = txn['edge_type_id'], txn['tx_id']

        if from_addr not in infrastructure:
            wallet_data[from_addr]['tokens_sold'] += amount
            if not wallet_data[from_addr]['first_seen']:
                wallet_data[from_addr]['first_seen'] = dt
            wallet_data[from_addr]['last_seen'] = dt
            if edge_type == 3:
                wallet_data[from_addr]['sell_count'] += 1
                for sf in sol_by_tx.get(tx_id, []):
                    if sf['to_addr'] == from_addr and sf['edge_type_id'] == 4:
                        wallet_data[from_addr]['sol_in'] += sf['amount'] / (10 ** sf['decimals'])

        if to_addr not in infrastructure:
            if not wallet_data[to_addr]['first_seen']:
                wallet_data[to_addr]['first_seen'] = dt
            wallet_data[to_addr]['last_seen'] = dt
            if edge_type == 4:
                wallet_data[to_addr]['tokens_bought'] += amount
                wallet_data[to_addr]['buy_count'] += 1
                for sf in sol_by_tx.get(tx_id, []):
                    if sf['from_addr'] == to_addr and sf['edge_type_id'] == 3:
                        wallet_data[to_addr]['sol_out'] += sf['amount'] / (10 ** sf['decimals'])
            elif edge_type == 2:
                wallet_data[to_addr]['tokens_received'] += amount

    # Find organic winners
    organic = []
    for addr, d in wallet_data.items():
        if addr in suspect:
            continue
        if d['buy_count'] == 0 or d['sell_count'] == 0:
            continue

        net_sol = d['sol_in'] - d['sol_out']
        if net_sol <= 0.01:
            continue

        total_acquired = d['tokens_bought'] + d['tokens_received']
        if total_acquired == 0:
            continue
        bought_pct = d['tokens_bought'] / total_acquired * 100

        organic.append({
            'wallet': addr,
            'pnl': net_sol,
            'spent': d['sol_out'],
            'recv': d['sol_in'],
            'buys': d['buy_count'],
            'sells': d['sell_count'],
            'bought_pct': bought_pct,
            'tokens_bought': d['tokens_bought'],
            'tokens_received': d['tokens_received'],
            'first': d['first_seen'],
            'last': d['last_seen'],
            'days': (d['last_seen'] - d['first_seen']).days + 1 if d['first_seen'] and d['last_seen'] else 0
        })

    organic.sort(key=lambda x: -x['pnl'])

    # Categorize
    pure_organic = [w for w in organic if w['bought_pct'] >= 95]
    mostly_organic = [w for w in organic if 50 <= w['bought_pct'] < 95]
    mixed = [w for w in organic if w['bought_pct'] < 50]

    print('=' * 90)
    print('SOLTIT ORGANIC WINNERS ANALYSIS')
    print('=' * 90)
    print(f'Excluded: {len(suspect)} suspect wallets (insiders, early, large recipients)')
    print()
    print(f'Total profitable organic traders: {len(organic)}')
    print(f'  - Pure organic (>=95% bought): {len(pure_organic)}')
    print(f'  - Mostly organic (50-95% bought): {len(mostly_organic)}')
    print(f'  - Mixed (<50% bought): {len(mixed)}')
    print()

    print('=' * 90)
    print('PURE ORGANIC WINNERS (>=95% of tokens were BOUGHT, not received)')
    print('=' * 90)
    print(f'{"Wallet":<44} {"P&L":>8} {"Spent":>7} {"Buys":>5} {"Sells":>5} {"Days":>5}')
    print('-' * 90)

    for w in pure_organic[:20]:
        print(f'{w["wallet"]:<44} {w["pnl"]:>+7.2f} {w["spent"]:>7.2f} {w["buys"]:>5} {w["sells"]:>5} {w["days"]:>5}')

    if pure_organic:
        print()
        print(f'Total pure organic profit: {sum(w["pnl"] for w in pure_organic):.2f} SOL')

    print()
    print('=' * 90)
    print('MOSTLY ORGANIC WINNERS (50-95% bought)')
    print('=' * 90)
    print(f'{"Wallet":<44} {"P&L":>8} {"Spent":>7} {"Bought%":>8} {"Days":>5}')
    print('-' * 90)

    for w in mostly_organic[:15]:
        print(f'{w["wallet"]:<44} {w["pnl"]:>+7.2f} {w["spent"]:>7.2f} {w["bought_pct"]:>7.1f}% {w["days"]:>5}')

    if mostly_organic:
        print()
        print(f'Total mostly organic profit: {sum(w["pnl"] for w in mostly_organic):.2f} SOL')

    # Write to CSV
    with open('soltit---organic_winners.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['wallet', 'category', 'net_sol_pnl', 'sol_spent', 'sol_received',
                        'buys', 'sells', 'bought_pct', 'days_active', 'first_seen', 'last_seen'])
        for w in pure_organic:
            writer.writerow([w['wallet'], 'pure_organic', f"{w['pnl']:.4f}", f"{w['spent']:.4f}",
                           f"{w['recv']:.4f}", w['buys'], w['sells'], f"{w['bought_pct']:.1f}",
                           w['days'], w['first'].strftime('%Y-%m-%d'), w['last'].strftime('%Y-%m-%d')])
        for w in mostly_organic:
            writer.writerow([w['wallet'], 'mostly_organic', f"{w['pnl']:.4f}", f"{w['spent']:.4f}",
                           f"{w['recv']:.4f}", w['buys'], w['sells'], f"{w['bought_pct']:.1f}",
                           w['days'], w['first'].strftime('%Y-%m-%d'), w['last'].strftime('%Y-%m-%d')])
        for w in mixed:
            writer.writerow([w['wallet'], 'mixed', f"{w['pnl']:.4f}", f"{w['spent']:.4f}",
                           f"{w['recv']:.4f}", w['buys'], w['sells'], f"{w['bought_pct']:.1f}",
                           w['days'], w['first'].strftime('%Y-%m-%d'), w['last'].strftime('%Y-%m-%d')])

    print()
    print('CSV saved: soltit---organic_winners.csv')

if __name__ == '__main__':
    main()
