#!/usr/bin/env python3
"""
Guide Clipper - Detect clipping patterns using tx_guide edges

Detects patterns where:
1. Wallet A makes a significant BUY (swap_in)
2. Within a brief window, other wallets SELL (swap_out) significant amounts
3. These sellers may be coordinated (same entity, wash trading ring)

Usage:
    python guide-clipper.py --token <mint_or_symbol>
    python guide-clipper.py --token SOLTIT --window 30
    python guide-clipper.py --token SOLTIT --window 60 --gexf clipper.gexf
"""

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone

import mysql.connector
import networkx as nx


def connect_db(host='localhost', port=3396, user='root', password='rootpassword', database='t16o_db'):
    return mysql.connector.connect(
        host=host, port=port, user=user, password=password, database=database
    )


def get_token_trades(cursor, token_filter: str, min_amount: float = 0):
    """Get all swap_in (buys) and swap_out (sells) for a token"""

    query = """
        SELECT
            g.id as edge_id,
            g.block_time,
            gt.type_code as edge_type,
            fa.address as from_address,
            ta.address as to_address,
            tk.token_symbol,
            mint.address as token_mint,
            g.amount,
            g.decimals,
            t.signature as tx_signature
        FROM tx_guide g
        JOIN tx_guide_type gt ON g.edge_type_id = gt.id
        JOIN tx_address fa ON g.from_address_id = fa.id
        JOIN tx_address ta ON g.to_address_id = ta.id
        JOIN tx t ON g.tx_id = t.id
        LEFT JOIN tx_token tk ON g.token_id = tk.id
        LEFT JOIN tx_address mint ON tk.mint_address_id = mint.id
        WHERE gt.type_code IN ('swap_in', 'swap_out')
          AND (tk.token_symbol = %s OR mint.address = %s)
        ORDER BY g.block_time ASC
    """

    cursor.execute(query, [token_filter, token_filter])
    return cursor.fetchall()


def classify_trades(rows) -> list:
    """Convert rows to classified trade records"""
    trades = []

    for row in rows:
        (edge_id, block_time, edge_type, from_addr, to_addr,
         token_symbol, token_mint, amount, decimals, tx_sig) = row

        human_amount = amount / (10 ** decimals) if amount and decimals else 0

        # Convert block_time to UTC string
        block_time_utc = datetime.fromtimestamp(block_time, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if block_time else ''

        # swap_in: to_address is the BUYER (receiving token)
        # swap_out: from_address is the SELLER (giving token)
        if edge_type == 'swap_in':
            trader = to_addr
            trade_type = 'BUY'
        elif edge_type == 'swap_out':
            trader = from_addr
            trade_type = 'SELL'
        else:
            continue

        trades.append({
            'edge_id': edge_id,
            'block_time': block_time,
            'block_time_utc': block_time_utc,
            'trade_type': trade_type,
            'trader': trader,
            'amount': human_amount,
            'token_symbol': token_symbol,
            'token_mint': token_mint,
            'tx_signature': tx_sig
        })

    return trades


def find_clip_events(trades: list, window_seconds: int, min_amount: float = 0) -> list:
    """
    Find clip events: buys followed by sells from OTHER wallets within the window
    """
    clip_events = []

    buys = [t for t in trades if t['trade_type'] == 'BUY' and t['amount'] >= min_amount and t['trader']]
    sells = [t for t in trades if t['trade_type'] == 'SELL' and t['amount'] >= min_amount and t['trader']]

    # Dedupe by (trader, block_time) keeping highest amount
    buy_map = {}
    for buy in buys:
        key = (buy['trader'], buy['block_time'])
        if key not in buy_map or buy['amount'] > buy_map[key]['amount']:
            buy_map[key] = buy
    buys = list(buy_map.values())

    sell_map = {}
    for sell in sells:
        key = (sell['trader'], sell['block_time'])
        if key not in sell_map or sell['amount'] > sell_map[key]['amount']:
            sell_map[key] = sell
    sells = list(sell_map.values())

    print(f"Found {len(buys)} unique BUYs and {len(sells)} unique SELLs")

    for buy in buys:
        buy_time = buy['block_time']
        buy_wallet = buy['trader']

        window_sells = []
        seen_sellers = set()

        for sell in sells:
            if sell['trader'] == buy_wallet:
                continue

            time_diff = sell['block_time'] - buy_time

            if 0 < time_diff <= window_seconds:
                if sell['trader'] in seen_sellers:
                    continue
                seen_sellers.add(sell['trader'])

                sell_copy = sell.copy()
                sell_copy['time_diff'] = time_diff
                window_sells.append(sell_copy)

        if window_sells:
            total_sell_amount = sum(s['amount'] for s in window_sells)
            avg_time_diff = sum(s['time_diff'] for s in window_sells) / len(window_sells)

            clip_score = (
                len(window_sells) * 10 +
                (total_sell_amount / buy['amount'] if buy['amount'] > 0 else 0) * 50 +
                (window_seconds - avg_time_diff) / window_seconds * 40
            )

            clip_events.append({
                'buy': buy,
                'sells': window_sells,
                'clip_score': clip_score,
                'total_sell_amount': total_sell_amount,
                'sell_count': len(window_sells),
                'avg_time_diff': avg_time_diff
            })

    clip_events.sort(key=lambda x: x['clip_score'], reverse=True)
    return clip_events


def build_clipper_graph(clip_events: list) -> nx.DiGraph:
    """Build directed graph: buyer -> seller edges weighted by clip count"""
    G = nx.DiGraph()

    edge_weights = defaultdict(lambda: {'count': 0, 'total_amount': 0, 'avg_time': []})

    for event in clip_events:
        buyer = event['buy']['trader']

        for sell in event['sells']:
            seller = sell['trader']
            key = (buyer, seller)

            edge_weights[key]['count'] += 1
            edge_weights[key]['total_amount'] += sell['amount']
            edge_weights[key]['avg_time'].append(sell['time_diff'])

    for (buyer, seller), data in edge_weights.items():
        avg_time = sum(data['avg_time']) / len(data['avg_time'])

        G.add_edge(buyer, seller,
                   count=data['count'],
                   total_amount=data['total_amount'],
                   avg_time_diff=avg_time,
                   weight=data['count'])

        if 'clip_count' not in G.nodes[buyer]:
            G.nodes[buyer]['clip_count'] = 0
            G.nodes[buyer]['role'] = 'buyer'
        G.nodes[buyer]['clip_count'] += data['count']

        if 'clip_count' not in G.nodes[seller]:
            G.nodes[seller]['clip_count'] = 0
            G.nodes[seller]['role'] = 'seller'
        G.nodes[seller]['clip_count'] += data['count']

    return G


def analyze_clippers(G: nx.DiGraph, clip_events: list):
    """Print clipper analysis report"""

    print(f"\n{'='*80}")
    print("CLIPPER ANALYSIS")
    print(f"{'='*80}")

    if G.number_of_nodes() == 0:
        print("No clip events detected")
        return

    print(f"Wallets involved: {G.number_of_nodes()}")
    print(f"Clip relationships: {G.number_of_edges()}")

    # Top clippers (most frequent sellers after buys)
    print(f"\n{'='*80}")
    print("TOP CLIPPERS (sell immediately after others buy)")
    print(f"{'='*80}")

    clipper_stats = []
    for node in G.nodes():
        in_edges = list(G.in_edges(node, data=True))
        total_clips = sum(e[2].get('count', 0) for e in in_edges)
        total_amount = sum(e[2].get('total_amount', 0) for e in in_edges)
        unique_buyers = len(in_edges)

        if total_clips > 0:
            clipper_stats.append({
                'wallet': node,
                'total_clips': total_clips,
                'unique_buyers': unique_buyers,
                'total_amount': total_amount
            })

    clipper_stats.sort(key=lambda x: x['total_clips'], reverse=True)

    for i, stat in enumerate(clipper_stats[:15]):
        print(f"\n{i+1}. {stat['wallet']}")
        print(f"   Clips: {stat['total_clips']} | Unique victims: {stat['unique_buyers']} | Amount: {stat['total_amount']:,.4f}")

    # Most clipped buyers
    print(f"\n{'='*80}")
    print("MOST CLIPPED BUYERS (targeted by clippers)")
    print(f"{'='*80}")

    victim_stats = []
    for node in G.nodes():
        out_edges = list(G.out_edges(node, data=True))
        total_clips = sum(e[2].get('count', 0) for e in out_edges)
        unique_clippers = len(out_edges)

        if total_clips > 0:
            victim_stats.append({
                'wallet': node,
                'times_clipped': total_clips,
                'unique_clippers': unique_clippers
            })

    victim_stats.sort(key=lambda x: x['times_clipped'], reverse=True)

    for i, stat in enumerate(victim_stats[:10]):
        print(f"\n{i+1}. {stat['wallet']}")
        print(f"   Clipped: {stat['times_clipped']} times by {stat['unique_clippers']} different wallets")

    # Clipper rings
    print(f"\n{'='*80}")
    print("CLIPPER RINGS (wallets that clip the same buyers)")
    print(f"{'='*80}")

    victim_to_clippers = defaultdict(set)
    for u, v, data in G.edges(data=True):
        victim_to_clippers[u].add(v)

    clipper_pairs = defaultdict(int)
    for victim, clippers in victim_to_clippers.items():
        clippers_list = list(clippers)
        for i in range(len(clippers_list)):
            for j in range(i+1, len(clippers_list)):
                pair = tuple(sorted([clippers_list[i], clippers_list[j]]))
                clipper_pairs[pair] += 1

    ring_candidates = sorted(clipper_pairs.items(), key=lambda x: x[1], reverse=True)

    if ring_candidates:
        print(f"Found {len([r for r in ring_candidates if r[1] >= 2])} potential clipper pairs (2+ shared victims)")
        for (c1, c2), shared in ring_candidates[:10]:
            if shared >= 2:
                print(f"\n  {c1}")
                print(f"  {c2}")
                print(f"  ^ share {shared} victims")

    # Top clip events
    print(f"\n{'='*80}")
    print("TOP CLIP EVENTS (highest score)")
    print(f"{'='*80}")

    for i, event in enumerate(clip_events[:10]):
        buy = event['buy']
        print(f"\nEvent {i+1}: Score={event['clip_score']:.1f}")
        print(f"  BUYER: {buy['trader']}")
        print(f"  BUY: {buy['amount']:,.4f} {buy['token_symbol']} @ block_time {buy['block_time']}")
        print(f"  CLIPPERS ({event['sell_count']} within {event['avg_time_diff']:.1f}s avg):")
        for sell in event['sells'][:5]:
            print(f"    - {sell['trader']}")
            print(f"      SOLD {sell['amount']:,.4f} after {sell['time_diff']}s")
        if len(event['sells']) > 5:
            print(f"    ... and {len(event['sells']) - 5} more")


def sanitize_for_gexf(G: nx.DiGraph) -> nx.DiGraph:
    """Create copy with None values replaced for GEXF export"""
    H = nx.DiGraph()

    for node, attrs in G.nodes(data=True):
        clean = {'label': str(node)}  # Set label to node ID (address)
        clean.update({k: ('' if v is None else v) for k, v in attrs.items()})
        H.add_node(node, **clean)

    for u, v, attrs in G.edges(data=True):
        clean = {k: ('' if v is None else v) for k, v in attrs.items()}
        H.add_edge(u, v, **clean)

    return H


def export_json(G: nx.DiGraph, clip_events: list, token: str, window: int, filepath: str):
    """Export full analysis to JSON"""
    clipper_stats = []
    for node in G.nodes():
        in_edges = list(G.in_edges(node, data=True))
        total_clips = sum(e[2].get('count', 0) for e in in_edges)
        total_amount = sum(e[2].get('total_amount', 0) for e in in_edges)

        if total_clips > 0:
            clipper_stats.append({
                'wallet': node,
                'total_clips': total_clips,
                'unique_buyers_clipped': len(in_edges),
                'total_amount_sold': total_amount
            })
    clipper_stats.sort(key=lambda x: x['total_clips'], reverse=True)

    victim_stats = []
    for node in G.nodes():
        out_edges = list(G.out_edges(node, data=True))
        total_clips = sum(e[2].get('count', 0) for e in out_edges)

        if total_clips > 0:
            victim_stats.append({
                'wallet': node,
                'times_clipped': total_clips,
                'unique_clippers': len(out_edges)
            })
    victim_stats.sort(key=lambda x: x['times_clipped'], reverse=True)

    events_json = []
    for event in clip_events:
        buy = event['buy']
        events_json.append({
            'clip_score': event['clip_score'],
            'buyer': buy['trader'],
            'buy_amount': buy['amount'],
            'buy_token': buy.get('token_symbol'),
            'buy_block_time': buy.get('block_time'),
            'buy_block_time_utc': buy.get('block_time_utc'),
            'sell_count': event['sell_count'],
            'total_sell_amount': event['total_sell_amount'],
            'avg_time_diff_seconds': event['avg_time_diff'],
            'clippers': [
                {
                    'wallet': s['trader'],
                    'amount': s['amount'],
                    'block_time': s.get('block_time'),
                    'block_time_utc': s.get('block_time_utc'),
                    'time_diff_seconds': s['time_diff']
                }
                for s in event['sells']
            ]
        })

    output = {
        'token': token,
        'window_seconds': window,
        'summary': {
            'total_clip_events': len(clip_events),
            'wallets_involved': G.number_of_nodes(),
            'clip_relationships': G.number_of_edges()
        },
        'top_clippers': clipper_stats,
        'most_clipped_buyers': victim_stats,
        'all_clip_events': events_json
    }

    with open(filepath, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nJSON exported to {filepath}")


def main():
    parser = argparse.ArgumentParser(description='Clipper detection using tx_guide')
    parser.add_argument('--token', required=True, help='Token symbol or mint address')
    parser.add_argument('--window', type=int, default=60, help='Time window in seconds (default: 60)')
    parser.add_argument('--min-amount', type=float, default=0, help='Minimum trade amount')
    parser.add_argument('--json', help='Export results to JSON file')
    parser.add_argument('--gexf', help='Export graph to GEXF file (for Gephi)')
    parser.add_argument('--db-host', default='localhost')
    parser.add_argument('--db-port', type=int, default=3396)
    parser.add_argument('--db-user', default='root')
    parser.add_argument('--db-pass', default='rootpassword')
    parser.add_argument('--db-name', default='t16o_db')

    args = parser.parse_args()

    print(f"GUIDE CLIPPER DETECTION")
    print(f"Token: {args.token}")
    print(f"Window: {args.window}s after buy")
    if args.min_amount > 0:
        print(f"Min amount: {args.min_amount}")
    print(f"{'='*80}")

    print("\nConnecting to database...")
    conn = connect_db(args.db_host, args.db_port, args.db_user, args.db_pass, args.db_name)
    cursor = conn.cursor()

    print("Fetching token trades...")
    raw_trades = get_token_trades(cursor, args.token, args.min_amount)
    print(f"Found {len(raw_trades)} trade records")

    if not raw_trades:
        print("No trades found for this token")
        cursor.close()
        conn.close()
        return

    print("Classifying trades...")
    trades = classify_trades(raw_trades)

    print(f"Searching for clip events (window={args.window}s)...")
    clip_events = find_clip_events(trades, args.window, args.min_amount)
    print(f"Found {len(clip_events)} clip events")

    if clip_events:
        G = build_clipper_graph(clip_events)
        analyze_clippers(G, clip_events)

        if args.json:
            export_json(G, clip_events, args.token, args.window, args.json)

        if args.gexf:
            print(f"\nExporting to GEXF: {args.gexf}...")
            clean_graph = sanitize_for_gexf(G)
            nx.write_gexf(clean_graph, args.gexf)
            print(f"Exported! Open in Gephi for visualization.")

    cursor.close()
    conn.close()

    print(f"\n{'='*80}")
    print("Done!")


if __name__ == '__main__':
    main()
