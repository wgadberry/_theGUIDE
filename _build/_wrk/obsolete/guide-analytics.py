#!/usr/bin/env python3
"""
Guide Analytics - Graph analytics for detecting wash trading, arbitrage, and bot patterns
Uses NetworkX for advanced graph algorithms on tx_guide data.

Usage:
    python guide-analytics.py                      # Full wash trading analysis
    python guide-analytics.py --address <addr>    # Analyze specific address
    python guide-analytics.py --token SOLTIT      # Filter by token
    python guide-analytics.py --cycles            # Find circular flows
    python guide-analytics.py --clusters          # Find sybil clusters

    # Arbitrage Detection
    python guide-analytics.py --arbitrage                    # Find all arbitrage txs
    python guide-analytics.py --arbitrage --token BoatKid    # Filter by token
    python guide-analytics.py --tx-id 12345                  # Analyze specific tx
    python guide-analytics.py --arb-wallet <addr>            # Analyze wallet's arb activity

    # Timeline & Path Analysis
    python guide-analytics.py --timeline <addr>              # Chronological activity
    python guide-analytics.py --path <addr> --depth 3        # Fund flow paths

    # Export
    python guide-analytics.py --arbitrage --json output.json
    python guide-analytics.py --token X --gexf graph.gexf    # For Gephi visualization
"""

import argparse
import json
from datetime import datetime, timezone
from collections import defaultdict
from typing import Optional, Dict, List, Set, Tuple

import mysql.connector
import networkx as nx


def connect_db(host='localhost', port=3396, user='root', password='rootpassword', database='t16o_db'):
    """Connect to MySQL database"""
    return mysql.connector.connect(
        host=host, port=port, user=user, password=password, database=database
    )


def build_graph(cursor, token_filter: Optional[List[str]] = None,
                address_filter: Optional[str] = None,
                start_time: Optional[int] = None,
                end_time: Optional[int] = None) -> nx.MultiDiGraph:
    """Build NetworkX graph from tx_guide"""

    query = """
        SELECT
            g.id as edge_id,
            fa.address as from_address,
            ta.address as to_address,
            t.signature as tx_signature,
            g.block_time,
            tk.token_symbol,
            g.amount,
            g.decimals,
            gt.type_code as edge_type
        FROM tx_guide g
        JOIN tx_address fa ON g.from_address_id = fa.id
        JOIN tx_address ta ON g.to_address_id = ta.id
        JOIN tx t ON g.tx_id = t.id
        JOIN tx_guide_type gt ON g.edge_type_id = gt.id
        LEFT JOIN tx_token tk ON g.token_id = tk.id
        WHERE 1=1
    """
    params = []

    if token_filter:
        placeholders = ','.join(['%s'] * len(token_filter))
        query += f" AND tk.token_symbol IN ({placeholders})"
        params.extend(token_filter)

    if address_filter:
        query += " AND (fa.address = %s OR ta.address = %s)"
        params.extend([address_filter, address_filter])

    if start_time:
        query += " AND g.block_time >= %s"
        params.append(start_time)

    if end_time:
        query += " AND g.block_time <= %s"
        params.append(end_time)

    cursor.execute(query, params)

    G = nx.MultiDiGraph()

    for row in cursor.fetchall():
        (edge_id, from_addr, to_addr, tx_sig, block_time,
         token_symbol, amount, decimals, edge_type) = row

        human_amount = amount / (10 ** decimals) if amount and decimals else 0
        block_time_utc = datetime.fromtimestamp(block_time, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if block_time else ''

        G.add_edge(from_addr, to_addr,
                   edge_id=edge_id,
                   tx_signature=tx_sig,
                   block_time=block_time,
                   block_time_utc=block_time_utc,
                   token_symbol=token_symbol or 'SOL',
                   amount=human_amount,
                   edge_type=edge_type)

    return G


def find_wash_cycles(G: nx.MultiDiGraph, max_length: int = 4) -> List[List[str]]:
    """Find circular flows (potential wash trading)"""
    cycles = []

    try:
        # Find simple cycles up to max_length
        for cycle in nx.simple_cycles(G, length_bound=max_length):
            if len(cycle) >= 2:
                cycles.append(cycle)
    except Exception as e:
        print(f"  Cycle detection error: {e}")

    return cycles


def find_high_frequency_pairs(G: nx.MultiDiGraph, min_edges: int = 5) -> List[Dict]:
    """Find address pairs with many edges between them"""
    pair_counts = defaultdict(lambda: {'count': 0, 'volume': 0, 'tokens': set()})

    for u, v, data in G.edges(data=True):
        pair = tuple(sorted([u, v]))
        pair_counts[pair]['count'] += 1
        pair_counts[pair]['volume'] += data.get('amount', 0)
        pair_counts[pair]['tokens'].add(data.get('token_symbol', 'SOL'))

    results = []
    for (addr1, addr2), stats in pair_counts.items():
        if stats['count'] >= min_edges:
            results.append({
                'address_1': addr1,
                'address_2': addr2,
                'edge_count': stats['count'],
                'total_volume': stats['volume'],
                'tokens': list(stats['tokens'])
            })

    return sorted(results, key=lambda x: -x['edge_count'])


def find_rapid_roundtrips(G: nx.MultiDiGraph, max_seconds: int = 60) -> List[Dict]:
    """Find A→B→A patterns within time window"""
    roundtrips = []

    for u in G.nodes():
        for v in G.successors(u):
            if u == v:
                continue

            # Get edges u→v
            out_edges = [(k, d) for k, d in G[u][v].items()]
            # Get edges v→u
            if G.has_edge(v, u):
                return_edges = [(k, d) for k, d in G[v][u].items()]
            else:
                continue

            for out_key, out_data in out_edges:
                out_time = out_data.get('block_time', 0)

                for ret_key, ret_data in return_edges:
                    ret_time = ret_data.get('block_time', 0)

                    if ret_time > out_time and (ret_time - out_time) <= max_seconds:
                        roundtrips.append({
                            'wallet_a': u,
                            'wallet_b': v,
                            'out_amount': out_data.get('amount', 0),
                            'return_amount': ret_data.get('amount', 0),
                            'seconds_between': ret_time - out_time,
                            'token': out_data.get('token_symbol'),
                            'out_tx': out_data.get('tx_signature'),
                            'return_tx': ret_data.get('tx_signature')
                        })

    return sorted(roundtrips, key=lambda x: x['seconds_between'])


def analyze_address(G: nx.MultiDiGraph, address: str) -> Dict:
    """Detailed analysis of a specific address"""
    if address not in G.nodes():
        return {'error': 'Address not found in graph'}

    in_edges = list(G.in_edges(address, data=True))
    out_edges = list(G.out_edges(address, data=True))

    # Counterparty analysis
    senders = defaultdict(lambda: {'count': 0, 'volume': 0})
    receivers = defaultdict(lambda: {'count': 0, 'volume': 0})

    for u, v, data in in_edges:
        senders[u]['count'] += 1
        senders[u]['volume'] += data.get('amount', 0)

    for u, v, data in out_edges:
        receivers[v]['count'] += 1
        receivers[v]['volume'] += data.get('amount', 0)

    # Token breakdown
    tokens_in = defaultdict(float)
    tokens_out = defaultdict(float)

    for u, v, data in in_edges:
        tokens_in[data.get('token_symbol', 'SOL')] += data.get('amount', 0)

    for u, v, data in out_edges:
        tokens_out[data.get('token_symbol', 'SOL')] += data.get('amount', 0)

    return {
        'address': address,
        'in_degree': G.in_degree(address),
        'out_degree': G.out_degree(address),
        'total_edges': G.in_degree(address) + G.out_degree(address),
        'unique_senders': len(senders),
        'unique_receivers': len(receivers),
        'top_senders': sorted(senders.items(), key=lambda x: -x[1]['count'])[:5],
        'top_receivers': sorted(receivers.items(), key=lambda x: -x[1]['count'])[:5],
        'tokens_received': dict(tokens_in),
        'tokens_sent': dict(tokens_out)
    }


def detect_arbitrage_in_tx(cursor, tx_id: int) -> Optional[Dict]:
    """
    Detect multi-hop arbitrage in a single transaction.
    Pattern: wallet -> pool1 -> pool2 -> ... -> wallet with net positive return
    """
    cursor.execute("""
        SELECT
            g.id,
            fa.address AS from_address,
            ta.address AS to_address,
            COALESCE(tk.token_symbol, 'SOL') AS token_symbol,
            g.amount,
            g.decimals,
            gt.type_code AS edge_type
        FROM tx_guide g
        JOIN tx_address fa ON fa.id = g.from_address_id
        JOIN tx_address ta ON ta.id = g.to_address_id
        LEFT JOIN tx_token tk ON tk.id = g.token_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        WHERE g.tx_id = %s
        ORDER BY g.id
    """, (tx_id,))

    edges = cursor.fetchall()
    if not edges:
        return None

    # Find wallets (addresses that both send and receive)
    senders = set()
    receivers = set()

    for row in edges:
        _, from_addr, to_addr, _, _, _, _ = row
        senders.add(from_addr)
        receivers.add(to_addr)

    wallets = senders & receivers

    # Calculate net flows per wallet per token
    for wallet in wallets:
        net_flows = defaultdict(float)
        pools_used = set()
        swap_count = 0

        for row in edges:
            _, from_addr, to_addr, token_symbol, amount, decimals, edge_type = row
            human_amount = (amount or 0) / (10 ** (decimals or 0)) if amount else 0

            if from_addr == wallet:
                net_flows[token_symbol] -= human_amount
                pools_used.add(to_addr)
            if to_addr == wallet:
                net_flows[token_symbol] += human_amount
                pools_used.add(from_addr)

            if 'swap' in edge_type:
                swap_count += 1

        # Check for profit in base tokens
        base_tokens = ['SOL', 'WSOL', 'USDC', 'USDT']
        for base in base_tokens:
            if base in net_flows and net_flows[base] > 0:
                # Calculate profit percentage
                total_out = abs(sum(v for t, v in net_flows.items() if v < 0 and t == base))
                profit_pct = (net_flows[base] / total_out * 100) if total_out > 0 else 0

                return {
                    'tx_id': tx_id,
                    'wallet': wallet,
                    'swap_count': swap_count,
                    'pools_used': len(pools_used),
                    'tokens_involved': [t for t, v in net_flows.items() if v != 0],
                    'profit_token': base,
                    'profit_amount': net_flows[base],
                    'profit_pct': profit_pct,
                    'net_flows': dict(net_flows)
                }

    return None


def find_arbitrage_transactions(cursor, token_symbol: str = None, limit: int = 100) -> List[Dict]:
    """
    Find all transactions with potential arbitrage patterns.
    Looks for txs with 2+ swaps involving the same wallet.
    """
    # Find transactions with multiple swaps
    if token_symbol:
        cursor.execute("""
            SELECT
                g.tx_id,
                t.signature,
                COUNT(*) AS edge_count,
                SUM(CASE WHEN gt.type_code LIKE %s THEN 1 ELSE 0 END) AS swap_count
            FROM tx_guide g
            JOIN tx t ON t.id = g.tx_id
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            JOIN tx_token tk ON tk.id = g.token_id
            WHERE tk.token_symbol = %s
            GROUP BY g.tx_id, t.signature
            HAVING swap_count >= 2
            ORDER BY swap_count DESC
            LIMIT %s
        """, ('%swap%', token_symbol, limit))
    else:
        cursor.execute("""
            SELECT
                g.tx_id,
                t.signature,
                COUNT(*) AS edge_count,
                SUM(CASE WHEN gt.type_code LIKE %s THEN 1 ELSE 0 END) AS swap_count
            FROM tx_guide g
            JOIN tx t ON t.id = g.tx_id
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            GROUP BY g.tx_id, t.signature
            HAVING swap_count >= 2
            ORDER BY swap_count DESC
            LIMIT %s
        """, ('%swap%', limit))

    results = []
    for tx_id, signature, edge_count, swap_count in cursor.fetchall():
        arb = detect_arbitrage_in_tx(cursor, tx_id)
        if arb:
            arb['signature'] = signature
            results.append(arb)

    return results


def analyze_arbitrage_wallet(cursor, address: str, limit: int = 1000) -> Dict:
    """
    Analyze a wallet for arbitrage activity patterns.
    """
    # Find all transactions involving this wallet with swaps
    cursor.execute("""
        SELECT DISTINCT g.tx_id, t.signature, t.block_time
        FROM tx_guide g
        JOIN tx t ON t.id = g.tx_id
        JOIN tx_address a ON a.id IN (g.from_address_id, g.to_address_id)
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        WHERE a.address = %s
        AND gt.type_code LIKE '%swap%'
        ORDER BY t.block_time DESC
        LIMIT %s
    """, (address, limit))

    txs = cursor.fetchall()

    arbitrage_txs = []
    total_profit = defaultdict(float)

    for tx_id, signature, block_time in txs:
        arb = detect_arbitrage_in_tx(cursor, tx_id)
        if arb and arb['wallet'] == address:
            arb['signature'] = signature
            arb['block_time'] = block_time
            arbitrage_txs.append(arb)
            total_profit[arb['profit_token']] += arb['profit_amount']

    return {
        'address': address,
        'total_swap_txs': len(txs),
        'arbitrage_txs': len(arbitrage_txs),
        'total_profit': dict(total_profit),
        'recent_arbitrages': arbitrage_txs[:20]
    }


def print_arbitrage_report(cursor, token_symbol: str = None, limit: int = 50):
    """Print arbitrage analysis report"""

    print(f"\n{'='*80}")
    print("ARBITRAGE DETECTION REPORT")
    print(f"{'='*80}")

    if token_symbol:
        print(f"Token filter: {token_symbol}")

    arbs = find_arbitrage_transactions(cursor, token_symbol, limit)

    print(f"\nFound {len(arbs)} arbitrage transactions")

    # Group by wallet
    wallet_stats = defaultdict(lambda: {'count': 0, 'profit': defaultdict(float)})

    for arb in arbs:
        wallet = arb['wallet']
        wallet_stats[wallet]['count'] += 1
        wallet_stats[wallet]['profit'][arb['profit_token']] += arb['profit_amount']

    print(f"\n{'='*80}")
    print("TOP ARBITRAGE WALLETS")
    print(f"{'='*80}")

    for wallet, stats in sorted(wallet_stats.items(), key=lambda x: -x[1]['count'])[:15]:
        print(f"\n{wallet}")
        print(f"  Arb transactions: {stats['count']}")
        for token, profit in stats['profit'].items():
            print(f"  Total {token} profit: {profit:,.9f}")

    print(f"\n{'='*80}")
    print("RECENT ARBITRAGE TRANSACTIONS")
    print(f"{'='*80}")

    for i, arb in enumerate(arbs[:20]):
        print(f"\n{i+1}. TX: {arb.get('signature', 'N/A')[:20]}...")
        print(f"   Wallet: {arb['wallet'][:20]}...")
        print(f"   Swaps: {arb['swap_count']}, Pools: {arb['pools_used']}")
        print(f"   Tokens: {arb['tokens_involved']}")
        print(f"   Profit: {arb['profit_amount']:,.9f} {arb['profit_token']} ({arb['profit_pct']:.4f}%)")


def find_clipping_suspects(G: nx.MultiDiGraph, cursor) -> List[Dict]:
    """
    Find addresses that might be clip wallets:
    - Receive from many different sources
    - Send to few destinations (consolidation)
    - High volume throughput
    """
    suspects = []

    for node in G.nodes():
        in_deg = G.in_degree(node)
        out_deg = G.out_degree(node)

        if in_deg < 3:
            continue

        # Calculate sender/receiver ratio
        senders = set(u for u, v in G.in_edges(node))
        receivers = set(v for u, v in G.out_edges(node))

        if len(receivers) == 0:
            continue

        ratio = len(senders) / len(receivers)

        # Suspicious if many senders but few receivers
        if ratio >= 3 and len(senders) >= 5:
            # Calculate volume
            in_volume = sum(d.get('amount', 0) for u, v, d in G.in_edges(node, data=True))
            out_volume = sum(d.get('amount', 0) for u, v, d in G.out_edges(node, data=True))

            suspects.append({
                'address': node,
                'unique_senders': len(senders),
                'unique_receivers': len(receivers),
                'sender_receiver_ratio': round(ratio, 2),
                'in_volume': round(in_volume, 4),
                'out_volume': round(out_volume, 4),
                'in_edges': in_deg,
                'out_edges': out_deg
            })

    return sorted(suspects, key=lambda x: -x['sender_receiver_ratio'])


def print_wash_report(G: nx.MultiDiGraph, cursor):
    """Print comprehensive wash trading analysis report"""

    print(f"\n{'='*80}")
    print("WASH TRADING & CLIPPING ANALYSIS REPORT")
    print(f"{'='*80}")
    print(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    print(f"Generated: {datetime.now(timezone.utc).isoformat()}")

    # High frequency pairs
    print(f"\n{'='*80}")
    print("HIGH-FREQUENCY TRADING PAIRS (10+ transfers)")
    print(f"{'='*80}")

    pairs = find_high_frequency_pairs(G, min_edges=10)
    for i, pair in enumerate(pairs[:15]):
        print(f"\n{i+1}. {pair['address_1']} <-> {pair['address_2']}")
        print(f"   Edges: {pair['edge_count']}, Volume: {pair['total_volume']:,.2f}, Tokens: {pair['tokens']}")

    # Rapid roundtrips
    print(f"\n{'='*80}")
    print("RAPID ROUND-TRIPS (A->B->A within 60 seconds)")
    print(f"{'='*80}")

    roundtrips = find_rapid_roundtrips(G, max_seconds=60)
    for i, rt in enumerate(roundtrips[:15]):
        print(f"\n{i+1}. {rt['wallet_a']} -> {rt['wallet_b']} -> back")
        print(f"   Out: {rt['out_amount']:,.4f} {rt['token']}, Return: {rt['return_amount']:,.4f}")
        print(f"   Time: {rt['seconds_between']}s")

    # Clipping suspects
    print(f"\n{'='*80}")
    print("CLIPPING/CONSOLIDATION SUSPECTS (many senders, few receivers)")
    print(f"{'='*80}")

    suspects = find_clipping_suspects(G, cursor)
    for i, s in enumerate(suspects[:15]):
        print(f"\n{i+1}. {s['address']}")
        print(f"   Senders: {s['unique_senders']}, Receivers: {s['unique_receivers']}, Ratio: {s['sender_receiver_ratio']}")
        print(f"   In: {s['in_volume']:,.4f}, Out: {s['out_volume']:,.4f}")

    # Cycle detection
    print(f"\n{'='*80}")
    print("CIRCULAR FLOWS (wash cycles)")
    print(f"{'='*80}")

    cycles = find_wash_cycles(G, max_length=4)
    print(f"Found {len(cycles)} cycles")
    for i, cycle in enumerate(cycles[:50]):
        cycle_str = " -> ".join(cycle)
        print(f"  {i+1}. {cycle_str} -> {cycle[0]}")


def build_path_graph(G: nx.MultiDiGraph, start_address: str, max_depth: int = 3,
                     direction: str = 'both') -> nx.MultiDiGraph:
    """
    Build a subgraph showing all paths from/to a specific address up to max_depth.
    direction: 'out' (outbound only), 'in' (inbound only), 'both'
    """
    if start_address not in G.nodes():
        print(f"Address {start_address} not found in graph")
        return nx.MultiDiGraph()

    path_graph = nx.MultiDiGraph()
    visited = set()

    def traverse_out(node, depth):
        if depth > max_depth or node in visited:
            return
        visited.add(node)

        for successor in G.successors(node):
            for key, data in G[node][successor].items():
                path_graph.add_edge(node, successor, **data)
            traverse_out(successor, depth + 1)

    def traverse_in(node, depth):
        if depth > max_depth or node in visited:
            return
        visited.add(node)

        for predecessor in G.predecessors(node):
            for key, data in G[predecessor][node].items():
                path_graph.add_edge(predecessor, node, **data)
            traverse_in(predecessor, depth + 1)

    if direction in ['out', 'both']:
        visited.clear()
        traverse_out(start_address, 0)

    if direction in ['in', 'both']:
        visited.clear()
        traverse_in(start_address, 0)

    return path_graph


def print_path_graph(G: nx.MultiDiGraph, path_graph: nx.MultiDiGraph, start_address: str):
    """Print path graph analysis"""
    print(f"\n{'='*80}")
    print(f"PATH GRAPH FOR: {start_address}")
    print(f"{'='*80}")
    print(f"Nodes: {path_graph.number_of_nodes()}, Edges: {path_graph.number_of_edges()}")

    # Inbound summary
    in_edges = list(path_graph.in_edges(start_address, data=True))
    out_edges = list(path_graph.out_edges(start_address, data=True))

    print(f"\nDirect connections: {len(in_edges)} inbound, {len(out_edges)} outbound")

    # Group by counterparty
    print(f"\n{'='*80}")
    print("OUTBOUND FLOWS (who received from this wallet)")
    print(f"{'='*80}")

    outbound = defaultdict(lambda: {'count': 0, 'volume': 0, 'tokens': set()})
    for u, v, data in path_graph.out_edges(start_address, data=True):
        outbound[v]['count'] += 1
        outbound[v]['volume'] += data.get('amount', 0)
        outbound[v]['tokens'].add(data.get('token_symbol', 'SOL'))

    for addr, stats in sorted(outbound.items(), key=lambda x: -x[1]['volume'])[:20]:
        tokens = ', '.join(stats['tokens'])
        print(f"\n  -> {addr}")
        print(f"     Transfers: {stats['count']}, Volume: {stats['volume']:,.4f}, Tokens: {tokens}")

    print(f"\n{'='*80}")
    print("INBOUND FLOWS (who sent to this wallet)")
    print(f"{'='*80}")

    inbound = defaultdict(lambda: {'count': 0, 'volume': 0, 'tokens': set()})
    for u, v, data in path_graph.in_edges(start_address, data=True):
        inbound[u]['count'] += 1
        inbound[u]['volume'] += data.get('amount', 0)
        inbound[u]['tokens'].add(data.get('token_symbol', 'SOL'))

    for addr, stats in sorted(inbound.items(), key=lambda x: -x[1]['volume'])[:20]:
        tokens = ', '.join(stats['tokens'])
        print(f"\n  <- {addr}")
        print(f"     Transfers: {stats['count']}, Volume: {stats['volume']:,.4f}, Tokens: {tokens}")

    # Second-degree connections
    print(f"\n{'='*80}")
    print("SECOND-DEGREE FLOWS (2 hops away)")
    print(f"{'='*80}")

    second_degree_out = set()
    second_degree_in = set()

    for first_hop in outbound.keys():
        for second_hop in path_graph.successors(first_hop):
            if second_hop != start_address:
                second_degree_out.add(second_hop)

    for first_hop in inbound.keys():
        for second_hop in path_graph.predecessors(first_hop):
            if second_hop != start_address:
                second_degree_in.add(second_hop)

    print(f"\nOutbound 2nd degree: {len(second_degree_out)} unique addresses")
    print(f"Inbound 2nd degree: {len(second_degree_in)} unique addresses")

    # Common connections (potential wash partners)
    common = set(outbound.keys()) & set(inbound.keys())
    if common:
        print(f"\n{'='*80}")
        print("BIDIRECTIONAL FLOWS (sent AND received - potential wash)")
        print(f"{'='*80}")
        for addr in common:
            out_stats = outbound[addr]
            in_stats = inbound[addr]
            print(f"\n  <-> {addr}")
            print(f"      Sent: {out_stats['count']} transfers, {out_stats['volume']:,.4f}")
            print(f"      Received: {in_stats['count']} transfers, {in_stats['volume']:,.4f}")


def get_wallet_timeline(cursor, address: str, token_filter: List[str] = None,
                        start_time: int = None, end_time: int = None) -> list:
    """Get chronological list of all activities for a wallet"""

    query = """
        SELECT
            g.id as edge_id,
            g.block_time,
            FROM_UNIXTIME(g.block_time) as block_time_utc,
            gt.type_code as edge_type,
            gt.category,
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
        WHERE (fa.address = %s OR ta.address = %s)
    """
    params = [address, address]

    if token_filter:
        placeholders = ','.join(['%s'] * len(token_filter))
        query += f" AND (tk.token_symbol IN ({placeholders}) OR mint.address IN ({placeholders}))"
        params.extend(token_filter)
        params.extend(token_filter)

    if start_time:
        query += " AND g.block_time >= %s"
        params.append(start_time)

    if end_time:
        query += " AND g.block_time <= %s"
        params.append(end_time)

    query += " ORDER BY g.block_time ASC, g.id ASC"

    cursor.execute(query, params)

    activities = []
    for row in cursor.fetchall():
        (edge_id, block_time, block_time_utc, edge_type, category,
         from_addr, to_addr, token_symbol, token_mint,
         amount, decimals, tx_sig) = row

        human_amount = amount / (10 ** decimals) if amount and decimals else 0

        # Determine direction relative to our wallet
        if from_addr == address:
            direction = 'OUT'
            counterparty = to_addr
        else:
            direction = 'IN'
            counterparty = from_addr

        activities.append({
            'edge_id': edge_id,
            'block_time': block_time,
            'block_time_utc': str(block_time_utc),
            'edge_type': edge_type,
            'category': category,
            'direction': direction,
            'counterparty': counterparty,
            'token_symbol': token_symbol or 'SOL',
            'token_mint': token_mint,
            'amount': human_amount,
            'amount_raw': amount,
            'tx_signature': tx_sig
        })

    return activities


def print_wallet_timeline(activities: list, address: str):
    """Print chronological timeline of wallet activities"""

    print(f"\n{'='*100}")
    print(f"ACTIVITY TIMELINE FOR: {address}")
    print(f"{'='*100}")
    print(f"Total activities: {len(activities)}")

    if not activities:
        print("No activities found")
        return

    # Summary stats
    in_count = sum(1 for a in activities if a['direction'] == 'IN')
    out_count = sum(1 for a in activities if a['direction'] == 'OUT')
    print(f"Inbound: {in_count} | Outbound: {out_count}")

    # Group by category
    categories = {}
    for a in activities:
        cat = a['category'] or 'unknown'
        categories[cat] = categories.get(cat, 0) + 1
    print(f"Categories: {categories}")

    # Token breakdown
    tokens = {}
    for a in activities:
        tok = a['token_symbol']
        if tok not in tokens:
            tokens[tok] = {'in': 0, 'out': 0, 'in_vol': 0, 'out_vol': 0}
        if a['direction'] == 'IN':
            tokens[tok]['in'] += 1
            tokens[tok]['in_vol'] += a['amount']
        else:
            tokens[tok]['out'] += 1
            tokens[tok]['out_vol'] += a['amount']

    print(f"\nToken Summary:")
    for tok, stats in sorted(tokens.items(), key=lambda x: -(x[1]['in_vol'] + x[1]['out_vol'])):
        print(f"  {tok}: IN {stats['in']} ({stats['in_vol']:,.4f}) | OUT {stats['out']} ({stats['out_vol']:,.4f})")

    # Print timeline
    print(f"\n{'='*100}")
    print("CHRONOLOGICAL EVENTS")
    print(f"{'='*100}")

    for i, a in enumerate(activities):
        direction_symbol = '<-' if a['direction'] == 'IN' else '->'
        print(f"\n{i+1}. [{a['block_time_utc']}] {a['edge_type']} ({a['category']})")
        print(f"   {a['direction']} {direction_symbol} {a['counterparty']}")
        print(f"   {a['amount']:,.4f} {a['token_symbol']}")
        print(f"   TX: {a['tx_signature']}")


def build_timeline_graph(activities: list, address: str) -> nx.DiGraph:
    """
    Build a sequential graph showing wallet activity over time.
    Each activity becomes a node, connected chronologically.
    """
    G = nx.DiGraph()

    # Add central wallet node
    G.add_node(address, node_type='wallet', label='TARGET')

    prev_node = None
    for i, a in enumerate(activities):
        # Create activity node
        node_id = f"activity_{i}"
        G.add_node(node_id,
                   node_type='activity',
                   seq=i,
                   block_time=a['block_time'],
                   edge_type=a['edge_type'],
                   category=a['category'],
                   direction=a['direction'],
                   amount=a['amount'],
                   token=a['token_symbol'],
                   counterparty=a['counterparty'][:16] + '...',
                   tx=a['tx_signature'][:16] + '...')

        # Connect to previous activity (chronological chain)
        if prev_node:
            time_diff = a['block_time'] - activities[i-1]['block_time']
            G.add_edge(prev_node, node_id, edge_type='next', time_diff=time_diff)

        # Connect to/from wallet based on direction
        if a['direction'] == 'IN':
            G.add_edge(node_id, address,
                       edge_type=a['edge_type'],
                       amount=a['amount'],
                       token=a['token_symbol'])
        else:
            G.add_edge(address, node_id,
                       edge_type=a['edge_type'],
                       amount=a['amount'],
                       token=a['token_symbol'])

        prev_node = node_id

    return G


def export_timeline_json(activities: list, address: str, filepath: str):
    """Export timeline to JSON"""
    # Summary stats
    tokens = {}
    for a in activities:
        tok = a['token_symbol']
        if tok not in tokens:
            tokens[tok] = {'in_count': 0, 'out_count': 0, 'in_volume': 0, 'out_volume': 0}
        if a['direction'] == 'IN':
            tokens[tok]['in_count'] += 1
            tokens[tok]['in_volume'] += a['amount']
        else:
            tokens[tok]['out_count'] += 1
            tokens[tok]['out_volume'] += a['amount']

    data = {
        'wallet': address,
        'total_activities': len(activities),
        'inbound_count': sum(1 for a in activities if a['direction'] == 'IN'),
        'outbound_count': sum(1 for a in activities if a['direction'] == 'OUT'),
        'token_summary': tokens,
        'first_activity': activities[0] if activities else None,
        'last_activity': activities[-1] if activities else None,
        'activities': activities
    }

    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"Timeline exported to {filepath}")


def sanitize_for_gexf(G: nx.MultiDiGraph) -> nx.MultiDiGraph:
    """Create a copy of graph with None values replaced for GEXF export"""
    H = nx.MultiDiGraph()

    # Copy nodes with sanitized attributes
    for node, attrs in G.nodes(data=True):
        clean_attrs = {'label': str(node)}  # Set label to node ID (address)
        for k, v in attrs.items():
            if v is None:
                clean_attrs[k] = ''
            elif isinstance(v, (set, list)):
                clean_attrs[k] = ','.join(str(x) for x in v)
            else:
                clean_attrs[k] = v
        H.add_node(node, **clean_attrs)

    # Copy edges with sanitized attributes
    for u, v, key, attrs in G.edges(keys=True, data=True):
        clean_attrs = {}
        for k, val in attrs.items():
            if val is None:
                clean_attrs[k] = ''
            elif isinstance(val, (set, list)):
                clean_attrs[k] = ','.join(str(x) for x in val)
            else:
                clean_attrs[k] = val
        H.add_edge(u, v, key=key, **clean_attrs)

    return H


def export_path_graph(path_graph: nx.MultiDiGraph, start_address: str, filepath: str):
    """Export path graph to JSON"""
    data = {
        'start_address': start_address,
        'nodes': list(path_graph.nodes()),
        'node_count': path_graph.number_of_nodes(),
        'edge_count': path_graph.number_of_edges(),
        'edges': []
    }

    for u, v, key, attrs in path_graph.edges(keys=True, data=True):
        edge = {
            'from': u,
            'to': v,
            'tx_signature': attrs.get('tx_signature'),
            'amount': attrs.get('amount'),
            'token': attrs.get('token_symbol'),
            'edge_type': attrs.get('edge_type'),
            'block_time': attrs.get('block_time')
        }
        data['edges'].append(edge)

    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"Path graph exported to {filepath}")


def main():
    parser = argparse.ArgumentParser(description='Graph analytics for wash trading detection')
    parser.add_argument('--address', help='Analyze specific address')
    parser.add_argument('--timeline', help='Show chronological activity timeline for address')
    parser.add_argument('--path', help='Generate path graph for address')
    parser.add_argument('--depth', type=int, default=2, help='Max depth for path graph (default: 2)')
    parser.add_argument('--direction', choices=['in', 'out', 'both'], default='both',
                        help='Direction for path graph')
    parser.add_argument('--token', nargs='+', help='Filter by token symbol(s) - can specify multiple: --token WSOL SOLTIT')
    parser.add_argument('--start-time', type=int, help='Filter: minimum block_time (unix timestamp)')
    parser.add_argument('--end-time', type=int, help='Filter: maximum block_time (unix timestamp)')
    parser.add_argument('--start-date', help='Filter: start date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end-date', help='Filter: end date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--cycles', action='store_true', help='Focus on cycle detection')
    parser.add_argument('--clusters', action='store_true', help='Focus on sybil clusters')
    parser.add_argument('--arbitrage', action='store_true', help='Detect arbitrage patterns')
    parser.add_argument('--arb-wallet', help='Analyze specific wallet for arbitrage activity')
    parser.add_argument('--tx-id', type=int, help='Analyze specific transaction by tx.id')
    parser.add_argument('--json', help='Export results to JSON file')
    parser.add_argument('--gexf', help='Export to GEXF format (for Gephi)')
    parser.add_argument('--db-host', default='localhost')
    parser.add_argument('--db-port', type=int, default=3396)
    parser.add_argument('--db-user', default='root')
    parser.add_argument('--db-pass', default='rootpassword')
    parser.add_argument('--db-name', default='t16o_db')

    args = parser.parse_args()

    # Initialize time filters
    if not hasattr(args, 'start_time') or args.start_time is None:
        args.start_time = None
    if not hasattr(args, 'end_time') or args.end_time is None:
        args.end_time = None

    # Convert date strings to unix timestamps
    if args.start_date:
        try:
            dt = datetime.strptime(args.start_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            dt = datetime.strptime(args.start_date, '%Y-%m-%d')
        args.start_time = int(dt.replace(tzinfo=timezone.utc).timestamp())
        print(f"Start date {args.start_date} -> {args.start_time}")

    if args.end_date:
        try:
            dt = datetime.strptime(args.end_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            dt = datetime.strptime(args.end_date, '%Y-%m-%d')
            dt = dt.replace(hour=23, minute=59, second=59)  # End of day
        args.end_time = int(dt.replace(tzinfo=timezone.utc).timestamp())
        print(f"End date {args.end_date} -> {args.end_time}")

    print("Connecting to database...")
    conn = connect_db(args.db_host, args.db_port, args.db_user, args.db_pass, args.db_name)
    cursor = conn.cursor()

    # Single transaction analysis
    if args.tx_id:
        print(f"\nAnalyzing transaction ID: {args.tx_id}")
        arb = detect_arbitrage_in_tx(cursor, args.tx_id)
        if arb:
            print(f"\n{'='*80}")
            print("ARBITRAGE DETECTED")
            print(f"{'='*80}")
            print(f"Wallet: {arb['wallet']}")
            print(f"Swaps: {arb['swap_count']}, Pools: {arb['pools_used']}")
            print(f"Tokens: {arb['tokens_involved']}")
            print(f"Profit: {arb['profit_amount']:,.9f} {arb['profit_token']} ({arb['profit_pct']:.4f}%)")
            print(f"\nNet flows:")
            for token, flow in arb['net_flows'].items():
                direction = '+' if flow > 0 else ''
                print(f"  {token}: {direction}{flow:,.9f}")
        else:
            print("No arbitrage pattern detected in this transaction")

        if args.json:
            with open(args.json, 'w') as f:
                json.dump(arb, f, indent=2, default=str)
            print(f"\nExported to {args.json}")

        cursor.close()
        conn.close()
        return

    # Arbitrage wallet analysis
    if args.arb_wallet:
        print(f"\nAnalyzing arbitrage activity for: {args.arb_wallet}")
        analysis = analyze_arbitrage_wallet(cursor, args.arb_wallet)

        print(f"\n{'='*80}")
        print("ARBITRAGE WALLET ANALYSIS")
        print(f"{'='*80}")
        print(f"Address: {analysis['address']}")
        print(f"Total swap transactions: {analysis['total_swap_txs']}")
        print(f"Arbitrage transactions: {analysis['arbitrage_txs']}")
        print(f"\nTotal profits:")
        for token, profit in analysis['total_profit'].items():
            print(f"  {token}: {profit:,.9f}")

        if analysis['recent_arbitrages']:
            print(f"\n{'='*80}")
            print("RECENT ARBITRAGES")
            print(f"{'='*80}")
            for i, arb in enumerate(analysis['recent_arbitrages'][:10]):
                print(f"\n{i+1}. {arb.get('signature', 'N/A')[:30]}...")
                print(f"   Profit: {arb['profit_amount']:,.9f} {arb['profit_token']} ({arb['profit_pct']:.4f}%)")
                print(f"   Swaps: {arb['swap_count']}, Pools: {arb['pools_used']}")

        if args.json:
            with open(args.json, 'w') as f:
                json.dump(analysis, f, indent=2, default=str)
            print(f"\nExported to {args.json}")

        cursor.close()
        conn.close()
        return

    # Arbitrage detection mode
    if args.arbitrage:
        token_filter = args.token[0] if args.token else None
        print_arbitrage_report(cursor, token_filter, limit=100)

        if args.json:
            arbs = find_arbitrage_transactions(cursor, token_filter, limit=100)
            with open(args.json, 'w') as f:
                json.dump(arbs, f, indent=2, default=str)
            print(f"\nExported to {args.json}")

        cursor.close()
        conn.close()
        return

    # Timeline mode - skip graph building, query directly
    if args.timeline:
        print(f"\nFetching timeline for: {args.timeline}")
        if args.start_time or args.end_time:
            print(f"  Time filter: {args.start_time or 'any'} to {args.end_time or 'any'}")
        activities = get_wallet_timeline(cursor, args.timeline, args.token,
                                         args.start_time, args.end_time)
        print_wallet_timeline(activities, args.timeline)

        if args.json:
            export_timeline_json(activities, args.timeline, args.json)

        if args.gexf:
            print(f"\nBuilding timeline graph...")
            timeline_graph = build_timeline_graph(activities, args.timeline)
            print(f"Timeline graph: {timeline_graph.number_of_nodes()} nodes, {timeline_graph.number_of_edges()} edges")
            print(f"Exporting to GEXF: {args.gexf}...")
            # Sanitize for GEXF (DiGraph not MultiDiGraph)
            H = nx.DiGraph()
            for node, attrs in timeline_graph.nodes(data=True):
                clean = {'label': str(node)}  # Set label for Gephi
                clean.update({k: ('' if v is None else v) for k, v in attrs.items()})
                H.add_node(node, **clean)
            for u, v, attrs in timeline_graph.edges(data=True):
                clean = {k: ('' if v is None else v) for k, v in attrs.items()}
                H.add_edge(u, v, **clean)
            nx.write_gexf(H, args.gexf)
            print(f"Exported!")

        cursor.close()
        conn.close()
        return

    print("Building graph...")
    if args.start_time or args.end_time:
        print(f"  Time filter: {args.start_time or 'any'} to {args.end_time or 'any'}")
    G = build_graph(cursor, token_filter=args.token, address_filter=args.address,
                    start_time=args.start_time, end_time=args.end_time)
    print(f"Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    if args.path:
        print(f"\nBuilding path graph for: {args.path}")
        print(f"Depth: {args.depth}, Direction: {args.direction}")
        path_graph = build_path_graph(G, args.path, max_depth=args.depth, direction=args.direction)
        print_path_graph(G, path_graph, args.path)

        if args.json:
            export_path_graph(path_graph, args.path, args.json)
    elif args.address:
        print(f"\nAnalyzing address: {args.address}")
        analysis = analyze_address(G, args.address)
        print(json.dumps(analysis, indent=2, default=str))
    else:
        print_wash_report(G, cursor)

    if args.json and not args.path:
        results = {
            'high_freq_pairs': find_high_frequency_pairs(G, min_edges=5),
            'rapid_roundtrips': find_rapid_roundtrips(G, max_seconds=300),
            'clipping_suspects': find_clipping_suspects(G, cursor),
            'cycle_count': len(find_wash_cycles(G, max_length=4))
        }
        with open(args.json, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nResults exported to {args.json}")

    if args.gexf:
        print(f"\nExporting to GEXF: {args.gexf}...")
        clean_graph = sanitize_for_gexf(G)
        nx.write_gexf(clean_graph, args.gexf)
        print(f"Exported! Open in Gephi for visualization.")

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
