#!/usr/bin/env python3
"""
Guide to NetworkX - Export tx_guide edges to NetworkX MultiDiGraph
Creates a graph ready for forensic analysis with node attributes and edge metadata.

Usage:
    python guide-to-networkx.py                    # Export all edges
    python guide-to-networkx.py --token SOLTIT    # Filter by token symbol
    python guide-to-networkx.py --address J2UXqJ1HYRwjCviLEqkY9mhoEPJ5UnoHEnbeijCweREV  # Filter by address
    python guide-to-networkx.py --limit 10000      # Limit edges
    python guide-to-networkx.py --output graph.pkl # Save to pickle file
    python guide-to-networkx.py --json graph.json  # Export to JSON with full data
    python guide-to-networkx.py --gexf graph.gexf  # Export for Gephi
"""

import argparse
import json
import pickle
from datetime import datetime, timezone
from typing import Optional, Any

# MySQL connector
try:
    import mysql.connector
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

# NetworkX
try:
    import networkx as nx
    HAS_NX = True
except ImportError:
    HAS_NX = False


def build_graph(cursor, token_filter: Optional[str] = None,
                address_filter: Optional[str] = None,
                limit: int = 0) -> nx.MultiDiGraph:
    """Build NetworkX MultiDiGraph from tx_guide edges"""

    G = nx.MultiDiGraph()

    # Build query
    query = """
        SELECT
            g.id as edge_id,
            fa.address as from_address,
            fa.address_type as from_type,
            fa.label as from_label,
            fa.funded_by_address_id,
            ta.address as to_address,
            ta.address_type as to_type,
            ta.label as to_label,
            t.signature as tx_signature,
            g.block_time,
            tk.token_symbol,
            mint.address as token_mint,
            g.amount,
            g.decimals,
            gt.type_code as edge_type,
            gt.category as edge_category,
            gt.risk_weight
        FROM tx_guide g
        JOIN tx_address fa ON g.from_address_id = fa.id
        JOIN tx_address ta ON g.to_address_id = ta.id
        JOIN tx t ON g.tx_id = t.id
        JOIN tx_guide_type gt ON g.edge_type_id = gt.id
        LEFT JOIN tx_token tk ON g.token_id = tk.id
        LEFT JOIN tx_address mint ON tk.mint_address_id = mint.id
        WHERE 1=1
    """
    params = []

    if token_filter:
        query += " AND (tk.token_symbol LIKE %s OR mint.address = %s)"
        params.extend([f'%{token_filter}%', token_filter])

    if address_filter:
        query += " AND (fa.address = %s OR ta.address = %s)"
        params.extend([address_filter, address_filter])

    query += " ORDER BY g.block_time ASC"

    if limit > 0:
        query += f" LIMIT {limit}"

    cursor.execute(query, params)

    # Track unique addresses for node creation
    nodes_seen = set()
    edge_count = 0

    for row in cursor.fetchall():
        (edge_id, from_addr, from_type, from_label, from_funder_id,
         to_addr, to_type, to_label, tx_sig, block_time,
         token_symbol, token_mint, amount, decimals,
         edge_type, edge_category, risk_weight) = row

        # Add from node
        if from_addr not in nodes_seen:
            G.add_node(from_addr,
                       address_type=from_type,
                       label=from_label,
                       has_funder=from_funder_id is not None)
            nodes_seen.add(from_addr)

        # Add to node
        if to_addr not in nodes_seen:
            G.add_node(to_addr,
                       address_type=to_type,
                       label=to_label)
            nodes_seen.add(to_addr)

        # Calculate human-readable amount
        human_amount = None
        if amount is not None and decimals is not None:
            human_amount = amount / (10 ** decimals)

        # Convert block_time to UTC string
        block_time_utc = datetime.fromtimestamp(block_time, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if block_time else ''

        # Add edge
        G.add_edge(from_addr, to_addr,
                   edge_id=edge_id,
                   tx_signature=tx_sig,
                   block_time=block_time,
                   block_time_utc=block_time_utc,
                   token_symbol=token_symbol or 'SOL',
                   token_mint=token_mint,
                   amount_raw=amount,
                   amount=human_amount,
                   decimals=decimals,
                   edge_type=edge_type,
                   edge_category=edge_category,
                   risk_weight=risk_weight)

        edge_count += 1

    print(f"Built graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    return G


def add_funding_data(G: nx.MultiDiGraph, cursor):
    """Add funding relationship data to nodes"""

    # Get funding data for all addresses in graph
    addresses = list(G.nodes())
    if not addresses:
        return

    placeholders = ','.join(['%s'] * len(addresses))
    cursor.execute(f"""
        SELECT
            w.address as wallet,
            f.address as funder,
            w.funding_amount / 1e9 as funding_sol,
            w.first_seen_block_time
        FROM tx_address w
        LEFT JOIN tx_address f ON w.funded_by_address_id = f.id
        WHERE w.address IN ({placeholders})
    """, addresses)

    funded_count = 0
    for row in cursor.fetchall():
        wallet, funder, funding_sol, first_seen = row
        if wallet in G.nodes:
            G.nodes[wallet]['funder'] = funder
            G.nodes[wallet]['funding_sol'] = funding_sol
            G.nodes[wallet]['first_seen'] = first_seen
            if funder:
                funded_count += 1

    print(f"Added funding data: {funded_count} wallets have identified funders")


def print_graph_summary(G: nx.MultiDiGraph):
    """Print summary statistics for the graph"""

    print(f"\n{'='*80}")
    print("GRAPH SUMMARY")
    print(f"{'='*80}")
    print(f"Nodes: {G.number_of_nodes()}")
    print(f"Edges: {G.number_of_edges()}")
    print(f"Density: {nx.density(G):.6f}")

    # Node type breakdown
    print(f"\nNode Types:")
    type_counts = {}
    for node, data in G.nodes(data=True):
        t = data.get('address_type', 'unknown')
        type_counts[t] = type_counts.get(t, 0) + 1
    for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {t}: {c}")

    # Edge type breakdown
    print(f"\nEdge Types:")
    edge_counts = {}
    for u, v, data in G.edges(data=True):
        t = data.get('edge_type', 'unknown')
        edge_counts[t] = edge_counts.get(t, 0) + 1
    for t, c in sorted(edge_counts.items(), key=lambda x: -x[1]):
        print(f"  {t}: {c}")

    # Top nodes by degree
    print(f"\nTop 10 Nodes by Degree:")
    degrees = sorted(G.degree(), key=lambda x: -x[1])[:10]
    for addr, deg in degrees:
        label = G.nodes[addr].get('label', '')
        funder = G.nodes[addr].get('funder', '')
        label_str = f" [{label}]" if label else ""
        funder_str = f"\n      funded by: {funder}" if funder else ""
        print(f"  {addr} : {deg}{label_str}{funder_str}")

    # Nodes with common funders
    print(f"\nFunder Analysis:")
    funder_counts = {}
    for node, data in G.nodes(data=True):
        funder = data.get('funder')
        if funder:
            funder_counts[funder] = funder_counts.get(funder, 0) + 1

    common_funders = [(f, c) for f, c in funder_counts.items() if c > 1]
    if common_funders:
        print(f"  Common funders (funded >1 wallet in graph):")
        for funder, count in sorted(common_funders, key=lambda x: -x[1])[:10]:
            print(f"    {funder} funded {count} wallets")
    else:
        print(f"  No common funders detected in graph")


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


def export_to_json(G: nx.MultiDiGraph, filepath: str):
    """Export graph to JSON format with full data"""

    def serialize_value(val: Any) -> Any:
        """Convert values to JSON-serializable types"""
        if val is None:
            return None
        if isinstance(val, (int, float, str, bool)):
            return val
        if isinstance(val, datetime):
            return val.isoformat()
        return str(val)

    # Build JSON structure
    data = {
        "metadata": {
            "nodes": G.number_of_nodes(),
            "edges": G.number_of_edges(),
            "density": nx.density(G),
            "exported_at": datetime.utcnow().isoformat() + "Z"
        },
        "nodes": [],
        "edges": [],
        "funders": {}
    }

    # Export nodes
    for node, attrs in G.nodes(data=True):
        node_data = {
            "address": node,
            "address_type": attrs.get('address_type'),
            "label": attrs.get('label'),
            "funder": attrs.get('funder'),
            "funding_sol": attrs.get('funding_sol'),
            "first_seen": attrs.get('first_seen'),
            "degree": G.degree(node),
            "in_degree": G.in_degree(node),
            "out_degree": G.out_degree(node)
        }
        data["nodes"].append(node_data)

        # Track funders
        funder = attrs.get('funder')
        if funder:
            if funder not in data["funders"]:
                data["funders"][funder] = []
            data["funders"][funder].append(node)

    # Export edges
    for u, v, key, attrs in G.edges(keys=True, data=True):
        edge_data = {
            "from": u,
            "to": v,
            "edge_key": key,
            "edge_id": attrs.get('edge_id'),
            "tx_signature": attrs.get('tx_signature'),
            "block_time": attrs.get('block_time'),
            "token_symbol": attrs.get('token_symbol'),
            "token_mint": attrs.get('token_mint'),
            "amount": serialize_value(attrs.get('amount')),
            "amount_raw": attrs.get('amount_raw'),
            "decimals": attrs.get('decimals'),
            "edge_type": attrs.get('edge_type'),
            "edge_category": attrs.get('edge_category'),
            "risk_weight": attrs.get('risk_weight')
        }
        data["edges"].append(edge_data)

    # Sort funders by wallet count
    data["funders_summary"] = [
        {"funder": f, "wallets_funded": len(w), "wallets": w}
        for f, w in sorted(data["funders"].items(), key=lambda x: -len(x[1]))
        if len(w) > 1
    ]
    del data["funders"]

    # Write JSON
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"  Exported {len(data['nodes'])} nodes, {len(data['edges'])} edges")
    print(f"  Common funders: {len(data['funders_summary'])}")


def main():
    parser = argparse.ArgumentParser(description='Export tx_guide to NetworkX MultiDiGraph')
    parser.add_argument('--token', help='Filter by token symbol or mint address')
    parser.add_argument('--address', help='Filter by address involvement')
    parser.add_argument('--limit', type=int, default=0, help='Limit number of edges')
    parser.add_argument('--output', '-o', help='Output pickle file (default: prints summary)')
    parser.add_argument('--gexf', help='Export to GEXF format (for Gephi)')
    parser.add_argument('--json', help='Export to JSON format with full data')
    parser.add_argument('--db-host', default='localhost', help='MySQL host')
    parser.add_argument('--db-port', type=int, default=3396, help='MySQL port')
    parser.add_argument('--db-user', default='root', help='MySQL user')
    parser.add_argument('--db-pass', default='rootpassword', help='MySQL password')
    parser.add_argument('--db-name', default='t16o_db', help='MySQL database')

    args = parser.parse_args()

    if not HAS_MYSQL:
        print("Error: mysql-connector-python not installed")
        return 1

    if not HAS_NX:
        print("Error: networkx not installed")
        print("Install with: pip install networkx")
        return 1

    print(f"Guide to NetworkX - Graph Export")
    print(f"{'='*60}")

    # Connect to DB
    print(f"Connecting to MySQL {args.db_host}:{args.db_port}/{args.db_name}...")
    conn = mysql.connector.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_pass,
        database=args.db_name
    )
    cursor = conn.cursor()

    # Build graph
    print(f"\nBuilding graph...")
    if args.token:
        print(f"  Token filter: {args.token}")
    if args.address:
        print(f"  Address filter: {args.address}")
    if args.limit:
        print(f"  Limit: {args.limit}")

    G = build_graph(cursor, args.token, args.address, args.limit)

    # Add funding data
    print(f"\nAdding funding relationships...")
    add_funding_data(G, cursor)

    # Print summary
    print_graph_summary(G)

    # Save outputs
    if args.output:
        print(f"\nSaving to {args.output}...")
        with open(args.output, 'wb') as f:
            pickle.dump(G, f)
        print(f"  Saved!")

    if args.gexf:
        print(f"\nExporting to GEXF: {args.gexf}...")
        clean_graph = sanitize_for_gexf(G)
        nx.write_gexf(clean_graph, args.gexf)
        print(f"  Exported! Open in Gephi for visualization.")

    if args.json:
        print(f"\nExporting to JSON: {args.json}...")
        export_to_json(G, args.json)
        print(f"  Saved!")

    cursor.close()
    conn.close()

    # Return graph for interactive use
    return G


if __name__ == '__main__':
    G = main()
