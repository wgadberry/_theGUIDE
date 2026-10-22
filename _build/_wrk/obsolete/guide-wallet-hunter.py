#!/usr/bin/env python3
"""
Guide Wallet Hunter - Deep forensic investigation of wallet activity and contacts

Traces all activities for a wallet address, tags every contact, then investigates
common token activity between the suspect and contacts to uncover coordination.

Usage:
    python guide-wallet-hunter.py <wallet_address>
    python guide-wallet-hunter.py <wallet_address> --deep              # Full investigation
    python guide-wallet-hunter.py <wallet_address> --json output.json
    python guide-wallet-hunter.py <wallet_address> --gexf network.gexf
"""

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional, Dict, List, Set, Tuple

import mysql.connector
import networkx as nx

# RabbitMQ (optional)
try:
    import pika
    HAS_PIKA = True
except ImportError:
    HAS_PIKA = False


def connect_db(host='localhost', port=3396, user='root', password='rootpassword', database='t16o_db'):
    return mysql.connector.connect(
        host=host, port=port, user=user, password=password, database=database
    )


def get_wallet_activity(cursor, wallet_address: str) -> List[Dict]:
    """Get all tx_guide edges involving this wallet, chronologically"""

    query = """
        SELECT
            g.id as edge_id,
            g.block_time,
            t.signature as tx_signature,
            gt.type_code as edge_type,
            gt.category as edge_category,
            fa.address as from_address,
            fa.address_type as from_type,
            fa.label as from_label,
            ta.address as to_address,
            ta.address_type as to_type,
            ta.label as to_label,
            tk.token_symbol,
            mint.address as token_mint,
            g.amount,
            g.decimals,
            CASE
                WHEN fa.address = %s THEN 'OUT'
                WHEN ta.address = %s THEN 'IN'
                ELSE 'UNKNOWN'
            END as direction
        FROM tx_guide g
        JOIN tx_guide_type gt ON g.edge_type_id = gt.id
        JOIN tx_address fa ON g.from_address_id = fa.id
        JOIN tx_address ta ON g.to_address_id = ta.id
        JOIN tx t ON g.tx_id = t.id
        LEFT JOIN tx_token tk ON g.token_id = tk.id
        LEFT JOIN tx_address mint ON tk.mint_address_id = mint.id
        WHERE fa.address = %s OR ta.address = %s
        ORDER BY g.block_time ASC
    """

    cursor.execute(query, [wallet_address, wallet_address, wallet_address, wallet_address])

    activities = []
    for row in cursor.fetchall():
        (edge_id, block_time, tx_sig, edge_type, edge_category,
         from_addr, from_type, from_label, to_addr, to_type, to_label,
         token_symbol, token_mint, amount, decimals, direction) = row

        human_amount = amount / (10 ** decimals) if amount and decimals else 0
        block_time_utc = datetime.fromtimestamp(block_time, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if block_time else ''

        # Determine the counterparty (the other address)
        if direction == 'OUT':
            counterparty = to_addr
            counterparty_type = to_type
            counterparty_label = to_label
        else:
            counterparty = from_addr
            counterparty_type = from_type
            counterparty_label = from_label

        activities.append({
            'edge_id': edge_id,
            'block_time': block_time,
            'block_time_utc': block_time_utc,
            'tx_signature': tx_sig,
            'direction': direction,
            'edge_type': edge_type,
            'edge_category': edge_category,
            'from_address': from_addr,
            'from_type': from_type,
            'from_label': from_label,
            'to_address': to_addr,
            'to_type': to_type,
            'to_label': to_label,
            'counterparty': counterparty,
            'counterparty_type': counterparty_type,
            'counterparty_label': counterparty_label,
            'token_symbol': token_symbol or 'SOL',
            'token_mint': token_mint,
            'amount': human_amount,
            'amount_raw': amount,
            'decimals': decimals
        })

    return activities


def get_wallet_tokens(cursor, wallet_address: str) -> Dict[str, Dict]:
    """Get all tokens a wallet has interacted with, with stats"""

    query = """
        SELECT
            tk.token_symbol,
            mint.address as token_mint,
            gt.type_code as edge_type,
            COUNT(*) as tx_count,
            SUM(g.amount) / POW(10, MAX(g.decimals)) as total_volume,
            MIN(g.block_time) as first_tx,
            MAX(g.block_time) as last_tx
        FROM tx_guide g
        JOIN tx_guide_type gt ON g.edge_type_id = gt.id
        JOIN tx_address fa ON g.from_address_id = fa.id
        JOIN tx_address ta ON g.to_address_id = ta.id
        LEFT JOIN tx_token tk ON g.token_id = tk.id
        LEFT JOIN tx_address mint ON tk.mint_address_id = mint.id
        WHERE (fa.address = %s OR ta.address = %s)
          AND mint.address IS NOT NULL
        GROUP BY tk.token_symbol, mint.address, gt.type_code
    """

    cursor.execute(query, [wallet_address, wallet_address])

    tokens = {}
    for row in cursor.fetchall():
        symbol, mint, edge_type, tx_count, volume, first_tx, last_tx = row

        if mint not in tokens:
            tokens[mint] = {
                'symbol': symbol,
                'mint': mint,
                'edge_types': {},
                'total_txs': 0,
                'total_volume': 0,
                'first_tx': first_tx,
                'last_tx': last_tx
            }

        tokens[mint]['edge_types'][edge_type] = {
            'count': tx_count,
            'volume': float(volume) if volume else 0
        }
        tokens[mint]['total_txs'] += tx_count
        tokens[mint]['total_volume'] += float(volume) if volume else 0

        if first_tx and (tokens[mint]['first_tx'] is None or first_tx < tokens[mint]['first_tx']):
            tokens[mint]['first_tx'] = first_tx
        if last_tx and (tokens[mint]['last_tx'] is None or last_tx > tokens[mint]['last_tx']):
            tokens[mint]['last_tx'] = last_tx

    return tokens


def get_token_traders(cursor, token_mint: str) -> List[Dict]:
    """Get all wallets that traded a specific token"""

    query = """
        SELECT
            a.address,
            a.address_type,
            a.label,
            gt.type_code as edge_type,
            COUNT(*) as tx_count,
            SUM(g.amount) / POW(10, MAX(g.decimals)) as total_volume,
            MIN(g.block_time) as first_tx,
            MAX(g.block_time) as last_tx
        FROM tx_guide g
        JOIN tx_guide_type gt ON g.edge_type_id = gt.id
        JOIN tx_address fa ON g.from_address_id = fa.id
        JOIN tx_address ta ON g.to_address_id = ta.id
        JOIN tx_token tk ON g.token_id = tk.id
        JOIN tx_address mint ON tk.mint_address_id = mint.id
        JOIN tx_address a ON (a.id = g.from_address_id OR a.id = g.to_address_id)
        WHERE mint.address = %s
          AND a.address_type IN ('wallet', 'unknown')
        GROUP BY a.address, a.address_type, a.label, gt.type_code
        ORDER BY total_volume DESC
    """

    cursor.execute(query, [token_mint])

    traders = {}
    for row in cursor.fetchall():
        addr, addr_type, label, edge_type, tx_count, volume, first_tx, last_tx = row

        if addr not in traders:
            traders[addr] = {
                'address': addr,
                'address_type': addr_type,
                'label': label,
                'edge_types': {},
                'total_txs': 0,
                'total_volume': 0,
                'first_tx': first_tx,
                'last_tx': last_tx
            }

        traders[addr]['edge_types'][edge_type] = {
            'count': tx_count,
            'volume': float(volume) if volume else 0
        }
        traders[addr]['total_txs'] += tx_count
        traders[addr]['total_volume'] += float(volume) if volume else 0

    return list(traders.values())


def get_wallet_funding(cursor, wallet_address: str) -> Optional[Dict]:
    """Get funding info for this wallet"""

    cursor.execute("""
        SELECT
            w.first_seen_block_time,
            w.funding_amount,
            f.address as funder_address,
            f.label as funder_label,
            ft.signature as funding_tx
        FROM tx_address w
        LEFT JOIN tx_address f ON w.funded_by_address_id = f.id
        LEFT JOIN tx ft ON w.funding_tx_id = ft.id
        WHERE w.address = %s
    """, (wallet_address,))

    row = cursor.fetchone()
    if row:
        first_seen, funding_amount, funder, funder_label, funding_tx = row
        first_seen_utc = datetime.fromtimestamp(first_seen, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if first_seen else ''
        return {
            'first_seen': first_seen,
            'first_seen_utc': first_seen_utc,
            'funding_sol': funding_amount / 1e9 if funding_amount else None,
            'funder': funder,
            'funder_label': funder_label,
            'funding_tx': funding_tx
        }
    return None


def extract_contacts(wallet_address: str, activities: List[Dict]) -> Dict[str, Dict]:
    """Extract unique contacts from activities"""

    contacts = {}

    for act in activities:
        cp = act['counterparty']
        if cp == wallet_address:
            continue

        if cp not in contacts:
            contacts[cp] = {
                'address': cp,
                'address_type': act['counterparty_type'],
                'label': act['counterparty_label'],
                'interactions': 0,
                'in_count': 0,
                'out_count': 0,
                'tokens_touched': set(),
                'first_contact': act['block_time'],
                'last_contact': act['block_time'],
                'tx_signatures': set()
            }

        contacts[cp]['interactions'] += 1
        contacts[cp]['tx_signatures'].add(act['tx_signature'])

        if act['direction'] == 'IN':
            contacts[cp]['in_count'] += 1
        else:
            contacts[cp]['out_count'] += 1

        if act['token_mint']:
            contacts[cp]['tokens_touched'].add(act['token_mint'])

        if act['block_time'] < contacts[cp]['first_contact']:
            contacts[cp]['first_contact'] = act['block_time']
        if act['block_time'] > contacts[cp]['last_contact']:
            contacts[cp]['last_contact'] = act['block_time']

    return contacts


def get_wallet_activity_count(cursor, wallet_address: str) -> int:
    """Quick count of wallet's activity"""
    cursor.execute("""
        SELECT COUNT(*) FROM tx_guide g
        JOIN tx_address fa ON g.from_address_id = fa.id
        JOIN tx_address ta ON g.to_address_id = ta.id
        WHERE fa.address = %s OR ta.address = %s
    """, [wallet_address, wallet_address])
    return cursor.fetchone()[0]


def investigate_deep(cursor, wallet_address: str, activities: List[Dict],
                     contacts: Dict[str, Dict], max_contact_activity: int = 1000) -> Dict:
    """Deep investigation: find common tokens and expand network"""

    print(f"\n{'='*100}")
    print("DEEP INVESTIGATION")
    print(f"{'='*100}")

    # Get suspect's tokens
    print(f"\nAnalyzing suspect's token activity...")
    suspect_tokens = get_wallet_tokens(cursor, wallet_address)
    print(f"  Suspect traded {len(suspect_tokens)} unique tokens")

    # For each contact (wallet type only), get their tokens
    wallet_contacts = {k: v for k, v in contacts.items()
                       if v['address_type'] in ('wallet', 'unknown')}

    print(f"\nAnalyzing {len(wallet_contacts)} wallet contacts (max activity: {max_contact_activity})...")

    contact_tokens = {}
    common_tokens = defaultdict(list)  # token_mint -> list of contacts who also traded it
    skipped_high_activity = []

    for i, (addr, contact) in enumerate(wallet_contacts.items()):
        # Check activity count first
        activity_count = get_wallet_activity_count(cursor, addr)
        if activity_count > max_contact_activity:
            print(f"  Skipping {addr[:20]}... ({activity_count} edges - likely pool/whale)")
            skipped_high_activity.append({'address': addr, 'activity': activity_count})
            continue

        tokens = get_wallet_tokens(cursor, addr)
        contact_tokens[addr] = tokens

        # Find common tokens
        for mint in tokens:
            if mint in suspect_tokens:
                common_tokens[mint].append({
                    'address': addr,
                    'contact_data': contact,
                    'token_data': tokens[mint]
                })

        if (i + 1) % 5 == 0:
            print(f"  Processed {i+1}/{len(wallet_contacts)} contacts...")

    if skipped_high_activity:
        print(f"\n  Skipped {len(skipped_high_activity)} high-activity wallets")

    # Report common tokens
    print(f"\n{'='*100}")
    print("COMMON TOKEN ANALYSIS (tokens traded by both suspect and contacts)")
    print(f"{'='*100}")

    # Exclude SOL/WSOL - they're noise
    excluded_tokens = {
        'So11111111111111111111111111111111111111111',   # Native SOL
        'So11111111111111111111111111111111111111112',   # Wrapped SOL variant
    }
    excluded_symbols = {'SOL', 'WSOL'}

    suspicious_tokens = {}

    for mint, traders in sorted(common_tokens.items(), key=lambda x: -len(x[1])):
        suspect_data = suspect_tokens[mint]
        symbol = suspect_data['symbol'] or mint[:16]

        # Skip SOL/WSOL
        if mint in excluded_tokens or symbol in excluded_symbols:
            print(f"\n  [SKIPPED] {symbol} - native currency, not useful for investigation")
            continue

        print(f"\n  TOKEN: {symbol}")
        print(f"  Mint:  {mint}")
        print(f"  Suspect activity: {suspect_data['total_txs']} txs, {suspect_data['total_volume']:,.4f} volume")
        print(f"  Contacts who also traded: {len(traders)}")

        if len(traders) >= 1:  # Flag if any contact also traded this token
            suspicious_tokens[mint] = {
                'symbol': symbol,
                'mint': mint,
                'suspect_data': suspect_data,
                'contacts_trading': traders
            }

            for t in traders[:5]:
                first_utc = datetime.fromtimestamp(t['token_data']['first_tx'], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if t['token_data']['first_tx'] else ''
                print(f"    - {t['address']}")
                print(f"      {t['token_data']['total_txs']} txs, {t['token_data']['total_volume']:,.4f} vol, first: {first_utc}")

            if len(traders) > 5:
                print(f"    ... and {len(traders) - 5} more contacts")

    # For most suspicious tokens, get ALL traders
    print(f"\n{'='*100}")
    print("EXPANDED TOKEN INVESTIGATION")
    print(f"{'='*100}")

    token_networks = {}

    # Sort by number of contacts trading (most suspicious first)
    top_suspicious = sorted(suspicious_tokens.items(), key=lambda x: -len(x[1]['contacts_trading']))[:5]

    for mint, token_info in top_suspicious:
        symbol = token_info['symbol']
        print(f"\n  Expanding investigation for {symbol} ({mint[:20]}...)...")

        all_traders = get_token_traders(cursor, mint)
        token_networks[mint] = {
            'symbol': symbol,
            'mint': mint,
            'total_traders': len(all_traders),
            'traders': all_traders
        }

        print(f"  Found {len(all_traders)} total wallets trading this token")

        # Show top traders
        print(f"  Top traders:")
        for trader in all_traders[:10]:
            label = f" [{trader['label']}]" if trader.get('label') else ""
            is_suspect = " [SUSPECT]" if trader['address'] == wallet_address else ""
            is_contact = " [CONTACT]" if trader['address'] in contacts else ""
            print(f"    {trader['address']}{label}{is_suspect}{is_contact}")
            print(f"      {trader['total_txs']} txs, {trader['total_volume']:,.4f} volume")

    return {
        'suspect_tokens': suspect_tokens,
        'contact_tokens': contact_tokens,
        'common_tokens': dict(common_tokens),
        'suspicious_tokens': suspicious_tokens,
        'token_networks': token_networks
    }


def build_investigation_graph(wallet_address: str, activities: List[Dict],
                              contacts: Dict[str, Dict],
                              investigation: Optional[Dict] = None) -> nx.DiGraph:
    """Build comprehensive investigation graph"""

    G = nx.DiGraph()
    G.add_node(wallet_address, node_type='suspect', label='SUSPECT')

    # Add contacts
    for addr, contact in contacts.items():
        first_utc = datetime.fromtimestamp(contact['first_contact'], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if contact['first_contact'] else ''
        last_utc = datetime.fromtimestamp(contact['last_contact'], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if contact['last_contact'] else ''

        # Check if this contact shares tokens with suspect
        shared_tokens = []
        if investigation and addr in investigation.get('contact_tokens', {}):
            for mint in investigation['contact_tokens'][addr]:
                if mint in investigation.get('suspect_tokens', {}):
                    shared_tokens.append(mint)

        G.add_node(addr,
                   node_type='contact',
                   address_type=contact['address_type'],
                   label=contact['label'] or '',
                   interactions=contact['interactions'],
                   first_contact=contact['first_contact'],
                   first_contact_utc=first_utc,
                   last_contact=contact['last_contact'],
                   last_contact_utc=last_utc,
                   shared_token_count=len(shared_tokens),
                   shared_tokens=','.join(shared_tokens[:5]))

        # Edge from suspect to contact or vice versa
        if contact['out_count'] > 0:
            G.add_edge(wallet_address, addr,
                       direction='OUT',
                       count=contact['out_count'])
        if contact['in_count'] > 0:
            G.add_edge(addr, wallet_address,
                       direction='IN',
                       count=contact['in_count'])

    # Add expanded network from token investigation
    if investigation and 'token_networks' in investigation:
        for mint, network in investigation['token_networks'].items():
            # Add token node
            G.add_node(mint,
                       node_type='token',
                       label=network['symbol'],
                       total_traders=network['total_traders'])

            # Connect traders to token
            for trader in network['traders']:
                if trader['address'] not in G.nodes:
                    G.add_node(trader['address'],
                               node_type='network_wallet',
                               address_type=trader['address_type'],
                               label=trader.get('label', ''))

                G.add_edge(trader['address'], mint,
                           edge_type='trades',
                           volume=trader['total_volume'],
                           tx_count=trader['total_txs'])

    return G


def print_timeline(wallet_address: str, activities: List[Dict], funding: Optional[Dict]):
    """Print chronological timeline"""

    print(f"\n{'='*100}")
    print(f"WALLET HUNTER: {wallet_address}")
    print(f"{'='*100}")

    # Funding info
    if funding:
        print(f"\nFUNDING INFO:")
        print(f"  First seen:  {funding['first_seen_utc']}")
        if funding['funder']:
            print(f"  Funded by:   {funding['funder']}")
            if funding['funder_label']:
                print(f"               [{funding['funder_label']}]")
            print(f"  Amount:      {funding['funding_sol']:.6f} SOL")
            print(f"  Funding TX:  {funding['funding_tx']}")

    # Activity summary
    print(f"\nACTIVITY SUMMARY:")
    print(f"  Total activities: {len(activities)}")

    if activities:
        first = activities[0]
        last = activities[-1]
        print(f"  First activity:   {first['block_time_utc']}")
        print(f"  Last activity:    {last['block_time_utc']}")

        # Count by type
        type_counts = defaultdict(int)
        direction_counts = {'IN': 0, 'OUT': 0}
        for act in activities:
            type_counts[act['edge_type']] += 1
            direction_counts[act['direction']] += 1

        print(f"  Inbound:          {direction_counts['IN']}")
        print(f"  Outbound:         {direction_counts['OUT']}")
        print(f"\n  By type:")
        for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
            print(f"    {t}: {c}")

    # Timeline
    print(f"\n{'='*100}")
    print("TIMELINE")
    print(f"{'='*100}")

    current_date = None
    for act in activities:
        date_part = act['block_time_utc'][:10]

        if date_part != current_date:
            current_date = date_part
            print(f"\n--- {current_date} ---")

        time_part = act['block_time_utc'][11:]
        direction_arrow = "<<<" if act['direction'] == 'IN' else ">>>"
        cp_label = f" [{act['counterparty_label']}]" if act['counterparty_label'] else ""

        print(f"  {time_part} {direction_arrow} {act['edge_type']:20} {act['amount']:>18,.6f} {act['token_symbol']:8} {act['counterparty']}{cp_label}")


def print_contacts(wallet_address: str, contacts: Dict[str, Dict]):
    """Print contact analysis"""

    print(f"\n{'='*100}")
    print("CONTACTS (addresses this wallet touched)")
    print(f"{'='*100}")

    contact_list = list(contacts.values())
    contact_list.sort(key=lambda x: -x['interactions'])

    print(f"\nTotal unique contacts: {len(contact_list)}")

    # By address type
    type_counts = defaultdict(int)
    for c in contact_list:
        type_counts[c.get('address_type', 'unknown')] += 1

    print(f"\nBy address type:")
    for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {t}: {c}")

    # Top contacts
    print(f"\nTop 20 contacts by interaction count:")
    for i, contact in enumerate(contact_list[:20]):
        label = f" [{contact.get('label')}]" if contact.get('label') else ""
        addr_type = contact.get('address_type', 'unknown')
        interactions = contact.get('interactions', 0)
        first_utc = datetime.fromtimestamp(contact['first_contact'], tz=timezone.utc).strftime('%Y-%m-%d') if contact.get('first_contact') else ''
        last_utc = datetime.fromtimestamp(contact['last_contact'], tz=timezone.utc).strftime('%Y-%m-%d') if contact.get('last_contact') else ''

        print(f"\n  {i+1}. {contact['address']}")
        print(f"     Type: {addr_type}{label}")
        print(f"     Interactions: {interactions} ({first_utc} to {last_utc})")
        print(f"     In: {contact['in_count']} | Out: {contact['out_count']}")
        print(f"     Tokens: {len(contact['tokens_touched'])}")
        print(f"     TX Signatures: {len(contact['tx_signatures'])}")


def submit_signatures_to_queue(signatures: Set[str], queue_name: str = "tx.guide.signatures",
                               batch_size: int = 20, priority: int = 10,
                               rabbitmq_host: str = 'localhost',
                               rabbitmq_port: int = 5692, rabbitmq_user: str = 'admin',
                               rabbitmq_pass: str = 'admin123'):
    """Submit transaction signatures to RabbitMQ queue for further processing

    Batches signatures into groups of batch_size (default 20) to match guide-producer format.
    """

    if not HAS_PIKA:
        print("Error: pika not installed, cannot submit to queue")
        return 0

    try:
        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=credentials)
        )
        channel = connection.channel()

        # Declare queue with priority support
        channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments={'x-max-priority': 10}
        )

        # Batch signatures into groups of batch_size
        sig_list = list(signatures)
        batches_sent = 0

        for i in range(0, len(sig_list), batch_size):
            batch = sig_list[i:i + batch_size]
            message = {"signatures": batch}

            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(message).encode('utf-8'),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # persistent
                    content_type='application/json',
                    priority=priority
                )
            )
            batches_sent += 1

        connection.close()
        print(f"  Submitted {len(signatures)} signatures in {batches_sent} batches (priority {priority})")
        return len(signatures)

    except Exception as e:
        print(f"Error connecting to RabbitMQ at {rabbitmq_host}:{rabbitmq_port}: {e}")
        return 0


def collect_all_signatures(activities: List[Dict], contacts: Dict[str, Dict]) -> Set[str]:
    """Collect all transaction signatures from activities and contacts"""

    signatures = set()

    # From activities
    for act in activities:
        if act.get('tx_signature'):
            signatures.add(act['tx_signature'])

    # From contacts
    for contact in contacts.values():
        signatures.update(contact.get('tx_signatures', set()))

    return signatures


def sanitize_for_gexf(G: nx.DiGraph) -> nx.DiGraph:
    """Create copy with sanitized attributes for GEXF export"""
    H = nx.DiGraph()

    for node, attrs in G.nodes(data=True):
        clean = {'label': str(node)}
        for k, v in attrs.items():
            if v is None:
                clean[k] = ''
            elif isinstance(v, (dict, set, list)):
                clean[k] = json.dumps(v) if isinstance(v, dict) else ','.join(str(x) for x in v)
            else:
                clean[k] = v
        H.add_node(node, **clean)

    for u, v, attrs in G.edges(data=True):
        clean = {}
        for k, val in attrs.items():
            if val is None:
                clean[k] = ''
            elif isinstance(val, (dict, set, list)):
                clean[k] = json.dumps(val) if isinstance(val, dict) else ','.join(str(x) for x in val)
            else:
                clean[k] = val
        H.add_edge(u, v, **clean)

    return H


def export_json(wallet_address: str, activities: List[Dict], funding: Optional[Dict],
                contacts: Dict[str, Dict], investigation: Optional[Dict], filepath: str):
    """Export full investigation to JSON"""

    # Convert sets to lists for JSON serialization
    contacts_serializable = {}
    for addr, contact in contacts.items():
        c = contact.copy()
        c['tokens_touched'] = list(c['tokens_touched'])
        c['tx_signatures'] = list(c['tx_signatures'])
        contacts_serializable[addr] = c

    output = {
        'wallet': wallet_address,
        'funding': funding,
        'summary': {
            'total_activities': len(activities),
            'total_contacts': len(contacts),
            'first_activity': activities[0]['block_time_utc'] if activities else None,
            'last_activity': activities[-1]['block_time_utc'] if activities else None
        },
        'contacts': contacts_serializable,
        'timeline': activities
    }

    if investigation:
        # Serialize investigation data
        inv_serializable = {
            'suspect_tokens': investigation.get('suspect_tokens', {}),
            'common_tokens': {k: len(v) for k, v in investigation.get('common_tokens', {}).items()},
            'suspicious_tokens': {},
            'token_networks': {}
        }

        for mint, data in investigation.get('suspicious_tokens', {}).items():
            inv_serializable['suspicious_tokens'][mint] = {
                'symbol': data['symbol'],
                'suspect_data': data['suspect_data'],
                'contacts_trading_count': len(data['contacts_trading']),
                'contacts_trading': [t['address'] for t in data['contacts_trading']]
            }

        for mint, network in investigation.get('token_networks', {}).items():
            inv_serializable['token_networks'][mint] = {
                'symbol': network['symbol'],
                'total_traders': network['total_traders'],
                'traders': network['traders'][:50]  # Limit for JSON size
            }

        output['investigation'] = inv_serializable

    with open(filepath, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nJSON exported to {filepath}")


def main():
    parser = argparse.ArgumentParser(description='Deep wallet forensic investigation')
    parser.add_argument('wallet', help='Wallet address to investigate')
    parser.add_argument('--deep', action='store_true', help='Run deep investigation (analyze contact tokens)')
    parser.add_argument('--queue', action='store_true', help='Submit all signatures to RabbitMQ queue')
    parser.add_argument('--queue-name', default='tx.guide.signatures', help='Queue name (default: tx.guide.signatures)')
    parser.add_argument('--batch-size', type=int, default=20, help='Signatures per batch (default: 20)')
    parser.add_argument('--priority', type=int, default=10, help='Message priority 1-10 (default: 10)')
    parser.add_argument('--rabbitmq-host', default='localhost', help='RabbitMQ host')
    parser.add_argument('--rabbitmq-port', type=int, default=5692, help='RabbitMQ port (default: 5692)')
    parser.add_argument('--rabbitmq-user', default='admin', help='RabbitMQ username (default: admin)')
    parser.add_argument('--rabbitmq-pass', default='admin123', help='RabbitMQ password (default: admin123)')
    parser.add_argument('--json', help='Export to JSON file')
    parser.add_argument('--gexf', help='Export investigation graph to GEXF (Gephi)')
    parser.add_argument('--db-host', default='localhost')
    parser.add_argument('--db-port', type=int, default=3396)
    parser.add_argument('--db-user', default='root')
    parser.add_argument('--db-pass', default='rootpassword')
    parser.add_argument('--db-name', default='t16o_db')

    args = parser.parse_args()

    conn = connect_db(args.db_host, args.db_port, args.db_user, args.db_pass, args.db_name)
    cursor = conn.cursor()

    try:
        # Get funding info
        funding = get_wallet_funding(cursor, args.wallet)

        # Get all activity
        print(f"Fetching activity for {args.wallet}...")
        activities = get_wallet_activity(cursor, args.wallet)
        print(f"Found {len(activities)} activities")

        if not activities:
            print("No activity found for this wallet")
            return 0

        # Extract contacts
        contacts = extract_contacts(args.wallet, activities)

        # Print basic reports
        print_timeline(args.wallet, activities, funding)
        print_contacts(args.wallet, contacts)

        # Deep investigation
        investigation = None
        if args.deep:
            investigation = investigate_deep(cursor, args.wallet, activities, contacts)

        # Build graph
        G = build_investigation_graph(args.wallet, activities, contacts, investigation)

        # Exports
        if args.json:
            export_json(args.wallet, activities, funding, contacts, investigation, args.json)

        if args.gexf:
            print(f"\nExporting to GEXF: {args.gexf}...")
            clean_graph = sanitize_for_gexf(G)
            nx.write_gexf(clean_graph, args.gexf)
            print(f"Exported! Open in Gephi for visualization.")
            print(f"Graph contains {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")

        # Submit signatures to queue
        if args.queue:
            print(f"\n{'='*100}")
            print("QUEUE SUBMISSION")
            print(f"{'='*100}")

            all_signatures = collect_all_signatures(activities, contacts)
            print(f"Collected {len(all_signatures)} unique transaction signatures")

            if all_signatures:
                print(f"Submitting to queue '{args.queue_name}' with priority {args.priority}...")
                submitted = submit_signatures_to_queue(
                    all_signatures,
                    queue_name=args.queue_name,
                    batch_size=args.batch_size,
                    priority=args.priority,
                    rabbitmq_host=args.rabbitmq_host,
                    rabbitmq_port=args.rabbitmq_port,
                    rabbitmq_user=args.rabbitmq_user,
                    rabbitmq_pass=args.rabbitmq_pass
                )
                if submitted > 0:
                    print(f"Submitted {submitted} signatures to queue")
                else:
                    print("Failed to submit signatures - check RabbitMQ connection")

    finally:
        cursor.close()
        conn.close()

    print(f"\n{'='*100}")
    print("Done!")

    return 0


if __name__ == '__main__':
    exit(main())
