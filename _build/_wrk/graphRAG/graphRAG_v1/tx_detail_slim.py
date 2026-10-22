"""
tx_detail_slim.py -- Translation layer: Solscan detail/multi -> slim tx JSON

Derives edges from token_bal_change and sol_bal_change by pairing
senders (negative change) with receivers (positive change) per token
within each transaction. Single API call, no /actions/multi needed.

Usage:
    from tx_detail_slim import from_detail, from_detail_batch
"""


def _pair_edges(changes, token, decimals=0):
    """Pair senders with receivers for a single token within one transaction.

    Each decrease is matched to increases proportionally. For simple 1:1
    transfers this produces exact edges. For multi-party txs (swaps),
    it produces the dominant flow paths.
    """
    senders = []   # (owner, abs_amount)
    receivers = []  # (owner, amount)

    for c in changes:
        owner = c.get('owner', c.get('address', ''))
        amt = c.get('change_amount', 0)
        if isinstance(amt, str):
            amt = int(amt)
        if amt < 0:
            senders.append((owner, abs(amt)))
        elif amt > 0:
            receivers.append((owner, amt))

    edges = []
    # Simple case: 1 sender, 1+ receivers (or vice versa)
    if len(senders) == 1:
        src = senders[0][0]
        for dst, amt in receivers:
            if src and dst and src != dst:
                edges.append({
                    'from': src,
                    'to': dst,
                    'transfer_type': 'spl_transfer',
                    'token': token,
                    'amount': amt,
                    'decimals': decimals,
                })
    elif len(receivers) == 1:
        dst = receivers[0][0]
        for src, amt in senders:
            if src and dst and src != dst:
                edges.append({
                    'from': src,
                    'to': dst,
                    'transfer_type': 'spl_transfer',
                    'token': token,
                    'amount': amt,
                    'decimals': decimals,
                })
    else:
        # Multi-sender, multi-receiver: pair largest sender with largest receiver
        senders.sort(key=lambda x: x[1], reverse=True)
        receivers.sort(key=lambda x: x[1], reverse=True)
        used_recv = set()
        for src, s_amt in senders:
            for i, (dst, r_amt) in enumerate(receivers):
                if i in used_recv:
                    continue
                if src and dst and src != dst:
                    edges.append({
                        'from': src,
                        'to': dst,
                        'transfer_type': 'spl_transfer',
                        'token': token,
                        'amount': min(s_amt, r_amt),
                        'decimals': decimals,
                    })
                    used_recv.add(i)
                    break

    return edges


def from_detail(sig, detail, metadata=None):
    """Build slim JSON from a single Solscan detail/multi response.

    Derives edges from balance changes — no /actions/multi needed.

    Args:
        sig: Transaction signature.
        detail: Single tx dict from detail/multi data[].
        metadata: Top-level metadata dict from detail/multi response.

    Returns slim JSON dict or None.
    """
    if not detail:
        return None

    meta_tokens = (metadata or {}).get('tokens', {})

    signers = detail.get('signer', [])
    if isinstance(signers, str):
        signers = [signers]

    # SOL changes
    sol_changes = []
    sol_change_map = {}
    for sbc in (detail.get('sol_bal_change') or []):
        change = int(sbc.get('change_amount', 0))
        if change != 0:
            sol_changes.append({
                'address': sbc['address'],
                'pre_balance': int(sbc.get('pre_balance', 0)),
                'post_balance': int(sbc.get('post_balance', 0)),
                'change_amount': change,
            })
            sol_change_map[sbc['address']] = change

    # Token changes — group by mint for edge pairing
    token_changes = []
    tokens = {}
    by_mint = {}  # mint -> [change entries with owner]

    for tbc in (detail.get('token_bal_change') or []):
        mint = tbc.get('token_address', '')
        pre = int(tbc.get('pre_balance', 0))
        post = int(tbc.get('post_balance', 0))
        change = post - pre

        token_changes.append({
            'ata': tbc.get('address', ''),
            'owner': tbc.get('owner', ''),
            'token': mint,
            'decimals': tbc.get('decimals', 0),
            'pre_balance': pre,
            'post_balance': post,
            'change_amount': change,
            'event_type': tbc.get('change_type', 'inc' if change > 0 else 'dec'),
        })

        if mint and mint not in tokens:
            mt = meta_tokens.get(mint, {})
            tokens[mint] = {
                'symbol': mt.get('token_symbol', ''),
                'name': mt.get('token_name', ''),
                'icon': mt.get('token_icon', ''),
            }

        if change != 0 and mint:
            if mint not in by_mint:
                by_mint[mint] = {'changes': [], 'decimals': tbc.get('decimals', 0)}
            by_mint[mint]['changes'].append({
                'owner': tbc.get('owner', ''),
                'change_amount': change,
            })

    # Derive edges from balance change pairing
    edges = []
    for mint, info in by_mint.items():
        edges.extend(_pair_edges(info['changes'], mint, info['decimals']))

    # SOL transfer edges (exclude fee payer's fee-only change)
    fee = detail.get('fee', 0)
    sol_senders = []
    sol_receivers = []
    for sc in sol_changes:
        addr = sc['address']
        change = sc['change_amount']
        # Fee payer: adjust for fee
        if addr in set(signers):
            adjusted = change + fee  # add fee back to see the real transfer
            if adjusted < 0:
                sol_senders.append((addr, abs(adjusted)))
            elif adjusted > 0:
                sol_receivers.append((addr, adjusted))
        else:
            if change < 0:
                sol_senders.append((addr, abs(change)))
            elif change > 0:
                sol_receivers.append((addr, change))

    # Pair SOL edges (same logic as token edges)
    if sol_senders and sol_receivers:
        if len(sol_senders) == 1:
            src = sol_senders[0][0]
            for dst, amt in sol_receivers:
                if src != dst:
                    edges.append({'from': src, 'to': dst, 'transfer_type': 'sol_transfer',
                                  'token': 'So11111111111111111111111111111111111111112',
                                  'amount': amt, 'decimals': 9})
        elif len(sol_receivers) == 1:
            dst = sol_receivers[0][0]
            for src, amt in sol_senders:
                if src != dst:
                    edges.append({'from': src, 'to': dst, 'transfer_type': 'sol_transfer',
                                  'token': 'So11111111111111111111111111111111111111112',
                                  'amount': amt, 'decimals': 9})

    # Swap inference: if a signer sent token A and received token B in the
    # same tx, reclassify those edges as swap_in/swap_out and identify the pool.
    signer_set = set(signers)
    by_owner = {}
    for tc in token_changes:
        by_owner.setdefault(tc['owner'], []).append(tc)

    swap_pools = set()  # track identified pool addresses
    for signer in signers:
        signer_tc = by_owner.get(signer, [])
        sent_mints = {tc['token'] for tc in signer_tc if tc['change_amount'] < 0}
        recv_mints = {tc['token'] for tc in signer_tc if tc['change_amount'] > 0}

        if sent_mints and recv_mints and sent_mints != recv_mints:
            # It's a swap — find pool: who received what the signer sent?
            for mint in sent_mints:
                pool = None
                for owner, tcs in by_owner.items():
                    if owner == signer:
                        continue
                    if any(tc['token'] == mint and tc['change_amount'] > 0 for tc in tcs):
                        pool = owner
                        swap_pools.add(pool)
                        break

                # Reclassify edges: signer->pool for sent mint = swap_in
                for e in edges:
                    if e['from'] == signer and e['token'] == mint:
                        e['transfer_type'] = 'swap_in'
                        if pool:
                            e['pool'] = pool

            for mint in recv_mints:
                # Find who sent the received token to signer
                pool = None
                for owner, tcs in by_owner.items():
                    if owner == signer:
                        continue
                    if any(tc['token'] == mint and tc['change_amount'] < 0 for tc in tcs):
                        pool = owner
                        swap_pools.add(pool)
                        break

                # Reclassify edges: pool->signer for received mint = swap_out
                for e in edges:
                    if e['to'] == signer and e['token'] == mint:
                        e['transfer_type'] = 'swap_out'
                        if pool:
                            e['pool'] = pool

    # Nodes
    nodes = {}
    program_set = set(detail.get('programs_involved', []))
    all_addrs = set()

    for sc in sol_changes:
        all_addrs.add(sc['address'])
    for tc in token_changes:
        if tc['owner']: all_addrs.add(tc['owner'])
    for e in edges:
        if e['from']: all_addrs.add(e['from'])
        if e['to']: all_addrs.add(e['to'])

    for addr in all_addrs:
        if not addr:
            continue
        is_signer = addr in signer_set
        is_program = addr in program_set
        is_pool = addr in swap_pools
        nodes[addr] = {
            'label': None,
            'type': 'program' if is_program else ('pool' if is_pool else ('wallet' if is_signer else 'unknown')),
            'is_signer': is_signer,
            'is_program': is_program,
            'sol_change': sol_change_map.get(addr, 0),
        }

    return {
        'tx_hash': sig,
        'block_time': detail.get('block_time', 0),
        'signers': signers,
        'fee': detail.get('fee', 0),
        'priority_fee': detail.get('priority_fee', 0),
        'activity_type': None,  # Derived from render_one_line_action if needed
        'edges': edges,
        'token_changes': token_changes,
        'sol_changes': sol_changes,
        'nodes': nodes,
        'tokens': tokens,
    }


def from_detail_batch(detail_response):
    """Build slim JSON for a batch of transactions from detail/multi response.

    Args:
        detail_response: Full Solscan detail/multi response (with metadata).

    Returns dict: {sig: slim_json}.
    """
    metadata = detail_response.get('metadata', {})
    results = {}
    for item in (detail_response.get('data') or []):
        sig = item.get('tx_hash', '')
        if not sig:
            continue
        slim = from_detail(sig, item, metadata)
        if slim:
            results[sig] = slim
    return results


# -- CLI test --
if __name__ == '__main__':
    import json, os, requests, sys, io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    _dir = os.path.dirname(os.path.abspath(__file__))
    _config_path = os.path.join(os.path.dirname(_dir), 'guide-config.json')
    with open(_config_path) as f:
        cfg = json.load(f)

    sig = sys.argv[1] if len(sys.argv) > 1 else '36jtscWZKTEqa1ALtL9ivbGRQxhdVnG6q3a9k2qNPjWuite2erhnfJxqEc9Ai5M4mG6LyhEA7zPL9bhPrjuSu6cp'

    # Single call — detail/multi only
    resp = requests.get(f'{cfg["SOLSCAN_API"]}/transaction/detail/multi?tx[]={sig}',
        headers={'token': cfg['SOLSCAN_TOKEN']}, timeout=30).json()

    results = from_detail_batch(resp)
    slim = results.get(sig)

    if slim:
        print(f'edges: {len(slim["edges"])}  token_changes: {len(slim["token_changes"])}  '
              f'sol_changes: {len(slim["sol_changes"])}  nodes: {len(slim["nodes"])}')
        print(f'tokens: { {k: v["symbol"] or "?" for k, v in slim["tokens"].items()} }')
        print()
        for e in slim['edges']:
            print(f'  {e["transfer_type"]:14s} {e["from"][:16]}... -> {e["to"][:16]}...  amt={e["amount"]}  token={e["token"][:16]}...')
    else:
        print('No data')
