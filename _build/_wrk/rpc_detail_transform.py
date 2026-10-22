"""
rpc_detail_transform.py — Transform Solana RPC getTransaction (jsonParsed)
response into the Solscan /transaction/detail/multi format that detail_v2.py
expects.

This allows replacing Solscan detail API calls with direct Chainstack RPC
calls, eliminating Solscan API quota consumption for the detailer worker.

Usage:
    from rpc_detail_transform import rpc_to_solscan_detail

    # Single tx:
    rpc_response = session.post(rpc_url, json=payload).json()
    solscan_format = rpc_to_solscan_detail(signature, rpc_response)

    # Multi tx (batch of sigs):
    solscan_batch = rpc_batch_to_solscan_detail([(sig, rpc_resp), ...])
"""


def _transform_single(signature: str, rpc_result: dict) -> dict:
    """Transform one RPC getTransaction result dict into Solscan detail format.

    Args:
        signature: The transaction signature string.
        rpc_result: The 'result' value from RPC getTransaction response.

    Returns:
        dict matching Solscan /transaction/detail/multi data[] element shape.
    """
    if rpc_result is None:
        return None

    meta = rpc_result.get('meta', {})
    tx_msg = rpc_result.get('transaction', {}).get('message', {})
    account_keys = tx_msg.get('accountKeys', [])

    # Build address list (ordered by account index)
    addresses = [ak['pubkey'] for ak in account_keys]

    # --- signer (fee payer = first signer) ---
    signers = [ak['pubkey'] for ak in account_keys if ak.get('signer')]

    # --- sol_bal_change ---
    pre_balances = meta.get('preBalances', [])
    post_balances = meta.get('postBalances', [])
    sol_bal_change = []
    for i, addr in enumerate(addresses):
        if i < len(pre_balances) and i < len(post_balances):
            pre = pre_balances[i]
            post = post_balances[i]
            change = post - pre
            sol_bal_change.append({
                'address': addr,
                'pre_balance': str(pre),
                'post_balance': str(post),
                'change_amount': str(change),
            })

    # --- token_bal_change ---
    # Build lookup maps by accountIndex
    pre_tok_map = {}
    for tb in meta.get('preTokenBalances', []):
        pre_tok_map[tb['accountIndex']] = tb

    post_tok_map = {}
    for tb in meta.get('postTokenBalances', []):
        post_tok_map[tb['accountIndex']] = tb

    all_tok_indices = sorted(set(list(pre_tok_map.keys()) + list(post_tok_map.keys())))

    token_bal_change = []
    for idx in all_tok_indices:
        pre_tb = pre_tok_map.get(idx)
        post_tb = post_tok_map.get(idx)
        ref = pre_tb or post_tb  # whichever exists for metadata

        addr = addresses[idx] if idx < len(addresses) else None
        if addr is None:
            continue

        mint = ref.get('mint', '')
        owner = ref.get('owner', '')
        decimals = ref.get('uiTokenAmount', {}).get('decimals', 0)

        pre_amt_str = pre_tb['uiTokenAmount']['amount'] if pre_tb else '0'
        post_amt_str = post_tb['uiTokenAmount']['amount'] if post_tb else '0'

        pre_amt = int(pre_amt_str)
        post_amt = int(post_amt_str)
        change = post_amt - pre_amt

        # Solscan uses "inc" / "dec" for change_type.
        # Solscan quirk: zero-change entries are labeled "dec" (not "inc" or "").
        if change > 0:
            change_type = 'inc'
        else:
            change_type = 'dec'

        # Owner resolution: pre_owner from pre, post_owner from post
        pre_owner = pre_tb.get('owner', owner) if pre_tb else owner
        post_owner = post_tb.get('owner', owner) if post_tb else owner

        token_bal_change.append({
            'address': addr,
            'change_type': change_type,
            'change_amount': str(change),
            'decimals': decimals,
            'post_balance': str(post_amt),
            'pre_balance': str(pre_amt),
            'token_address': mint,
            'owner': owner,
            'post_owner': post_owner,
            'pre_owner': pre_owner,
        })

    # --- Build Solscan-shaped tx object ---
    return {
        'tx_hash': signature,
        'block_id': rpc_result.get('slot'),
        'block_time': rpc_result.get('blockTime'),
        'fee': meta.get('fee', 0),
        'signer': signers,
        'sol_bal_change': sol_bal_change,
        'token_bal_change': token_bal_change,
        'status': 1 if meta.get('err') is None else 0,
        'compute_units_consumed': meta.get('computeUnitsConsumed', 0),
        'version': rpc_result.get('version', 'legacy'),
    }


def rpc_to_solscan_detail(signature: str, rpc_response: dict) -> dict:
    """Transform a single RPC getTransaction response to Solscan detail format.

    Args:
        signature: Transaction signature.
        rpc_response: Full RPC JSON response (with 'result' key).

    Returns:
        dict in Solscan /transaction/detail/multi shape:
        {'success': True, 'data': [tx_dict]}
    """
    rpc_result = rpc_response.get('result')
    if rpc_result is None:
        return {'success': False, 'data': []}

    tx = _transform_single(signature, rpc_result)
    if tx is None:
        return {'success': False, 'data': []}

    return {'success': True, 'data': [tx]}


def rpc_batch_to_solscan_detail(items: list) -> dict:
    """Transform multiple RPC getTransaction responses to Solscan detail format.

    Args:
        items: list of (signature_str, rpc_response_dict) tuples.

    Returns:
        dict in Solscan /transaction/detail/multi shape:
        {'success': True, 'data': [tx_dict, ...]}
    """
    data = []
    for sig, rpc_response in items:
        rpc_result = rpc_response.get('result')
        if rpc_result is None:
            continue
        tx = _transform_single(sig, rpc_result)
        if tx is not None:
            data.append(tx)

    return {
        'success': len(data) > 0,
        'data': data,
    }
