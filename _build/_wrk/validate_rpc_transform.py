"""
validate_rpc_transform.py — Validate rpc_detail_transform against Solscan.

Fetches the same transactions from both Chainstack RPC and Solscan detail API,
transforms the RPC response, and compares field-by-field.
"""

import json, time, requests, sys
from rpc_detail_transform import rpc_to_solscan_detail, rpc_batch_to_solscan_detail

with open('guide-config.json') as f:
    cfg = json.load(f)

RPC_URLS = cfg['RPC_URLs']
SOLSCAN_API = cfg['SOLSCAN_API']
SOLSCAN_TOKEN = cfg['SOLSCAN_TOKEN']

# ── Test signatures ──────────────────────────────────────────────────────────
# Mix of tx types: swaps, transfers, token creates, etc.
TEST_SIGS = [
    "36jtscWZKTEqa1ALtL9ivbGRQxhdVnG6q3a9k2qNPjWuite2erhnfJxqEc9Ai5M4mG6LyhEA7zPL9bhPrjuSu6cp",
]

# Grab a few more sigs from the first address in benchmark
def fetch_extra_sigs(count=4):
    payload = {
        "id": 1, "jsonrpc": "2.0",
        "method": "getSignaturesForAddress",
        "params": ["6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN", {"limit": count}]
    }
    r = requests.post(RPC_URLS[0], json=payload, timeout=15)
    data = r.json()
    return [s['signature'] for s in data.get('result', [])]

extra = fetch_extra_sigs(4)
TEST_SIGS.extend(extra)
print(f"Testing {len(TEST_SIGS)} transactions\n")


# ── Fetch from both sources ──────────────────────────────────────────────────

def fetch_rpc(sig, rpc_url):
    payload = {
        "id": 1, "jsonrpc": "2.0", "method": "getTransaction",
        "params": [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
    }
    r = requests.post(rpc_url, json=payload, timeout=30)
    return r.json()


def fetch_solscan(sigs):
    params = '&'.join(f'tx[]={s}' for s in sigs)
    url = f"{SOLSCAN_API}/transaction/detail/multi?{params}"
    r = requests.get(url, headers={'token': SOLSCAN_TOKEN}, timeout=30)
    return r.json()


# ── Comparison logic ─────────────────────────────────────────────────────────

def compare_tx(solscan_tx, rpc_tx, sig_short):
    """Compare a Solscan tx dict with a transformed RPC tx dict.
    Returns list of difference strings (empty = perfect match)."""
    diffs = []

    # Scalar fields
    for field in ['block_id', 'block_time', 'fee', 'status', 'compute_units_consumed']:
        sv = solscan_tx.get(field)
        rv = rpc_tx.get(field)
        if sv != rv:
            diffs.append(f"  {field}: solscan={sv} rpc={rv}")

    # Signer
    if solscan_tx.get('signer') != rpc_tx.get('signer'):
        diffs.append(f"  signer: solscan={solscan_tx.get('signer')} rpc={rpc_tx.get('signer')}")

    # SOL balance changes — compare by address
    s_sol = {e['address']: e for e in solscan_tx.get('sol_bal_change', [])}
    r_sol = {e['address']: e for e in rpc_tx.get('sol_bal_change', [])}

    if set(s_sol.keys()) != set(r_sol.keys()):
        diffs.append(f"  sol_bal_change address mismatch: "
                     f"solscan={len(s_sol)} rpc={len(r_sol)}")
    else:
        sol_mismatches = 0
        for addr in s_sol:
            se = s_sol[addr]
            re = r_sol[addr]
            for f in ['pre_balance', 'post_balance', 'change_amount']:
                if str(se.get(f)) != str(re.get(f)):
                    sol_mismatches += 1
                    if sol_mismatches <= 3:
                        diffs.append(f"  sol[{addr[:8]}].{f}: "
                                     f"solscan={se.get(f)} rpc={re.get(f)}")
        if sol_mismatches > 3:
            diffs.append(f"  ... and {sol_mismatches - 3} more sol mismatches")

    # Token balance changes — compare by address+token_address
    def tok_key(e):
        return (e.get('address', ''), e.get('token_address', ''))

    s_tok = {tok_key(e): e for e in solscan_tx.get('token_bal_change', [])}
    r_tok = {tok_key(e): e for e in rpc_tx.get('token_bal_change', [])}

    if set(s_tok.keys()) != set(r_tok.keys()):
        s_only = set(s_tok.keys()) - set(r_tok.keys())
        r_only = set(r_tok.keys()) - set(s_tok.keys())
        diffs.append(f"  token_bal_change key mismatch: "
                     f"solscan_only={len(s_only)} rpc_only={len(r_only)}")
        for k in list(s_only)[:2]:
            diffs.append(f"    solscan_only: {k[0][:8]}.../{k[1][:8]}...")
        for k in list(r_only)[:2]:
            diffs.append(f"    rpc_only: {k[0][:8]}.../{k[1][:8]}...")
    else:
        tok_mismatches = 0
        for key in s_tok:
            se = s_tok[key]
            re = r_tok[key]
            for f in ['pre_balance', 'post_balance', 'change_amount',
                       'decimals', 'owner', 'change_type']:
                sv = se.get(f)
                rv = re.get(f)
                if str(sv) != str(rv):
                    tok_mismatches += 1
                    if tok_mismatches <= 3:
                        diffs.append(f"  tok[{key[0][:8]}].{f}: "
                                     f"solscan={sv} rpc={rv}")
        if tok_mismatches > 3:
            diffs.append(f"  ... and {tok_mismatches - 3} more token mismatches")

    return diffs


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    # Fetch all from Solscan in one batch
    print("Fetching from Solscan...")
    t0 = time.perf_counter()
    solscan_resp = fetch_solscan(TEST_SIGS)
    solscan_time = time.perf_counter() - t0
    print(f"  Solscan: {len(solscan_resp.get('data', []))} txs in {solscan_time:.2f}s")

    solscan_by_sig = {tx['tx_hash']: tx for tx in solscan_resp.get('data', [])}

    # Fetch all from RPC (round-robin across endpoints)
    print("Fetching from Chainstack RPC...")
    t0 = time.perf_counter()
    rpc_items = []
    for i, sig in enumerate(TEST_SIGS):
        rpc_url = RPC_URLS[i % len(RPC_URLS)]
        rpc_resp = fetch_rpc(sig, rpc_url)
        rpc_items.append((sig, rpc_resp))
    rpc_time = time.perf_counter() - t0
    print(f"  RPC: {len(rpc_items)} txs in {rpc_time:.2f}s")

    # Transform RPC -> Solscan format
    print("\nTransforming RPC -> Solscan format...")
    transformed = rpc_batch_to_solscan_detail(rpc_items)
    rpc_by_sig = {tx['tx_hash']: tx for tx in transformed.get('data', [])}

    # Compare
    print(f"\n{'='*70}")
    print(f"VALIDATION: {len(TEST_SIGS)} transactions")
    print(f"{'='*70}\n")

    total_pass = 0
    total_fail = 0
    total_diffs = 0

    for sig in TEST_SIGS:
        short = sig[:16] + "..."
        s_tx = solscan_by_sig.get(sig)
        r_tx = rpc_by_sig.get(sig)

        if not s_tx:
            print(f"  SKIP {short} — not in Solscan response")
            continue
        if not r_tx:
            print(f"  FAIL {short} — not in RPC response")
            total_fail += 1
            continue

        diffs = compare_tx(s_tx, r_tx, short)
        if not diffs:
            print(f"  PASS {short} — sol={len(s_tx.get('sol_bal_change',[]))} "
                  f"tok={len(s_tx.get('token_bal_change',[]))}")
            total_pass += 1
        else:
            print(f"  DIFF {short} — {len(diffs)} difference(s):")
            for d in diffs:
                print(f"       {d}")
            total_diffs += 1

    print(f"\n{'='*70}")
    print(f"RESULTS: {total_pass} PASS, {total_diffs} DIFF, {total_fail} FAIL")
    print(f"Solscan API: {solscan_time:.2f}s | Chainstack RPC: {rpc_time:.2f}s")
    print(f"{'='*70}")


if __name__ == '__main__':
    main()
