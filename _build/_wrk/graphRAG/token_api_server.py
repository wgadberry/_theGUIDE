#!/usr/bin/env python3
"""FastAPI wrapper for token_api.py — runs on port 5100"""
import os
import time
from fastapi import FastAPI, Query, Request
from fastapi.responses import ORJSONResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from token_api import handle_get_token

app = FastAPI(title='GraphRAG Token API')

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)

ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY', '')

@app.get('/api/token/{mint}')
async def get_token(
    mint: str,
    profile: str = Query(None),
    sections: str = Query(None),
    signals: str = Query(None),
    fast: bool = Query(False),
):
    if fast and not profile:
        profile = 'fast'
    data, status = handle_get_token(mint, profile, sections, signals)
    return ORJSONResponse(content=data, status_code=status)


@app.post('/api/token/{mint}/explain')
async def explain_token(mint: str, request: Request):
    """AI explanation of the current graph state using Claude."""
    if not ANTHROPIC_API_KEY:
        return JSONResponse({'error': 'ANTHROPIC_API_KEY not configured'}, 500)

    try:
        import anthropic
        body = await request.json()
    except Exception as e:
        return JSONResponse({'error': f'Invalid request: {e}'}, 400)

    # Build a summary of the graph state for Claude
    meta = (body.get('meta') or {}).get('token') or {}
    nodes = body.get('nodes') or []
    edges = body.get('edges') or []
    holders = body.get('holders') or []
    raw_signals = body.get('signals') or {}
    # Handle both grouped dict and flat array formats
    if isinstance(raw_signals, list):
        signals = {}
        for s in raw_signals:
            st = s.get('signal_type', 'unknown')
            if st not in signals:
                signals[st] = []
            signals[st].append(s)
    else:
        signals = raw_signals
    swap_stats = body.get('swapStats') or []

    node_count = len(nodes)
    edge_count = len(edges)

    # Use holders array if available, otherwise extract from nodes with holder_rank
    if not holders:
        holders = sorted(
            [{'owner': n.get('address',''), 'percentage': n.get('holder_pct',0), 'rank': n.get('holder_rank',0),
              'amount': n.get('holder_amount','')}
             for n in nodes if n.get('holder_rank')],
            key=lambda h: h.get('rank', 999)
        )
    holder_count = len(holders)

    # Summarize node types
    type_counts = {}
    for n in nodes:
        t = n.get('type', 'unknown')
        type_counts[t] = type_counts.get(t, 0) + 1

    # Graph.js summary stats (if present)
    summary = body.get('summary') or {}
    clustered = summary.get('clustered', 0)
    unclustered = summary.get('unclustered', 0)
    cluster_count = summary.get('clusters', 0)
    pool_count_vis = summary.get('pools', 0)

    # Summarize signals
    sig_summary = {}
    for sig_type, sig_list in signals.items():
        if isinstance(sig_list, list):
            sig_summary[sig_type] = len(sig_list)

    # Top holders (by percentage)
    sorted_holders = sorted(holders, key=lambda h: h.get('rank', 999))[:15]
    top_holders = []
    for h in sorted_holders:
        addr = h.get('owner', '?')[:12]
        pct = h.get('percentage', 0)
        rank = h.get('rank', '?')
        amt = h.get('amount', '')
        top_holders.append(f"  #{rank}: {addr}... holding {pct:.2f}% (amount: {amt})")

    # Top edges by tx_count
    sorted_edges = sorted(edges, key=lambda e: e.get('tx_count', 0), reverse=True)[:15]
    top_edges = []
    for e in sorted_edges:
        src = e.get('source_label') or e.get('source', '')[:12] + '...'
        tgt = e.get('target_label') or e.get('target', '')[:12] + '...'
        amt = e.get('total_amount', '')
        amt_str = f", amount={amt}" if amt else ''
        top_edges.append(f"  {src} ({e.get('source_type','')}) → {tgt} ({e.get('target_type','')}) — {e.get('tx_count',0)} txs{amt_str}")

    # Shared funders
    sf = signals.get('shared_funder', [])
    funder_lines = []
    for s in sf[:8]:
        wallets = s.get('wallets', [])
        funder_addr = s.get('signal_value', '?')[:16]
        funder_lines.append(f"  Funder {funder_addr}... → {len(wallets)} wallets: {', '.join(w[:8]+'...' for w in wallets[:5])}")

    # Bidirectional transfers
    bi = signals.get('bidirectional_transfer', [])
    bidir_lines = []
    for s in bi[:5]:
        bidir_lines.append(f"  {s.get('wallet_a','?')[:12]}... ↔ {s.get('wallet_b','?')[:12]}... ({s.get('signal_value',0)} combined txs)")

    # Fan-out distributors
    fo = signals.get('fan_out', [])
    fanout_lines = []
    for s in fo[:5]:
        fanout_lines.append(f"  {s.get('signal_value','?')[:12]}... → {s.get('wallet_count',0)} targets, {s.get('out_tx',0)} outbound txs, {s.get('in_deg',0)} inbound")

    # Pool chain
    pc = signals.get('pool_chain', [])
    poolchain_lines = []
    for s in pc[:5]:
        pools = s.get('pools', [])
        pool_names = ', '.join(p.get('label', p.get('address', '?')[:8]) for p in pools[:3])
        poolchain_lines.append(f"  {s.get('signal_value','?')[:12]}... funded {len(pools)} pool(s): {pool_names}")

    # Nodes with special flags
    fan_out_nodes = [n for n in nodes if n.get('isFanOut')]
    bridge_nodes = [n for n in nodes if n.get('isBridge')]
    pool_funder_nodes = [n for n in nodes if n.get('isPoolFunder')]

    # Cluster reason summary
    clustered_nodes = [n for n in nodes if n.get('ci', -1) >= 0]
    reason_counts = {}
    for n in clustered_nodes:
        for r in (n.get('reasons') or []):
            rt = r.get('type', 'unknown')
            reason_counts[rt] = reason_counts.get(rt, 0) + 1

    # Same-funder edge pairs
    same_funder_edges = [e for e in edges if e.get('source_funded_by') and e.get('target_funded_by') and e['source_funded_by'] == e['target_funded_by'] and e['source_funded_by'] != 'none']
    unique_same_funders = set(e['source_funded_by'] for e in same_funder_edges)

    prompt = f"""Analyze this Solana token's on-chain graph data and provide a concise, actionable intelligence summary. This data represents EXACTLY what the analyst is currently viewing in their graph visualization tool.

**Token:** {meta.get('name', 'Unknown')} ({meta.get('symbol', '?')}) — Mint: {mint}

**Current View (visible in map):**
- {node_count} visible nodes ({', '.join(f'{v} {k}' for k,v in type_counts.items())})
- {edge_count} visible edges (transfer + swap connections)
- {holder_count} holders with on-chain balance data
- {cluster_count} clusters detected ({clustered} clustered wallets, {unclustered} unclustered)
- {len(fan_out_nodes)} fan-out distributors flagged
- {len(bridge_nodes)} cross-cluster bridges detected
- {len(pool_funder_nodes)} pool funders identified

**Holder Distribution (top 15 by balance):**
{chr(10).join(top_holders) if top_holders else '  No holder balance data available'}

**Swap Activity:**
{chr(10).join(f"  {s.get('direction','?')}: {s.get('count',0)} txs from {s.get('unique_wallets',0)} unique wallets" for s in swap_stats) if swap_stats else '  No swap data'}

**Signal Detection Summary:**
{chr(10).join(f"  {k}: {v} signal(s)" for k,v in sig_summary.items()) if sig_summary else '  No signals detected'}

**Cluster Membership Reasons:**
{chr(10).join(f"  {k}: {v} node(s)" for k,v in reason_counts.items()) if reason_counts else '  No cluster reasons'}

**Top 15 Transfer/Swap Edges (by transaction count):**
{chr(10).join(top_edges) if top_edges else '  No edges'}

**Shared Funder Clusters (wallets funded by same entity):**
{chr(10).join(funder_lines) if funder_lines else '  None detected'}

**Bidirectional Transfers (wallets sending AND receiving from each other):**
{chr(10).join(bidir_lines) if bidir_lines else '  None detected'}

**Fan-Out Distributors (high out-degree, low in-degree wallets):**
{chr(10).join(fanout_lines) if fanout_lines else '  None detected'}

**Pool Funding Chain (wallets that funded pool/AMM addresses):**
{chr(10).join(poolchain_lines) if poolchain_lines else '  None detected'}

**Same-Funder Edge Pairs:** {len(same_funder_edges)} edges connect wallets funded by the same entity ({len(unique_same_funders)} unique funders)

Please provide:
1. **Overview** — What kind of token is this? Describe the holder distribution. Is it healthy or concentrated? Note: these are CURRENT holders with real on-chain balances.
2. **Suspicious Patterns** — Any signs of wash trading, sybil attacks, coordinated buying, insider accumulation, or artificial volume? Be specific with addresses and tx counts.
3. **Cluster Analysis** — What do the shared funder clusters and bidirectional transfers reveal about wallet coordination? How many wallets appear to be under common control?
4. **Key Actors** — Identify the most significant wallets by role: top holders, distributors, bridges, pool funders. What role does each play in the token ecosystem?
5. **Risk Assessment** — Rate the overall risk level (Low/Medium/High/Critical) with specific justification based on the data above.

Be specific. Reference actual addresses (abbreviated) and exact numbers from the data. Do not speculate beyond what the data shows."""

    t0 = time.time()
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model='claude-sonnet-4-20250514',
            max_tokens=1500,
            messages=[{'role': 'user', 'content': prompt}],
        )
        elapsed = round((time.time() - t0) * 1000)
        text = response.content[0].text
        usage = response.usage
        input_cost = usage.input_tokens * 0.003 / 1000
        output_cost = usage.output_tokens * 0.015 / 1000

        return JSONResponse({
            'explanation': text,
            'model': response.model,
            'usage': {
                'input_tokens': usage.input_tokens,
                'output_tokens': usage.output_tokens,
                'cost_usd': round(input_cost + output_cost, 6),
            },
            'elapsed_ms': elapsed,
        })
    except Exception as e:
        return JSONResponse({'error': f'Claude API error: {e}'}, 500)


@app.delete('/api/token/{mint}/cache')
async def clear_cache(mint: str):
    """Clear display cache for a token. Dedup keys (gk:sig:*) are preserved."""
    import redis as redis_lib
    try:
        rdb = redis_lib.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            decode_responses=True, socket_timeout=5,
        )
        pipe = rdb.pipeline(transaction=False)

        # Graph cache keys
        for suffix in ('complete', 'meta', 'nodes', 'edges', 'holders', 'swapstats', 'signals', 'fast', 'full'):
            pipe.delete(f'graph:{mint}:{suffix}')

        # Token hash + holder cache (top-holders dedup)
        pipe.delete(f'token:{mint}', f'holders:{mint}')

        pipe.execute()

        # Fan-out wallet keys (SCAN)
        cursor = 0
        while True:
            cursor, keys = rdb.scan(cursor, match=f'graph:{mint}:fanout:*', count=100)
            if keys:
                rdb.delete(*keys)
            if cursor == 0:
                break

        return JSONResponse({
            'status': 'cleared',
            'mint': mint,
        })
    except Exception as e:
        return JSONResponse({'error': str(e)}, 500)


@app.get('/health')
async def health():
    return {'status': 'ok', 'service': 'token-api'}

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=5100)
