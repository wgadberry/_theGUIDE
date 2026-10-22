"""Quick test: embed a query with OpenAI and search Qdrant."""
import json, os, sys, requests
from qdrant_client_t16o import get_client

_config_candidates = [
    os.path.join(os.path.dirname(__file__), '..', 'guide-config.json'),
    os.path.join(os.path.dirname(__file__), 'guide-config.json'),
    os.path.join(os.getcwd(), 'guide-config.json'),
]
for cp in _config_candidates:
    if os.path.exists(cp):
        OPENAI_API_KEY = json.load(open(cp)).get('QDRANT_API', '')
        break

query = sys.argv[1] if len(sys.argv) > 1 else "large SOL swap on Raydium"
top_k = int(sys.argv[2]) if len(sys.argv) > 2 else 5

vc = get_client()
info = vc.collection_info()
pts = info['points_count']
print(f"\nCollection: {pts} points")

# Embed query via OpenAI
resp = requests.post("https://api.openai.com/v1/embeddings",
    headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
    json={"model": "text-embedding-3-small", "input": [query], "dimensions": 384},
    timeout=30)
vec = resp.json()["data"][0]["embedding"]

results = vc.search_edges(vec, top_k=top_k, score_threshold=0.2)
print(f"Query: \"{query}\"  ({len(results)} results)\n")

for r in results:
    p = r['payload']
    score = r['score']
    edge = p.get('edge_type_code', '?')
    token = p.get('token_symbol', '?')
    dex = p.get('dex', '')
    sig = p.get('signature', '')[:20]
    frm = p.get('from_address', '')[:12]
    to = p.get('to_address', '')[:12]
    amt = p.get('amount', '')
    time_utc = str(p.get('block_time_utc', ''))[:19]

    print(f"  {score:.3f}  {edge:15s}  {token:10s}  {dex:15s}  {frm}→{to}  amt={amt}  {time_utc}  {sig}...")

print()
