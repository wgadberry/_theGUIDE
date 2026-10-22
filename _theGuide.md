# T16O: theGuide
## Graph of Underlying Interconnected Distributed Events

### Overview

**theGuide** is the umbrella project for T16O's graph and traversal layer, focused on Solana blockchain forensic analysis. It encompasses the components that transform raw transaction data into traversable graph structures for "follow the money" investigations.

---

### Component Architecture

| Component | Role |
|-----------|------|
| **theGuide** | Umbrella—graph layer & traversal intelligence |
| **Bloodhound** | Detection, tx mapping, sniffing out patterns |
| **Shredder** | JSON parsing, edge extraction |
| **ChainWalker** | (potential) The traversal engine itself |

---

### Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│  Raw JSON Storage (bloodhound approach)                     │
│  - Full transaction JSON preserved                          │
│  - Parsed metadata fields (program, success, fee, etc.)     │
└─────────────────────┬───────────────────────────────────────┘
                      │ Shredder extracts
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  tx_edges (Graph-Optimized)                                 │
│  - One row per value movement                               │
│  - from_account, to_account, amount, mint, edge_type        │
│  - Feeds directly into NetworkX / Neo4j                     │
└─────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  Node Enrichment Tables                                     │
│  - wallet_labels (address, label, category, risk_score)     │
│  - token_metadata (mint, symbol, decimals, logo)            │
│  - program_labels (program_id, name, category)              │
└─────────────────────────────────────────────────────────────┘
```

---

### Graph Design Decisions

#### Why Narrow/Edge-Centric Tables Win

- **Graph algorithms ingest edge lists**: `from_node → to_node [+ attributes]`
- **Single Solana tx = multiple edges**: One swap can produce 3+ edges (swap in, token out, fees)
- **Graph queries become trivial**: Recursive CTEs for multi-hop traversal
- **Indexing is straightforward**: Two indexes (`from_account`, `to_account`) cover most patterns

#### Recommended Graph Type

**`nx.MultiDiGraph`** — Solana data is:
- **Directed** (SOL/tokens flow from → to)
- **Multi-edge** (same wallet pair can have many transactions)
- **Heterogeneous nodes** (wallets, programs, pools, mints)

---

### Core Edge Table Schema

```sql
CREATE TABLE tx_edges (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    tx_signature VARCHAR(88) NOT NULL,
    from_account VARCHAR(44) NOT NULL,
    to_account VARCHAR(44) NOT NULL,
    amount DECIMAL(30,9),
    mint VARCHAR(44),          -- NULL for SOL
    instruction_index TINYINT,
    block_time INT,
    edge_type ENUM('transfer','swap_in','swap_out','fee','create_ata','close_ata'),
    
    INDEX idx_from (from_account, block_time),
    INDEX idx_to (to_account, block_time),
    INDEX idx_tx (tx_signature),
    INDEX idx_time (block_time)
);
```

---

### Recommended NetworkX Algorithms

| Use Case | Algorithm | Notes |
|----------|-----------|-------|
| Trace fund flows | `nx.all_simple_paths(G, source, target, cutoff=6)` | Find all routes between wallets |
| Find intermediaries | `nx.betweenness_centrality()` | Identifies mixer/laundering wallets |
| Identify key actors | `nx.pagerank()` | High-influence wallets |
| Cluster related wallets | `nx.community.louvain_communities()` | Groups wallets transacting together |
| Immediate connections | `nx.ego_graph(G, wallet, radius=2)` | Subgraph around target |
| Detect funding sources | `nx.ancestors(G, wallet)` | All wallets that funded this one |
| Track downstream | `nx.descendants(G, wallet)` | Where did funds go? |

---

### Visualization Layouts

| Layout | Best For | Performance |
|--------|----------|-------------|
| `spring_layout` | Showing clusters, exploration | Slow on large graphs |
| `kamada_kawai_layout` | Cleaner spacing, <500 nodes | Slow but pretty |
| `circular_layout` | Hub-and-spoke patterns | Fast |
| `shell_layout` | Layered flow visualization | Fast, good for tracing |
| Graphviz `dot` | Hierarchical/DAG flows | Excellent for tx chains |

#### For Production Scale
- **Gephi** (GEXF export) — handles 100K+ nodes
- **D3.js** — web dashboards
- **Pyvis** — quick interactive HTML from NetworkX

---

### Performance Tips

- **Subgraph first**: Use `ego_graph()` or time-bounded filtering before expensive algorithms
- **For massive scale**: Consider `igraph` (C-based) or `graph-tool` (10-100x faster)
- **Lazy iteration**: Use generators with `cutoff` parameters

---

### Example: NetworkX Integration

```python
import networkx as nx

# Build graph from tx_edges data
G = nx.MultiDiGraph()

for row in transaction_rows:
    G.add_edge(
        row['from_account'], 
        row['to_account'],
        tx_sig=row['tx_signature'],
        amount=row['amount'],
        token=row['mint'],
        block_time=row['block_time']
    )

# Forensic query: all paths from suspect to exchange
target_wallet = "SuspectXYZ..."
exchange_wallet = "BinanceHotWallet..."

paths = list(nx.all_simple_paths(G, target_wallet, exchange_wallet, cutoff=5))

# Subgraph for visualization
subG = nx.ego_graph(G, target_wallet, radius=3)
pos = nx.spring_layout(subG, k=2, iterations=50)
```

---

### Multi-Hop SQL Traversal Example

```sql
-- 2-hop downstream from suspect wallet
WITH RECURSIVE hops AS (
    SELECT to_account, 1 as depth
    FROM tx_edges WHERE from_account = 'SuspectWallet...'
    UNION
    SELECT e.to_account, h.depth + 1
    FROM tx_edges e
    JOIN hops h ON e.from_account = h.to_account
    WHERE h.depth < 3
)
SELECT DISTINCT to_account, MIN(depth) FROM hops GROUP BY to_account;
```

---

### Schema Design Philosophy

**Separation of concerns:**

- **Wide table** (bloodhound_tx_map) = "What happened in this transaction?" (forensic detail)
- **Edge table** (tx_edges) = "How do entities relate over time?" (graph traversal)
- **Node tables** = "What do I know about this entity?" (enrichment)

Shredder emits to *both*—wide table for audit/detail, edge table for graph analysis.

---

### Next Steps

- [ ] Finalize `tx_edges` schema
- [ ] Modify Shredder to emit graph-ready edge data
- [ ] Build NetworkX loader from MySQL
- [ ] Implement ChainWalker traversal patterns
- [ ] Design visualization export pipeline

---

*Document generated from Claude conversation - December 2024*
