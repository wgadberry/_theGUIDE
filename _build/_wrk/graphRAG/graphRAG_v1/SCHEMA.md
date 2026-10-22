# ArangoDB Schema — T16O Graph Layer

## Collections

### `addresses` (document)
Every Solana account that participates in a transaction.

| Field | Type | Description |
|---|---|---|
| `_key` | string | Solana pubkey (natural key, 32-44 chars) |
| `type` | string | `wallet` \| `pool` \| `mint` \| `program` \| `vault` \| `ata` \| `bot` \| `market` \| `dex_wallet` \| `unknown` |
| `label` | string? | Human-readable label from enrichment |
| `first_seen` | number | Unix timestamp of first observed activity |
| `last_seen` | number | Unix timestamp of most recent activity |
| `funded_by` | string? | Address that funded this account's creation (`none` if checked but N/A) |
| `funded_by_tx` | string? | Transaction signature of the funding tx |
| `funded_by_time` | number? | Block time of the funding tx |
| `tags` | array? | Account tags from Solscan (e.g., `["pump.fun amm", "market"]`) |
| `active_age` | number? | Account age in days from Solscan |
| `enriched_at` | number? | Timestamp when enrichment was last run |

### `tokens` (document)
SPL token mint metadata.

| Field | Type | Description |
|---|---|---|
| `_key` | string | Mint pubkey (same as the address in `addresses`) |
| `symbol` | string? | Token symbol (e.g., `SOL`, `USDC`, `MOON`) |
| `name` | string? | Full token name |
| `decimals` | number | Token decimal places |

### `transactions` (document)
Processing ledger — stubs created on signature discovery, enriched on completion with parsed fields and raw API responses.

| Field | Type | Description |
|---|---|---|
| `_key` | string | Transaction signature |
| `status` | string | `pending` \| `processed` \| `failed` |
| `block_time` | number | Unix timestamp (from getSignaturesForAddress) |
| `wallet` | string | Address that triggered this discovery |
| `discovered_at` | number | Unix timestamp when stub was created |
| `processed_at` | number? | Unix timestamp when processing completed |
| `error` | string? | Error message if status is `failed` |
| `slot` | number? | Solana slot number |
| `fee` | number? | Total fee in lamports |
| `priority_fee` | number? | Priority fee (total fee minus base fee) |
| `compute_units` | number? | Compute units consumed |
| `signer` | string? | Fee payer pubkey |
| `activity_type` | string? | Solscan activity classification (e.g., `ACTIVITY_AGG_TOKEN_SWAP`) |
| `version` | any? | Transaction version (`0` or `"legacy"`) |
| `detail` | object? | Full RPC getTransaction response (for reprocessing) |
| `decode` | object? | Full Solscan actions response (for reprocessing) |

### `edges` (edge)
Every on-chain transfer, swap leg, burn, mint, or close between two addresses.

| Field | Type | Description |
|---|---|---|
| `_from` | string | `addresses/<pubkey>` — source address |
| `_to` | string | `addresses/<pubkey>` — destination address |
| `edge_type` | string | `spl_transfer` \| `swap_in` \| `swap_out` \| `burn` \| `mint` \| `close_ata` \| `fee` \| `funded_by` |
| `token` | string | `tokens/<mint_pubkey>` — token involved |
| `amount` | string | Raw token amount (string to avoid precision loss) |
| `decimals` | number | Token decimals at time of tx (denormalized for query convenience) |
| `signature` | string | Transaction signature |
| `block_time` | number | Unix timestamp |
| `pool` | string? | `addresses/<pubkey>` — pool/AMM address (swap edges only) |
| `program` | string? | `addresses/<pubkey>` — program that executed this |

## Named Graph

```
graph: t16o_graph
  edge collection: edges
  from: addresses
  to:   addresses
```

## Index Strategy

```
edges:         persistent index on [signature]              — group edges by transaction
edges:         persistent index on [block_time]          — time-range queries
edges:         persistent index on [token, block_time]   — token-specific activity
edges:         persistent index on [edge_type]           — filter by type
transactions:  persistent index on [status]              — find pending/failed work
transactions:  persistent index on [wallet, block_time]  — wallet activity timeline
```

## Design Notes

- `addresses._key` is the raw Solana pubkey — no synthetic IDs, no joins needed
- `tokens._key` matches `addresses._key` for mints — a mint address exists in both collections
- `edges.token` is a reference string (`tokens/<key>`), not embedded metadata — keeps edges lean, token metadata lives in one place
- Swap legs are separate edges: `swap_in` (user -> pool) + `swap_out` (pool -> user), linked by same `signature` + `pool`
- `amount` is a string to handle meme token supplies near 10^19 without precision loss
- `decimals` is denormalized on edges so services don't need to join tokens for display
- `block_time` on both `addresses` and `edges` enables temporal queries at both the node and relationship level
