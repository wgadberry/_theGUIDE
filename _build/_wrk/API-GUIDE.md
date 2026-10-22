# T16O BubbleMap API Integration Guide

> **Version:** 2.0
> **Last Updated:** March 19, 2026
> **Base URL:** `https://svcs.the16oracles.io`
> **Authentication:** All endpoints require an `X-API-Key` header.

---

## Table of Contents

0. [Unified Transaction Query (NEW)](#0-unified-transaction-query)
1. [Retrieving BubbleMap Data](#1-retrieving-bubblemap-data)
2. [Retrieving Wallet Transaction History](#2-retrieving-wallet-transaction-history)
   - 2b. [Wallet Txs v3.1 — Grouped + Compressed](#2b-retrieving-wallet-transaction-history-v31--grouped--compressed)
3. [Sourcing Transaction Data](#3-sourcing-transaction-data-via-the-producer)
4. [Time-Filtered Sourcing](#4-time-filtered-sourcing)
5. [Pipeline Monitoring](#5-pipeline-monitoring)
6. [End-to-End Workflow](#6-end-to-end-workflow)
7. [Reference](#7-reference)

---

## 0. Unified Transaction Query

### `GET /api/txs`

The unified transaction query endpoint. Replaces both the BubbleMap (`/api/bmap/get`) and wallet transaction history (`/api/bmap/v3.1/get-wallet-txs`) endpoints with a single call. Supports two modes — **wallet mode** (address provided) and **graph mode** (address omitted) — with a `result_mask` bitmask that controls which data sections are computed and returned.

#### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `address` | string | no | — | Wallet address. If provided, filters edges to this wallet's perspective. If omitted, returns the full token graph. |
| `mint` | string | no | — | Token mint address. Required in graph mode. Optional in wallet mode (omit for all tokens). |
| `symbol` | string | no | — | Token symbol lookup (alternative to mint). Resolves to the most active token with this symbol. |
| `signature` | string | no | — | Anchor transaction signature. Results center around this tx (half before, half after). |
| `limit` | integer | no | 50/10 | Max transactions. Default 50 in wallet mode, 10 in graph mode. Max 200 wallet, 100 graph. |
| `after` | integer | no | — | Only include tx with `block_time` > this Unix timestamp. |
| `before` | integer | no | — | Only include tx with `block_time` < this Unix timestamp. |
| `type_mask` | integer | no | — | Bitmask filter on edge type indicator. Only edges with matching type bits are returned. |
| `result_mask` | integer | no | 255 | Bitmask controlling which data sections are computed. See below. |

#### Result Mask

Each bit controls an independent section of the response. Sections that aren't requested are completely skipped — no computation, no JSON, faster response.

| Bit | Value | Section | Cost | Description |
|-----|-------|---------|------|-------------|
| 0 | 1 | **Edges** | Low | Core edge list with source/target/amount/type |
| 1 | 2 | **Nodes** | Medium | Distinct address nodes with labels, types, funded_by |
| 2 | 4 | **Balances** | High | Token + SOL balance on each node (requires bit 1) |
| 3 | 8 | **Interactions** | High | Pool/program/dex lists per node (requires bit 1) |
| 4 | 16 | **Wallet** | Low | direction/party/post_balance on each edge |
| 5 | 32 | **Transactions** | Low | Flat wallet transaction rows (wallet mode only) |
| 6 | 64 | **Navigation** | Medium | Prev/next transaction lists with edge_types |
| 7 | 128 | **Related** | Medium | Other tokens seen in swap edges |

#### Presets

| Mask | Name | Use Case |
|------|------|----------|
| `113` | **Wallet Table** | Wallet history table (EDGES + WALLET + TRANSACTIONS + NAVIGATION) |
| `207` | **Bubble Map** | Full graph visualization (EDGES + NODES + BALANCES + INTERACTIONS + NAVIGATION + RELATED) |
| `255` | **Full** | Everything — both views in one call |
| `1` | **Edges Only** | Lightweight — just the edge data for API consumers |

---

#### 0a. Wallet Transaction History

Returns all edges involving this wallet, with direction (in/out), counterparty, post-balance, and dex information. Use `result_mask=113` for optimal performance.

```bash
curl -s "https://svcs.the16oracles.io/api/txs?address=92e59wnU1rQb2uj92dpiSJWQEvqmuNkxjZQGGRh9rLj3&limit=50&result_mask=113" \
  -H "X-API-Key: BMFer"
```

---

#### 0b. Wallet History Filtered by Token

Only show edges for a specific token. Significantly reduces result size for active wallets.

```bash
curl -s "https://svcs.the16oracles.io/api/txs?address=92e59wnU1rQb2uj92dpiSJWQEvqmuNkxjZQGGRh9rLj3&mint=Ha4oiA7TJtoE1jPk8m6r5GPYm1ERmT9oKU5CZAGTpump&limit=50&result_mask=113" \
  -H "X-API-Key: BMFer"
```

---

#### 0c. Bubble Map for a Token

Returns the full transaction graph — nodes (with balances and interactions) and edges (with swap routing through pools). No address filter.

```bash
curl -s "https://svcs.the16oracles.io/api/txs?mint=Ha4oiA7TJtoE1jPk8m6r5GPYm1ERmT9oKU5CZAGTpump&limit=10&result_mask=207" \
  -H "X-API-Key: BMFer"
```

---

#### 0d. Bubble Map by Token Symbol

Resolves the symbol to the most active token with that name.

```bash
curl -s "https://svcs.the16oracles.io/api/txs?symbol=PUMP&limit=10&result_mask=207" \
  -H "X-API-Key: BMFer"
```

---

#### 0e. Bubble Map Centered on a Transaction

Uses a signature as the anchor point. Half the results are before this tx, half after.

```bash
curl -s "https://svcs.the16oracles.io/api/txs?mint=Ha4oiA7TJtoE1jPk8m6r5GPYm1ERmT9oKU5CZAGTpump&signature=4fXARrfq3prCyfoP354k2dKQaL34eiJt4jQVVbCAD4VDsBqonXuY3XERygrcqaTT5XxdM3grVKJYiu9v1hhB1coC&limit=20&result_mask=207" \
  -H "X-API-Key: BMFer"
```

---

#### 0f. Wallet History with Time Range

Fetch a wallet's activity within a specific time window using Unix timestamps.

```bash
curl -s "https://svcs.the16oracles.io/api/txs?address=952zmBcstby9cF1UGPBUwRADSNtrMxC2iJ5PYvGmfs2G&mint=Ha4oiA7TJtoE1jPk8m6r5GPYm1ERmT9oKU5CZAGTpump&after=1771900000&before=1773900000&limit=100&result_mask=113" \
  -H "X-API-Key: BMFer"
```

---

#### 0g. Lightweight Edges Only (API Consumer)

Minimal response — just the edges array. Fastest possible query for programmatic use.

```bash
curl -s "https://svcs.the16oracles.io/api/txs?address=92e59wnU1rQb2uj92dpiSJWQEvqmuNkxjZQGGRh9rLj3&limit=20&result_mask=1" \
  -H "X-API-Key: BMFer"
```

---

#### 0h. Full Result (Both Views)

Returns everything — edges, nodes with balances, wallet perspective, transactions, navigation, related tokens. Largest payload but enables both bubble map and wallet table rendering from one call.

```bash
curl -s "https://svcs.the16oracles.io/api/txs?address=92e59wnU1rQb2uj92dpiSJWQEvqmuNkxjZQGGRh9rLj3&mint=Ha4oiA7TJtoE1jPk8m6r5GPYm1ERmT9oKU5CZAGTpump&limit=10&result_mask=255" \
  -H "X-API-Key: BMFer"
```

---

#### 0i. Signature-Only Mode (Derive Token from Transaction)

Pass only a signature — the SP derives the token automatically from the transaction's activity. Useful when you know a transaction but not which token was involved.

```bash
curl -s "https://svcs.the16oracles.io/api/txs?signature=4fXARrfq3prCyfoP354k2dKQaL34eiJt4jQVVbCAD4VDsBqonXuY3XERygrcqaTT5XxdM3grVKJYiu9v1hhB1coC&limit=1&result_mask=207" \
  -H "X-API-Key: BMFer"
```

---

#### 0j. Filter by Edge Type

Use `type_mask` to filter by edge type indicator bitmask. Only edges whose type has a matching bit will be returned. Useful for showing only swaps, only transfers, etc.

```bash
curl -s "https://svcs.the16oracles.io/api/txs?address=92e59wnU1rQb2uj92dpiSJWQEvqmuNkxjZQGGRh9rLj3&limit=50&type_mask=12&result_mask=113" \
  -H "X-API-Key: BMFer"
```

---

#### Response Structure

```json
{
    "stage_seq": 42,
    "token": { "mint": "Ha4oi...", "symbol": "ROOKIE", "token_type": "fungible", "decimals": 6 },
    "address": "92e59...",
    "address_funded_by": "Awpq5...",
    "count": 10,
    "edge_count": 14,
    "current_tx": { "signature": "...", "block_time": 1773423608, "block_time_utc": "...", "signer": "...", "edge_types": ["swap_in", "spl_transfer"] },
    "prev": [ { "signature": "...", "block_time_utc": "...", "edge_types": [...] } ],
    "next": [ { "signature": "...", "block_time_utc": "...", "edge_types": [...] } ],
    "nodes": [ { "address": "...", "label": "wallet", "address_type": "wallet", "balance": 1234.56, "sol_balance": 10.25, "funded_by": "...", "pool_label": null, "token_name": null, "interactions": { "pools": [...], "programs": [...], "dexes": [...] } } ],
    "edges": [ { "source": "...", "source_label": "...", "target": "...", "target_label": "...", "amount": "1.5", "type": "swap_out", "token_symbol": "ROOKIE", "mint_address": "Ha4oi...", "dex": "Raydium", "pool_label": "Raydium (USDC-ROOKIE) Market", "ins_index": 0, "signature": "...", "block_time": 1773423608, "block_time_utc": "...", "tax_bps": null, "tax_amount": null, "direction": "in", "party": "...", "party_label": "Raydium", "party_funded_by": "...", "mint_funded_by": "...", "post_balance": "1234.56" } ],
    "related_tokens": [ { "mint": "So11...", "symbol": "WSOL", "name": "Wrapped SOL", "token_type": "fungible", "swap_count": 5, "edge_count": 12, "total_volume": 21.58 } ],
    "transactions": [ { "signature": "...", "block_time": 1773423608, "block_time_utc": "...", "type_code": "swap_out", "direction": "in", "amount": "1.5", "token_symbol": "ROOKIE", "mint_address": "Ha4oi...", "party": "...", "party_label": "Raydium", "dex": "Raydium", "post_balance": "1234.56", "mint_funded_by": "...", "party_funded_by": "..." } ]
}
```

**Key fields:**
- `stage_seq` — Monotonic sequence ID. Can be used to re-read the same result without re-executing the query.
- `edges` — Graph edges with both source/target routing (for bubble map) AND wallet perspective (direction/party/post_balance).
- `transactions` — Flat rows for wallet table rendering. Only populated when address is provided and SECT_TRANSACTIONS (32) is in the mask.
- `nodes` — Participant nodes with balances and interactions. Only when SECT_NODES (2) is in the mask.
- `related_tokens` — Other tokens seen in swap edges. Only when SECT_RELATED (128) is in the mask.

#### Performance Guidelines

| Query Type | Recommended Mask | Typical Response Time |
|-----------|-----------------|----------------------|
| Wallet table (5-50 txs) | 113 | 70-200ms |
| Wallet table (100+ txs) | 113 | 200-500ms |
| Bubble map (10 txs) | 207 | 300-700ms |
| Full result (10 txs) | 255 | 400-1000ms |
| Edges only (any limit) | 1 | 30-150ms |

**Tip:** For wallet table views, always use `result_mask=113`. Using 255 doubles the JSON payload size (edges + transactions contain overlapping data) and adds node/balance computation overhead.

---

## 1. Retrieving BubbleMap Data

### `GET /api/bmap/get`

The BubbleMap endpoint is the primary data retrieval interface. It returns a transaction graph composed of nodes (wallets, pools, mints) and edges (token transfers, swaps, burns) for a given token or transaction. The response is designed to be rendered as an interactive force-directed graph. You must provide at least one identifier — a token symbol, mint address, or transaction signature — to anchor the query.

#### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `token_symbol` | string | conditional | — | Token ticker symbol (e.g., `TRUMP`, `SOL`, `USDC`) |
| `token_name` | string | conditional | — | Full token name (e.g., `Wrapped SOL`) |
| `mint_address` | string | conditional | — | On-chain mint address of the token |
| `signature` | string | conditional | — | A specific transaction signature to visualize |
| `block_time` | integer | no | — | Unix timestamp — navigate to the nearest transaction at this time |
| `tx_limit` | integer | no | `10` | Number of transactions in the window. Accepted values: `1`, `10`, `20`, `50`, `100` |

> At least one of `token_symbol`, `token_name`, `mint_address`, or `signature` must be provided.

---

#### 1a. By Token Symbol

When you know the ticker symbol of a token, this is the simplest way to retrieve its activity graph. The API resolves the symbol to a mint address internally and returns the most recent transactions. If multiple tokens share the same symbol, the one with the most transaction activity is selected.

```bash
curl -s "https://svcs.the16oracles.io/api/bmap/get?token_symbol=TRUMP&tx_limit=10" \
  -H "X-API-Key: BMFer"
```

---

#### 1b. By Mint Address

Using the mint address is the most precise way to retrieve data for a specific token. Unlike symbol-based lookups, there is no ambiguity — every SPL token on Solana has a unique mint address. This is the recommended approach when integrating programmatically.

```bash
curl -s "https://svcs.the16oracles.io/api/bmap/get?mint_address=6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN&tx_limit=10" \
  -H "X-API-Key: BMFer"
```

---

#### 1c. By Transaction Signature

Passing a transaction signature retrieves the graph centered on that specific transaction. The API enters "signature-only mode," which displays all token activity within the transaction — not just a single token. This is useful for investigating a specific on-chain event without knowing which token was involved.

```bash
curl -s "https://svcs.the16oracles.io/api/bmap/get?signature=2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv&tx_limit=10" \
  -H "X-API-Key: BMFer"
```

---

#### 1d. By Signature Scoped to a Token

When both a signature and a mint address are provided, the API returns the graph for that specific transaction but filters edges to only show activity involving the specified token. This is useful when a transaction involves multiple tokens and you want to focus on one.

```bash
curl -s "https://svcs.the16oracles.io/api/bmap/get?signature=2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv&mint_address=6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN&tx_limit=10" \
  -H "X-API-Key: BMFer"
```

---

#### 1e. Navigate by Block Time

If you know an approximate time of interest, you can provide a Unix timestamp and the API will find the nearest transaction to that point. This is commonly used for timeline navigation — scrubbing through a token's history to a specific date.

```bash
curl -s "https://svcs.the16oracles.io/api/bmap/get?mint_address=6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN&block_time=1773645117&tx_limit=10" \
  -H "X-API-Key: BMFer"
```

---

#### Response Structure

A successful response returns HTTP 200 with a JSON object containing:

```
{
  "result": {
    "token":           { symbol, mint, decimals, token_type },
    "txs":             { signature, block_time_utc, signer, prev/next navigation },
    "nodes":           [ { address, label, address_type, balance, ... } ],
    "edges":           [ { source, target, amount, type, token_symbol, dex, pool_label, ... } ],
    "related_tokens":  [ { symbol, mint, tx_count } ]
  }
}
```

If no data exists, the API returns HTTP 404 with `{"result": {"error": "Token not found"}}`.

---

## 2. Retrieving Wallet Transaction History

### `GET /api/bmap/get-wallet-txs`

This endpoint returns a chronological list of transactions for a specific wallet or entity address. Unlike the BubbleMap endpoint which returns a graph, this returns a flat table of transactions with direction (in/out), amounts, counterparties, and balance information. It supports optional filtering by token and can center results around a specific transaction for contextual browsing.

#### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `address` | string | **yes** | — | The wallet, pool, or entity address to query |
| `mint` | string | no | — | Filter results to a specific token's activity |
| `signature` | string | no | — | Center results around this transaction (half before, half after) |
| `limit` | integer | no | `50` | Maximum rows to return (max 200) |

Also accepts POST with a JSON body using the same field names.

---

#### 2a. All Activity for a Wallet

Returns the most recent transactions across all tokens for the given address. This provides a complete picture of all token flows — transfers, swaps, burns, and mints — involving this wallet.

```bash
curl -s "https://svcs.the16oracles.io/api/bmap/get-wallet-txs?address=BAoVYrCEjFk7bgP2fRp2ahLcH6ktYUVSgXWHT88jYymy&limit=50" \
  -H "X-API-Key: BMFer"
```

---

#### 2b. Activity Filtered by Token

When a mint address is provided, results are filtered to only show transactions involving that specific token. This is useful for tracking a wallet's buy/sell history for a particular asset.

```bash
curl -s "https://svcs.the16oracles.io/api/bmap/get-wallet-txs?address=BAoVYrCEjFk7bgP2fRp2ahLcH6ktYUVSgXWHT88jYymy&mint=6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN&limit=50" \
  -H "X-API-Key: BMFer"
```

---

#### 2c. Centered Around a Transaction

When a signature is provided, the results are split — half are transactions that occurred before the anchor, and half are after. The anchor transaction itself is included. This is ideal for providing context around a specific event, such as showing what a wallet did immediately before and after a large swap.

```bash
curl -s "https://svcs.the16oracles.io/api/bmap/get-wallet-txs?address=BAoVYrCEjFk7bgP2fRp2ahLcH6ktYUVSgXWHT88jYymy&mint=6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN&signature=2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv&limit=50" \
  -H "X-API-Key: BMFer"
```

---

## 2b. Retrieving Wallet Transaction History (v3.1 — Grouped + Compressed)

### `GET /api/bmap/v3.1/get-wallet-txs`

Enhanced version of the wallet transaction endpoint. Returns the same underlying data as v2 but with two key differences: edges are **grouped by token** within each transaction (rather than flat rows), and the response is **gzip-compressed**. This is the recommended endpoint for client applications that render transaction history in a UI.

Uses the same stored procedure as v3 (`sp_tx_bmap_get_wallet_txs_v3`).

#### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `address` | string | **yes** | — | The wallet, pool, or entity address to query |
| `mint` | string | no | — | Filter results to a specific token's activity |
| `signature` | string | no | — | Center results around this transaction (half before, half after) |
| `limit` | integer | no | `50` | Maximum transactions to return (max 10,000) |
| `after` | integer | no | — | Only include transactions with `block_time` greater than this Unix timestamp |
| `before` | integer | no | — | Only include transactions with `block_time` less than this Unix timestamp |
| `type_mask` | integer | no | — | Bitmask filter on edge type indicator (e.g., swaps only, transfers only) |

Also accepts POST with a JSON body using the same field names.

#### Response Headers

| Header | Description |
|--------|-------------|
| `Content-Encoding` | `gzip` — response body is gzip-compressed |
| `X-Uncompressed-Length` | Size of the JSON payload before compression (bytes) |
| `X-Compressed-Length` | Size of the compressed response (bytes) |

#### Response Structure

The response groups edges by transaction, then by token (mint address) within each transaction:

```json
{
  "address": "BAoVYr...",
  "address_funded_by": "7xK...",
  "count": 3,
  "edge_count": 7,
  "mint": null,
  "transactions": [
    {
      "signature": "5abc...",
      "block_time": 1710000000,
      "block_time_utc": "2025-03-10T12:00:00",
      "edges": {
        "So11111111111111111111111111111111111111112": {
          "token_symbol": "SOL",
          "funded_by": null,
          "activity": [
            {
              "party": "7xK...",
              "party_funded_by": "9aB...",
              "type_code": "swap_out",
              "direction": "out",
              "amount": "1.5",
              "party_label": "Raydium",
              "dex": "Raydium",
              "post_balance": "10.25"
            }
          ]
        },
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {
          "token_symbol": "USDC",
          "funded_by": null,
          "activity": [
            {
              "party": "7xK...",
              "party_funded_by": null,
              "type_code": "swap_in",
              "direction": "in",
              "amount": "225.50",
              "party_label": "Raydium",
              "dex": "Raydium",
              "post_balance": "1500.00"
            }
          ]
        }
      }
    }
  ]
}
```

#### Edge Fields

| Field | Description |
|-------|-------------|
| `party` | The counterparty address (the other side of the transfer/swap) |
| `party_funded_by` | Address that originally funded the counterparty (funding trail) |
| `type_code` | Edge type code: `spl_transfer`, `swap_in`, `swap_out`, `burn`, `mint`, `fee`, etc. |
| `direction` | Direction relative to the queried wallet: `in`, `out` |
| `amount` | Human-readable amount (decimal string, e.g., `"1.5"` not `1500000000`) |
| `party_label` | Label for the counterparty if known (e.g., `"Raydium"`, `"Signer"`) |
| `dex` | DEX name for swap edges (e.g., `"Raydium"`, `"Orca"`, `"Jupiter"`) |
| `post_balance` | Wallet's token balance after this transaction (decimal string) |

---

#### 2b-i. All Activity (Compressed)

```bash
curl -s --compressed "https://svcs.the16oracles.io/api/bmap/v3.1/get-wallet-txs?address=BAoVYrCEjFk7bgP2fRp2ahLcH6ktYUVSgXWHT88jYymy&limit=50" \
  -H "X-API-Key: BMFer"
```

---

#### 2b-ii. Filtered by Token

```bash
curl -s --compressed "https://svcs.the16oracles.io/api/bmap/v3.1/get-wallet-txs?address=BAoVYrCEjFk7bgP2fRp2ahLcH6ktYUVSgXWHT88jYymy&mint=6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN&limit=50" \
  -H "X-API-Key: BMFer"
```

---

#### 2b-iii. Time-Bounded Query

Retrieve transactions within a specific time window using Unix timestamps:

```bash
curl -s --compressed "https://svcs.the16oracles.io/api/bmap/v3.1/get-wallet-txs?address=BAoVYrCEjFk7bgP2fRp2ahLcH6ktYUVSgXWHT88jYymy&after=1709251200&before=1711929600&limit=100" \
  -H "X-API-Key: BMFer"
```

---

#### 2b-iv. Filtered by Edge Type

Use `type_mask` to filter by edge type indicator bitmask. Only edges whose type has a matching indicator bit will be returned:

```bash
curl -s --compressed "https://svcs.the16oracles.io/api/bmap/v3.1/get-wallet-txs?address=BAoVYrCEjFk7bgP2fRp2ahLcH6ktYUVSgXWHT88jYymy&type_mask=12&limit=50" \
  -H "X-API-Key: BMFer"
```

---

#### Differences from v2 (`/api/bmap/get-wallet-txs`)

| Feature | v2 | v3.1 |
|---------|-----|------|
| Response shape | Flat rows (one row per edge) | Grouped: transaction > token > activity |
| Compression | None | gzip (`--compressed` in curl) |
| Time filtering | Not supported | `after` and `before` (Unix timestamps) |
| Type filtering | Not supported | `type_mask` bitmask |
| Limit cap | 200 | 10,000 |
| Key ordering | Original | Narrative: who, what, when, how much |

---

## 3. Sourcing Transaction Data via the Producer

### `POST /api/trigger/producer`

When the BubbleMap endpoint returns no data, it means the pipeline has not yet ingested transactions for that token or address. The producer is responsible for fetching transaction signatures from the Solana blockchain (via RPC), then cascading them to the decoder and detailer workers for processing. Once processing completes (typically 5–30 seconds depending on volume), the BubbleMap data becomes available.

All producer requests are asynchronous — the endpoint returns immediately with a confirmation, and processing happens in the background via RabbitMQ worker queues.

#### Standard Headers

Every producer call should include these headers:

| Header | Value | Description |
|--------|-------|-------------|
| `Content-Type` | `application/json` | Required for JSON body |
| `X-API-Key` | `BMFer` | API key for authentication and billing |
| `X-Priority` | `8` | Queue priority (1–10). User-initiated calls should use 8. |
| `X-Request-Id` | `<UUID>` | Unique identifier for this request. Auto-generate per call. |
| `X-Correlation-Id` | `<UUID>` | Groups related requests together. Use the same value across a logical workflow. |

---

### 3a. Source by Mint Address

The most common producer call. Given a token's mint address, the producer fetches transaction signatures from the Solana RPC and sends them through the decode/detail pipeline. The `address_sig_cnt` parameter controls how many signatures to fetch — this represents the maximum number of qualifying transactions to retrieve, not a page size.

```bash
curl -s -X POST "https://svcs.the16oracles.io/api/trigger/producer" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: BMFer" \
  -H "X-Priority: 8" \
  -H "X-Request-Id: $(uuidgen)" \
  -H "X-Correlation-Id: $(uuidgen)" \
  -d '{
    "batch": {
      "filters": {
        "addresses": ["6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN"]
      },
      "address_sig_cnt": 50
    }
  }'
```

---

### 3b. Source by Entity Address

Works identically to mint-based sourcing, but targets any Solana address — wallets, pools, ATAs, markets, or programs. The producer does not distinguish between address types; it fetches all transaction signatures where the address appears. The downstream workers determine the nature of each transaction.

```bash
curl -s -X POST "https://svcs.the16oracles.io/api/trigger/producer" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: BMFer" \
  -H "X-Priority: 8" \
  -H "X-Request-Id: $(uuidgen)" \
  -H "X-Correlation-Id: $(uuidgen)" \
  -d '{
    "batch": {
      "filters": {
        "addresses": ["BAoVYrCEjFk7bgP2fRp2ahLcH6ktYUVSgXWHT88jYymy"]
      },
      "address_sig_cnt": 50
    }
  }'
```

---

### 3c. Source by Signature (Context Mode)

When a single signature string is provided (not an array), the producer enters context mode. It resolves the address associated with the transaction, then fetches surrounding signatures — providing a window of activity before and after the target. The `sig_adjacent_cnt` parameter controls how many signatures to fetch on each side. This is the programmatic equivalent of the BubbleMap viewer's double-click behavior.

```bash
curl -s -X POST "https://svcs.the16oracles.io/api/trigger/producer" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: BMFer" \
  -H "X-Priority: 8" \
  -H "X-Request-Id: $(uuidgen)" \
  -H "X-Correlation-Id: $(uuidgen)" \
  -d '{
    "batch": {
      "filters": {
        "signature": "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv",
        "sig_adjacent_cnt": 10
      }
    }
  }'
```

---

### 3d. Source Multiple Addresses

Multiple addresses can be submitted in a single request. The producer processes them in parallel using a configurable thread pool. Each address is independently fetched from the RPC and cascaded to the decoder/detailer. Use the same `X-Correlation-Id` to track all results as a single logical operation.

```bash
curl -s -X POST "https://svcs.the16oracles.io/api/trigger/producer" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: BMFer" \
  -H "X-Priority: 8" \
  -H "X-Request-Id: $(uuidgen)" \
  -H "X-Correlation-Id: $(uuidgen)" \
  -d '{
    "batch": {
      "filters": {
        "addresses": [
          "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN",
          "LegiuxbrPh7SKEpnTN1nB2AjtrmSGP7XJtYZV4spump",
          "BAoVYrCEjFk7bgP2fRp2ahLcH6ktYUVSgXWHT88jYymy"
        ]
      },
      "address_sig_cnt": 20
    }
  }'
```

---

### 3e. Reprocess Known Signatures (Array Mode)

When `signature` is provided as an array instead of a string, the producer bypasses the RPC entirely and cascades the signatures directly to the decoder and detailer queues. This is used for reprocessing transactions that previously failed or were orphaned. No RPC calls are made — the signatures are assumed to be valid and are sent immediately for processing.

```bash
curl -s -X POST "https://svcs.the16oracles.io/api/trigger/producer" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: BMFer" \
  -H "X-Priority: 8" \
  -H "X-Request-Id: $(uuidgen)" \
  -H "X-Correlation-Id: $(uuidgen)" \
  -d '{
    "batch": {
      "filters": {
        "signature": [
          "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv",
          "5yvYJM3LHEYbHvt7b7vUyXA3osRpxNPitKRCdyC44hawg",
          "tVUjmc6tRe9kwAnfU9hZvqf2uq7GVzHqaFPoXJiQTG7"
        ]
      }
    }
  }'
```

---

### 3f. Combined: Signature Context + Address

Both a signature and an address can be provided together. The producer first fetches the context around the signature (adjacent transactions), cascades those immediately, and then separately fetches signatures for the address. Both sets of results share the same correlation ID. This is useful when you want to see a specific transaction in context while also loading the broader history for the associated token.

```bash
curl -s -X POST "https://svcs.the16oracles.io/api/trigger/producer" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: BMFer" \
  -H "X-Priority: 8" \
  -H "X-Request-Id: $(uuidgen)" \
  -H "X-Correlation-Id: $(uuidgen)" \
  -d '{
    "batch": {
      "filters": {
        "addresses": ["6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN"],
        "signature": "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv",
        "sig_adjacent_cnt": 10
      },
      "address_sig_cnt": 50
    }
  }'
```

---

## 4. Time-Filtered Sourcing

The producer supports boundary-based filtering using `before` and `until` parameters. These accept either a legacy signature string or a structured object with `block_time`, `block_id`, or `signature`. When time boundaries are used, the producer pages through RPC results and only counts transactions that fall within the specified window toward the `address_sig_cnt` limit.

---

### 4a. Most Recent (No Filter)

Without any time boundaries, the producer fetches the N most recent transaction signatures for the address. This is the default behavior and the simplest form of the call.

```bash
curl -s -X POST "https://svcs.the16oracles.io/api/trigger/producer" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: BMFer" \
  -H "X-Priority: 8" \
  -H "X-Request-Id: $(uuidgen)" \
  -H "X-Correlation-Id: $(uuidgen)" \
  -d '{
    "batch": {
      "filters": {
        "addresses": ["6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN"]
      },
      "address_sig_cnt": 50
    }
  }'
```

---

### 4b. After a Cutoff Date (Until)

The `until` boundary tells the producer to stop fetching when it reaches transactions older than the specified time. The RPC returns signatures newest-first, so this effectively means "fetch the most recent N signatures, but don't go earlier than this date." Transactions older than the cutoff are not counted toward `address_sig_cnt`.

```bash
curl -s -X POST "https://svcs.the16oracles.io/api/trigger/producer" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: BMFer" \
  -H "X-Priority: 8" \
  -H "X-Request-Id: $(uuidgen)" \
  -H "X-Correlation-Id: $(uuidgen)" \
  -d '{
    "batch": {
      "filters": {
        "addresses": ["6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN"],
        "until": {
          "block_time": "2025-01-01T00:00:00Z"
        }
      },
      "address_sig_cnt": 50
    }
  }'
```

---

### 4c. Before a Starting Point

The `before` boundary tells the producer to begin fetching from a specific point in time rather than from the most recent transaction. This is useful for historical analysis — "show me the 50 most recent transactions before June 2026." The producer resolves the time to an RPC-compatible signature cursor internally.

```bash
curl -s -X POST "https://svcs.the16oracles.io/api/trigger/producer" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: BMFer" \
  -H "X-Priority: 8" \
  -H "X-Request-Id: $(uuidgen)" \
  -H "X-Correlation-Id: $(uuidgen)" \
  -d '{
    "batch": {
      "filters": {
        "addresses": ["6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN"],
        "before": {
          "block_time": "2026-06-01T00:00:00Z"
        }
      },
      "address_sig_cnt": 50
    }
  }'
```

---

### 4d. Within a Date Range (Before + Until)

Combining both boundaries creates a time window. The producer starts from the `before` point and walks backwards through history, only counting transactions whose `block_time` falls within the window. It stops when it reaches the `until` boundary or has collected enough qualifying signatures. This is the most precise form of time-filtered fetching.

```bash
curl -s -X POST "https://svcs.the16oracles.io/api/trigger/producer" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: BMFer" \
  -H "X-Priority: 8" \
  -H "X-Request-Id: $(uuidgen)" \
  -H "X-Correlation-Id: $(uuidgen)" \
  -d '{
    "batch": {
      "filters": {
        "addresses": ["6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN"],
        "before": {
          "block_time": "2027-01-20T00:00:00Z"
        },
        "until": {
          "block_time": "2025-01-01T00:00:00Z"
        }
      },
      "address_sig_cnt": 50
    }
  }'
```

---

### 4e. Context Around a Signature

This is the programmatic equivalent of the BubbleMap viewer's double-click action. The producer locates the target transaction, then fetches N transactions before it and N transactions after it, providing a symmetric window of activity centered on the event of interest. The `sig_adjacent_cnt` parameter controls the window size on each side.

```bash
curl -s -X POST "https://svcs.the16oracles.io/api/trigger/producer" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: BMFer" \
  -H "X-Priority: 8" \
  -H "X-Request-Id: $(uuidgen)" \
  -H "X-Correlation-Id: $(uuidgen)" \
  -d '{
    "batch": {
      "filters": {
        "signature": "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv",
        "sig_adjacent_cnt": 20
      }
    }
  }'
```

---

### 4f. Full Combination: Address + Signature + Date Range

The most comprehensive form of a producer request. The producer first fetches context around the signature (adjacent transactions), then independently fetches signatures for the address within the specified time window. Both result sets are cascaded to the pipeline under the same correlation ID. This is useful for building a complete picture: "show me what happened around this transaction, plus the broader activity for this token during Q1 2026."

```bash
curl -s -X POST "https://svcs.the16oracles.io/api/trigger/producer" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: BMFer" \
  -H "X-Priority: 8" \
  -H "X-Request-Id: $(uuidgen)" \
  -H "X-Correlation-Id: $(uuidgen)" \
  -d '{
    "batch": {
      "filters": {
        "addresses": ["6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN"],
        "signature": "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv",
        "sig_adjacent_cnt": 10,
        "before": {
          "block_time": "2026-03-31T00:00:00Z"
        },
        "until": {
          "block_time": "2026-01-01T00:00:00Z"
        }
      },
      "address_sig_cnt": 50
    }
  }'
```

---

## 5. Pipeline Monitoring

### 5a. Pipeline Statistics

Returns real-time metrics on pipeline throughput, transaction state distribution, worker health, data volumes, and token/pool coverage. Optionally filter by the `created_utc` date range of transactions.

```bash
curl -s "https://svcs.the16oracles.io/api/bmap/stats" \
  -H "X-API-Key: BMFer"
```

With date filter:

```bash
curl -s "https://svcs.the16oracles.io/api/bmap/stats?start=2026-03-15%2000:00:00&end=2026-03-15%2023:59:59" \
  -H "X-API-Key: BMFer"
```

---

### 5b. Request Status

Track the status of a specific request by its ID. Returns processing state, counts of items processed, and any errors encountered.

```bash
curl -s "https://svcs.the16oracles.io/api/status/<request_id>" \
  -H "X-API-Key: BMFer"
```

---

### 5c. Pipeline Progress by Correlation ID

When multiple requests share a correlation ID (e.g., a multi-address load), this endpoint aggregates their collective progress. It shows how many requests have completed, failed, or are still pending across all workers in the pipeline.

```bash
curl -s "https://svcs.the16oracles.io/api/pipeline/<correlation_id>" \
  -H "X-API-Key: BMFer"
```

---

### 5d. Worker Status

Lists all registered workers with their current thread counts, queue depths, consumer counts, and configuration. Useful for operational visibility into the pipeline's processing capacity.

```bash
curl -s "https://svcs.the16oracles.io/api/workers" \
  -H "X-API-Key: BMFer"
```

---

## 6. End-to-End Workflow

A typical integration follows this pattern:

### Step 1 — Attempt to retrieve BubbleMap data

```bash
curl -s "https://svcs.the16oracles.io/api/bmap/get?mint_address=6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN&tx_limit=10" \
  -H "X-API-Key: BMFer"
```

If the response contains `"error": "Token not found"`, proceed to Step 2.

### Step 2 — Trigger the producer to source data

```bash
CORR_ID=$(uuidgen)

curl -s -X POST "https://svcs.the16oracles.io/api/trigger/producer" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: BMFer" \
  -H "X-Priority: 8" \
  -H "X-Request-Id: $(uuidgen)" \
  -H "X-Correlation-Id: $CORR_ID" \
  -d '{
    "batch": {
      "filters": {
        "addresses": ["6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN"]
      },
      "address_sig_cnt": 50
    }
  }'
```

### Step 3 — Monitor progress (optional)

```bash
curl -s "https://svcs.the16oracles.io/api/pipeline/$CORR_ID" \
  -H "X-API-Key: BMFer"
```

### Step 4 — Retry the BubbleMap call

Allow 5–30 seconds for processing, then retry the original call:

```bash
curl -s "https://svcs.the16oracles.io/api/bmap/get?mint_address=6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN&tx_limit=10" \
  -H "X-API-Key: BMFer"
```

---

## 7. Reference

### Boundary Formats

The `before` and `until` fields accept multiple formats:

```json
// ISO 8601 datetime (resolved to nearest RPC signature via binary search)
"before": { "block_time": "2026-06-15T00:00:00Z" }

// Slot number
"before": { "block_id": 350000000 }

// Specific transaction signature
"before": { "signature": "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv" }

// Legacy format: plain signature string
"before": "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv"
```

### Priority Levels

| Priority | Intended Use |
|----------|-------------|
| `10` | Critical real-time operations |
| `8` | User-initiated requests (recommended default) |
| `5` | Normal background processing |
| `1` | Bulk batch operations |

Higher-priority messages are consumed before lower-priority messages within the same queue.

### Rate Limiting

Rate limits are enforced per API key using a sliding 60-second window. When the limit is exceeded, the API returns HTTP 429 with a `Retry-After` header indicating the number of seconds to wait before retrying.

### HTTP Status Codes

| Code | Meaning |
|------|---------|
| `200` | Success — data returned |
| `202` | Accepted — async processing started |
| `400` | Bad request — missing or invalid parameters |
| `401` | Unauthorized — missing or invalid API key |
| `403` | Forbidden — feature not enabled for this API key |
| `404` | Not found — no data exists for the query |
| `429` | Rate limit exceeded |
| `500` | Internal server error |
