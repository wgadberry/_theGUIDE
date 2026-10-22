# sp_tx_bmap_get_token_state

Time-navigable cluster map for token analysis. Returns current holders (balance > 0) and a sliding window of transaction activity.

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `p_token_name` | VARCHAR(128) | Token name (optional) |
| `p_token_symbol` | VARCHAR(128) | Token symbol (optional) |
| `p_mint_address` | VARCHAR(44) | Token mint address (preferred) |
| `p_signature` | VARCHAR(88) | Transaction signature (optional - if NULL, uses p_block_time or most recent) |
| `p_block_time` | BIGINT UNSIGNED | Unix timestamp (optional - find nearest tx; ignored if p_signature provided) |

## Token Resolution Priority

1. If `p_mint_address` provided, use it
2. Else if `p_token_symbol` provided, use it
3. Else if `p_token_name` provided, use it
4. Else if `p_signature` provided (only), derive token from tx_guide activity
   - Prefers "interesting" tokens over base currencies (SOL, WSOL, USDC, USDT, etc.)

## Transaction Resolution Priority

1. If `p_signature` provided, use that tx (`p_block_time` ignored)
2. Else if `p_block_time` provided, find nearest tx to that time
3. Else use most recent tx

## Usage Examples

```sql
-- By mint address (preferred), most recent tx
CALL sp_tx_bmap_get_token_state(NULL, NULL, 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263', NULL, NULL);

-- By token symbol, most recent tx
CALL sp_tx_bmap_get_token_state(NULL, 'BONK', NULL, NULL, NULL);

-- By token name
CALL sp_tx_bmap_get_token_state('Bonk', NULL, NULL, NULL, NULL);

-- By signature only (derives token automatically)
CALL sp_tx_bmap_get_token_state(NULL, NULL, NULL, '5KtP...abc', NULL);

-- Specific signature for a token
CALL sp_tx_bmap_get_token_state(NULL, 'BONK', NULL, '5KtP...abc', NULL);

-- Token with nearest tx to block_time
CALL sp_tx_bmap_get_token_state(NULL, 'BONK', NULL, NULL, 1703000000);

-- Mint address with nearest tx to block_time
CALL sp_tx_bmap_get_token_state(NULL, NULL, 'DezXAZ...', NULL, 1703000000);
```

## Response Structure

### Success Response

```json
{
  "result": {
    "txs": {
      "signature": "5KtP...abc",
      "block_time": 1703000000,
      "block_time_utc": "2023-12-19 12:00:00",
      "edge_types": ["spl_transfer", "swap_in"],
      "prev": [
        {
          "signature": "4JkM...xyz",
          "block_time": 1702999900,
          "block_time_utc": "2023-12-19 11:58:20",
          "edge_types": ["spl_transfer"]
        }
      ],
      "next": [
        {
          "signature": "6LnQ...def",
          "block_time": 1703000100,
          "block_time_utc": "2023-12-19 12:01:40",
          "edge_types": ["swap_out"]
        }
      ]
    },
    "token": {
      "mint": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
      "symbol": "BONK"
    },
    "nodes": [
      {
        "address": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "label": "whale",
        "balance": 1000000.123456,
        "sol_balance": 5.432100000,
        "funded_by": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"
      }
    ],
    "edges": [
      {
        "source": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "target": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
        "amount": 50000.000000,
        "type": "spl_transfer",
        "signature": "5KtP...abc",
        "block_time": 1703000000,
        "block_time_utc": "2023-12-19 12:00:00"
      }
    ]
  }
}
```

### Error Responses

```json
// Token not found
{"result": {"error": "Token not found"}}

// Signature exists but not for this token
{"result": {"error": "Signature not found for this token", "signature": "...", "mint": "..."}}

// Signature doesn't exist
{"result": {"error": "Signature not found", "signature": "..."}}

// No transactions for token
{"result": {"error": "No transactions found for this token", "mint": "..."}}
```

## Output Details

### txs (Transaction Navigation)
- `signature`: Current transaction signature
- `block_time`: Unix timestamp
- `block_time_utc`: Human-readable UTC timestamp
- `edge_types`: Array of edge types in current transaction
- `prev`: Array of up to 5 previous transactions (most recent first)
- `next`: Array of up to 5 next transactions (oldest first)

### nodes (Token Holders)
- Only addresses with balance > 0 at the current `block_time`
- `label`: Address label or address_type if no label
- `balance`: Token balance (human-readable, adjusted for decimals)
- `sol_balance`: SOL balance at current block_time
- `funded_by`: Address that initially funded this wallet (for clustering)

### edges (Activity)
- Token-specific edges from the 11-tx sliding window (5 prev + current + 5 next)
- SOL transfers between token holders within the sliding window
- `type`: Edge type code (spl_transfer, swap_in, swap_out, sol_transfer, etc.)

## Notes

- When deriving token from signature only, base currencies are deprioritized:
  - SOL, WSOL (native/wrapped)
  - USDC, USDT, PYUSD, USDH, UXD (stablecoins)
  - MSOL, JITOSOL, BSOL, LSTSOL (liquid staking tokens)
- Nodes represent the state of token holders at the specified transaction's block_time
- Use `prev`/`next` arrays to navigate through transaction history

## Sample Output

```sql
CALL sp_tx_bmap_get_token_state(NULL, 'soltit', NULL, NULL, NULL);
```

```json
{
  "result": {
    "txs": {
      "next": [],
      "prev": [
        {
          "signature": "3yuhfUEBRXVgK3f5jDKNEEHYthdtXBqeej9tQzcukfaVRidsccs54kTPexdFizjSXRU7x6zDLxCwZ1jMLCJRZh5",
          "block_time": 1766112594,
          "edge_types": ["spl_transfer", "swap_in"],
          "block_time_utc": "2025-12-19 02:49:54.000000"
        },
        {
          "signature": "3LQoRuP8ZMaq2xWjCXiFA9h2ChMq7VvtCbrvNi43h5ANGdh6egqmfwS7LRriqRaivKUKvo69rKZSkmxmwRwnaaS4",
          "block_time": 1766108823,
          "edge_types": ["spl_transfer", "swap_in"],
          "block_time_utc": "2025-12-19 01:47:03.000000"
        },
        {
          "signature": "DzzMiUsxWXn4gedwaC1JtcSGscWVNYZyqdUHW8mBygKwxpkpvNHzpEChkLyqSPqXku9toohwrwosyxSG8Y11fmL",
          "block_time": 1766096784,
          "edge_types": ["spl_transfer", "swap_in"],
          "block_time_utc": "2025-12-18 22:26:24.000000"
        },
        {
          "signature": "Gvz7qW5XKKVRgf4VVxLneZNDNTVbacnDXuprvLHcBoVR1KDAfsdEN1HZxt1DBrRjkSTUczUg2N8tQp2XurEyQjS",
          "block_time": 1766096726,
          "edge_types": ["spl_transfer", "swap_out"],
          "block_time_utc": "2025-12-18 22:25:26.000000"
        },
        {
          "signature": "4PmbXoNA4qqAwqTf2ENQcDFuaVCpiWmHDfZGsWnmYboF8741m4KtL55vCBvM8r7jX5aunUW3oZc5PEe4hYnusyvy",
          "block_time": 1766096702,
          "edge_types": ["spl_transfer", "swap_out"],
          "block_time_utc": "2025-12-18 22:25:02.000000"
        }
      ],
      "signature": "54GtCFf4Bvkqjv8RB2sqpx4yiD68yowH85MeK2ck4NyZqDu3mJDQzbCJXTuLdmm2RSfAmGZfy5uNqGFng7ex5mCr",
      "block_time": 1766130323,
      "edge_types": ["spl_transfer", "swap_in"],
      "block_time_utc": "2025-12-19 07:45:23.000000"
    },
    "edges": [
      {
        "type": "spl_transfer",
        "amount": 5327335.0,
        "source": "GbLeL5XcQAhZALcFA8RMVnpuCLTt5Bh6tKNZDPdyrGz4",
        "target": "GpMZbSM2GgvTKHJirzeGfMFoaZ8UR2X7F4v8vHTvxFbL",
        "signature": "54GtCFf4Bvkqjv8RB2sqpx4yiD68yowH85MeK2ck4NyZqDu3mJDQzbCJXTuLdmm2RSfAmGZfy5uNqGFng7ex5mCr",
        "block_time": 1766130323,
        "block_time_utc": "2025-12-19 07:45:23.000000"
      },
      {
        "type": "swap_in",
        "amount": 5327335.0,
        "source": "GbLeL5XcQAhZALcFA8RMVnpuCLTt5Bh6tKNZDPdyrGz4",
        "target": "Arf5iyDLVnVCSgx33Fzzu16nBWk6k7jEqAzWtTvHmPs7",
        "signature": "54GtCFf4Bvkqjv8RB2sqpx4yiD68yowH85MeK2ck4NyZqDu3mJDQzbCJXTuLdmm2RSfAmGZfy5uNqGFng7ex5mCr",
        "block_time": 1766130323,
        "block_time_utc": "2025-12-19 07:45:23.000000"
      }
    ],
    "nodes": [
      {
        "label": "wallet",
        "address": "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",
        "balance": 206900000.0,
        "funded_by": null,
        "sol_balance": 0.0
      },
      {
        "label": "wallet",
        "address": "6qZRYpsJbx9WDtDfUkbCwopMGK6eh2yEP25Nzb2J4RxM",
        "balance": 10000000.0,
        "funded_by": "fr6yQkDmWy6R6pecbUsxXaw6EvRJznZ2HsK5frQgud8",
        "sol_balance": 0.0
      }
    ],
    "token": {
      "mint": "J2UXqJ1HYRwjCviLEqkY9mhoEPJ5UnoHEnbeijCweREV",
      "symbol": "SOLTIT"
    }
  }
}
```
