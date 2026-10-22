# theGuide

## Solana Transaction Flow Analysis Pipeline

**Version:** 1.0
**Last Updated:** December 2025

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Directory Structure](#3-directory-structure)
4. [Infrastructure & Services](#4-infrastructure--services)
5. [Database Schema](#5-database-schema)
6. [Message Queues](#6-message-queues)
7. [Worker Scripts](#7-worker-scripts)
8. [Configuration System](#8-configuration-system)
9. [Data Flow](#9-data-flow)
10. [Forensic Detection Patterns](#10-forensic-detection-patterns)
11. [Setup & Deployment](#11-setup--deployment)
12. [Usage Guide](#12-usage-guide)
13. [Common Queries](#13-common-queries)
14. [Performance & Scaling](#14-performance--scaling)
15. [Troubleshooting](#15-troubleshooting)

---

## 1. Overview

**theGuide** is a comprehensive Solana transaction analysis system designed to detect suspicious patterns including:

- **Wash Trading** - Artificial volume through coordinated trading
- **Chart Clipping** - Front-running large purchases to dump on buyers
- **Funding Chain Analysis** - Tracing wallet funding relationships
- **Sybil Clusters** - Multiple wallets controlled by single actors
- **Circular Flows** - Money laundering patterns

The system uses a worker-based architecture with:
- **RabbitMQ** for message queuing
- **MySQL** for persistent storage
- **Python** workers for data processing
- **NetworkX** for graph analysis

---

## 2. Architecture

```
                                    +------------------+
                                    |   Chainstack     |
                                    |   Solana RPC     |
                                    +--------+---------+
                                             |
                                             v
+----------------+              +------------------------+
|  guide-welcome |------------->|   guide-producer.py    |
|  (Interactive) |              |  Fetch signatures      |
+----------------+              +------------+-----------+
                                             |
                                             v
                                +------------------------+
                                |  tx.guide.signatures   |
                                |     (RabbitMQ)         |
                                +------------+-----------+
                                             |
                                             v
                                +------------------------+
                                |   guide-shredder.py    |
                                |  Decode transactions   |
                                +------------+-----------+
                                             |
                         +-------------------+-------------------+
                         |                                       |
                         v                                       v
              +--------------------+                   +--------------------+
              |     tx_guide       |                   | tx.guide.addresses |
              |  (MySQL edges)     |                   |    (RabbitMQ)      |
              +--------------------+                   +---------+----------+
                                                                 |
                                                                 v
                                                      +--------------------+
                                                      |  guide-funder.py   |
                                                      |  Trace funders     |
                                                      +---------+----------+
                                                                 |
                                                                 v
                                                      +--------------------+
                                                      |    tx_address      |
                                                      | (funded_by_id)     |
                                                      +--------------------+
                                                                 |
                                                                 v
                                                      +--------------------+
                                                      |guide-sync-funding  |
                                                      |  Aggregate edges   |
                                                      +---------+----------+
                                                                 |
                                                                 v
                                                      +--------------------+
                                                      |  tx_funding_edge   |
                                                      |tx_token_participant|
                                                      +--------------------+
                                                                 |
                         +-------------------+-------------------+
                         |                   |                   |
                         v                   v                   v
              +----------------+   +----------------+   +----------------+
              | guide-clipper  |   | guide-circular |   | guide-analytics|
              | Clip detection |   | Cycle detection|   | Graph analysis |
              +----------------+   +----------------+   +----------------+
```

---

## 3. Directory Structure

```
_theGuide/_build_all/
|
+-- .env                           # Environment configuration
+-- requirements.txt               # Python dependencies
+-- docker-compose.yml             # Docker service definitions
|
+-- 00-setup-all.py                # Master setup orchestrator
+-- 01-check-dependencies.py       # Dependency checker/installer
+-- 02-create-schema.py            # Database schema builder
+-- 03-create-queues.py            # RabbitMQ queue setup
+-- theGuide.md                    # This documentation
|
+-- _db/                           # Database definitions
|   +-- build-all.sql              # Master build script
|   +-- tables/                    # 23 table definitions
|   |   +-- config.sql
|   |   +-- tx.sql
|   |   +-- tx_address.sql
|   |   +-- tx_guide.sql
|   |   +-- tx_token.sql
|   |   +-- tx_pool.sql
|   |   +-- tx_program.sql
|   |   +-- tx_funding_edge.sql
|   |   +-- tx_token_participant.sql
|   |   +-- ...
|   +-- functions/                 # 8 SQL functions
|   |   +-- fn_tx_ensure_address.sql
|   |   +-- fn_tx_ensure_token.sql
|   |   +-- fn_tx_ensure_program.sql
|   |   +-- fn_tx_ensure_pool.sql
|   |   +-- fn_tx_extract_addresses.sql
|   |   +-- fn_tx_get_token_name.sql
|   |   +-- fn_get_guide_by_wallet.sql
|   |   +-- fn_get_guide_by_token.sql
|   +-- procedures/                # 14 stored procedures
|   |   +-- sp_config_get.sql
|   |   +-- sp_config_set.sql
|   |   +-- sp_tx_prime_batch.sql
|   |   +-- sp_tx_shred_batch.sql
|   |   +-- sp_tx_detect_chart_clipping.sql
|   |   +-- sp_tx_funding_chain.sql
|   |   +-- sp_tx_backfill_funding.sql
|   |   +-- ...
|   +-- views/                     # 13 analytical views
|   |   +-- vw_tx_token_info.sql
|   |   +-- vw_tx_funding_tree.sql
|   |   +-- vw_tx_funding_chain.sql
|   |   +-- vw_tx_common_funders.sql
|   |   +-- vw_tx_sybil_clusters.sql
|   |   +-- vw_tx_wash_roundtrip.sql
|   |   +-- vw_tx_wash_triangle.sql
|   |   +-- vw_tx_high_freq_pairs.sql
|   |   +-- ...
|   +-- data/                      # Seed data
|       +-- seed_all.sql           # Master seed script
|       +-- seed_tx_guide_type.sql # 42 transaction types
|       +-- seed_tx_guide_source.sql # 5 source types
|       +-- seed_config.sql        # Default configuration
|       +-- seed_tx_program.sql    # Known Solana programs
|
+-- _mq/                           # Message queue configuration
|   +-- rabbitmq-definitions.json  # Queue definitions
|
+-- _wrk/                          # Worker scripts
    +-- guide-welcome.py           # Interactive launcher
    +-- guide-health.py            # Health check/monitoring
    +-- guide-stop.py              # Worker shutdown utility
    |
    +-- guide-producer.py          # Signature fetcher
    +-- guide-shredder.py          # Transaction decoder
    +-- guide-funder.py            # Funding tracer
    +-- guide-sync-funding.py      # Edge aggregator
    |
    +-- guide-price-loader.py      # Token price fetcher
    +-- guide-market-loader.py     # Market data loader
    +-- guide-pool-enricher.py     # Pool metadata enricher
    +-- guide-backfill-tokens.py   # Token metadata backfill
    +-- guide-load-programs.py     # Program loader
    +-- guide-mint-scanner.py      # Mint address scanner
    |
    +-- guide-clipper.py           # Clipping detection
    +-- guide-circular-flow.py     # Circular flow detection
    +-- guide-wallet-hunter.py     # Deep wallet investigation
    +-- guide-token-forensic.py    # Token-level forensics
    +-- guide-analytics.py         # Graph-based analysis
    +-- guide-address-classifier.py # Address type classification
    +-- guide-to-networkx.py       # Export to graph formats
```

---

## 4. Infrastructure & Services

### Docker Services

| Service | Container | Ports | Purpose |
|---------|-----------|-------|---------|
| MySQL 8.0 | t16o_v1_mysql | 3396:3306 | Database backend |
| RabbitMQ 3-mgmt | t16o_v1_rabbitmq | 5692:5672, 15692:15672 | Message broker |

### Connection Details

**MySQL:**
```
Host: localhost
Port: 3396
User: root
Password: rootpassword
Database: t16o_db
Charset: utf8mb4
```

**RabbitMQ:**
```
Host: localhost
Port: 5692 (AMQP)
Port: 15692 (Management UI)
User: admin
Password: admin123
Virtual Host: /
```

### Docker Compose Configuration

```yaml
version: '3.8'
services:
  mysql:
    image: mysql:8.0
    container_name: t16o_v1_mysql
    ports:
      - "3396:3306"
    environment:
      MYSQL_ROOT_PASSWORD: rootpassword
      MYSQL_DATABASE: t16o_db
    volumes:
      - mysql_data:/var/lib/mysql

  rabbitmq:
    image: rabbitmq:3-management
    container_name: t16o_v1_rabbitmq
    ports:
      - "5692:5672"
      - "15692:15672"
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: admin123
```

---

## 5. Database Schema

### 5.1 Core Tables (23 total)

#### Lookup/Reference Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `config` | Runtime configuration | config_type, config_key, config_value, is_runtime_editable |
| `tx_guide_type` | Transaction type classifications | type_code, category, direction, risk_weight |
| `tx_guide_source` | Edge source types | source_code, source_name |

#### Entity Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `tx_address` | All addresses (wallets, pools, programs) | address (unique), address_type, funded_by_address_id |
| `tx_token` | Token metadata | mint_address_id, token_name, token_symbol, decimals |
| `tx_program` | Solana programs | program_address_id, name, program_type |
| `tx_pool` | DEX liquidity pools | pool_address_id, token1_id, token2_id |

#### Transaction Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `tx` | Transaction records | signature (unique), block_time, signer_address_id, tx_json |
| `tx_guide` | Fund flow edges | from_address_id, to_address_id, token_id, amount, edge_type_id |
| `tx_signer` | Transaction signers | tx_id, signer_address_id, signer_index |
| `tx_account` | Account state changes | tx_id, account_address_id, pre_balance, post_balance |
| `tx_instruction` | Decoded instructions | tx_id, program_id, parsed_data |
| `tx_transfer` | SPL token transfers | tx_id, from_address, to_address, token_id, amount |
| `tx_swap` | Detected swaps | tx_id, token_in, token_out, amount_in, amount_out |

#### Aggregation Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `tx_funding_edge` | Aggregated funding flows | from_address_id, to_address_id, total_sol, transfer_count |
| `tx_token_participant` | Token participation stats | token_id, address_id, buy_count, sell_count, net_position |
| `tx_token_holder` | Token holder tracking | token_id, holder_address_id, balance |

#### Supporting Tables

| Table | Purpose |
|-------|---------|
| `tx_activity` | Activity records from Solscan |
| `tx_party` | Token balance changes |
| `tx_sol_balance_change` | SOL balance deltas |
| `tx_token_balance_change` | Token balance deltas |
| `tx_token_market` | Market/pool data |
| `tx_token_price` | Historical token prices |

### 5.2 Functions (8)

| Function | Purpose | Returns |
|----------|---------|---------|
| `fn_tx_ensure_address(addr, type)` | Get or create address | address_id |
| `fn_tx_ensure_token(mint_id)` | Get or create token | token_id |
| `fn_tx_ensure_program(addr, name, type)` | Get or create program | program_id |
| `fn_tx_ensure_pool(pool_addr)` | Get or create pool | pool_id |
| `fn_tx_extract_addresses(json)` | Extract addresses from tx JSON | TABLE |
| `fn_tx_get_token_name(mint)` | Get token name | VARCHAR |
| `fn_get_guide_by_wallet(addr)` | Query edges for wallet | TABLE |
| `fn_get_guide_by_token(mint)` | Query edges for token | TABLE |

### 5.3 Stored Procedures (14)

| Procedure | Purpose |
|-----------|---------|
| `sp_config_get(type, key)` | Get configuration value |
| `sp_config_get_by_type(type)` | Get all configs of type |
| `sp_config_get_changes(since)` | Get config changes since version |
| `sp_config_set(type, key, val, by)` | Update configuration |
| `sp_tx_prime_batch(json)` | Insert batch of primed transactions |
| `sp_tx_shred_batch(json)` | Shred decoded transactions, extract edges |
| `sp_tx_detect_chart_clipping(token)` | Detect clipping patterns |
| `sp_tx_funding_chain(addr)` | Trace funding chains |
| `sp_tx_backfill_funding(batch)` | Backfill funding info |
| `sp_tx_clear_tables()` | Truncate all tx tables |
| `sp_tx_hound_indexes(bits)` | Build detection indexes |
| `sp_tx_release_hound(limit)` | Release hound results |

### 5.4 Analytical Views (13)

| View | Purpose |
|------|---------|
| `vw_tx_token_info` | Token metadata with aggregates |
| `vw_tx_funding_tree` | Funding relationships |
| `vw_tx_funding_chain` | Transitive funding chains |
| `vw_tx_common_funders` | Wallets with same funder |
| `vw_tx_flow_concentration` | Flow concentration metrics |
| `vw_tx_address_risk_score` | Computed risk scores |
| `vw_tx_high_freq_pairs` | High-frequency trading pairs |
| `vw_tx_high_freq_pairs2` | Alternative pair detection |
| `vw_tx_high_freq_pairs3` | Third pair detection method |
| `vw_tx_rapid_fire` | Rapid-fire transactions |
| `vw_tx_sybil_clusters` | Detected Sybil clusters |
| `vw_tx_wash_roundtrip` | Roundtrip wash trades |
| `vw_tx_wash_triangle` | Triangle flow patterns |

### 5.5 Transaction Type Classifications (42 types)

**Categories:**

| Category | Types | Risk Range |
|----------|-------|------------|
| Transfer | sol_transfer, spl_transfer, wallet_funded | 0-20 |
| Swap | swap_in, swap_out | 10-30 |
| Fees | transaction_fee, priority_fee, protocol_fee | 0-10 |
| Account | create_ata, close_ata | 0-10 |
| Lending | deposit, withdraw, borrow, repay, liquidation | 10-60 |
| Staking | stake, unstake, rewards | 5-20 |
| Liquidity | add, remove, rewards, farming | 10-40 |
| Bridge | bridge_in, bridge_out | 20-50 |
| Perpetuals | deposit, withdraw, open, close, liquidation | 20-70 |
| NFT | transfer, sale, mint | 10-30 |
| Other | mint, burn, airdrop, unknown | 0-50 |

---

## 6. Message Queues

### Queue Configuration

| Queue | Durable | Max Priority | Purpose |
|-------|---------|--------------|---------|
| `tx.guide.signatures` | Yes | 10 | Transaction signatures for shredding |
| `tx.guide.addresses` | Yes | 10 | Addresses needing funding lookup |
| `tx.funding.addresses` | Yes | 10 | Addresses for funder identification |
| `tx.enriched` | Yes | None | Final enriched output |

### Queue Flow

```
tx.guide.signatures
    |
    +---> guide-shredder.py consumes
    |         |
    |         +---> tx.guide.addresses (produced)
    |
tx.guide.addresses
    |
    +---> guide-funder.py consumes
              |
              +---> tx.funding.addresses (produced)
```

### Priority Levels

- **10** - Highest priority (urgent processing)
- **5** - Normal priority (default)
- **0** - Lowest priority (background processing)

---

## 7. Worker Scripts

### 7.1 Core Pipeline Workers

| Script | Purpose | Input | Output |
|--------|---------|-------|--------|
| `guide-producer.py` | Fetch transaction signatures | Chainstack RPC | mq.guide.decoder/detailer.request |
| `guide-decoder.py` | Fetch decoded tx data from Solscan | mq.guide.decoder.request | t16o_db_staging.txs |
| `guide-detailer.py` | Fetch detail/balance data from Solscan | mq.guide.detailer.request | t16o_db_staging.txs |
| `guide-shredder.py` | Parse staging rows into tx tables | t16o_db_staging.txs (poll) | tx_guide, tx_transfer, tx_swap |
| `guide-enricher.py` | Enrich tokens/pools via Solscan API | DB poll | tx_token, tx_pool |
| `guide-funder.py` | Classify addresses, trace funders | DB poll | tx_address |
| `guide-aggregator.py` | Aggregate edges, compute stats | DB poll | tx_funding_edge, tx_token_participant |
| `guide-synchronizer.py` | Fill gaps in mint tx history | mq.guide.synchronizer.request | t16o_db_staging.txs_sync → decoder/detailer |

**Producer Usage:**
```bash
python guide-producer.py <MINT_ADDRESS> [--max-signatures 5000]
```

**Shredder Usage:**
```bash
python guide-shredder.py [--prefetch 5]
```

**Funder Usage:**
```bash
python guide-funder.py [--prefetch 50]
```

**Sync Funding Usage:**
```bash
python guide-sync-funding.py --daemon --interval 60
```

### 7.2 Data Enrichment Workers

| Script | Purpose | Data Source |
|--------|---------|-------------|
| `guide-price-loader.py` | Fetch historical prices | Solscan API |
| `guide-market-loader.py` | Load market data | Solscan API |
| `guide-pool-enricher.py` | Enrich pool metadata | Solscan API |
| `guide-backfill-tokens.py` | Backfill token info | Solscan API |
| `guide-load-programs.py` | Load known programs | CSV/API |
| `guide-mint-scanner.py` | Scan for mints | Database |

### 7.3 Forensic Analysis Workers

| Script | Purpose | Technique |
|--------|---------|-----------|
| `guide-clipper.py` | Detect chart clipping | Time-window pattern matching |
| `guide-circular-flow.py` | Find circular flows | NetworkX cycle detection |
| `guide-wallet-hunter.py` | Deep wallet investigation | Contact tracing |
| `guide-token-forensic.py` | Token-level forensics | Price/holder analysis |
| `guide-analytics.py` | Graph-based analysis | Clustering, centrality |
| `guide-address-classifier.py` | Classify address types | Behavioral heuristics |

### 7.4 Utility Workers

| Script | Purpose | Usage |
|--------|---------|-------|
| `guide-welcome.py` | Interactive launcher | `python guide-welcome.py` |
| `guide-health.py` | Health check | `python guide-health.py [--watch]` |
| `guide-stop.py` | Stop workers | `python guide-stop.py [--force]` |
| `guide-to-networkx.py` | Export to GEXF | `python guide-to-networkx.py` |

---

## 8. Configuration System

### 8.1 Config Table Schema

```sql
config (
  id INT AUTO_INCREMENT,
  config_type VARCHAR(64),
  config_key VARCHAR(64),
  config_value VARCHAR(1024),
  value_type ENUM('string','int','decimal','bool','json'),
  description VARCHAR(512),
  default_value VARCHAR(1024),
  is_sensitive TINYINT(1),
  is_runtime_editable TINYINT(1),
  requires_restart TINYINT(1),
  version INT,
  updated_by VARCHAR(64)
)
```

### 8.2 Configuration Categories

| Type | Keys | Description |
|------|------|-------------|
| batch | mint_batch_size, transaction_batch_size | Batch processing sizes |
| cache | mint_cache_enabled, asset_cache_ttl | Caching behavior |
| feature | dry_run_mode, maintenance_mode | Feature flags |
| fetcher.asset | max_concurrent_requests, rate_limit_ms | Asset fetcher config |
| fetcher.transaction | max_concurrent_requests, max_retry | Tx fetcher config |
| logging | default_level, console_enabled | Logging settings |
| rabbitmq | host, port, virtual_host | RabbitMQ connection |
| worker.prefetch | tx.fetch.rpc, tx.fetch.db | Queue prefetch counts |

### 8.3 Environment Variables (.env)

```bash
# Database
DB_HOST=localhost
DB_PORT=3396
DB_USER=root
DB_PASSWORD=rootpassword
DB_NAME=t16o_db

# RabbitMQ
MQ_HOST=localhost
MQ_PORT=5692
MQ_MGMT_PORT=15692
MQ_USER=admin
MQ_PASSWORD=admin123

# APIs
SOLANA_RPC_URL=https://solana-mainnet.core.chainstack.com/...
SOLSCAN_API_TOKEN=eyJhbGciOiJIUzI1NiIs...
SOLSCAN_API_BASE=https://pro-api.solscan.io/v2.0
HELIUS_API_KEY=...

# Worker Settings
SYNC_FUNDING_INTERVAL=60
PRODUCER_MAX_SIGNATURES=10000
SHREDDER_PREFETCH=5
FUNDER_PREFETCH=50
```

### 8.4 Runtime Configuration Updates

```sql
-- Update a config value
CALL sp_config_set('worker', 'shredder_prefetch', '10', 'admin');

-- Get current value
CALL sp_config_get('worker', 'shredder_prefetch');

-- Get all changes since version
CALL sp_config_get_changes(100);
```

---

## 9. Data Flow

### 9.1 Transaction Shredding Process

```
1. PRODUCER: Fetch signatures from RPC
   |
   v
2. SHREDDER: For each signature batch:
   a. Fetch decoded tx from Solscan
   b. Extract addresses (wallets, ATAs, mints, pools, programs)
   c. Create address records (fn_tx_ensure_address)
   d. Parse transfers -> tx_guide edges
   e. Parse swaps -> tx_guide edges
   f. Parse fees -> tx_guide edges
   g. Store full JSON in tx.tx_json
   |
   v
3. FUNDER: For each new address:
   a. Fetch first transactions from Solscan
   b. Find first SOL inflow (funding tx)
   c. Update tx_address.funded_by_address_id
   |
   v
4. SYNC-FUNDING: Periodically:
   a. Aggregate tx_guide edges -> tx_funding_edge
   b. Compute tx_token_participant stats
```

### 9.2 Edge Types in tx_guide

| Edge Type | Direction | Description |
|-----------|-----------|-------------|
| transfer_in | inflow | SOL/token received |
| transfer_out | outflow | SOL/token sent |
| swap_in | inflow | Token received from swap |
| swap_out | outflow | Token sent to swap |
| fee_paid | outflow | Transaction/priority fee |
| protocol_fee | outflow | DEX protocol fee |
| liquidity_add | outflow | LP deposit |
| liquidity_remove | inflow | LP withdrawal |

### 9.3 Funding Chain Tracing

```sql
Address A
  +-- funded_by_address_id --> Address B
       +-- funded_by_address_id --> Address C
            +-- funded_by_address_id --> NULL (origin)
```

The `vw_tx_funding_tree` view shows:
- wallet_id, wallet_address
- funder_id, funder_address
- funding_sol (amount)
- funding_tx_signature

---

## 10. Forensic Detection Patterns

### 10.1 Chart Clipping

**Pattern:** Large buyer front-run by coordinated sellers

```
Time T0:     Wallet A buys 1M tokens (swap_in)
Time T0-30s: Wallets B,C,D sold to create the "clip"
```

**Detection:** `sp_tx_detect_chart_clipping` or `guide-clipper.py`

**Indicators:**
- Multiple sells in 30-60 second window before large buy
- Sellers share common funder
- High concentration of sells to single buyer

### 10.2 Circular Flows

**Pattern:** Funds flowing in a cycle (A -> B -> C -> A)

```
Wallet A ---> Wallet B
   ^            |
   |            v
Wallet C <--- (cycle)
```

**Detection:** `guide-circular-flow.py` using NetworkX cycle detection

**Indicators:**
- Closed loops in fund flow graph
- Similar amounts in cycle
- Short time between transfers

### 10.3 Sybil Clusters

**Pattern:** Multiple wallets controlled by single actor

```
Wallets A, B, C, D:
- Same funding source
- Same tokens traded
- Same timing patterns
- Similar amounts
```

**Detection:** `vw_tx_sybil_clusters`

**Indicators:**
- Common funder (depth 1-3)
- Behavioral clustering (tokens, timing)
- Coordinated trading patterns

### 10.4 Wash Trading

**Pattern:** Artificial volume through self-trading

**Roundtrip Detection:** (`vw_tx_wash_roundtrip`)
```
Wallet A: buys 1M TOKEN
Wallet A: sells 1M TOKEN (same session)
```

**Triangle Detection:** (`vw_tx_wash_triangle`)
```
A --> B --> C --> A (coordinated 3-way)
```

### 10.5 High Frequency Pairs

**Pattern:** Same wallet pair trading repeatedly

```
Wallet A <--> Wallet B: 100+ transactions/day
```

**Detection:** `vw_tx_high_freq_pairs`

**Indicators:**
- Transaction count > threshold
- Both directions (bidirectional flow)
- Short time intervals

---

## 11. Setup & Deployment

### 11.1 Prerequisites

- Python 3.10+
- Docker & Docker Compose
- Git
- 4GB+ RAM recommended

### 11.2 Quick Start

```bash
cd _theGuide/_build_all

# Full automated setup
python 00-setup-all.py

# Launch pipeline
cd _wrk
python guide-welcome.py
```

### 11.3 Manual Setup Steps

```bash
# Step 1: Check and install dependencies
python 01-check-dependencies.py --install --start-docker

# Step 2: Create database schema
python 02-create-schema.py --with-seed

# Step 3: Create RabbitMQ queues
python 03-create-queues.py

# Step 4: Verify setup
python _wrk/guide-health.py
```

### 11.4 Python Dependencies

```
mysql-connector-python>=8.0.0
pika>=1.3.0
requests>=2.28.0
networkx>=3.0
matplotlib>=3.6.0
orjson>=3.8.0
numpy>=1.24.0        # optional
pandas>=2.0.0        # optional
scipy>=1.10.0        # optional
```

---

## 12. Usage Guide

### 12.1 Starting the Pipeline

**Interactive Mode:**
```bash
cd _wrk
python guide-welcome.py
# Enter mint address when prompted
```

**Manual Mode:**
```bash
# Terminal 1: Producer
python guide-producer.py 7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU

# Terminal 2: Shredder (daemon)
python guide-shredder.py

# Terminal 3: Funder (daemon)
python guide-funder.py

# Terminal 4: Sync (daemon)
python guide-sync-funding.py --daemon --interval 60
```

### 12.2 Running Forensic Analysis

**Clipping Detection:**
```bash
python guide-clipper.py --token SOLTIT --window 30 --gexf clipper.gexf
```

**Wallet Investigation:**
```bash
python guide-wallet-hunter.py GbLeL5Xc... --deep --json report.json
```

**Token Forensics:**
```bash
python guide-token-forensic.py J2UXqJ1H... --gexf forensics.gexf
```

**Graph Analytics:**
```bash
python guide-analytics.py --token SOLTIT --cycles --clusters
```

### 12.3 Health Monitoring

```bash
# Full health check
python guide-health.py

# Quick check (services only)
python guide-health.py --quick

# Continuous monitoring (30s interval)
python guide-health.py --watch

# JSON output
python guide-health.py --json
```

### 12.4 Stopping Workers

```bash
# List running workers
python guide-stop.py --list

# Stop all workers (with confirmation)
python guide-stop.py

# Force stop (no confirmation)
python guide-stop.py --force
```

---

## 13. Common Queries

### 13.1 Find All Transactions for a Token

```sql
SELECT
    g.id,
    fa.address AS from_wallet,
    ta.address AS to_wallet,
    g.amount / POW(10, g.decimals) AS amount,
    gt.type_code,
    FROM_UNIXTIME(g.block_time) AS tx_time
FROM tx_guide g
JOIN tx_address fa ON g.from_address_id = fa.id
JOIN tx_address ta ON g.to_address_id = ta.id
JOIN tx_guide_type gt ON g.edge_type_id = gt.id
JOIN tx_token t ON g.token_id = t.id
JOIN tx_address ma ON t.mint_address_id = ma.id
WHERE ma.address = 'TOKEN_MINT_ADDRESS'
ORDER BY g.block_time DESC
LIMIT 100;
```

### 13.2 Trace Funding Chain

```sql
WITH RECURSIVE funding_chain AS (
    SELECT id, address, funded_by_address_id, 1 AS depth
    FROM tx_address
    WHERE address = 'WALLET_ADDRESS'

    UNION ALL

    SELECT a.id, a.address, a.funded_by_address_id, fc.depth + 1
    FROM tx_address a
    JOIN funding_chain fc ON a.id = fc.funded_by_address_id
    WHERE fc.depth < 10
)
SELECT * FROM funding_chain;
```

### 13.3 Find Wallets with Common Funder

```sql
SELECT
    funder.address AS funder_address,
    COUNT(DISTINCT ta.id) AS funded_wallet_count,
    GROUP_CONCAT(ta.address SEPARATOR ', ') AS funded_wallets
FROM tx_address ta
JOIN tx_address funder ON ta.funded_by_address_id = funder.id
WHERE ta.address_type IN ('wallet', 'unknown')
GROUP BY funder.id
HAVING funded_wallet_count > 5
ORDER BY funded_wallet_count DESC;
```

### 13.4 Detect Rapid-Fire Transactions

```sql
SELECT
    fa.address,
    COUNT(*) AS tx_count,
    COUNT(DISTINCT g.token_id) AS token_count,
    MIN(FROM_UNIXTIME(g.block_time)) AS first_tx,
    MAX(FROM_UNIXTIME(g.block_time)) AS last_tx
FROM tx_guide g
JOIN tx_address fa ON g.from_address_id = fa.id
WHERE g.block_time > UNIX_TIMESTAMP(NOW() - INTERVAL 24 HOUR)
GROUP BY g.from_address_id
HAVING tx_count > 100
ORDER BY tx_count DESC;
```

### 13.5 Find High-Risk Addresses

```sql
SELECT * FROM vw_tx_address_risk_score
WHERE risk_score > 70
ORDER BY risk_score DESC
LIMIT 50;
```

### 13.6 Token Participant Analysis

```sql
SELECT
    a.address,
    tp.buy_count,
    tp.sell_count,
    tp.buy_volume / POW(10, t.decimals) AS buy_volume,
    tp.sell_volume / POW(10, t.decimals) AS sell_volume,
    tp.net_position / POW(10, t.decimals) AS net_position
FROM tx_token_participant tp
JOIN tx_token t ON tp.token_id = t.id
JOIN tx_address a ON tp.address_id = a.id
JOIN tx_address ma ON t.mint_address_id = ma.id
WHERE ma.address = 'TOKEN_MINT_ADDRESS'
ORDER BY tp.sell_count DESC
LIMIT 50;
```

---

## 14. Performance & Scaling

### 14.1 Key Indexes

```sql
-- tx table
CREATE INDEX idx_tx_block_time ON tx(block_time DESC);
CREATE UNIQUE INDEX idx_tx_signature ON tx(signature);

-- tx_guide table
CREATE INDEX idx_guide_from_time ON tx_guide(from_address_id, block_time);
CREATE INDEX idx_guide_to_time ON tx_guide(to_address_id, block_time);
CREATE INDEX idx_guide_token ON tx_guide(token_id, block_time);

-- tx_address table
CREATE UNIQUE INDEX idx_address ON tx_address(address);
CREATE INDEX idx_address_funder ON tx_address(funded_by_address_id);

-- tx_funding_edge table
CREATE UNIQUE INDEX idx_funding_edge ON tx_funding_edge(from_address_id, to_address_id);

-- tx_token_participant table
CREATE INDEX idx_participant_sells ON tx_token_participant(token_id, sell_count DESC);
CREATE INDEX idx_participant_buys ON tx_token_participant(token_id, buy_count DESC);
```

### 14.2 MySQL Tuning

```ini
[mysqld]
innodb_buffer_pool_size = 2G
innodb_log_buffer_size = 256M
innodb_flush_log_at_trx_commit = 2
innodb_io_capacity = 8000
innodb_io_capacity_max = 16000
binlog_expire_logs_seconds = 259200
```

### 14.3 Worker Optimization

| Setting | Recommended | Impact |
|---------|-------------|--------|
| shredder_prefetch | 5-10 | Memory vs throughput |
| funder_prefetch | 50-100 | API rate vs speed |
| sync_interval | 60s | DB load vs freshness |
| batch_size | 50-100 | Commit frequency |

### 14.4 Scaling Options

**Horizontal:**
- Multiple shredder instances (RabbitMQ distributes)
- Multiple funder instances
- Read replicas for analytics

**Vertical:**
- Increase worker prefetch counts
- More MySQL buffer pool
- SSD storage for database

---

## 15. Troubleshooting

### 15.1 Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Connection refused (MySQL) | Container not running | `docker-compose up -d` |
| Channel closed by broker | Queue doesn't exist | `python 03-create-queues.py` |
| API rate limit exceeded | Too many requests | Reduce prefetch, add delays |
| Foreign key constraint | Missing address | Call fn_tx_ensure_address first |
| Duplicate entry | Race condition | Use INSERT IGNORE |

### 15.2 Diagnostic Commands

```bash
# Check Docker containers
docker-compose ps
docker-compose logs t16o_v1_mysql
docker-compose logs t16o_v1_rabbitmq

# Verify database schema
python 02-create-schema.py --verify

# Check queue status
python 03-create-queues.py --status

# Full health check
python _wrk/guide-health.py

# Check running workers
python _wrk/guide-stop.py --list
```

### 15.3 Log Locations

| Component | Log Location |
|-----------|--------------|
| MySQL | `docker logs t16o_v1_mysql` |
| RabbitMQ | `docker logs t16o_v1_rabbitmq` |
| Workers | Console output (stdout) |

### 15.4 Recovery Procedures

**Reset Database:**
```bash
# Caution: Destroys all data
python 02-create-schema.py --drop-first --with-seed
```

**Purge Queues:**
```bash
python 03-create-queues.py --purge
```

**Restart Services:**
```bash
docker-compose restart
```

---

## Appendix A: Seed Data Reference

### Transaction Types (tx_guide_type)

| ID | Type Code | Category | Direction | Risk Weight |
|----|-----------|----------|-----------|-------------|
| 1 | sol_transfer | transfer | neutral | 10 |
| 2 | spl_transfer | transfer | neutral | 10 |
| 3 | wallet_funded | transfer | inflow | 5 |
| 4 | swap_in | swap | inflow | 20 |
| 5 | swap_out | swap | outflow | 20 |
| 6 | transaction_fee | fee | outflow | 0 |
| 7 | priority_fee | fee | outflow | 5 |
| ... | ... | ... | ... | ... |

### Source Types (tx_guide_source)

| ID | Source Code | Source Name |
|----|-------------|-------------|
| 1 | solscan | Solscan API |
| 2 | helius | Helius RPC |
| 3 | chainstack | Chainstack RPC |
| 4 | manual | Manual Entry |
| 5 | derived | Derived/Computed |

### Known Programs (tx_program)

| Name | Address | Type |
|------|---------|------|
| System Program | 11111111111111111111111111111111 | system |
| Token Program | TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA | token |
| Associated Token | ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL | token |
| Jupiter v6 | JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4 | router |
| Raydium AMM | 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 | dex |
| Raydium CPMM | CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C | dex |
| Orca Whirlpool | whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc | dex |
| Meteora DLMM | LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo | dex |
| Meteora Pools | Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB | dex |

---

## Appendix B: File Inventory

### Setup Scripts (4)
- `00-setup-all.py` - Master orchestrator
- `01-check-dependencies.py` - Dependency checker
- `02-create-schema.py` - Schema builder
- `03-create-queues.py` - Queue setup

### Database Objects (58)
- Tables: 23
- Functions: 8
- Procedures: 14
- Views: 13

### Worker Scripts (21)
- Core pipeline: 4
- Data enrichment: 6
- Forensic analysis: 6
- Utilities: 5

### Configuration Files (4)
- `.env` - Environment variables
- `docker-compose.yml` - Service definitions
- `requirements.txt` - Python packages
- `rabbitmq-definitions.json` - Queue definitions

---

*Generated: December 2025*
*theGuide v1.0 - Solana Transaction Flow Analysis Pipeline*
