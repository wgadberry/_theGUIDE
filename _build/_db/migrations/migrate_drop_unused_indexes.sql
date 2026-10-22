-- ============================================================================
-- migrate_drop_unused_indexes.sql
-- Generated: 2026-02-14
-- Source: performance_schema.table_io_waits_summary_by_index_usage
-- Context: MySQL uptime ~4hrs, most tables 0 rows (recently rebuilt)
--          ALL non-PRIMARY indexes show count_star=0
--
-- WARNING: Review each section before executing. Some indexes may be needed
--          for queries not yet run since last restart, or for FK constraints.
-- ============================================================================

-- ============================================================================
-- TABLE: config (41 rows) — tiny lookup table
-- ============================================================================
-- idx_config_type (config_type) — used by sp_config_get WHERE config_type = ?
-- idx_updated_at  (updated_at)  — administrative
-- RECOMMENDATION: KEEP both — tiny table, no cost, config_type is queried by SPs

-- ALTER TABLE config DROP INDEX idx_config_type;
-- ALTER TABLE config DROP INDEX idx_updated_at;

-- ============================================================================
-- TABLE: tx (0 rows) — main transaction table
-- ============================================================================
-- idx_agg_account        (agg_account_address_id)     — FK index for aggregation
-- idx_agg_token_in       (agg_token_in_id)            — FK index for aggregation
-- idx_agg_token_out      (agg_token_out_id)           — FK index for aggregation
-- idx_signer             (signer_address_id)          — wallet lookups
-- idx_tx_blocktime       (id, block_time)             — time-range queries
-- idx_tx_guide_pending   (is_guide_loaded, id)        — OLD: replaced by tx_state bitmask
-- idx_tx_request_log_id  (request_log_id)             — FK index
-- idx_tx_type_state      (type_state)                 — OLD: replaced by tx_state bitmask
-- idx_type_state         (id, type_state)             — OLD: replaced by tx_state bitmask
-- tx_ibfk_agg_fee_token  (agg_fee_token_id)           — FK index for aggregation
-- tx_ibfk_agg_program    (agg_program_id)             — FK index for aggregation

-- SAFE TO DROP — obsolete columns from pre-bitmask era:
ALTER TABLE tx DROP INDEX idx_tx_guide_pending;    -- is_guide_loaded replaced by tx_state & 32
ALTER TABLE tx DROP INDEX idx_tx_type_state;       -- type_state replaced by tx_state bitmask
ALTER TABLE tx DROP INDEX idx_type_state;          -- type_state replaced by tx_state bitmask

-- REVIEW — FK/aggregation indexes (may be needed by .NET side):
-- ALTER TABLE tx DROP INDEX idx_agg_account;
-- ALTER TABLE tx DROP INDEX idx_agg_token_in;
-- ALTER TABLE tx DROP INDEX idx_agg_token_out;
-- ALTER TABLE tx DROP INDEX idx_signer;
-- ALTER TABLE tx DROP INDEX idx_tx_blocktime;
-- ALTER TABLE tx DROP INDEX idx_tx_request_log_id;
-- ALTER TABLE tx DROP INDEX tx_ibfk_agg_fee_token;
-- ALTER TABLE tx DROP INDEX tx_ibfk_agg_program;

-- ============================================================================
-- TABLE: tx_activity (0 rows)
-- ============================================================================
-- idx_account                       (account_address_id) — FK index
-- idx_activity_type                 (activity_type)      — low cardinality
-- idx_guide_loaded                  (guide_loaded)       — OLD: boolean flag, replaced by tx_state
-- idx_program                       (program_id)         — FK index
-- tx_activity_ibfk_outer_program    (outer_program_id)   — FK index

-- SAFE TO DROP — obsolete:
ALTER TABLE tx_activity DROP INDEX idx_guide_loaded;  -- guide_loaded boolean, superseded by tx_state

-- REVIEW — FK indexes (needed if FK constraints exist):
-- ALTER TABLE tx_activity DROP INDEX idx_account;
-- ALTER TABLE tx_activity DROP INDEX idx_activity_type;
-- ALTER TABLE tx_activity DROP INDEX idx_program;
-- ALTER TABLE tx_activity DROP INDEX tx_activity_ibfk_outer_program;

-- ============================================================================
-- TABLE: tx_activity_type_map (25 rows) — tiny lookup table
-- ============================================================================
-- activity_type  (activity_type) — UNIQUE index
-- guide_type_id  (guide_type_id) — FK index
-- RECOMMENDATION: KEEP both — tiny table, no cost

-- ALTER TABLE tx_activity_type_map DROP INDEX activity_type;
-- ALTER TABLE tx_activity_type_map DROP INDEX guide_type_id;

-- ============================================================================
-- TABLE: tx_address (4 rows, currently)
-- ============================================================================
-- idx_addr_type          (id, address_type)                                — composite for type lookups
-- idx_first_seen         (first_seen_block_time)                           — time-range queries
-- idx_funded_by          (funded_by_address_id)                            — funding tree lookups
-- idx_funding_checked    (funding_checked_at)                              — guide-funder.py batch selection
-- idx_funding_lookup     (address_type, funded_by_address_id, funding_checked_at) — guide-funder.py
-- idx_program            (program_id)                                      — FK index
-- idx_tx_address_request_log_id (request_log_id)                           — FK index
-- RECOMMENDATION: KEEP all — used by guide-funder.py, bmap viewer, funding analysis

-- ALTER TABLE tx_address DROP INDEX idx_addr_type;
-- ALTER TABLE tx_address DROP INDEX idx_first_seen;
-- ALTER TABLE tx_address DROP INDEX idx_funded_by;
-- ALTER TABLE tx_address DROP INDEX idx_funding_checked;
-- ALTER TABLE tx_address DROP INDEX idx_funding_lookup;
-- ALTER TABLE tx_address DROP INDEX idx_program;
-- ALTER TABLE tx_address DROP INDEX idx_tx_address_request_log_id;

-- ============================================================================
-- TABLE: tx_api_key (6 rows) — tiny table
-- ============================================================================
-- idx_active   (active)   — boolean filter
-- idx_api_key  (api_key)  — key lookups
-- RECOMMENDATION: KEEP both — tiny table, api_key lookup is auth-critical

-- ALTER TABLE tx_api_key DROP INDEX idx_active;
-- ALTER TABLE tx_api_key DROP INDEX idx_api_key;

-- ============================================================================
-- TABLE: tx_bmap_state (0 rows)
-- ============================================================================
-- fk_bmap_address       (address_id)                    — FK index
-- ix_token_addr_time    (token_id, address_id, block_time) — bmap queries
-- ix_token_time         (token_id, block_time)          — bmap time-range
-- ix_tx                 (tx_id)                         — FK index
-- uq_token_tx_addr      (token_id, tx_id, address_id)  — UNIQUE constraint

-- REVIEW — these support bmap viewer queries:
-- ALTER TABLE tx_bmap_state DROP INDEX fk_bmap_address;
-- ALTER TABLE tx_bmap_state DROP INDEX ix_token_addr_time;
-- ALTER TABLE tx_bmap_state DROP INDEX ix_token_time;
-- ALTER TABLE tx_bmap_state DROP INDEX ix_tx;
-- ALTER TABLE tx_bmap_state DROP INDEX uq_token_tx_addr;

-- ============================================================================
-- TABLE: tx_funding_edge (0 rows)
-- ============================================================================
-- idx_from            (from_address_id)    — funding graph traversal
-- idx_last_transfer   (last_transfer_time) — time-based queries
-- idx_to              (to_address_id)      — funding graph traversal
-- idx_total_sol       (total_sol)          — sorting/filtering by amount

-- CANDIDATE TO DROP — low-cardinality sort index:
ALTER TABLE tx_funding_edge DROP INDEX idx_total_sol;    -- sorting by SOL amount, unlikely to help
ALTER TABLE tx_funding_edge DROP INDEX idx_last_transfer; -- time sort on small table

-- REVIEW — graph traversal indexes (needed for funding analysis):
-- ALTER TABLE tx_funding_edge DROP INDEX idx_from;
-- ALTER TABLE tx_funding_edge DROP INDEX idx_to;

-- ============================================================================
-- TABLE: tx_guide (0 rows) — main guide edges table
-- ============================================================================
-- idx_block_time      (block_time)                  — time-range queries
-- idx_from_ata        (from_token_account_id)       — ATA lookups
-- idx_source          (source_id, source_row_id)    — dedup/source tracking
-- idx_swap_direction  (swap_direction)               — low cardinality (buy/sell/null)
-- idx_to_ata          (to_token_account_id)         — ATA lookups

-- SAFE TO DROP — low cardinality, won't help:
ALTER TABLE tx_guide DROP INDEX idx_swap_direction;  -- only 3 values (buy/sell/null)

-- REVIEW — may be needed for bmap/analysis queries:
-- ALTER TABLE tx_guide DROP INDEX idx_block_time;
-- ALTER TABLE tx_guide DROP INDEX idx_from_ata;
-- ALTER TABLE tx_guide DROP INDEX idx_source;
-- ALTER TABLE tx_guide DROP INDEX idx_to_ata;

-- ============================================================================
-- TABLE: tx_guide_source (5 rows) — tiny lookup
-- ============================================================================
-- uk_code (source_code) — UNIQUE constraint
-- RECOMMENDATION: KEEP — unique constraint on lookup table

-- ALTER TABLE tx_guide_source DROP INDEX uk_code;

-- ============================================================================
-- TABLE: tx_guide_type (42 rows) — tiny lookup
-- ============================================================================
-- idx_category  (category)    — category filter
-- idx_risk      (risk_weight) — risk sorting
-- RECOMMENDATION: KEEP both — tiny table, used by views

-- ALTER TABLE tx_guide_type DROP INDEX idx_category;
-- ALTER TABLE tx_guide_type DROP INDEX idx_risk;

-- ============================================================================
-- TABLE: tx_participant (0 rows)
-- ============================================================================
-- idx_address_id  (address_id)          — FK index
-- idx_fee_payer   (is_fee_payer, address_id) — fee payer lookups
-- idx_signer      (is_signer, address_id)    — signer lookups
-- idx_tx_id       (tx_id)               — FK index
-- uk_tx_addr      (tx_id, address_id)   — UNIQUE constraint

-- REVIEW — all seem structurally important:
-- ALTER TABLE tx_participant DROP INDEX idx_address_id;
-- ALTER TABLE tx_participant DROP INDEX idx_fee_payer;
-- ALTER TABLE tx_participant DROP INDEX idx_signer;
-- ALTER TABLE tx_participant DROP INDEX idx_tx_id;
-- ALTER TABLE tx_participant DROP INDEX uk_tx_addr;

-- ============================================================================
-- TABLE: tx_pool (0 rows)
-- ============================================================================
-- idx_lp_token     (lp_token_id)       — LP token lookups
-- idx_pool_addr    (pool_address_id)   — pool address lookups (sp_tx_guide_loader)
-- idx_program      (program_id)        — FK index
-- idx_token1       (token1_id)         — token pair lookups
-- idx_token2       (token2_id)         — token pair lookups
-- tx_pool_ibfk_tx  (first_seen_tx_id)  — FK index
-- RECOMMENDATION: KEEP all — actively used by guide loader and pool backfill

-- ALTER TABLE tx_pool DROP INDEX idx_lp_token;
-- ALTER TABLE tx_pool DROP INDEX idx_pool_addr;
-- ALTER TABLE tx_pool DROP INDEX idx_program;
-- ALTER TABLE tx_pool DROP INDEX idx_token1;
-- ALTER TABLE tx_pool DROP INDEX idx_token2;
-- ALTER TABLE tx_pool DROP INDEX tx_pool_ibfk_tx;

-- ============================================================================
-- TABLE: tx_program (0 rows) — lookup table
-- ============================================================================
-- idx_type (program_type) — type filter
-- RECOMMENDATION: KEEP — tiny lookup table

-- ALTER TABLE tx_program DROP INDEX idx_type;

-- ============================================================================
-- TABLE: tx_request_log (0 rows)
-- ============================================================================
-- idx_api_key_id      (api_key_id)        — FK index
-- idx_correlation_id  (correlation_id)    — request tracing
-- idx_created_at      (created_at)        — time-range queries
-- idx_request_id      (request_id)        — request lookup
-- idx_source_status   (source, status)    — composite filter
-- idx_status          (status)            — status filter
-- idx_target_worker   (target_worker)     — worker routing

-- CANDIDATE TO DROP — redundant or low-value:
ALTER TABLE tx_request_log DROP INDEX idx_status;  -- covered by idx_source_status prefix

-- REVIEW — may be needed by .NET workers:
-- ALTER TABLE tx_request_log DROP INDEX idx_api_key_id;
-- ALTER TABLE tx_request_log DROP INDEX idx_correlation_id;
-- ALTER TABLE tx_request_log DROP INDEX idx_created_at;
-- ALTER TABLE tx_request_log DROP INDEX idx_request_id;
-- ALTER TABLE tx_request_log DROP INDEX idx_source_status;
-- ALTER TABLE tx_request_log DROP INDEX idx_target_worker;

-- ============================================================================
-- TABLE: tx_signer (0 rows)
-- ============================================================================
-- idx_signer (signer_address_id) — signer lookups
-- RECOMMENDATION: KEEP — only non-PK index on table

-- ALTER TABLE tx_signer DROP INDEX idx_signer;

-- ============================================================================
-- TABLE: tx_sol_balance_change (0 rows)
-- ============================================================================
-- idx_address       (address_id)                                  — FK index
-- idx_sbc_addr_tx   (address_id, tx_id)                           — composite
-- idx_sbc_tx_addr   (tx_id, address_id, pre_balance, post_balance) — covering index
-- idx_tx            (tx_id)                                       — FK index

-- CANDIDATE TO DROP — redundant indexes:
ALTER TABLE tx_sol_balance_change DROP INDEX idx_address;  -- prefix of idx_sbc_addr_tx
ALTER TABLE tx_sol_balance_change DROP INDEX idx_tx;       -- prefix of idx_sbc_tx_addr

-- REVIEW:
-- ALTER TABLE tx_sol_balance_change DROP INDEX idx_sbc_addr_tx;
-- ALTER TABLE tx_sol_balance_change DROP INDEX idx_sbc_tx_addr;

-- ============================================================================
-- TABLE: tx_swap (0 rows) — has MANY FK indexes
-- ============================================================================
-- idx_account            (account_address_id)       — FK index
-- idx_amm                (amm_id)                   — pool/AMM lookups (guide loader)
-- idx_swap_tx_token1     (tx_id, token_1_id, amount_1)  — composite
-- idx_swap_tx_token2     (tx_id, token_2_id, amount_2)  — composite
-- idx_token_1            (token_1_id)               — FK index
-- idx_token_2            (token_2_id)               — FK index
-- idx_tx_token2_amt2     (tx_id, token_2_id, amount_2)  — DUPLICATE of idx_swap_tx_token2
-- tx_swap_ibfk_fee_token    (fee_token_id)          — FK index
-- tx_swap_ibfk_outer_program (outer_program_id)     — FK index
-- tx_swap_ibfk_owner_1      (owner_1_address_id)    — FK index
-- tx_swap_ibfk_owner_2      (owner_2_address_id)    — FK index
-- tx_swap_ibfk_program      (program_id)            — FK index
-- tx_swap_ibfk_ta_1_1       (token_account_1_1_address_id) — FK index
-- tx_swap_ibfk_ta_1_2       (token_account_1_2_address_id) — FK index
-- tx_swap_ibfk_ta_2_1       (token_account_2_1_address_id) — FK index
-- tx_swap_ibfk_ta_2_2       (token_account_2_2_address_id) — FK index

-- SAFE TO DROP — exact duplicate:
ALTER TABLE tx_swap DROP INDEX idx_tx_token2_amt2;  -- duplicate of idx_swap_tx_token2 (same columns)

-- REVIEW — FK indexes (may be required by foreign key constraints):
-- ALTER TABLE tx_swap DROP INDEX idx_account;
-- ALTER TABLE tx_swap DROP INDEX idx_amm;
-- ALTER TABLE tx_swap DROP INDEX idx_swap_tx_token1;
-- ALTER TABLE tx_swap DROP INDEX idx_swap_tx_token2;
-- ALTER TABLE tx_swap DROP INDEX idx_token_1;
-- ALTER TABLE tx_swap DROP INDEX idx_token_2;
-- ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_fee_token;
-- ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_outer_program;
-- ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_owner_1;
-- ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_owner_2;
-- ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_program;
-- ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_ta_1_1;
-- ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_ta_1_2;
-- ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_ta_2_1;
-- ALTER TABLE tx_swap DROP INDEX tx_swap_ibfk_ta_2_2;

-- ============================================================================
-- TABLE: tx_token_balance_change (0 rows) — MOST INDEXES OF ANY TABLE
-- ============================================================================
-- fk_token_bal_account     (token_account_address_id)                          — FK
-- idx_owner                (owner_address_id)                                  — FK
-- idx_owner_token          (owner_address_id, token_id)                        — composite
-- idx_tbc_acct_token_tx    (token_account_address_id, token_id, tx_id)         — composite
-- idx_tbc_owner_token_tx   (owner_address_id, token_id, tx_id)                — composite
-- idx_tbc_tx_token_owner   (tx_id, token_id, owner_address_id, pre_balance, post_balance) — covering
-- idx_token                (token_id)                                          — FK
-- idx_token_account_token  (token_account_address_id, token_id)               — composite
-- idx_tx                   (tx_id)                                            — FK
-- idx_tx_owner_token       (tx_id, owner_address_id, token_id)                — composite
-- idx_tx_token_account     (tx_id, token_account_address_id)                  — UNIQUE

-- SAFE TO DROP — redundant (prefixes of larger composites):
ALTER TABLE tx_token_balance_change DROP INDEX fk_token_bal_account;  -- prefix of idx_tbc_acct_token_tx
ALTER TABLE tx_token_balance_change DROP INDEX idx_owner;             -- prefix of idx_tbc_owner_token_tx
ALTER TABLE tx_token_balance_change DROP INDEX idx_owner_token;       -- prefix of idx_tbc_owner_token_tx
ALTER TABLE tx_token_balance_change DROP INDEX idx_token_account_token; -- prefix of idx_tbc_acct_token_tx
ALTER TABLE tx_token_balance_change DROP INDEX idx_tx;                -- prefix of idx_tbc_tx_token_owner

-- REVIEW — remaining composites and covering indexes:
-- ALTER TABLE tx_token_balance_change DROP INDEX idx_token;            -- only single-col token_id index
-- ALTER TABLE tx_token_balance_change DROP INDEX idx_tbc_acct_token_tx;
-- ALTER TABLE tx_token_balance_change DROP INDEX idx_tbc_owner_token_tx;
-- ALTER TABLE tx_token_balance_change DROP INDEX idx_tbc_tx_token_owner;
-- ALTER TABLE tx_token_balance_change DROP INDEX idx_tx_owner_token;   -- subset of idx_tbc_tx_token_owner
-- ALTER TABLE tx_token_balance_change DROP INDEX idx_tx_token_account; -- UNIQUE constraint

-- ============================================================================
-- TABLE: tx_token_market (0 rows)
-- ============================================================================
-- idx_program  (program_id) — FK index
-- idx_token1   (token1_id)  — FK index
-- idx_token2   (token2_id)  — FK index
-- uk_pool      (pool_id)    — UNIQUE constraint
-- RECOMMENDATION: KEEP all — structural integrity

-- ALTER TABLE tx_token_market DROP INDEX idx_program;
-- ALTER TABLE tx_token_market DROP INDEX idx_token1;
-- ALTER TABLE tx_token_market DROP INDEX idx_token2;
-- ALTER TABLE tx_token_market DROP INDEX uk_pool;

-- ============================================================================
-- TABLE: tx_token_participant (0 rows)
-- ============================================================================
-- idx_address        (address_id)              — FK index
-- idx_last_seen      (last_seen)               — time-range
-- idx_token_buyers   (token_id, buy_count)     — leaderboard queries
-- idx_token_sellers  (token_id, sell_count)     — leaderboard queries
-- idx_token_volume   (token_id, sell_volume)    — leaderboard queries

-- REVIEW — analytics indexes, may not be queried yet:
-- ALTER TABLE tx_token_participant DROP INDEX idx_address;
-- ALTER TABLE tx_token_participant DROP INDEX idx_last_seen;
-- ALTER TABLE tx_token_participant DROP INDEX idx_token_buyers;
-- ALTER TABLE tx_token_participant DROP INDEX idx_token_sellers;
-- ALTER TABLE tx_token_participant DROP INDEX idx_token_volume;

-- ============================================================================
-- TABLE: tx_token_price (0 rows)
-- ============================================================================
-- idx_date (date) — time-range queries
-- RECOMMENDATION: KEEP — only non-PK index

-- ALTER TABLE tx_token_price DROP INDEX idx_date;

-- ============================================================================
-- TABLE: tx_transfer (0 rows)
-- ============================================================================
-- idx_source_owner             (source_owner_address_id) — FK index
-- idx_token                    (token_id)                — FK index
-- tx_transfer_ibfk_base_token  (base_token_id)           — FK index
-- tx_transfer_ibfk_dest        (destination_address_id)  — FK index
-- tx_transfer_ibfk_outer_program (outer_program_id)      — FK index
-- tx_transfer_ibfk_program     (program_id)              — FK index
-- tx_transfer_ibfk_source      (source_address_id)       — FK index

-- REVIEW — all are FK indexes, likely required by constraints:
-- ALTER TABLE tx_transfer DROP INDEX idx_source_owner;
-- ALTER TABLE tx_transfer DROP INDEX idx_token;
-- ALTER TABLE tx_transfer DROP INDEX tx_transfer_ibfk_base_token;
-- ALTER TABLE tx_transfer DROP INDEX tx_transfer_ibfk_dest;
-- ALTER TABLE tx_transfer DROP INDEX tx_transfer_ibfk_outer_program;
-- ALTER TABLE tx_transfer DROP INDEX tx_transfer_ibfk_program;
-- ALTER TABLE tx_transfer DROP INDEX tx_transfer_ibfk_source;


-- ============================================================================
-- SUMMARY
-- ============================================================================
-- Total unused indexes found:  113
-- Uncommented (safe to drop):   14
--   - 3x tx (obsolete type_state/is_guide_loaded columns)
--   - 1x tx_activity (obsolete guide_loaded boolean)
--   - 2x tx_funding_edge (low-value sort indexes)
--   - 1x tx_guide (low-cardinality swap_direction)
--   - 1x tx_request_log (redundant status, covered by source_status)
--   - 2x tx_sol_balance_change (redundant prefixes)
--   - 1x tx_swap (exact duplicate idx_tx_token2_amt2)
--   - 5x tx_token_balance_change (redundant prefixes of composites)
-- Commented (need discussion):  99
--   - FK indexes that may be required by foreign key constraints
--   - Indexes on tables that haven't been queried since restart
--   - Structural/UNIQUE indexes
-- ============================================================================
