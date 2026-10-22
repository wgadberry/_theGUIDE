-- Migration: Add missing indexes for sp_tx_guide_loader performance
-- These indexes support the correlated balance subqueries and swap enrichment JOINs

-- tx_token_balance_change: same-tx fallback balance lookup (tx_id, owner_address_id, token_id)
-- Currently only idx_owner_token (owner_address_id, token_id) exists - missing tx_id leading column
ALTER TABLE tx_token_balance_change
  ADD INDEX idx_tx_owner_token (tx_id, owner_address_id, token_id);

-- tx_token_balance_change: LAG lookups filter on (token_account_address_id, token_id)
-- Currently only single-column fk_token_bal_account exists
ALTER TABLE tx_token_balance_change
  ADD INDEX idx_token_account_token (token_account_address_id, token_id);

-- tx_swap: swap enrichment JOIN on (tx_id, token_1_id, amount_1)
-- Currently only idx_tx (tx_id) exists, forcing row scanning after tx_id match
ALTER TABLE tx_swap
  ADD INDEX idx_tx_token1_amt1 (tx_id, token_1_id, amount_1);

-- tx_swap: swap enrichment JOIN on (tx_id, token_2_id, amount_2)
ALTER TABLE tx_swap
  ADD INDEX idx_tx_token2_amt2 (tx_id, token_2_id, amount_2);
