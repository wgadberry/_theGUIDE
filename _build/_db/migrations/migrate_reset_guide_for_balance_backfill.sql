-- Reset tx_state bit 32 (GUIDE_LOADED) for transactions where tx_guide has NULL balances
-- but tx_token_balance_change data now exists. The loader will re-process these on its next cycle.
--
-- Run once. Safe to re-run (idempotent — only targets rows with bit 32 set).

UPDATE tx t
JOIN (
    SELECT DISTINCT g.tx_id
    FROM tx_guide g
    WHERE g.token_id IS NOT NULL
      AND (g.from_token_post_balance IS NULL OR g.to_token_post_balance IS NULL)
      AND EXISTS (
          SELECT 1 FROM tx_token_balance_change tbc
          WHERE tbc.tx_id = g.tx_id AND tbc.token_id = g.token_id
      )
) affected ON affected.tx_id = t.id
SET t.tx_state = t.tx_state & ~32
WHERE t.tx_state & 32 = 32;

-- Reset bit 32 for SOL transfer edges with NULL balances where tx_sol_balance_change exists
UPDATE tx t
JOIN (
    SELECT DISTINCT g.tx_id
    FROM tx_guide g
    WHERE (g.from_token_post_balance IS NULL OR g.to_token_post_balance IS NULL)
      AND EXISTS (
          SELECT 1 FROM tx_sol_balance_change sbc
          WHERE sbc.tx_id = g.tx_id
            AND (sbc.address_id = g.from_address_id OR sbc.address_id = g.to_address_id)
      )
) affected ON affected.tx_id = t.id
SET t.tx_state = t.tx_state & ~32
WHERE t.tx_state & 32 = 32;

-- Default close_ata edges to 0 balance (account closures have no tbc data by design)
UPDATE tx_guide g
JOIN tx_guide_type gt ON gt.id = g.edge_type_id AND gt.type_code = 'close_ata'
SET g.from_token_post_balance  = COALESCE(g.from_token_post_balance, 0),
    g.from_token_pre_balance   = COALESCE(g.from_token_pre_balance, 0),
    g.to_token_post_balance    = COALESCE(g.to_token_post_balance, 0),
    g.to_token_pre_balance     = COALESCE(g.to_token_pre_balance, 0)
WHERE g.from_token_post_balance IS NULL
   OR g.to_token_post_balance IS NULL;
