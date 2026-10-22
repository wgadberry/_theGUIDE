-- tx_guide_source seed data
-- Edge source classifications for theGuide

TRUNCATE TABLE tx_guide_source;

INSERT INTO tx_guide_source (id, source_code, source_name, description, is_active) VALUES
(1, 'tx_transfer', 'Transfer', 'SPL token transfers from shredder', 1),
(2, 'tx_swap', 'Swap', 'DEX swap legs from shredder', 1),
(3, 'tx_sol_balance_change', 'SOL Balance Change', 'Native SOL balance deltas', 1),
(4, 'tx_fee', 'Transaction Fee', 'Derived from tx.fee / priority_fee', 1),
(5, 'manual', 'Manual Entry', 'Manually added edges for investigation', 1);
