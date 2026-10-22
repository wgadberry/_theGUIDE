-- ─────────────────────────────────────────────────────────────────────────────
-- sp_get_guide_edge_narratives
--
-- Builds embedding narratives for tx_guide edges, with full object resolution
-- across tx_address, tx_token, tx_pool, tx_program, tx_guide_type,
-- tx_guide_source. Returns one row per edge with:
--   - narrative  : human-readable text string to be embedded
--   - payload    : all scalar fields needed for Qdrant point storage
--
-- Called by T16OVectorClient in batches via p_start_id / p_end_id.
-- Batch size tuned in Python (default 1000 rows per call).
--
-- Notes:
--   token_id = 0 is native SOL — LEFT JOIN on tx_token skips cleanly
--   block_time_utc joined from tx (not on tx_guide) and stored as payload
--   All CONCAT sections use IFNULL / CONCAT_WS to handle NULLs gracefully
-- ─────────────────────────────────────────────────────────────────────────────

DELIMITER $$

DROP PROCEDURE IF EXISTS sp_get_guide_edge_narratives$$

CREATE PROCEDURE sp_get_guide_edge_narratives(
    IN  p_start_id  BIGINT UNSIGNED,
    IN  p_end_id    BIGINT UNSIGNED
)
BEGIN

    SELECT
        -- ── Point ID ──────────────────────────────────────────────────
        g.id                                                        AS guide_id,

        -- ── Narrative ─────────────────────────────────────────────────
        -- Each section is a separate CONCAT_WS arg so NULL sections are
        -- dropped cleanly from the final joined string.
        CONCAT_WS('\n',

            -- [1] Edge classification
            CONCAT(
                'Edge: ', gt.type_code,
                ' [', gt.category, '/', IFNULL(gt.direction, 'neutral'), ']',
                ' risk=', gt.risk_weight,
                IF(gt.indicator > 0, CONCAT(' indicator=', gt.indicator), '')
            ),

            -- [2] Transaction identity and cost
            CONCAT(
                'Transaction: ', t.signature,
                ' | block_time_utc: ',
                    IFNULL(DATE_FORMAT(t.block_time_utc, '%Y-%m-%d %H:%i:%s UTC'), 'unknown'),
                ' | fee: ', IFNULL(g.fee, 0), ' lamports',
                IF(g.priority_fee IS NOT NULL AND g.priority_fee > 0,
                   CONCAT(' | priority: ', g.priority_fee, ' lamports'), '')
            ),

            -- [3] From wallet (full detail)
            CONCAT(
                'From: ', fa.address,
                ' [', IFNULL(fa.address_type, 'unknown'), ']',
                IF(fa.label           IS NOT NULL, CONCAT(' label="',      fa.label,           '"'), ''),
                IF(fa.active_age_days IS NOT NULL, CONCAT(' age=',         fa.active_age_days, 'd'), ''),
                IF(fba.address        IS NOT NULL, CONCAT(' funded_by=',   fba.address),              '')
            ),

            -- [4] To wallet (full detail)
            CONCAT(
                'To: ', ta.address,
                ' [', IFNULL(ta.address_type, 'unknown'), ']',
                IF(ta.label IS NOT NULL, CONCAT(' label="', ta.label, '"'), '')
            ),

            -- [5] From ATA — NULL for SOL, present for SPL transfers
            IF(fata.address IS NOT NULL,
               CONCAT('From ATA: ', fata.address,
                      ' [', IFNULL(fata.address_type, 'ata'), ']'),
               NULL),

            -- [6] To ATA — NULL for SOL
            IF(tata.address IS NOT NULL,
               CONCAT('To ATA: ', tata.address,
                      ' [', IFNULL(tata.address_type, 'ata'), ']'),
               NULL),

            -- [7] Token identity (token_id=0 = native SOL)
            IF(g.token_id = 0,
               'Token: SOL (native) decimals=9',
               CONCAT(
                   'Token: ', IFNULL(tok.token_symbol, 'UNKNOWN'),
                   IF(tok.token_name IS NOT NULL,
                      CONCAT(' (', tok.token_name, ')'), ''),
                   ' mint=', IFNULL(tm.address, 'unknown'),
                   ' decimals=', IFNULL(g.decimals, IFNULL(tok.decimals, '?')),
                   IF(tok.token_type IS NOT NULL,
                      CONCAT(' type=', tok.token_type), '')
               )
            ),

            -- [8] Raw amount + token balance changes
            CONCAT_WS(' | ',
                IF(g.amount IS NOT NULL,
                   CONCAT('Amount: ', g.amount, ' raw'), NULL),
                IF(g.from_token_pre_balance IS NOT NULL
                   OR g.from_token_post_balance IS NOT NULL,
                   CONCAT('from_token pre=',
                          IFNULL(g.from_token_pre_balance, '?'),
                          ' post=',
                          IFNULL(g.from_token_post_balance, '?')), NULL),
                IF(g.to_token_pre_balance IS NOT NULL
                   OR g.to_token_post_balance IS NOT NULL,
                   CONCAT('to_token pre=',
                          IFNULL(g.to_token_pre_balance, '?'),
                          ' post=',
                          IFNULL(g.to_token_post_balance, '?')), NULL)
            ),

            -- [9] SOL balance changes — omit section if all NULL
            IF(g.from_sol_pre_balance IS NOT NULL
               OR g.to_sol_pre_balance IS NOT NULL,
               CONCAT_WS(' | ',
                   IF(g.from_sol_pre_balance IS NOT NULL,
                      CONCAT('SOL from pre=', g.from_sol_pre_balance,
                             ' post=', IFNULL(g.from_sol_post_balance, '?')), NULL),
                   IF(g.to_sol_pre_balance IS NOT NULL,
                      CONCAT('SOL to pre=', g.to_sol_pre_balance,
                             ' post=', IFNULL(g.to_sol_post_balance, '?')), NULL)
               ),
               NULL),

            -- [10] Tax — omit if no tax
            IF(g.tax_bps IS NOT NULL,
               CONCAT('Tax: ', g.tax_bps, ' bps',
                      IF(g.tax_amount IS NOT NULL,
                         CONCAT(' | tax_amount=', g.tax_amount), '')),
               NULL),

            -- [11] Pool — omit if no pool
            IF(pool_addr.address IS NOT NULL,
               CONCAT(
                   'Pool: ', pool_addr.address,
                   IF(pl.dex        IS NOT NULL, CONCAT(' [', pl.dex, ']'),         ''),
                   IF(pl.token_pair IS NOT NULL, CONCAT(' pair=', pl.token_pair),   ''),
                   IF(pl.pool_label IS NOT NULL, CONCAT(' label="', pl.pool_label, '"'), '')
               ),
               NULL),

            -- [12] Program (resolved via pool) — omit if no program
            IF(prg.name IS NOT NULL,
               CONCAT(
                   'Program: ', IFNULL(pra.address, ''),
                   ' [', IFNULL(prg.program_type, 'other'), '] ',
                   prg.name,
                   IF(prg.is_verified = 1, ' verified', '')
               ),
               NULL),

            -- [13] Source table and instruction index
            CONCAT_WS(' | ',
                IF(gs.source_code IS NOT NULL,
                   CONCAT('Source: ', gs.source_code,
                          IF(g.source_row_id IS NOT NULL,
                             CONCAT(' row=', g.source_row_id), '')),
                   NULL),
                IF(g.ins_index IS NOT NULL,
                   CONCAT('ins_index=', g.ins_index), NULL)
            ),

            -- [14] Swap direction — omit if not a swap edge
            IF(g.swap_direction IS NOT NULL,
               CONCAT('Swap direction: ', g.swap_direction),
               NULL)

        )                                                           AS narrative,

        -- ── Scalar Payload Fields ──────────────────────────────────────
        -- Stored as-is in Qdrant point payload.
        -- block_time (int) used for range filter; block_time_utc for display.

        -- Time
        g.block_time                                                AS block_time,
        t.block_time_utc                                            AS block_time_utc,

        -- Transaction
        t.signature                                                 AS signature,
        g.tx_id                                                     AS tx_id,
        g.ins_index                                                 AS ins_index,

        -- Edge classification
        gt.type_code                                                AS edge_type_code,
        gt.category                                                 AS edge_category,
        gt.direction                                                AS edge_direction,
        gt.risk_weight                                              AS risk_weight,
        gt.indicator                                                AS indicator,
        g.swap_direction                                            AS swap_direction,

        -- From wallet
        fa.address                                                  AS from_address,
        fa.address_type                                             AS from_address_type,
        fa.label                                                    AS from_label,
        fa.active_age_days                                          AS from_age_days,
        fba.address                                                 AS from_funded_by,
        fa.funding_amount                                           AS from_funding_amount,

        -- To wallet
        ta.address                                                  AS to_address,
        ta.address_type                                             AS to_address_type,
        ta.label                                                    AS to_label,

        -- ATAs
        fata.address                                                AS from_token_account,
        tata.address                                                AS to_token_account,

        -- Token
        g.token_id                                                  AS token_id,
        CASE WHEN g.token_id = 0 THEN 'SOL'
             ELSE IFNULL(tok.token_symbol, 'UNKNOWN')
        END                                                         AS token_symbol,
        CASE WHEN g.token_id = 0 THEN 'Solana'
             ELSE tok.token_name
        END                                                         AS token_name,
        CASE WHEN g.token_id = 0 THEN NULL
             ELSE tm.address
        END                                                         AS token_mint,
        CASE WHEN g.token_id = 0 THEN 9
             ELSE IFNULL(g.decimals, tok.decimals)
        END                                                         AS decimals,
        tok.token_type                                              AS token_type,

        -- Amount and balance changes
        g.amount                                                    AS amount,
        g.from_token_pre_balance                                    AS from_token_pre_balance,
        g.from_token_post_balance                                   AS from_token_post_balance,
        g.to_token_pre_balance                                      AS to_token_pre_balance,
        g.to_token_post_balance                                     AS to_token_post_balance,
        g.from_sol_pre_balance                                      AS from_sol_pre_balance,
        g.from_sol_post_balance                                     AS from_sol_post_balance,
        g.to_sol_pre_balance                                        AS to_sol_pre_balance,
        g.to_sol_post_balance                                       AS to_sol_post_balance,

        -- Tax
        g.tax_bps                                                   AS tax_bps,
        g.tax_amount                                                AS tax_amount,

        -- Fees
        g.fee                                                       AS fee,
        g.priority_fee                                              AS priority_fee,

        -- Pool
        pool_addr.address                                           AS pool_address,
        pl.pool_label                                               AS pool_label,
        pl.dex                                                      AS dex,
        pl.token_pair                                               AS token_pair,

        -- Program
        pra.address                                                 AS program_address,
        prg.name                                                    AS program_name,
        prg.program_type                                            AS program_type,
        prg.is_verified                                             AS program_verified,

        -- Source provenance
        gs.source_code                                              AS source_code,
        gs.source_name                                              AS source_name,
        g.source_row_id                                             AS source_row_id,

        -- Timestamps (converted to unix epoch in Python for Qdrant)
        g.created_utc                                               AS created_utc,
        g.updated_utc                                               AS updated_utc,

        -- Request provenance
        t.block_id                                                  AS block_id,
        rl.correlation_id                                           AS correlation_id,
        rl.request_id                                               AS request_id,
        rl.created_utc                                              AS request_date,
        ak.api_key                                                  AS api_key

    FROM      tx_guide         g

    -- Core transaction (block_time_utc lives here)
    JOIN      tx               t       ON  t.id    = g.tx_id

    -- From / To owner wallets (NOT NULL FKs — INNER JOIN safe)
    JOIN      tx_address       fa      ON  fa.id   = g.from_address_id
    JOIN      tx_address       ta      ON  ta.id   = g.to_address_id

    -- Source / Dest ATAs — NULL for native SOL transfers
    LEFT JOIN tx_address       fata    ON  fata.id = g.from_token_account_id
    LEFT JOIN tx_address       tata    ON  tata.id = g.to_token_account_id

    -- Wallet that first funded the from_address
    LEFT JOIN tx_address       fba     ON  fba.id  = fa.funded_by_address_id

    -- Token — skip join entirely when token_id = 0 (native SOL)
    LEFT JOIN tx_token         tok     ON  tok.id  = g.token_id AND g.token_id > 0
    LEFT JOIN tx_address       tm      ON  tm.id   = tok.mint_address_id

    -- Pool and pool address
    LEFT JOIN tx_pool          pl      ON  pl.id          = g.pool_id
    LEFT JOIN tx_address       pool_addr ON pool_addr.id  = pl.pool_address_id

    -- Program resolved via pool
    LEFT JOIN tx_program       prg     ON  prg.id  = pl.program_id
    LEFT JOIN tx_address       pra     ON  pra.id  = prg.program_address_id

    -- Edge type (NOT NULL FK — INNER JOIN safe)
    JOIN      tx_guide_type    gt      ON  gt.id   = g.edge_type_id

    -- Source table reference
    LEFT JOIN tx_guide_source  gs      ON  gs.id   = g.source_id

    -- Request provenance (tx → tx_request_log → tx_api_key)
    LEFT JOIN tx_request_log   rl      ON  rl.id   = t.request_log_id
    LEFT JOIN tx_api_key       ak      ON  ak.id   = rl.api_key_id

    WHERE g.id BETWEEN p_start_id AND p_end_id

    ORDER BY g.id ASC;

END$$

DELIMITER ;
