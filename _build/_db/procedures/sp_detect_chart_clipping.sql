-- sp_detect_chart_clipping stored procedure
-- Generated from t16o_db instance

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_detect_chart_clipping`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_detect_chart_clipping`(
    IN p_address_label VARCHAR(64),      -- Token to analyze (e.g., 'SOLTIT')
    IN p_buy_amt_sol DECIMAL(18,9),      -- Min SOL for qualifying buy (e.g., 5.0)
    IN p_time_window_seconds INT         -- Window for sells after buy (e.g., 300 = 5 min)
)
BEGIN
    DECLARE v_episode_id INT DEFAULT 0;
    DECLARE v_last_episode_end BIGINT DEFAULT 0;

    -- =========================================================
    -- Step 1: Create temp table of qualifying BUY transactions
    -- A buy = received token + spent WSOL in same tx
    -- Use token_account_id as unique key to avoid duplicates
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_qualifying_buys;

    CREATE TEMPORARY TABLE tmp_qualifying_buys AS
    SELECT
        buy_token.tx_id,
        buy_token.token_account_id,
        buy_token.token_account_address,
        buy_token.signature,
        buy_token.block_time,
        buy_token.block_time_utc,
        buy_token.owner_address AS buyer_wallet,
        buy_token.owner_label AS buyer_label,
        buy_token.mint_address,
        buy_token.mint_label,
        buy_token.ui_amount_abs AS token_amount_bought,
        COALESCE(wsol_out.sol_spent, 0) AS sol_spent,
        buy_token.program_address,
        buy_token.program_label
    FROM vw_party_activity buy_token
    -- Subquery to get total WSOL spent per tx+owner (avoids cartesian product)
    LEFT JOIN (
        SELECT tx_id, owner_address, SUM(ui_amount_abs) AS sol_spent
        FROM vw_party_activity
        WHERE mint_label = 'WSOL' AND direction = 'out'
        GROUP BY tx_id, owner_address
    ) wsol_out ON buy_token.tx_id = wsol_out.tx_id
              AND buy_token.owner_address = wsol_out.owner_address
    WHERE
        buy_token.mint_label = p_address_label
        AND buy_token.direction = 'in'                         -- Received the token (bought)
        AND buy_token.action_type = 'swap'
        AND COALESCE(wsol_out.sol_spent, 0) >= p_buy_amt_sol   -- Meets SOL threshold
    ORDER BY buy_token.block_time;

    -- Index for faster lookups
    ALTER TABLE tmp_qualifying_buys ADD INDEX idx_block_time (block_time);
    ALTER TABLE tmp_qualifying_buys ADD INDEX idx_mint (mint_address);

    -- =========================================================
    -- Step 2: Create temp table of ALL sells for this token
    -- A sell = sent token + received WSOL in same tx
    -- Use token_account_id as unique key to avoid duplicates
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_all_sells;

    CREATE TEMPORARY TABLE tmp_all_sells AS
    SELECT
        sell_token.tx_id,
        sell_token.token_account_id,
        sell_token.token_account_address,
        sell_token.signature,
        sell_token.block_time,
        sell_token.block_time_utc,
        sell_token.owner_address AS seller_wallet,
        sell_token.owner_label AS seller_label,
        sell_token.mint_address,
        sell_token.mint_label,
        sell_token.ui_amount_abs AS token_amount_sold,
        COALESCE(wsol_in.sol_received, 0) AS sol_received,
        sell_token.program_address,
        sell_token.program_label
    FROM vw_party_activity sell_token
    -- Subquery to get total WSOL received per tx+owner (avoids cartesian product)
    LEFT JOIN (
        SELECT tx_id, owner_address, SUM(ui_amount_abs) AS sol_received
        FROM vw_party_activity
        WHERE mint_label = 'WSOL' AND direction = 'in'
        GROUP BY tx_id, owner_address
    ) wsol_in ON sell_token.tx_id = wsol_in.tx_id
             AND sell_token.owner_address = wsol_in.owner_address
    WHERE
        sell_token.mint_label = p_address_label
        AND sell_token.direction = 'out'                        -- Sent the token (sold)
        AND sell_token.action_type = 'swap'
    ORDER BY sell_token.block_time;

    -- Index for faster lookups
    ALTER TABLE tmp_all_sells ADD INDEX idx_block_time (block_time);
    ALTER TABLE tmp_all_sells ADD INDEX idx_mint (mint_address);

    -- =========================================================
    -- Step 3: Assign episode_id to buys based on time gaps
    -- New episode starts if buy is > time_window after last episode ended
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_buys_with_episodes;

    CREATE TEMPORARY TABLE tmp_buys_with_episodes (
        episode_id INT,
        tx_id BIGINT UNSIGNED,
        token_account_id INT UNSIGNED,
        token_account_address VARCHAR(44),
        signature VARCHAR(100),
        block_time BIGINT,
        block_time_utc DATETIME,
        buyer_wallet VARCHAR(44),
        buyer_label VARCHAR(64),
        mint_address VARCHAR(44),
        mint_label VARCHAR(64),
        token_amount_bought DECIMAL(36,18),
        sol_spent DECIMAL(36,18),
        program_address VARCHAR(44),
        program_label VARCHAR(64),
        INDEX idx_episode (episode_id),
        INDEX idx_block_time (block_time),
        INDEX idx_token_account (token_account_id)
    );

    -- Use a cursor to assign episode IDs
    BEGIN
        DECLARE done INT DEFAULT FALSE;
        DECLARE v_tx_id BIGINT UNSIGNED;
        DECLARE v_token_account_id INT UNSIGNED;
        DECLARE v_token_account_address VARCHAR(44);
        DECLARE v_signature VARCHAR(100);
        DECLARE v_block_time BIGINT;
        DECLARE v_block_time_utc DATETIME;
        DECLARE v_buyer_wallet VARCHAR(44);
        DECLARE v_buyer_label VARCHAR(64);
        DECLARE v_mint_address VARCHAR(44);
        DECLARE v_mint_label VARCHAR(64);
        DECLARE v_token_amount DECIMAL(36,18);
        DECLARE v_sol_spent DECIMAL(36,18);
        DECLARE v_program_address VARCHAR(44);
        DECLARE v_program_label VARCHAR(64);

        DECLARE buy_cursor CURSOR FOR
            SELECT tx_id, token_account_id, token_account_address, signature, block_time, block_time_utc,
                   buyer_wallet, buyer_label, mint_address, mint_label,
                   token_amount_bought, sol_spent, program_address, program_label
            FROM tmp_qualifying_buys
            ORDER BY block_time;

        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

        OPEN buy_cursor;

        read_loop: LOOP
            FETCH buy_cursor INTO v_tx_id, v_token_account_id, v_token_account_address, v_signature, v_block_time, v_block_time_utc,
                                  v_buyer_wallet, v_buyer_label, v_mint_address, v_mint_label,
                                  v_token_amount, v_sol_spent, v_program_address, v_program_label;
            IF done THEN
                LEAVE read_loop;
            END IF;

            -- Start new episode if this buy is after the previous episode's window
            IF v_block_time > v_last_episode_end THEN
                SET v_episode_id = v_episode_id + 1;
            END IF;

            -- Episode ends at buy_time + window
            SET v_last_episode_end = v_block_time + p_time_window_seconds;

            INSERT INTO tmp_buys_with_episodes VALUES (
                v_episode_id, v_tx_id, v_token_account_id, v_token_account_address, v_signature, v_block_time, v_block_time_utc,
                v_buyer_wallet, v_buyer_label, v_mint_address, v_mint_label,
                v_token_amount, v_sol_spent, v_program_address, v_program_label
            );
        END LOOP;

        CLOSE buy_cursor;
    END;

    -- =========================================================
    -- Step 4: Create detailed transaction list with episode_id
    -- Includes both buys and sells
    -- =========================================================

    -- First, create episode sells table (avoids MySQL temp table reopen issue)
    -- Use token_account_id as unique key - each sell is one ATA's balance change
    DROP TEMPORARY TABLE IF EXISTS tmp_episode_sells;

    CREATE TEMPORARY TABLE tmp_episode_sells AS
    SELECT DISTINCT
        b.episode_id,
        s.tx_id,
        s.token_account_id,
        s.token_account_address,
        s.signature,
        s.block_time,
        s.block_time_utc,
        s.seller_wallet,
        s.seller_label,
        s.mint_label,
        s.token_amount_sold,
        s.sol_received,
        s.program_label
    FROM tmp_buys_with_episodes b
    INNER JOIN tmp_all_sells s
        ON s.mint_address = b.mint_address
        AND s.block_time > b.block_time
        AND s.block_time <= b.block_time + p_time_window_seconds;

    -- Get minimum block_time per episode for calculating seconds_after_episode_start
    DROP TEMPORARY TABLE IF EXISTS tmp_episode_start;

    CREATE TEMPORARY TABLE tmp_episode_start AS
    SELECT episode_id, MIN(block_time) AS episode_start_time
    FROM tmp_buys_with_episodes
    GROUP BY episode_id;

    ALTER TABLE tmp_episode_start ADD INDEX idx_episode (episode_id);

    -- Now create the combined transactions table
    DROP TEMPORARY TABLE IF EXISTS tmp_episode_transactions;

    CREATE TEMPORARY TABLE tmp_episode_transactions (
        episode_id INT,
        tx_type VARCHAR(4),
        signature VARCHAR(100),
        block_time BIGINT,
        block_time_utc DATETIME,
        token_account VARCHAR(44),
        wallet VARCHAR(44),
        wallet_label VARCHAR(64),
        token VARCHAR(64),
        token_amount DECIMAL(36,18),
        sol_amount DECIMAL(36,18),
        seconds_after_episode_start BIGINT,
        program_label VARCHAR(64),
        INDEX idx_episode (episode_id),
        INDEX idx_token_account (token_account),
        INDEX idx_wallet (wallet),
        INDEX idx_tx_type (tx_type)
    );

    -- Insert buys
    INSERT INTO tmp_episode_transactions
    SELECT
        b.episode_id,
        'BUY' AS tx_type,
        b.signature,
        b.block_time,
        b.block_time_utc,
        b.token_account_address AS token_account,
        b.buyer_wallet AS wallet,
        b.buyer_label AS wallet_label,
        b.mint_label AS token,
        b.token_amount_bought AS token_amount,
        b.sol_spent AS sol_amount,
        0 AS seconds_after_episode_start,
        b.program_label
    FROM tmp_buys_with_episodes b;

    -- Insert sells (already deduped in tmp_episode_sells)
    INSERT INTO tmp_episode_transactions
    SELECT
        s.episode_id,
        'SELL' AS tx_type,
        s.signature,
        s.block_time,
        s.block_time_utc,
        s.token_account_address AS token_account,
        s.seller_wallet AS wallet,
        s.seller_label AS wallet_label,
        s.mint_label AS token,
        s.token_amount_sold AS token_amount,
        s.sol_received AS sol_amount,
        s.block_time - es.episode_start_time AS seconds_after_episode_start,
        s.program_label
    FROM tmp_episode_sells s
    INNER JOIN tmp_episode_start es ON s.episode_id = es.episode_id;

    -- Create lookup table for episode buyers (to avoid temp table reopen issue)
    DROP TEMPORARY TABLE IF EXISTS tmp_episode_buyers;

    CREATE TEMPORARY TABLE tmp_episode_buyers AS
    SELECT DISTINCT episode_id, wallet
    FROM tmp_episode_transactions
    WHERE tx_type = 'BUY';

    ALTER TABLE tmp_episode_buyers ADD INDEX idx_episode_wallet (episode_id, wallet);

    -- =========================================================
    -- Step 5: Output Transaction Details (only result set)
    -- =========================================================
    SELECT
        e.episode_id,
        e.tx_type,
        e.signature,
        e.block_time_utc,
        e.token_account,
        e.wallet,
        e.wallet_label,
        e.token,
        ROUND(e.token_amount, 4) AS token_amount,
        ROUND(e.sol_amount, 4) AS sol_amount,
        COALESCE(e.seconds_after_episode_start, 0) AS secs_after_start,
        e.program_label,
        -- Flag if this seller was also a buyer in the same episode
        CASE
            WHEN e.tx_type = 'SELL' AND eb.wallet IS NOT NULL THEN 'FLIP'
            ELSE ''
        END AS flip_flag
    FROM tmp_episode_transactions e
    LEFT JOIN tmp_episode_buyers eb ON e.episode_id = eb.episode_id AND e.wallet = eb.wallet
    ORDER BY e.block_time DESC, e.tx_type;  -- Most recent first, SELL before BUY at same time

    -- Cleanup
    DROP TEMPORARY TABLE IF EXISTS tmp_qualifying_buys;
    DROP TEMPORARY TABLE IF EXISTS tmp_all_sells;
    DROP TEMPORARY TABLE IF EXISTS tmp_buys_with_episodes;
    DROP TEMPORARY TABLE IF EXISTS tmp_episode_sells;
    DROP TEMPORARY TABLE IF EXISTS tmp_episode_start;
    DROP TEMPORARY TABLE IF EXISTS tmp_episode_transactions;
    DROP TEMPORARY TABLE IF EXISTS tmp_episode_buyers;
    DROP TEMPORARY TABLE IF EXISTS tmp_summary_stats;

END;;

DELIMITER ;
