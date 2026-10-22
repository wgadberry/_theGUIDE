-- sp_tx_detect_chart_clipping stored procedure
-- Generated from t16o_db instance

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_detect_chart_clipping`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_detect_chart_clipping`(
    IN p_base_token_symbol VARCHAR(128),   
    IN p_target_token_symbol VARCHAR(128), 
    IN p_base_token_mint VARCHAR(44),      
    IN p_target_token_mint VARCHAR(44),    
    IN p_buy_amt_base DECIMAL(18,9),       
    IN p_time_window_seconds INT,          
    IN p_wallet_address VARCHAR(44)        
)
BEGIN
    DECLARE v_episode_id INT DEFAULT 0;
    DECLARE v_last_episode_end BIGINT DEFAULT 0;
    DECLARE v_base_token_id BIGINT;
    DECLARE v_target_token_id BIGINT;
    DECLARE v_wallet_address_id INT UNSIGNED;

    
    
    

    
    IF p_base_token_mint IS NOT NULL THEN
        
        SELECT t.id INTO v_base_token_id
        FROM tx_token t
        INNER JOIN tx_address a ON a.id = t.mint_address_id
        WHERE a.address = p_base_token_mint
        LIMIT 1;
    ELSE
        
        SELECT t.id INTO v_base_token_id
        FROM tx_token t
        WHERE t.token_symbol = p_base_token_symbol
        LIMIT 1;
    END IF;

    
    IF p_target_token_mint IS NOT NULL THEN
        
        SELECT t.id INTO v_target_token_id
        FROM tx_token t
        INNER JOIN tx_address a ON a.id = t.mint_address_id
        WHERE a.address = p_target_token_mint
        LIMIT 1;
    ELSE
        
        SELECT t.id INTO v_target_token_id
        FROM tx_token t
        WHERE t.token_symbol = p_target_token_symbol
        LIMIT 1;
    END IF;

    
    IF p_wallet_address IS NOT NULL THEN
        SELECT id INTO v_wallet_address_id
        FROM tx_address
        WHERE address = p_wallet_address
        LIMIT 1;
    END IF;

    
    
    
    
    
    
    DROP TEMPORARY TABLE IF EXISTS tmp_qualifying_buys;

    CREATE TEMPORARY TABLE tmp_qualifying_buys AS
    SELECT
        s.tx_id,
        s.id AS swap_id,
        tx.signature,
        tx.block_time,
        tx.block_time_utc,
        owner_addr.address AS buyer_wallet,
        owner_addr.label AS buyer_label,
        t_target.token_symbol AS target_symbol,
        s.amount_2 / POW(10, COALESCE(s.decimals_2, 0)) AS target_amount_bought,
        s.amount_1 / POW(10, COALESCE(s.decimals_1, 0)) AS base_spent,
        prog_addr.address AS program_address,
        COALESCE(prog_addr.label, prog.name) AS program_label
    FROM tx_swap s
    INNER JOIN tx ON tx.id = s.tx_id
    LEFT JOIN tx_address owner_addr ON owner_addr.id = s.owner_1_address_id
    LEFT JOIN tx_token t_target ON t_target.id = s.token_2_id
    LEFT JOIN tx_program prog ON prog.id = s.program_id
    LEFT JOIN tx_address prog_addr ON prog_addr.id = prog.program_address_id
    WHERE
        
        s.token_1_id = v_base_token_id
        
        AND s.token_2_id = v_target_token_id
        
        AND (s.amount_1 / POW(10, COALESCE(s.decimals_1, 0))) >= p_buy_amt_base
        
        AND (v_wallet_address_id IS NULL OR s.owner_1_address_id = v_wallet_address_id)
    ORDER BY tx.block_time;

    
    ALTER TABLE tmp_qualifying_buys ADD INDEX idx_block_time (block_time);

    
    
    
    
    
    DROP TEMPORARY TABLE IF EXISTS tmp_all_sells;

    CREATE TEMPORARY TABLE tmp_all_sells AS
    SELECT
        s.tx_id,
        s.id AS swap_id,
        tx.signature,
        tx.block_time,
        tx.block_time_utc,
        owner_addr.address AS seller_wallet,
        owner_addr.label AS seller_label,
        t_target.token_symbol AS target_symbol,
        s.amount_1 / POW(10, COALESCE(s.decimals_1, 0)) AS target_amount_sold,
        s.amount_2 / POW(10, COALESCE(s.decimals_2, 0)) AS base_received,
        prog_addr.address AS program_address,
        COALESCE(prog_addr.label, prog.name) AS program_label
    FROM tx_swap s
    INNER JOIN tx ON tx.id = s.tx_id
    LEFT JOIN tx_address owner_addr ON owner_addr.id = s.owner_1_address_id
    LEFT JOIN tx_token t_target ON t_target.id = s.token_1_id
    LEFT JOIN tx_program prog ON prog.id = s.program_id
    LEFT JOIN tx_address prog_addr ON prog_addr.id = prog.program_address_id
    WHERE
        
        s.token_1_id = v_target_token_id
        
        AND s.token_2_id = v_base_token_id
        
        AND (v_wallet_address_id IS NULL OR s.owner_1_address_id = v_wallet_address_id)
    ORDER BY tx.block_time;

    
    ALTER TABLE tmp_all_sells ADD INDEX idx_block_time (block_time);

    
    
    
    
    DROP TEMPORARY TABLE IF EXISTS tmp_buys_with_episodes;

    CREATE TEMPORARY TABLE tmp_buys_with_episodes (
        episode_id INT,
        tx_id BIGINT,
        swap_id BIGINT,
        signature VARCHAR(88),
        block_time BIGINT,
        block_time_utc DATETIME,
        buyer_wallet VARCHAR(44),
        buyer_label VARCHAR(200),
        target_symbol VARCHAR(20),
        target_amount_bought DECIMAL(36,18),
        base_spent DECIMAL(36,18),
        program_address VARCHAR(44),
        program_label VARCHAR(200),
        INDEX idx_episode (episode_id),
        INDEX idx_block_time (block_time)
    );

    
    BEGIN
        DECLARE done INT DEFAULT FALSE;
        DECLARE v_tx_id BIGINT;
        DECLARE v_swap_id BIGINT;
        DECLARE v_signature VARCHAR(88);
        DECLARE v_block_time BIGINT;
        DECLARE v_block_time_utc DATETIME;
        DECLARE v_buyer_wallet VARCHAR(44);
        DECLARE v_buyer_label VARCHAR(200);
        DECLARE v_target_symbol VARCHAR(20);
        DECLARE v_target_amount DECIMAL(36,18);
        DECLARE v_base_spent DECIMAL(36,18);
        DECLARE v_program_address VARCHAR(44);
        DECLARE v_program_label VARCHAR(200);

        DECLARE buy_cursor CURSOR FOR
            SELECT tx_id, swap_id, signature, block_time, block_time_utc,
                   buyer_wallet, buyer_label, target_symbol,
                   target_amount_bought, base_spent, program_address, program_label
            FROM tmp_qualifying_buys
            ORDER BY block_time;

        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

        OPEN buy_cursor;

        read_loop: LOOP
            FETCH buy_cursor INTO v_tx_id, v_swap_id, v_signature, v_block_time, v_block_time_utc,
                                  v_buyer_wallet, v_buyer_label, v_target_symbol,
                                  v_target_amount, v_base_spent, v_program_address, v_program_label;
            IF done THEN
                LEAVE read_loop;
            END IF;

            
            IF v_block_time > v_last_episode_end THEN
                SET v_episode_id = v_episode_id + 1;
            END IF;

            
            SET v_last_episode_end = v_block_time + p_time_window_seconds;

            INSERT INTO tmp_buys_with_episodes VALUES (
                v_episode_id, v_tx_id, v_swap_id, v_signature, v_block_time, v_block_time_utc,
                v_buyer_wallet, v_buyer_label, v_target_symbol,
                v_target_amount, v_base_spent, v_program_address, v_program_label
            );
        END LOOP;

        CLOSE buy_cursor;
    END;

    
    
    
    DROP TEMPORARY TABLE IF EXISTS tmp_episode_sells;

    CREATE TEMPORARY TABLE tmp_episode_sells AS
    SELECT DISTINCT
        b.episode_id,
        s.tx_id,
        s.swap_id,
        s.signature,
        s.block_time,
        s.block_time_utc,
        s.seller_wallet,
        s.seller_label,
        s.target_symbol,
        s.target_amount_sold,
        s.base_received,
        s.program_label
    FROM tmp_buys_with_episodes b
    INNER JOIN tmp_all_sells s
        ON s.block_time > b.block_time
        AND s.block_time <= b.block_time + p_time_window_seconds;

    
    DROP TEMPORARY TABLE IF EXISTS tmp_episode_start;

    CREATE TEMPORARY TABLE tmp_episode_start AS
    SELECT episode_id, MIN(block_time) AS episode_start_time
    FROM tmp_buys_with_episodes
    GROUP BY episode_id;

    ALTER TABLE tmp_episode_start ADD INDEX idx_episode (episode_id);

    
    
    
    DROP TEMPORARY TABLE IF EXISTS tmp_episode_transactions;

    CREATE TEMPORARY TABLE tmp_episode_transactions (
        episode_id INT,
        tx_type VARCHAR(4),
        signature VARCHAR(88),
        block_time BIGINT,
        block_time_utc DATETIME,
        wallet VARCHAR(44),
        wallet_label VARCHAR(200),
        target_token VARCHAR(20),
        target_amount DECIMAL(36,18),
        base_amount DECIMAL(36,18),
        seconds_after_episode_start BIGINT,
        program_label VARCHAR(200),
        INDEX idx_episode (episode_id),
        INDEX idx_wallet (wallet),
        INDEX idx_tx_type (tx_type)
    );

    
    INSERT INTO tmp_episode_transactions
    SELECT
        b.episode_id,
        'BUY' AS tx_type,
        b.signature,
        b.block_time,
        b.block_time_utc,
        b.buyer_wallet AS wallet,
        b.buyer_label AS wallet_label,
        b.target_symbol AS target_token,
        b.target_amount_bought AS target_amount,
        b.base_spent AS base_amount,
        0 AS seconds_after_episode_start,
        b.program_label
    FROM tmp_buys_with_episodes b;

    
    INSERT INTO tmp_episode_transactions
    SELECT
        s.episode_id,
        'SELL' AS tx_type,
        s.signature,
        s.block_time,
        s.block_time_utc,
        s.seller_wallet AS wallet,
        s.seller_label AS wallet_label,
        s.target_symbol AS target_token,
        s.target_amount_sold AS target_amount,
        s.base_received AS base_amount,
        s.block_time - es.episode_start_time AS seconds_after_episode_start,
        s.program_label
    FROM tmp_episode_sells s
    INNER JOIN tmp_episode_start es ON s.episode_id = es.episode_id;

    
    DROP TEMPORARY TABLE IF EXISTS tmp_episode_buyers;

    CREATE TEMPORARY TABLE tmp_episode_buyers AS
    SELECT DISTINCT episode_id, wallet
    FROM tmp_episode_transactions
    WHERE tx_type = 'BUY';

    ALTER TABLE tmp_episode_buyers ADD INDEX idx_episode_wallet (episode_id, wallet);

    
    
    
    SELECT
        e.episode_id,
        e.tx_type,
        e.signature,
        e.block_time_utc,
        e.wallet,
        e.wallet_label,
        e.target_token,
        ROUND(e.target_amount, 4) AS target_amount,
        ROUND(e.base_amount, 4) AS base_amount,
        COALESCE(e.seconds_after_episode_start, 0) AS secs_after_start,
        e.program_label,
        
        CASE
            WHEN e.tx_type = 'SELL' AND eb.wallet IS NOT NULL THEN 'FLIP'
            ELSE ''
        END AS flip_flag
    FROM tmp_episode_transactions e
    LEFT JOIN tmp_episode_buyers eb ON e.episode_id = eb.episode_id AND e.wallet = eb.wallet
    ORDER BY e.block_time DESC, e.tx_type;

    
    DROP TEMPORARY TABLE IF EXISTS tmp_qualifying_buys;
    DROP TEMPORARY TABLE IF EXISTS tmp_all_sells;
    DROP TEMPORARY TABLE IF EXISTS tmp_buys_with_episodes;
    DROP TEMPORARY TABLE IF EXISTS tmp_episode_sells;
    DROP TEMPORARY TABLE IF EXISTS tmp_episode_start;
    DROP TEMPORARY TABLE IF EXISTS tmp_episode_transactions;
    DROP TEMPORARY TABLE IF EXISTS tmp_episode_buyers;

END;;

DELIMITER ;
