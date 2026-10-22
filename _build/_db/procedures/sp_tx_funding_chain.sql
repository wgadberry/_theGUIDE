-- sp_tx_funding_chain stored procedure
-- Generated from t16o_db instance

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_funding_chain`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_funding_chain`(
    IN p_wallet_address VARCHAR(44),
    IN p_min_tx_state BIGINT UNSIGNED,
    IN p_max_depth INT,
    IN p_direction VARCHAR(10)
)
BEGIN
    DECLARE v_start_id INT UNSIGNED;
    DECLARE v_depth INT DEFAULT 0;
    DECLARE v_found INT DEFAULT 1;

    
    SET p_max_depth = COALESCE(p_max_depth, 10);
    SET p_direction = COALESCE(p_direction, 'up');
    SET p_min_tx_state = COALESCE(p_min_tx_state, 0);

    
    SELECT id INTO v_start_id
    FROM tx_address
    WHERE address = p_wallet_address
    LIMIT 1;

    IF v_start_id IS NULL THEN
        SELECT 'Wallet not found' AS error, p_wallet_address AS wallet;
    ELSE
        
        DROP TEMPORARY TABLE IF EXISTS tmp_chain;
        DROP TEMPORARY TABLE IF EXISTS tmp_frontier;

        CREATE TEMPORARY TABLE tmp_chain (
            depth INT,
            direction VARCHAR(10),
            wallet_id INT UNSIGNED,
            wallet_address VARCHAR(44),
            wallet_label VARCHAR(100),
            funder_id INT UNSIGNED,
            funder_address VARCHAR(44),
            funder_label VARCHAR(100),
            funding_sol DECIMAL(20,9),
            funding_tx_signature VARCHAR(88),
            tx_state BIGINT UNSIGNED,
            first_seen_utc DATETIME,
            PRIMARY KEY (wallet_id)
        );

        CREATE TEMPORARY TABLE tmp_frontier (
            wallet_id INT UNSIGNED PRIMARY KEY,
            funder_id INT UNSIGNED
        );

        
        INSERT INTO tmp_chain
        SELECT
            0,
            'start',
            w.id,
            w.address,
            w.label,
            f.id,
            f.address,
            f.label,
            w.funding_amount / 1e9,
            t.signature,
            COALESCE(CAST(t.tx_state AS UNSIGNED), 0),
            FROM_UNIXTIME(w.first_seen_block_time)
        FROM tx_address w
        LEFT JOIN tx_address f ON w.funded_by_address_id = f.id
        LEFT JOIN tx t ON w.funding_tx_id = t.id
        WHERE w.id = v_start_id;

        
        WHILE v_found > 0 AND v_depth < p_max_depth DO
            SET v_depth = v_depth + 1;
            SET v_found = 0;

            
            IF p_direction IN ('up', 'both') THEN
                
                TRUNCATE TABLE tmp_frontier;
                INSERT INTO tmp_frontier
                SELECT wallet_id, funder_id FROM tmp_chain WHERE depth = v_depth - 1;

                INSERT IGNORE INTO tmp_chain
                SELECT
                    v_depth,
                    'up',
                    w.id,
                    w.address,
                    w.label,
                    f.id,
                    f.address,
                    f.label,
                    w.funding_amount / 1e9,
                    t.signature,
                    COALESCE(CAST(t.tx_state AS UNSIGNED), 0),
                    FROM_UNIXTIME(w.first_seen_block_time)
                FROM tx_address w
                LEFT JOIN tx_address f ON w.funded_by_address_id = f.id
                LEFT JOIN tx t ON w.funding_tx_id = t.id
                INNER JOIN tmp_frontier tf ON w.id = tf.funder_id
                WHERE (p_min_tx_state = 0 OR COALESCE(CAST(t.tx_state AS UNSIGNED), 0) >= p_min_tx_state);

                SET v_found = v_found + ROW_COUNT();
            END IF;

            
            IF p_direction IN ('down', 'both') THEN
                
                TRUNCATE TABLE tmp_frontier;
                INSERT INTO tmp_frontier
                SELECT wallet_id, funder_id FROM tmp_chain WHERE depth = v_depth - 1;

                INSERT IGNORE INTO tmp_chain
                SELECT
                    v_depth,
                    'down',
                    w.id,
                    w.address,
                    w.label,
                    f.id,
                    f.address,
                    f.label,
                    w.funding_amount / 1e9,
                    t.signature,
                    COALESCE(CAST(t.tx_state AS UNSIGNED), 0),
                    FROM_UNIXTIME(w.first_seen_block_time)
                FROM tx_address w
                LEFT JOIN tx_address f ON w.funded_by_address_id = f.id
                LEFT JOIN tx t ON w.funding_tx_id = t.id
                INNER JOIN tmp_frontier tf ON w.funded_by_address_id = tf.wallet_id
                WHERE (p_min_tx_state = 0 OR COALESCE(CAST(t.tx_state AS UNSIGNED), 0) >= p_min_tx_state);

                SET v_found = v_found + ROW_COUNT();
            END IF;
        END WHILE;

        
        SELECT
            depth,
            direction,
            wallet_address,
            wallet_label,
            funder_address,
            funder_label,
            funding_sol,
            funding_tx_signature,
            tx_state,
            first_seen_utc
        FROM tmp_chain
        ORDER BY depth, direction, wallet_address;

        DROP TEMPORARY TABLE IF EXISTS tmp_chain;
        DROP TEMPORARY TABLE IF EXISTS tmp_frontier;
    END IF;
END;;

DELIMITER ;
