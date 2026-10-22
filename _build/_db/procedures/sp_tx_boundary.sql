-- sp_tx_boundary: Return the oldest or newest transaction signature for an address (mint or wallet)
-- Usage: CALL sp_tx_boundary('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'oldest');
--        CALL sp_tx_boundary('BKU7z...', 'latest');

DROP PROCEDURE IF EXISTS sp_tx_boundary;

DELIMITER $$

CREATE PROCEDURE sp_tx_boundary(
    IN p_address VARCHAR(64),
    IN p_direction VARCHAR(10)   -- 'oldest' or 'latest'
)
proc_body: BEGIN
    DECLARE v_address_id INT UNSIGNED;
    DECLARE v_token_id BIGINT;
    DECLARE v_signature VARCHAR(88);
    DECLARE v_block_time BIGINT UNSIGNED;
    DECLARE v_block_time_utc DATETIME;

    -- Resolve address to tx_address.id
    SELECT id INTO v_address_id
    FROM tx_address
    WHERE address = p_address
    LIMIT 1;

    IF v_address_id IS NULL THEN
        SELECT JSON_OBJECT('error', CONCAT('Address not found: ', LEFT(p_address, 12), '...')) AS result;
        LEAVE proc_body;
    END IF;

    -- Check if this is a mint address (has a tx_token entry)
    SELECT id INTO v_token_id
    FROM tx_token
    WHERE mint_address_id = v_address_id
    LIMIT 1;

    -- For mints: query via token_id (covers all edges involving this token)
    -- For wallets: query via from_address_id / to_address_id
    IF v_token_id IS NOT NULL THEN
        IF p_direction = 'oldest' THEN
            SELECT t.signature, t.block_time, t.block_time_utc
            INTO v_signature, v_block_time, v_block_time_utc
            FROM tx_guide g
            JOIN tx t ON t.id = g.tx_id
            WHERE g.token_id = v_token_id
            ORDER BY g.block_time ASC
            LIMIT 1;
        ELSE
            SELECT t.signature, t.block_time, t.block_time_utc
            INTO v_signature, v_block_time, v_block_time_utc
            FROM tx_guide g
            JOIN tx t ON t.id = g.tx_id
            WHERE g.token_id = v_token_id
            ORDER BY g.block_time DESC
            LIMIT 1;
        END IF;
    ELSE
        IF p_direction = 'oldest' THEN
            SELECT t.signature, t.block_time, t.block_time_utc
            INTO v_signature, v_block_time, v_block_time_utc
            FROM tx_guide g
            JOIN tx t ON t.id = g.tx_id
            WHERE g.from_address_id = v_address_id
               OR g.to_address_id = v_address_id
            ORDER BY g.block_time ASC
            LIMIT 1;
        ELSE
            SELECT t.signature, t.block_time, t.block_time_utc
            INTO v_signature, v_block_time, v_block_time_utc
            FROM tx_guide g
            JOIN tx t ON t.id = g.tx_id
            WHERE g.from_address_id = v_address_id
               OR g.to_address_id = v_address_id
            ORDER BY g.block_time DESC
            LIMIT 1;
        END IF;
    END IF;

    IF v_signature IS NULL THEN
        SELECT JSON_OBJECT('error', CONCAT('No transactions found for ', LEFT(p_address, 12), '...')) AS result;
    ELSE
        SELECT JSON_OBJECT(
            'signature', v_signature,
            'block_time', v_block_time,
            'block_time_utc', DATE_FORMAT(v_block_time_utc, '%Y-%m-%dT%H:%i:%s'),
            'direction', p_direction,
            'address', p_address
        ) AS result;
    END IF;
END$$

DELIMITER ;
