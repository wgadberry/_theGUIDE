-- TX Participant Table for Forensics
-- Captures ALL addresses involved in transactions, not just those with balance changes

-- Create table
CREATE TABLE IF NOT EXISTS tx_participant (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tx_id BIGINT NOT NULL,                          -- Links to transaction
    address_id INT UNSIGNED NOT NULL,               -- Links to tx_address
    is_signer TINYINT(1) NOT NULL DEFAULT 0,        -- Signed the transaction
    is_fee_payer TINYINT(1) NOT NULL DEFAULT 0,     -- Paid the fee (usually account[0])
    is_writable TINYINT(1) NOT NULL DEFAULT 0,      -- Account was writable
    is_program TINYINT(1) NOT NULL DEFAULT 0,       -- Is a program account
    account_index TINYINT UNSIGNED,                 -- Position in transaction accounts

    INDEX idx_tx_id (tx_id),
    INDEX idx_address_id (address_id),
    INDEX idx_signer (is_signer, address_id),
    INDEX idx_fee_payer (is_fee_payer, address_id),
    UNIQUE KEY uk_tx_addr (tx_id, address_id)
) ENGINE=InnoDB;

-- Stored procedure: Find shadow addresses for a token
DELIMITER //

DROP PROCEDURE IF EXISTS sp_tx_shadow_addresses //

CREATE PROCEDURE sp_tx_shadow_addresses(
    IN p_token_symbol VARCHAR(256),
    IN p_limit INT
)
BEGIN
    /*
    Find "shadow" addresses - involved in token transactions but never transfer.
    These could be authorities, fee payers, or hidden controllers.

    Usage:
        CALL sp_tx_shadow_addresses('SpaceAI', 50);
        CALL sp_tx_shadow_addresses('CFUCK', 0);  -- unlimited
    */

    SET p_limit = COALESCE(p_limit, 50);

    -- Get token ID
    SELECT id INTO @token_id FROM tx_token WHERE token_symbol = p_token_symbol LIMIT 1;

    IF @token_id IS NULL THEN
        SELECT 'Token not found' as error;
    ELSE
        IF p_limit > 0 THEN
            SELECT
                a.address,
                a.address_type,
                a.label,
                a.account_tags,
                COUNT(DISTINCT p.tx_id) as tx_appearances,
                SUM(p.is_signer) as times_signed,
                SUM(p.is_fee_payer) as times_paid_fee,
                SUM(p.is_writable) as times_writable,
                MAX(p.is_program) as is_program
            FROM tx_participant p
            JOIN tx_address a ON a.id = p.address_id
            WHERE p.tx_id IN (
                SELECT DISTINCT tx_id FROM tx_guide WHERE token_id = @token_id
            )
            AND p.address_id NOT IN (
                SELECT from_address_id FROM tx_guide WHERE token_id = @token_id
                UNION
                SELECT to_address_id FROM tx_guide WHERE token_id = @token_id
            )
            AND a.address_type NOT IN ('program', 'mint')
            GROUP BY a.id, a.address, a.address_type, a.label, a.account_tags
            ORDER BY tx_appearances DESC
            LIMIT p_limit;
        ELSE
            SELECT
                a.address,
                a.address_type,
                a.label,
                a.account_tags,
                COUNT(DISTINCT p.tx_id) as tx_appearances,
                SUM(p.is_signer) as times_signed,
                SUM(p.is_fee_payer) as times_paid_fee,
                SUM(p.is_writable) as times_writable,
                MAX(p.is_program) as is_program
            FROM tx_participant p
            JOIN tx_address a ON a.id = p.address_id
            WHERE p.tx_id IN (
                SELECT DISTINCT tx_id FROM tx_guide WHERE token_id = @token_id
            )
            AND p.address_id NOT IN (
                SELECT from_address_id FROM tx_guide WHERE token_id = @token_id
                UNION
                SELECT to_address_id FROM tx_guide WHERE token_id = @token_id
            )
            AND a.address_type NOT IN ('program', 'mint')
            GROUP BY a.id, a.address, a.address_type, a.label, a.account_tags
            ORDER BY tx_appearances DESC;
        END IF;
    END IF;
END //

-- Stored procedure: Find fee payers for a wallet
DROP PROCEDURE IF EXISTS sp_tx_fee_payer_analysis //

CREATE PROCEDURE sp_tx_fee_payer_analysis(
    IN p_wallet_address VARCHAR(64),
    IN p_limit INT
)
BEGIN
    /*
    Find who pays transaction fees for a specific wallet.
    Useful for identifying funding sources or related wallets.

    Usage:
        CALL sp_tx_fee_payer_analysis('WalletAddress123...', 20);
    */

    SET p_limit = COALESCE(p_limit, 20);

    -- Get wallet address ID
    SELECT id INTO @wallet_id FROM tx_address WHERE address = p_wallet_address LIMIT 1;

    IF @wallet_id IS NULL THEN
        SELECT 'Wallet not found' as error;
    ELSE
        SELECT
            payer.address as fee_payer,
            payer.address_type,
            payer.label,
            COUNT(DISTINCT p.tx_id) as txs_paid_for,
            SUM(CASE WHEN p.address_id != g.from_address_id THEN 1 ELSE 0 END) as paid_for_others
        FROM tx_participant p
        JOIN tx_address payer ON payer.id = p.address_id
        JOIN tx_guide g ON g.tx_id = p.tx_id
        WHERE p.is_fee_payer = 1
          AND (g.from_address_id = @wallet_id OR g.to_address_id = @wallet_id)
        GROUP BY payer.id, payer.address, payer.address_type, payer.label
        ORDER BY txs_paid_for DESC
        LIMIT p_limit;
    END IF;
END //

-- Stored procedure: Find authority patterns (signers who never transfer)
DROP PROCEDURE IF EXISTS sp_tx_authority_detection //

CREATE PROCEDURE sp_tx_authority_detection(
    IN p_min_signatures INT,
    IN p_limit INT
)
BEGIN
    /*
    Find addresses that frequently sign transactions but never have balance changes.
    These are likely authority/admin accounts.

    Usage:
        CALL sp_tx_authority_detection(10, 50);  -- Min 10 signatures, top 50
    */

    SET p_min_signatures = COALESCE(p_min_signatures, 10);
    SET p_limit = COALESCE(p_limit, 50);

    SELECT
        a.address,
        a.address_type,
        a.label,
        a.account_tags,
        COUNT(DISTINCT p.tx_id) as times_signed,
        COUNT(DISTINCT g_from.tx_id) as times_sent,
        COUNT(DISTINCT g_to.tx_id) as times_received
    FROM tx_participant p
    JOIN tx_address a ON a.id = p.address_id
    LEFT JOIN tx_guide g_from ON g_from.tx_id = p.tx_id AND g_from.from_address_id = a.id
    LEFT JOIN tx_guide g_to ON g_to.tx_id = p.tx_id AND g_to.to_address_id = a.id
    WHERE p.is_signer = 1
      AND a.address_type NOT IN ('program', 'mint')
    GROUP BY a.id, a.address, a.address_type, a.label, a.account_tags
    HAVING times_signed >= p_min_signatures
       AND times_sent = 0
       AND times_received = 0
    ORDER BY times_signed DESC
    LIMIT p_limit;
END //

DELIMITER ;
