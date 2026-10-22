-- sp_tx_hound_indexes stored procedure
-- Generated from t16o_db instance

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_hound_indexes`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_hound_indexes`(
    IN p_action VARCHAR(10)  -- 'drop', 'create', or 'status'
)
BEGIN
    IF p_action = 'drop' THEN
        -- Drop all non-PK, non-UK indexes
        -- Must disable FK checks and drop FK constraints first
        SELECT 'Dropping foreign keys and indexes...' AS status;

        SET FOREIGN_KEY_CHECKS = 0;

        -- Drop foreign key constraints
        ALTER TABLE tx_hound
            DROP FOREIGN KEY tx_hound_ibfk_base_token,
            DROP FOREIGN KEY tx_hound_ibfk_outer_program,
            DROP FOREIGN KEY tx_hound_ibfk_pool,
            DROP FOREIGN KEY tx_hound_ibfk_program,
            DROP FOREIGN KEY tx_hound_ibfk_token_1,
            DROP FOREIGN KEY tx_hound_ibfk_token_1_acct_1,
            DROP FOREIGN KEY tx_hound_ibfk_token_1_acct_2,
            DROP FOREIGN KEY tx_hound_ibfk_token_2,
            DROP FOREIGN KEY tx_hound_ibfk_token_2_acct_1,
            DROP FOREIGN KEY tx_hound_ibfk_token_2_acct_2,
            DROP FOREIGN KEY tx_hound_ibfk_tx,
            DROP FOREIGN KEY tx_hound_ibfk_wallet_1,
            DROP FOREIGN KEY tx_hound_ibfk_wallet_2;

        -- Drop indexes
        ALTER TABLE tx_hound
            DROP INDEX idx_tx,
            DROP INDEX idx_wallet_1,
            DROP INDEX idx_wallet_2,
            DROP INDEX idx_wallet_1_token,
            DROP INDEX idx_wallet_2_token,
            DROP INDEX idx_token_1,
            DROP INDEX idx_token_2,
            DROP INDEX idx_program,
            DROP INDEX idx_pool,
            DROP INDEX idx_block_time,
            DROP INDEX idx_activity_type,
            DROP INDEX idx_direction_1,
            DROP INDEX idx_direction_2,
            DROP INDEX tx_hound_ibfk_token_1_acct_1,
            DROP INDEX tx_hound_ibfk_token_1_acct_2,
            DROP INDEX tx_hound_ibfk_token_2_acct_1,
            DROP INDEX tx_hound_ibfk_token_2_acct_2,
            DROP INDEX tx_hound_ibfk_base_token,
            DROP INDEX tx_hound_ibfk_outer_program;

        SET FOREIGN_KEY_CHECKS = 1;

        SELECT 'Foreign keys and indexes dropped. Ready for bulk load.' AS status;

    ELSEIF p_action = 'create' THEN
        SELECT 'Creating indexes and foreign keys (this may take a while)...' AS status;

        -- Recreate all indexes in a single ALTER for efficiency
        ALTER TABLE tx_hound
            ADD INDEX idx_tx (tx_id),
            ADD INDEX idx_wallet_1 (wallet_1_address_id, block_time),
            ADD INDEX idx_wallet_2 (wallet_2_address_id, block_time),
            ADD INDEX idx_wallet_1_token (wallet_1_address_id, token_1_id),
            ADD INDEX idx_wallet_2_token (wallet_2_address_id, token_2_id),
            ADD INDEX idx_token_1 (token_1_id, block_time),
            ADD INDEX idx_token_2 (token_2_id, block_time),
            ADD INDEX idx_program (program_id),
            ADD INDEX idx_pool (pool_id),
            ADD INDEX idx_block_time (block_time),
            ADD INDEX idx_activity_type (activity_type),
            ADD INDEX idx_direction_1 (wallet_1_direction),
            ADD INDEX idx_direction_2 (wallet_2_direction),
            ADD INDEX tx_hound_ibfk_token_1_acct_1 (token_1_account_1_address_id),
            ADD INDEX tx_hound_ibfk_token_1_acct_2 (token_1_account_2_address_id),
            ADD INDEX tx_hound_ibfk_token_2_acct_1 (token_2_account_1_address_id),
            ADD INDEX tx_hound_ibfk_token_2_acct_2 (token_2_account_2_address_id),
            ADD INDEX tx_hound_ibfk_base_token (base_token_id),
            ADD INDEX tx_hound_ibfk_outer_program (outer_program_id);

        -- Recreate foreign key constraints
        ALTER TABLE tx_hound
            ADD CONSTRAINT tx_hound_ibfk_tx FOREIGN KEY (tx_id) REFERENCES tx(id),
            ADD CONSTRAINT tx_hound_ibfk_wallet_1 FOREIGN KEY (wallet_1_address_id) REFERENCES tx_address(id),
            ADD CONSTRAINT tx_hound_ibfk_wallet_2 FOREIGN KEY (wallet_2_address_id) REFERENCES tx_address(id),
            ADD CONSTRAINT tx_hound_ibfk_token_1 FOREIGN KEY (token_1_id) REFERENCES tx_token(id),
            ADD CONSTRAINT tx_hound_ibfk_token_2 FOREIGN KEY (token_2_id) REFERENCES tx_token(id),
            ADD CONSTRAINT tx_hound_ibfk_token_1_acct_1 FOREIGN KEY (token_1_account_1_address_id) REFERENCES tx_address(id),
            ADD CONSTRAINT tx_hound_ibfk_token_1_acct_2 FOREIGN KEY (token_1_account_2_address_id) REFERENCES tx_address(id),
            ADD CONSTRAINT tx_hound_ibfk_token_2_acct_1 FOREIGN KEY (token_2_account_1_address_id) REFERENCES tx_address(id),
            ADD CONSTRAINT tx_hound_ibfk_token_2_acct_2 FOREIGN KEY (token_2_account_2_address_id) REFERENCES tx_address(id),
            ADD CONSTRAINT tx_hound_ibfk_base_token FOREIGN KEY (base_token_id) REFERENCES tx_token(id),
            ADD CONSTRAINT tx_hound_ibfk_program FOREIGN KEY (program_id) REFERENCES tx_program(id),
            ADD CONSTRAINT tx_hound_ibfk_outer_program FOREIGN KEY (outer_program_id) REFERENCES tx_program(id),
            ADD CONSTRAINT tx_hound_ibfk_pool FOREIGN KEY (pool_id) REFERENCES tx_pool(id);

        SELECT 'Indexes and foreign keys created.' AS status;

    ELSEIF p_action = 'status' THEN
        SELECT
            COUNT(*) AS index_count,
            SUM(CASE WHEN Non_unique = 0 THEN 1 ELSE 0 END) AS unique_indexes,
            SUM(CASE WHEN Non_unique = 1 THEN 1 ELSE 0 END) AS non_unique_indexes
        FROM information_schema.STATISTICS
        WHERE table_schema = DATABASE()
          AND table_name = 'tx_hound';

    ELSE
        SELECT 'Invalid action. Use: drop, create, or status' AS error;
    END IF;
END;;

DELIMITER ;
