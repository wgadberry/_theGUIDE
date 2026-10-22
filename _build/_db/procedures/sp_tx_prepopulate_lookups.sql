-- sp_tx_prepopulate_lookups: Consolidated Phase 1 for all lookup tables
-- Extracts addresses, tokens, programs, pools from JSON
-- Uses temp table pre-filter to avoid INSERT IGNORE deadlocks:
--   1. Load all addresses into MEMORY temp table
--   2. Delete addresses that already exist in tx_address
--   3. INSERT only truly new rows (no duplicate key conflicts = no deadlocks)
--
-- Feature flags (bitmask):
--   FEATURE_ALL_ADDRESSES (2): Collect ATAs, vaults, pools in addition to core addresses

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_prepopulate_lookups`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_prepopulate_lookups`(
    IN p_txs_json JSON,
    IN p_request_log_id BIGINT UNSIGNED,
    IN p_features INT UNSIGNED
)
BEGIN
    DECLARE FEATURE_ALL_ADDRESSES INT UNSIGNED DEFAULT 2;
    DECLARE v_collect_all_addresses BOOLEAN;

    SET v_collect_all_addresses = (p_features & FEATURE_ALL_ADDRESSES) = FEATURE_ALL_ADDRESSES;

    -- =========================================================================
    -- Phase 1: Core addresses — extract into temp table, filter, insert new only
    -- =========================================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_new_addresses;
    CREATE TEMPORARY TABLE tmp_new_addresses (
        addr VARCHAR(44) NOT NULL PRIMARY KEY,
        addr_type VARCHAR(20) NOT NULL
    ) ENGINE=MEMORY;

    -- Load all core addresses from JSON
    INSERT IGNORE INTO tmp_new_addresses (addr, addr_type)
    SELECT DISTINCT addr, addr_type FROM (
        -- Signers
        SELECT t.signer_account AS addr, 'wallet' AS addr_type
        FROM JSON_TABLE(p_txs_json, '$.data[*]' COLUMNS (
            signer_account VARCHAR(44) PATH '$.one_line_summary.data.account'
        )) t WHERE t.signer_account IS NOT NULL AND t.signer_account != 'null'

        UNION

        SELECT t.fallback_signer, 'wallet'
        FROM JSON_TABLE(p_txs_json, '$.data[*]' COLUMNS (
            fallback_signer VARCHAR(44) PATH '$.transfers[0].source_owner'
        )) t WHERE t.fallback_signer IS NOT NULL AND t.fallback_signer != 'null'

        UNION

        -- Tokens from one_line_summary
        SELECT t.token_1, 'mint'
        FROM JSON_TABLE(p_txs_json, '$.data[*]' COLUMNS (
            token_1 VARCHAR(44) PATH '$.one_line_summary.data.token_1'
        )) t WHERE t.token_1 IS NOT NULL AND t.token_1 != 'null'

        UNION

        SELECT t.token_2, 'mint'
        FROM JSON_TABLE(p_txs_json, '$.data[*]' COLUMNS (
            token_2 VARCHAR(44) PATH '$.one_line_summary.data.token_2'
        )) t WHERE t.token_2 IS NOT NULL AND t.token_2 != 'null'

        UNION

        SELECT t.fee_token, 'mint'
        FROM JSON_TABLE(p_txs_json, '$.data[*]' COLUMNS (
            fee_token VARCHAR(44) PATH '$.one_line_summary.data.fee_token'
        )) t WHERE t.fee_token IS NOT NULL AND t.fee_token != 'null'

        UNION

        -- Programs from one_line_summary
        SELECT t.agg_program, 'program'
        FROM JSON_TABLE(p_txs_json, '$.data[*]' COLUMNS (
            agg_program VARCHAR(44) PATH '$.one_line_summary.program_id'
        )) t WHERE t.agg_program IS NOT NULL AND t.agg_program != 'null'

        UNION

        -- Transfer owners
        SELECT t.source_owner, 'wallet'
        FROM JSON_TABLE(p_txs_json, '$.data[*].transfers[*]' COLUMNS (
            source_owner VARCHAR(44) PATH '$.source_owner'
        )) t WHERE t.source_owner IS NOT NULL AND t.source_owner != 'null'

        UNION

        SELECT t.destination_owner, 'wallet'
        FROM JSON_TABLE(p_txs_json, '$.data[*].transfers[*]' COLUMNS (
            destination_owner VARCHAR(44) PATH '$.destination_owner'
        )) t WHERE t.destination_owner IS NOT NULL AND t.destination_owner != 'null'

        UNION

        -- Transfer tokens
        SELECT t.token_address, 'mint'
        FROM JSON_TABLE(p_txs_json, '$.data[*].transfers[*]' COLUMNS (
            token_address VARCHAR(44) PATH '$.token_address'
        )) t WHERE t.token_address IS NOT NULL AND t.token_address != 'null'

        UNION

        SELECT t.base_token, 'mint'
        FROM JSON_TABLE(p_txs_json, '$.data[*].transfers[*]' COLUMNS (
            base_token VARCHAR(44) PATH '$.base_value.token_address'
        )) t WHERE t.base_token IS NOT NULL AND t.base_token != 'null'

        UNION

        -- Transfer programs
        SELECT t.program_id, 'program'
        FROM JSON_TABLE(p_txs_json, '$.data[*].transfers[*]' COLUMNS (
            program_id VARCHAR(44) PATH '$.program_id'
        )) t WHERE t.program_id IS NOT NULL AND t.program_id != 'null'

        UNION

        SELECT t.outer_program_id, 'program'
        FROM JSON_TABLE(p_txs_json, '$.data[*].transfers[*]' COLUMNS (
            outer_program_id VARCHAR(44) PATH '$.outer_program_id'
        )) t WHERE t.outer_program_id IS NOT NULL AND t.outer_program_id != 'null'

        UNION

        -- Activity programs
        SELECT a.program_id, 'program'
        FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
            program_id VARCHAR(44) PATH '$.program_id'
        )) a WHERE a.program_id IS NOT NULL AND a.program_id != 'null'

        UNION

        SELECT a.outer_program_id, 'program'
        FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
            outer_program_id VARCHAR(44) PATH '$.outer_program_id'
        )) a WHERE a.outer_program_id IS NOT NULL AND a.outer_program_id != 'null'

        UNION

        -- Activity wallets
        SELECT a.account, 'wallet'
        FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
            account VARCHAR(44) PATH '$.data.account'
        )) a WHERE a.account IS NOT NULL AND a.account != 'null'

        UNION

        SELECT a.source, 'wallet'
        FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
            source VARCHAR(44) PATH '$.data.source'
        )) a WHERE a.source IS NOT NULL AND a.source != 'null'

        UNION

        SELECT a.owner_1, 'wallet'
        FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
            owner_1 VARCHAR(44) PATH '$.data.owner_1'
        )) a WHERE a.owner_1 IS NOT NULL AND a.owner_1 != 'null'

        UNION

        -- Activity tokens
        SELECT a.token_1, 'mint'
        FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
            token_1 VARCHAR(44) PATH '$.data.token_1'
        )) a WHERE a.token_1 IS NOT NULL AND a.token_1 != 'null'

        UNION

        SELECT a.token_2, 'mint'
        FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
            token_2 VARCHAR(44) PATH '$.data.token_2'
        )) a WHERE a.token_2 IS NOT NULL AND a.token_2 != 'null'

        UNION

        SELECT a.fee_token, 'mint'
        FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
            fee_token VARCHAR(44) PATH '$.data.fee_token'
        )) a WHERE a.fee_token IS NOT NULL AND a.fee_token != 'null'

    ) AS core_addresses;

    -- Remove addresses that already exist (SELECT only, no locks on tx_address)
    DELETE t FROM tmp_new_addresses t
    WHERE EXISTS (SELECT 1 FROM tx_address x WHERE x.address = t.addr);

    -- Insert only truly new addresses (no duplicates = no unique key deadlocks)
    INSERT INTO tx_address (address, address_type)
    SELECT addr, addr_type FROM tmp_new_addresses;

    -- =========================================================================
    -- Phase 2: Extended addresses (ATAs, vaults, pools) — same temp table pattern
    -- =========================================================================
    IF v_collect_all_addresses THEN
        TRUNCATE TABLE tmp_new_addresses;

        INSERT IGNORE INTO tmp_new_addresses (addr, addr_type)
        SELECT DISTINCT addr, addr_type FROM (
            -- Transfer ATAs
            SELECT t.source AS addr, 'ata' AS addr_type
            FROM JSON_TABLE(p_txs_json, '$.data[*].transfers[*]' COLUMNS (
                source VARCHAR(44) PATH '$.source'
            )) t WHERE t.source IS NOT NULL AND t.source != 'null'

            UNION

            SELECT t.destination, 'ata'
            FROM JSON_TABLE(p_txs_json, '$.data[*].transfers[*]' COLUMNS (
                destination VARCHAR(44) PATH '$.destination'
            )) t WHERE t.destination IS NOT NULL AND t.destination != 'null'

            UNION

            -- Activity ATAs
            SELECT a.new_account, 'ata'
            FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
                new_account VARCHAR(44) PATH '$.data.new_account'
            )) a WHERE a.new_account IS NOT NULL AND a.new_account != 'null'

            UNION

            SELECT a.init_account, 'ata'
            FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
                init_account VARCHAR(44) PATH '$.data.init_account'
            )) a WHERE a.init_account IS NOT NULL AND a.init_account != 'null'

            UNION

            -- Pools
            SELECT a.amm_id, 'pool'
            FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
                amm_id VARCHAR(44) PATH '$.data.amm_id'
            )) a WHERE a.amm_id IS NOT NULL AND a.amm_id != 'null'

            UNION

            SELECT a.owner_2, 'wallet'
            FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
                owner_2 VARCHAR(44) PATH '$.data.owner_2'
            )) a WHERE a.owner_2 IS NOT NULL AND a.owner_2 != 'null'

            UNION

            -- Vaults
            SELECT a.token_account_1_1, 'vault'
            FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
                token_account_1_1 VARCHAR(44) PATH '$.data.token_account_1_1'
            )) a WHERE a.token_account_1_1 IS NOT NULL AND a.token_account_1_1 != 'null'

            UNION

            SELECT a.token_account_1_2, 'vault'
            FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
                token_account_1_2 VARCHAR(44) PATH '$.data.token_account_1_2'
            )) a WHERE a.token_account_1_2 IS NOT NULL AND a.token_account_1_2 != 'null'

            UNION

            SELECT a.token_account_2_1, 'vault'
            FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
                token_account_2_1 VARCHAR(44) PATH '$.data.token_account_2_1'
            )) a WHERE a.token_account_2_1 IS NOT NULL AND a.token_account_2_1 != 'null'

            UNION

            SELECT a.token_account_2_2, 'vault'
            FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
                token_account_2_2 VARCHAR(44) PATH '$.data.token_account_2_2'
            )) a WHERE a.token_account_2_2 IS NOT NULL AND a.token_account_2_2 != 'null'

        ) AS extended_addresses;

        -- Remove existing
        DELETE t FROM tmp_new_addresses t
        WHERE EXISTS (SELECT 1 FROM tx_address x WHERE x.address = t.addr);

        -- Insert only new
        INSERT INTO tx_address (address, address_type)
        SELECT addr, addr_type FROM tmp_new_addresses;
    END IF;

    DROP TEMPORARY TABLE IF EXISTS tmp_new_addresses;

    -- =========================================================================
    -- Phase 3: Create tx_token/tx_program for batch addresses only
    -- =========================================================================
    INSERT IGNORE INTO tx_token (mint_address_id)
    SELECT DISTINCT a.id
    FROM tx_address a
    JOIN (
        SELECT t.token_1 AS addr FROM JSON_TABLE(p_txs_json, '$.data[*]' COLUMNS (
            token_1 VARCHAR(44) PATH '$.one_line_summary.data.token_1'
        )) t WHERE t.token_1 IS NOT NULL AND t.token_1 != 'null'
        UNION
        SELECT t.token_2 FROM JSON_TABLE(p_txs_json, '$.data[*]' COLUMNS (
            token_2 VARCHAR(44) PATH '$.one_line_summary.data.token_2'
        )) t WHERE t.token_2 IS NOT NULL AND t.token_2 != 'null'
        UNION
        SELECT t.fee_token FROM JSON_TABLE(p_txs_json, '$.data[*]' COLUMNS (
            fee_token VARCHAR(44) PATH '$.one_line_summary.data.fee_token'
        )) t WHERE t.fee_token IS NOT NULL AND t.fee_token != 'null'
        UNION
        SELECT t.token_address FROM JSON_TABLE(p_txs_json, '$.data[*].transfers[*]' COLUMNS (
            token_address VARCHAR(44) PATH '$.token_address'
        )) t WHERE t.token_address IS NOT NULL AND t.token_address != 'null'
        UNION
        SELECT t.base_token FROM JSON_TABLE(p_txs_json, '$.data[*].transfers[*]' COLUMNS (
            base_token VARCHAR(44) PATH '$.base_value.token_address'
        )) t WHERE t.base_token IS NOT NULL AND t.base_token != 'null'
        UNION
        SELECT a2.token_1 FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
            token_1 VARCHAR(44) PATH '$.data.token_1'
        )) a2 WHERE a2.token_1 IS NOT NULL AND a2.token_1 != 'null'
        UNION
        SELECT a2.token_2 FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
            token_2 VARCHAR(44) PATH '$.data.token_2'
        )) a2 WHERE a2.token_2 IS NOT NULL AND a2.token_2 != 'null'
        UNION
        SELECT a2.fee_token FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
            fee_token VARCHAR(44) PATH '$.data.fee_token'
        )) a2 WHERE a2.fee_token IS NOT NULL AND a2.fee_token != 'null'
    ) batch_mints ON a.address = batch_mints.addr
    WHERE a.address_type = 'mint';

    INSERT IGNORE INTO tx_program (program_address_id)
    SELECT DISTINCT a.id
    FROM tx_address a
    JOIN (
        SELECT t.agg_program AS addr FROM JSON_TABLE(p_txs_json, '$.data[*]' COLUMNS (
            agg_program VARCHAR(44) PATH '$.one_line_summary.program_id'
        )) t WHERE t.agg_program IS NOT NULL AND t.agg_program != 'null'
        UNION
        SELECT t.program_id FROM JSON_TABLE(p_txs_json, '$.data[*].transfers[*]' COLUMNS (
            program_id VARCHAR(44) PATH '$.program_id'
        )) t WHERE t.program_id IS NOT NULL AND t.program_id != 'null'
        UNION
        SELECT t.outer_program_id FROM JSON_TABLE(p_txs_json, '$.data[*].transfers[*]' COLUMNS (
            outer_program_id VARCHAR(44) PATH '$.outer_program_id'
        )) t WHERE t.outer_program_id IS NOT NULL AND t.outer_program_id != 'null'
        UNION
        SELECT a2.program_id FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
            program_id VARCHAR(44) PATH '$.program_id'
        )) a2 WHERE a2.program_id IS NOT NULL AND a2.program_id != 'null'
        UNION
        SELECT a2.outer_program_id FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
            outer_program_id VARCHAR(44) PATH '$.outer_program_id'
        )) a2 WHERE a2.outer_program_id IS NOT NULL AND a2.outer_program_id != 'null'
    ) batch_programs ON a.address = batch_programs.addr
    WHERE a.address_type = 'program';

    -- =========================================================================
    -- Phase 4: Pool creation and classification
    -- =========================================================================
    IF v_collect_all_addresses THEN
        INSERT IGNORE INTO tx_pool (pool_address_id)
        SELECT a.id
        FROM tx_address a
        WHERE a.address_type = 'pool'
          AND NOT EXISTS (SELECT 1 FROM tx_pool p WHERE p.pool_address_id = a.id);

        -- Reclassify owner_2 addresses as pools when paired with amm_id
        UPDATE tx_address a
        SET a.address_type = 'pool'
        WHERE a.address_type = 'wallet'
          AND a.address IN (
            SELECT DISTINCT j.owner_2
            FROM JSON_TABLE(p_txs_json, '$.data[*].activities[*]' COLUMNS (
                owner_2 VARCHAR(44) PATH '$.data.owner_2',
                amm_id VARCHAR(44) PATH '$.data.amm_id'
            )) j
            WHERE j.owner_2 IS NOT NULL
              AND j.amm_id IS NOT NULL
              AND j.owner_2 != 'null'
              AND j.amm_id != 'null'
          );

        INSERT IGNORE INTO tx_pool (pool_address_id)
        SELECT a.id
        FROM tx_address a
        WHERE a.address_type = 'pool'
          AND NOT EXISTS (SELECT 1 FROM tx_pool p WHERE p.pool_address_id = a.id);
    END IF;

END;;

DELIMITER ;
