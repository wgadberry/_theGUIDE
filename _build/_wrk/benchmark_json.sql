DELIMITER //

DROP PROCEDURE IF EXISTS bench_nested_path //

CREATE PROCEDURE bench_nested_path(IN p_json JSON)
BEGIN
    DECLARE v_i INT DEFAULT 0;
    DECLARE v_col_count INT DEFAULT 30;
    DECLARE v_addr_type VARCHAR(20);
    DECLARE v_type_map VARCHAR(500) DEFAULT 'wallet,wallet,program,mint,mint,mint,wallet,wallet,mint,mint,program,program,ata,ata,program,program,wallet,wallet,wallet,wallet,mint,mint,mint,pool,ata,ata,vault,vault,vault,vault';

    -- Step 1: Parse JSON ONCE with NESTED PATH
    DROP TEMPORARY TABLE IF EXISTS tmp_prepop_raw;
    CREATE TEMPORARY TABLE tmp_prepop_raw (
        col0 VARCHAR(44), col1 VARCHAR(44), col2 VARCHAR(44), col3 VARCHAR(44), col4 VARCHAR(44),
        col5 VARCHAR(44), col6 VARCHAR(44), col7 VARCHAR(44), col8 VARCHAR(44), col9 VARCHAR(44),
        col10 VARCHAR(44), col11 VARCHAR(44), col12 VARCHAR(44), col13 VARCHAR(44), col14 VARCHAR(44),
        col15 VARCHAR(44), col16 VARCHAR(44), col17 VARCHAR(44), col18 VARCHAR(44), col19 VARCHAR(44),
        col20 VARCHAR(44), col21 VARCHAR(44), col22 VARCHAR(44), col23 VARCHAR(44), col24 VARCHAR(44),
        col25 VARCHAR(44), col26 VARCHAR(44), col27 VARCHAR(44), col28 VARCHAR(44), col29 VARCHAR(44)
    );

    INSERT INTO tmp_prepop_raw
    SELECT * FROM JSON_TABLE(p_json, '$.data[*]' COLUMNS (
        col0 VARCHAR(44) PATH '$.one_line_summary.data.account',
        col1 VARCHAR(44) PATH '$.transfers[0].source_owner',
        col2 VARCHAR(44) PATH '$.one_line_summary.program_id',
        col3 VARCHAR(44) PATH '$.one_line_summary.data.token_1',
        col4 VARCHAR(44) PATH '$.one_line_summary.data.token_2',
        col5 VARCHAR(44) PATH '$.one_line_summary.data.fee_token',
        NESTED PATH '$.transfers[*]' COLUMNS (
            col6 VARCHAR(44) PATH '$.source_owner',
            col7 VARCHAR(44) PATH '$.destination_owner',
            col8 VARCHAR(44) PATH '$.token_address',
            col9 VARCHAR(44) PATH '$.base_value.token_address',
            col10 VARCHAR(44) PATH '$.program_id',
            col11 VARCHAR(44) PATH '$.outer_program_id',
            col12 VARCHAR(44) PATH '$.source',
            col13 VARCHAR(44) PATH '$.destination'
        ),
        NESTED PATH '$.activities[*]' COLUMNS (
            col14 VARCHAR(44) PATH '$.program_id',
            col15 VARCHAR(44) PATH '$.outer_program_id',
            col16 VARCHAR(44) PATH '$.data.account',
            col17 VARCHAR(44) PATH '$.data.source',
            col18 VARCHAR(44) PATH '$.data.owner_1',
            col19 VARCHAR(44) PATH '$.data.owner_2',
            col20 VARCHAR(44) PATH '$.data.token_1',
            col21 VARCHAR(44) PATH '$.data.token_2',
            col22 VARCHAR(44) PATH '$.data.fee_token',
            col23 VARCHAR(44) PATH '$.data.amm_id',
            col24 VARCHAR(44) PATH '$.data.new_account',
            col25 VARCHAR(44) PATH '$.data.init_account',
            col26 VARCHAR(44) PATH '$.data.token_account_1_1',
            col27 VARCHAR(44) PATH '$.data.token_account_1_2',
            col28 VARCHAR(44) PATH '$.data.token_account_2_1',
            col29 VARCHAR(44) PATH '$.data.token_account_2_2'
        )
    )) t;

    -- Step 2: Collect unique addresses via column loop
    DROP TEMPORARY TABLE IF EXISTS tmp_prepop_addresses;
    CREATE TEMPORARY TABLE tmp_prepop_addresses (
        addr VARCHAR(44) NOT NULL,
        addr_type VARCHAR(20) NOT NULL,
        UNIQUE KEY uk_addr (addr)
    );

    WHILE v_i < v_col_count DO
        SET v_addr_type = SUBSTRING_INDEX(SUBSTRING_INDEX(v_type_map, ',', v_i + 1), ',', -1);
        SET @sql = CONCAT(
            'INSERT IGNORE INTO tmp_prepop_addresses (addr, addr_type) ',
            'SELECT col', v_i, ', ''', v_addr_type, ''' FROM tmp_prepop_raw ',
            'WHERE col', v_i, ' IS NOT NULL AND col', v_i, ' != ''null'''
        );
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        SET v_i = v_i + 1;
    END WHILE;

    SELECT COUNT(*) AS raw_rows FROM tmp_prepop_raw;
    SELECT COUNT(*) AS unique_addresses FROM tmp_prepop_addresses;

    DROP TEMPORARY TABLE IF EXISTS tmp_prepop_raw;
    DROP TEMPORARY TABLE IF EXISTS tmp_prepop_addresses;
END //

DELIMITER ;
