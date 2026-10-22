-- Migration: Change tx_guide balance columns from BIGINT UNSIGNED to BIGINT (signed)
-- Source table tx_token_balance_change uses DECIMAL(38,0) which can exceed BIGINT UNSIGNED max.
-- The SP temp tables use BIGINT (signed) internally, causing overflow on INSERT into UNSIGNED columns.

ALTER TABLE tx_guide
    MODIFY from_token_pre_balance  BIGINT NULL,
    MODIFY from_token_post_balance BIGINT NULL,
    MODIFY to_token_pre_balance    BIGINT NULL,
    MODIFY to_token_post_balance   BIGINT NULL,
    MODIFY from_sol_pre_balance    BIGINT NULL,
    MODIFY from_sol_post_balance   BIGINT NULL,
    MODIFY to_sol_pre_balance      BIGINT NULL,
    MODIFY to_sol_post_balance     BIGINT NULL;
