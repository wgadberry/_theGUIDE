-- tx_program seed data
-- Known Solana programs for theGuide

-- This script ensures addresses exist first, then creates program entries

-- System Program
INSERT IGNORE INTO tx_address (address, address_type) VALUES ('11111111111111111111111111111111', 'program');
INSERT INTO tx_program (program_address_id, name, program_type, is_verified)
SELECT id, 'System Program', 'system', 0 FROM tx_address WHERE address = '11111111111111111111111111111111'
ON DUPLICATE KEY UPDATE name = 'System Program', program_type = 'system', is_verified = 0;

-- Compute Budget
INSERT IGNORE INTO tx_address (address, address_type) VALUES ('ComputeBudget111111111111111111111111111111', 'program');
INSERT INTO tx_program (program_address_id, name, program_type, is_verified)
SELECT id, 'Compute Budget', 'compute', 0 FROM tx_address WHERE address = 'ComputeBudget111111111111111111111111111111'
ON DUPLICATE KEY UPDATE name = 'Compute Budget', program_type = 'compute', is_verified = 0;

-- Jupiter v6
INSERT IGNORE INTO tx_address (address, address_type) VALUES ('JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4', 'program');
INSERT INTO tx_program (program_address_id, name, program_type, is_verified)
SELECT id, 'Jupiter v6', 'router', 0 FROM tx_address WHERE address = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
ON DUPLICATE KEY UPDATE name = 'Jupiter v6', program_type = 'router', is_verified = 0;

-- Associated Token
INSERT IGNORE INTO tx_address (address, address_type) VALUES ('ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL', 'program');
INSERT INTO tx_program (program_address_id, name, program_type, is_verified)
SELECT id, 'Associated Token', 'token', 0 FROM tx_address WHERE address = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL'
ON DUPLICATE KEY UPDATE name = 'Associated Token', program_type = 'token', is_verified = 0;

-- Token Program
INSERT IGNORE INTO tx_address (address, address_type) VALUES ('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', 'program');
INSERT INTO tx_program (program_address_id, name, program_type, is_verified)
SELECT id, 'Token Program', 'token', 0 FROM tx_address WHERE address = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
ON DUPLICATE KEY UPDATE name = 'Token Program', program_type = 'token', is_verified = 0;

-- Token-2022
INSERT IGNORE INTO tx_address (address, address_type) VALUES ('TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb', 'program');
INSERT INTO tx_program (program_address_id, name, program_type, is_verified)
SELECT id, 'Token-2022', 'token', 0 FROM tx_address WHERE address = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
ON DUPLICATE KEY UPDATE name = 'Token-2022', program_type = 'token', is_verified = 0;

-- Raydium CPMM
INSERT IGNORE INTO tx_address (address, address_type) VALUES ('CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C', 'program');
INSERT INTO tx_program (program_address_id, name, program_type, is_verified)
SELECT id, 'Raydium CPMM', 'dex', 0 FROM tx_address WHERE address = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C'
ON DUPLICATE KEY UPDATE name = 'Raydium CPMM', program_type = 'dex', is_verified = 0;

-- DFlow Router
INSERT IGNORE INTO tx_address (address, address_type) VALUES ('DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH', 'program');
INSERT INTO tx_program (program_address_id, name, program_type, is_verified)
SELECT id, 'DFlow Router', 'router', 0 FROM tx_address WHERE address = 'DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH'
ON DUPLICATE KEY UPDATE name = 'DFlow Router', program_type = 'router', is_verified = 0;

-- Meteora DLMM
INSERT IGNORE INTO tx_address (address, address_type) VALUES ('LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo', 'program');
INSERT INTO tx_program (program_address_id, name, program_type, is_verified)
SELECT id, 'Meteora DLMM', 'dex', 0 FROM tx_address WHERE address = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo'
ON DUPLICATE KEY UPDATE name = 'Meteora DLMM', program_type = 'dex', is_verified = 0;

-- Orca Whirlpool
INSERT IGNORE INTO tx_address (address, address_type) VALUES ('whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc', 'program');
INSERT INTO tx_program (program_address_id, name, program_type, is_verified)
SELECT id, 'Orca Whirlpool', 'dex', 0 FROM tx_address WHERE address = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
ON DUPLICATE KEY UPDATE name = 'Orca Whirlpool', program_type = 'dex', is_verified = 0;

-- Raydium AMM
INSERT IGNORE INTO tx_address (address, address_type) VALUES ('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', 'program');
INSERT INTO tx_program (program_address_id, name, program_type, is_verified)
SELECT id, 'Raydium AMM', 'dex', 0 FROM tx_address WHERE address = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
ON DUPLICATE KEY UPDATE name = 'Raydium AMM', program_type = 'dex', is_verified = 0;

-- Meteora Pools
INSERT IGNORE INTO tx_address (address, address_type) VALUES ('Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB', 'program');
INSERT INTO tx_program (program_address_id, name, program_type, is_verified)
SELECT id, 'Meteora Pools', 'dex', 0 FROM tx_address WHERE address = 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB'
ON DUPLICATE KEY UPDATE name = 'Meteora Pools', program_type = 'dex', is_verified = 0;

-- Propeller Router
INSERT IGNORE INTO tx_address (address, address_type) VALUES ('proVF4pMXVaYqmy4NjniPh4pqKNfMmsihgd4wdkCX3u', 'program');
INSERT INTO tx_program (program_address_id, name, program_type, is_verified)
SELECT id, 'Propeller Router', 'router', 0 FROM tx_address WHERE address = 'proVF4pMXVaYqmy4NjniPh4pqKNfMmsihgd4wdkCX3u'
ON DUPLICATE KEY UPDATE name = 'Propeller Router', program_type = 'router', is_verified = 0;

