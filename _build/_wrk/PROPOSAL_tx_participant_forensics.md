# TX Participant Forensics Design

## Problem Statement
The current tx_guide table only captures addresses with balance changes (edges). This misses "shadow" addresses involved in transactions without direct value movement:
- Signers/authorities controlling accounts
- Fee payers (someone else paying)
- Program authorities/admins
- Intermediary routing accounts
- Referenced but unchanged accounts

## Solution: tx_participant Table

### Table Schema

```sql
CREATE TABLE tx_participant (
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
```

### Why Flags Instead of ENUM
An address can have multiple roles simultaneously:
- Account[0] is typically: signer + fee_payer + writable
- A program is: readonly + program
- An authority might be: signer + readonly

### Data Volume Estimate
- Average transaction: 10-15 accounts
- If tx_guide has 200K unique transactions, expect 2-3M participant rows
- With proper indexing, queries remain fast

---

## Integration: Guide Loader Updates

The loader already processes transactions. We need to extract participant data from the transaction structure.

### Solana Transaction Structure
```json
{
  "transaction": {
    "message": {
      "accountKeys": ["addr1", "addr2", ...],
      "header": {
        "numRequiredSignatures": 1,
        "numReadonlySignedAccounts": 0,
        "numReadonlyUnsignedAccounts": 5
      }
    },
    "signatures": ["sig1", ...]
  }
}
```

### Parsing Logic
```python
def extract_participants(tx_data):
    """
    Extract all participants from a Solana transaction.

    Account classification based on header:
    - accounts[0..numRequiredSignatures-1] = signers
    - accounts[0] = fee payer (by convention)
    - accounts[numRequired..numRequired+numReadonlySigned-1] = readonly signers
    - Last numReadonlyUnsigned accounts = readonly unsigned
    - Everything else = writable unsigned
    """
    message = tx_data.get('transaction', {}).get('message', {})
    accounts = message.get('accountKeys', [])
    header = message.get('header', {})

    num_required_sigs = header.get('numRequiredSignatures', 0)
    num_readonly_signed = header.get('numReadonlySignedAccounts', 0)
    num_readonly_unsigned = header.get('numReadonlyUnsignedAccounts', 0)

    # Known program addresses (could expand this list)
    KNOWN_PROGRAMS = {
        '11111111111111111111111111111111',           # System Program
        'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', # Token Program
        'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL', # ATA Program
        # ... etc
    }

    participants = []
    total_accounts = len(accounts)
    readonly_start = total_accounts - num_readonly_unsigned

    for i, addr in enumerate(accounts):
        is_signer = i < num_required_sigs
        is_fee_payer = i == 0
        is_readonly_signed = is_signer and i >= (num_required_sigs - num_readonly_signed)
        is_readonly_unsigned = i >= readonly_start
        is_writable = not (is_readonly_signed or is_readonly_unsigned)
        is_program = addr in KNOWN_PROGRAMS or is_program_account(addr)

        participants.append({
            'address': addr,
            'account_index': i,
            'is_signer': is_signer,
            'is_fee_payer': is_fee_payer,
            'is_writable': is_writable,
            'is_program': is_program
        })

    return participants
```

---

## Forensic Query Examples

### 1. Shadow Addresses - Involved but Never Transferred
```sql
-- Addresses that appear in SpaceAI transactions but never send/receive
SELECT
    a.address,
    a.address_type,
    a.label,
    COUNT(DISTINCT p.tx_id) as tx_appearances,
    SUM(p.is_signer) as times_signed,
    SUM(p.is_fee_payer) as times_paid_fee
FROM tx_participant p
JOIN tx_address a ON a.id = p.address_id
WHERE p.tx_id IN (
    SELECT DISTINCT tx_id FROM tx_guide g
    JOIN tx_token t ON t.id = g.token_id
    WHERE t.token_symbol = 'SpaceAI'
)
AND p.address_id NOT IN (
    SELECT from_address_id FROM tx_guide g
    JOIN tx_token t ON t.id = g.token_id WHERE t.token_symbol = 'SpaceAI'
    UNION
    SELECT to_address_id FROM tx_guide g
    JOIN tx_token t ON t.id = g.token_id WHERE t.token_symbol = 'SpaceAI'
)
GROUP BY a.id, a.address, a.address_type, a.label
ORDER BY tx_appearances DESC
LIMIT 50;
```

### 2. Fee Payer Analysis - Who's Funding Transactions?
```sql
-- Find who pays fees for transactions involving a specific wallet
SELECT
    payer.address as fee_payer,
    payer.address_type,
    payer.label,
    COUNT(DISTINCT p.tx_id) as txs_paid_for,
    COUNT(DISTINCT g.from_address_id) as unique_wallets_funded
FROM tx_participant p
JOIN tx_address payer ON payer.id = p.address_id
JOIN tx_guide g ON g.tx_id = p.tx_id
WHERE p.is_fee_payer = 1
  AND p.address_id != g.from_address_id  -- Fee payer is not the sender
  AND g.from_address_id = ?  -- Target wallet
GROUP BY payer.id
ORDER BY txs_paid_for DESC;
```

### 3. Authority/Signer Pattern Detection
```sql
-- Find addresses that frequently sign but never transfer (potential authorities)
SELECT
    a.address,
    a.address_type,
    COUNT(DISTINCT p.tx_id) as times_signed,
    COUNT(DISTINCT CASE WHEN g.from_address_id = a.id OR g.to_address_id = a.id
                        THEN g.tx_id END) as times_transferred
FROM tx_participant p
JOIN tx_address a ON a.id = p.address_id
LEFT JOIN tx_guide g ON g.tx_id = p.tx_id
    AND (g.from_address_id = a.id OR g.to_address_id = a.id)
WHERE p.is_signer = 1
GROUP BY a.id, a.address, a.address_type
HAVING times_signed > 10 AND times_transferred = 0
ORDER BY times_signed DESC;
```

### 4. Wallet Clustering - Shared Signers
```sql
-- Find wallets that share a common signer (potential same owner)
SELECT
    a1.address as wallet1,
    a2.address as wallet2,
    signer.address as shared_signer,
    COUNT(DISTINCT p1.tx_id) as shared_txs
FROM tx_participant p1
JOIN tx_participant p2 ON p1.tx_id = p2.tx_id AND p1.address_id != p2.address_id
JOIN tx_participant ps ON ps.tx_id = p1.tx_id AND ps.is_signer = 1
JOIN tx_address a1 ON a1.id = p1.address_id
JOIN tx_address a2 ON a2.id = p2.address_id
JOIN tx_address signer ON signer.id = ps.address_id
WHERE p1.is_writable = 1 AND p2.is_writable = 1
  AND ps.address_id NOT IN (p1.address_id, p2.address_id)
GROUP BY a1.id, a2.id, signer.id
HAVING shared_txs >= 5
ORDER BY shared_txs DESC;
```

---

## Stored Procedure: sp_tx_participant_analysis

```sql
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

    Parameters:
        p_token_symbol - Token symbol to analyze
        p_limit - Max rows (0 = unlimited)
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
                SUM(p.is_program) as is_program
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
            AND a.address_type NOT IN ('program', 'mint')  -- Exclude known non-wallets
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
                SUM(p.is_program) as is_program
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

DELIMITER ;
```

---

## Implementation Plan

### Phase 1: Table Creation
1. Create tx_participant table
2. Create indexes
3. Create forensic stored procedures

### Phase 2: Loader Integration
1. Update guide-shredder to extract participants when processing transactions
2. Batch insert participants alongside guide edges
3. Ensure all addresses are added to tx_address first

### Phase 3: Backfill (Optional)
1. Create backfill script to re-process historical transactions
2. Extract participants from stored transaction data (if available)
3. Or re-fetch transactions from RPC/API

### Phase 4: Forensic Queries
1. Build stored procedures for common forensic patterns
2. Add UI in bmap-viewer for shadow address discovery
3. Create alerts for suspicious patterns

---

## Questions to Resolve

1. **Backfill strategy**: Do we have raw transaction data stored, or need to re-fetch?
2. **Program list**: Should we maintain a list of known programs to flag?
3. **Storage**: Should we partition by time or token for large datasets?
4. **Real-time**: Should forensic analysis run automatically on new transactions?
