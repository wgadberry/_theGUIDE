"""
Kafka Producer: MySQL (AWS) -> Kafka guide-edges topic

Reads tx_guide rows from AWS MySQL in batches and publishes
each edge as a JSON message to the 'guide-edges' Kafka topic.

Messages are keyed by transaction signature so all edges from
the same tx land in the same partition (preserves tx ordering).
"""

import hashlib
import json
import time
import mysql.connector
from confluent_kafka import Producer

# ── Config ──────────────────────────────────────────────────
MYSQL_CONFIG = {
    "host": "34.196.15.116",
    "port": 3397,
    "user": "root",
    "password": "rootpassword",
    "database": "t16o_db",
}

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "guide-producer",
    "linger.ms": 50,        # batch messages for 50ms before sending
    "batch.size": 65536,     # 64KB batch size
    "compression.type": "lz4",
}

TOPIC = "guide-edges"
BATCH_SIZE = 2000         # rows per MySQL query
START_ID = 1              # starting guide_id
END_ID = 1466430          # ending guide_id

# ── SQL ─────────────────────────────────────────────────────
QUERY = """
SELECT
    t.block_id AS block_id
    ,g.block_time AS block_time
    ,t.block_time_utc AS block_time_utc
    ,t.signature AS signature
    ,gt.type_code AS edge_type_code
    ,gt.category AS edge_category
    ,gt.direction AS edge_direction
    ,g.swap_direction AS swap_direction
    ,fa.address AS from_address
    ,fa.address_type AS from_address_type
    ,fa.label AS from_label
    ,fa.active_age_days AS from_age_days
    ,fba.address AS from_funded_by
    ,fa.funding_amount AS from_funding_amount
    ,ta.address AS to_address
    ,ta.address_type AS to_address_type
    ,ta.label AS to_label
    ,fata.address AS from_token_account
    ,tata.address AS to_token_account
    ,CASE WHEN g.token_id = 0 THEN 'SOL' ELSE IFNULL(tok.token_symbol, 'UNKNOWN') END AS token_symbol
    ,CASE WHEN g.token_id = 0 THEN 'Solana' ELSE tok.token_name END AS token_name
    ,CASE WHEN g.token_id = 0 THEN NULL ELSE tm.address END AS token_mint
    ,CASE WHEN g.token_id = 0 THEN 9 ELSE IFNULL(g.decimals, tok.decimals) END AS decimals
    ,tok.token_type AS token_type
    ,g.amount AS amount
    ,g.from_token_pre_balance AS from_token_pre_balance
    ,g.from_token_post_balance AS from_token_post_balance
    ,g.to_token_pre_balance AS to_token_pre_balance
    ,g.to_token_post_balance AS to_token_post_balance
    ,g.from_sol_pre_balance AS from_sol_pre_balance
    ,g.from_sol_post_balance AS from_sol_post_balance
    ,g.to_sol_pre_balance AS to_sol_pre_balance
    ,g.to_sol_post_balance AS to_sol_post_balance
    ,g.tax_bps AS tax_bps
    ,g.tax_amount AS tax_amount
    ,g.fee AS fee
    ,g.priority_fee AS priority_fee
    ,pool_addr.address AS pool_address
    ,pl.pool_label AS pool_label
    ,pl.dex AS dex
    ,pl.token_pair AS token_pair
    ,pra.address AS program_address
    ,prg.name AS program_name
    ,prg.program_type AS program_type
    ,gs.source_code AS source_code
    ,gs.source_name AS source_name
    ,g.source_row_id AS source_row_id
    ,g.created_utc AS created_utc
    ,g.updated_utc AS updated_utc
FROM tx_guide g
JOIN tx t ON t.id = g.tx_id
JOIN tx_guide_type gt ON gt.id = g.edge_type_id
JOIN tx_address fa ON fa.id = g.from_address_id
JOIN tx_address ta ON ta.id = g.to_address_id
LEFT JOIN tx_address fata ON fata.id = g.from_token_account_id
LEFT JOIN tx_address tata ON tata.id = g.to_token_account_id
LEFT JOIN tx_address fba ON fba.id = fa.funded_by_address_id
LEFT JOIN tx_token tok ON tok.id = g.token_id AND g.token_id > 0
LEFT JOIN tx_address tm ON tm.id = tok.mint_address_id
LEFT JOIN tx_pool pl ON pl.id = g.pool_id
LEFT JOIN tx_address pool_addr ON pool_addr.id = pl.pool_address_id
LEFT JOIN tx_program prg ON prg.id = pl.program_id
LEFT JOIN tx_address pra ON pra.id = prg.program_address_id
LEFT JOIN tx_guide_source gs ON gs.id = g.source_id
WHERE g.id BETWEEN %s AND %s
ORDER BY g.id ASC
"""


def delivery_callback(err, msg):
    """Called once per message to confirm delivery or report failure."""
    if err:
        print(f"  FAILED: {err}")


def compute_edge_hash(record):
    """Content-based hash from immutable on-chain fields only.
    Components: signature + from_address + to_address + edge_type_code + token_mint + amount
    """
    parts = "|".join([
        str(record.get("signature", "")),
        str(record.get("from_address", "")),
        str(record.get("to_address", "")),
        str(record.get("edge_type_code", "")),
        str(record.get("token_mint") or "SOL"),
        str(record.get("amount") or "0"),
    ])
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()[:16]


def serialize_row(row, columns):
    """Convert a MySQL row + column names into a JSON-safe dict."""
    record = {}
    for col, val in zip(columns, row):
        if val is None:
            record[col] = None
        elif isinstance(val, (int, float)):
            record[col] = val
        else:
            record[col] = str(val)
    record["edge_hash"] = compute_edge_hash(record)
    return record


def main():
    # ── Connect to MySQL ────────────────────────────────────
    print(f"Connecting to MySQL {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}...")
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    # ── Connect to Kafka ────────────────────────────────────
    print(f"Connecting to Kafka {KAFKA_CONFIG['bootstrap.servers']}...")
    producer = Producer(KAFKA_CONFIG)

    # ── Produce messages ────────────────────────────────────
    current_id = START_ID
    total_sent = 0

    while current_id <= END_ID:
        batch_end = min(current_id + BATCH_SIZE - 1, END_ID)
        cursor.execute(QUERY, (current_id, batch_end))
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        if not rows:
            break

        for row in rows:
            record = serialize_row(row, columns)
            key = record["signature"].encode("utf-8")
            value = json.dumps(record).encode("utf-8")

            producer.produce(
                topic=TOPIC,
                key=key,
                value=value,
                callback=delivery_callback,
            )
            total_sent += 1

        # Flush this batch to Kafka
        producer.flush()
        print(f"  Batch {current_id}-{batch_end}: {len(rows)} edges sent ({total_sent} total)")

        current_id = batch_end + 1

    # ── Cleanup ─────────────────────────────────────────────
    cursor.close()
    conn.close()
    print(f"\nDone. {total_sent} messages published to '{TOPIC}'")


if __name__ == "__main__":
    main()
