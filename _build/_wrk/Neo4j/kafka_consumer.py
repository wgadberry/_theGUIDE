"""
Kafka Consumer v2: guide-edges topic -> Neo4j (rich graph model)

Typed nodes:   Wallet, Pool, DexWallet, Vault, Bot, Market, Token, Program, Dex, Cex, Transaction
Typed edges:   SOL_TRANSFER, SPL_TRANSFER, SWAP_IN, SWAP_OUT, FEE, etc. (from tx_guide_type)
Linking edges: Pool-[:TRADES]->Token, Pool-[:RUNS_ON]->Program, Program-[:BELONGS_TO]->Dex
"""

import json
import re
import signal
from confluent_kafka import Consumer, KafkaError
from neo4j import GraphDatabase

# ── Config ──────────────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "neo4j-loader-v3",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

TOPIC = "guide-edges"

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "neo4jpassword"

BATCH_SIZE = 500

# ── Address type -> Neo4j label mapping ─────────────────────
ADDRESS_LABEL_MAP = {
    "wallet": "Wallet",
    "pool": "Pool",
    "dex_wallet": "DexWallet",
    "vault": "Vault",
    "bot": "Bot",
    "market": "Market",
    "ata": "Wallet",       # ATAs map to wallet for graph purposes
    "mint": "Wallet",      # mint addresses act as wallet nodes
    "lp_token": "Wallet",  # LP token addresses
    "unknown": "Wallet",   # default
}

# ── CEX detection ───────────────────────────────────────────
CEX_PATTERNS = {
    "Binance": re.compile(r"Binance", re.IGNORECASE),
    "Bybit": re.compile(r"Bybit", re.IGNORECASE),
    "OKX": re.compile(r"OKX", re.IGNORECASE),
    "Coinbase": re.compile(r"Coinbase", re.IGNORECASE),
    "Kraken": re.compile(r"Kraken", re.IGNORECASE),
    "KuCoin": re.compile(r"KuCoin", re.IGNORECASE),
    "Gate.io": re.compile(r"Gate\.io", re.IGNORECASE),
    "Bitget": re.compile(r"Bitget", re.IGNORECASE),
    "MEXC": re.compile(r"MEXC", re.IGNORECASE),
    "HTX": re.compile(r"HTX|Huobi", re.IGNORECASE),
    "Crypto.com": re.compile(r"Crypto\.com", re.IGNORECASE),
    "Gemini": re.compile(r"Gemini", re.IGNORECASE),
    "Upbit": re.compile(r"Upbit", re.IGNORECASE),
}


def detect_cex(label):
    """Return CEX name if label matches a known exchange, else None."""
    if not label:
        return None
    for cex_name, pattern in CEX_PATTERNS.items():
        if pattern.search(label):
            return cex_name
    return None


# ── Numeric casting ─────────────────────────────────────────
INT_FIELDS = [
    "block_id", "block_time", "decimals",
    "tax_bps", "tax_amount", "fee", "priority_fee",
    "from_age_days", "source_row_id", "from_funding_amount",
]
FLOAT_FIELDS = [
    "amount",
    "from_token_pre_balance", "from_token_post_balance",
    "to_token_pre_balance", "to_token_post_balance",
    "from_sol_pre_balance", "from_sol_post_balance",
    "to_sol_pre_balance", "to_sol_post_balance",
]


def cast_numerics(record):
    for field in INT_FIELDS:
        val = record.get(field)
        if val is not None:
            try:
                record[field] = int(val)
            except (ValueError, TypeError):
                pass
    for field in FLOAT_FIELDS:
        val = record.get(field)
        if val is not None:
            try:
                record[field] = float(val)
            except (ValueError, TypeError):
                pass
    return record


# ── Cypher builders ─────────────────────────────────────────

def build_node_cypher(label, key_field, key_value, props):
    """Build a MERGE statement for a typed node."""
    set_parts = []
    for k, v in props.items():
        if v is not None:
            set_parts.append(f"n.{k} = ${k}")
    on_create = ", ".join(set_parts) if set_parts else "n._created = true"
    return f"""
        MERGE (n:{label} {{{key_field}: ${key_field}}})
        ON CREATE SET {on_create}
    """


# Edge properties shared across all relationship types
EDGE_PROPS = """
    e.edge_hash = $edge_hash,
    e.tx_signature = $signature,
    e.token_symbol = $token_symbol,
    e.token_mint = $token_mint,
    e.decimals = $decimals,
    e.amount = $amount,
    e.from_token_pre_balance = $from_token_pre_balance,
    e.from_token_post_balance = $from_token_post_balance,
    e.to_token_pre_balance = $to_token_pre_balance,
    e.to_token_post_balance = $to_token_post_balance,
    e.from_sol_pre_balance = $from_sol_pre_balance,
    e.from_sol_post_balance = $from_sol_post_balance,
    e.to_sol_pre_balance = $to_sol_pre_balance,
    e.to_sol_post_balance = $to_sol_post_balance,
    e.tax_bps = $tax_bps,
    e.tax_amount = $tax_amount,
    e.fee = $fee,
    e.priority_fee = $priority_fee,
    e.pool_address = $pool_address,
    e.dex = $dex,
    e.program_name = $program_name,
    e.swap_direction = $swap_direction,
    e.source_code = $source_code,
    e.block_time = $block_time,
    e.block_time_utc = $block_time_utc
"""


def load_record(tx_neo, record):
    """Load one guide edge record into Neo4j with typed nodes and edges."""

    # ── Resolve node labels ─────────────────────────────────
    from_type = record.get("from_address_type") or "unknown"
    to_type = record.get("to_address_type") or "unknown"
    from_label = ADDRESS_LABEL_MAP.get(from_type, "Wallet")
    to_label = ADDRESS_LABEL_MAP.get(to_type, "Wallet")

    # Relationship type from edge_type_code (uppercase)
    edge_type = (record.get("edge_type_code") or "unknown").upper()

    # ── Transaction node ────────────────────────────────────
    tx_neo.run("""
        MERGE (tx:Transaction {signature: $signature})
        ON CREATE SET tx.block_id = $block_id,
                      tx.block_time = $block_time,
                      tx.block_time_utc = $block_time_utc,
                      tx.fee = $fee,
                      tx.priority_fee = $priority_fee
    """, **record)

    # ── From node (typed) ───────────────────────────────────
    tx_neo.run(f"""
        MERGE (n:{from_label} {{address: $from_address}})
        ON CREATE SET n.address_type = $from_address_type,
                      n.label = $from_label,
                      n.age_days = $from_age_days
        ON MATCH SET  n.label = COALESCE($from_label, n.label),
                      n.age_days = COALESCE($from_age_days, n.age_days)
    """, **record)

    # ── To node (typed) ─────────────────────────────────────
    tx_neo.run(f"""
        MERGE (n:{to_label} {{address: $to_address}})
        ON CREATE SET n.address_type = $to_address_type,
                      n.label = $to_label
        ON MATCH SET  n.label = COALESCE($to_label, n.label)
    """, **record)

    # ── Typed edge: from -> to ──────────────────────────────
    tx_neo.run(f"""
        MATCH (fa:{from_label} {{address: $from_address}})
        MATCH (ta:{to_label} {{address: $to_address}})
        MERGE (fa)-[e:{edge_type} {{edge_hash: $edge_hash}}]->(ta)
        ON CREATE SET {EDGE_PROPS}
    """, **record)

    # ── Token node + linking ────────────────────────────────
    token_mint = record.get("token_mint")
    if token_mint:
        tx_neo.run("""
            MERGE (tok:Token {mint: $token_mint})
            ON CREATE SET tok.symbol = $token_symbol,
                          tok.name = $token_name,
                          tok.decimals = $decimals,
                          tok.token_type = $token_type
            ON MATCH SET  tok.symbol = COALESCE($token_symbol, tok.symbol),
                          tok.name = COALESCE($token_name, tok.name)
        """, **record)

    # ── Pool node + linking ─────────────────────────────────
    pool_address = record.get("pool_address")
    if pool_address:
        tx_neo.run("""
            MERGE (pool:Pool {address: $pool_address})
            ON CREATE SET pool.label = $pool_label,
                          pool.dex = $dex,
                          pool.token_pair = $token_pair
            ON MATCH SET  pool.label = COALESCE($pool_label, pool.label),
                          pool.dex = COALESCE($dex, pool.dex)
        """, **record)

        # Pool -[:TRADES]-> Token
        if token_mint:
            tx_neo.run("""
                MATCH (pool:Pool {address: $pool_address})
                MATCH (tok:Token {mint: $token_mint})
                MERGE (pool)-[:TRADES]->(tok)
            """, **record)

    # ── Program node + linking ──────────────────────────────
    program_address = record.get("program_address")
    if program_address:
        tx_neo.run("""
            MERGE (prog:Program {address: $program_address})
            ON CREATE SET prog.name = $program_name,
                          prog.program_type = $program_type
        """, **record)

        # Pool -[:RUNS_ON]-> Program
        if pool_address:
            tx_neo.run("""
                MATCH (pool:Pool {address: $pool_address})
                MATCH (prog:Program {address: $program_address})
                MERGE (pool)-[:RUNS_ON]->(prog)
            """, **record)

    # ── Dex node (derived from program) ─────────────────────
    dex_name = record.get("dex")
    if dex_name:
        tx_neo.run("""
            MERGE (dx:Dex {name: $dex})
        """, **record)

        if program_address:
            tx_neo.run("""
                MATCH (prog:Program {address: $program_address})
                MATCH (dx:Dex {name: $dex})
                MERGE (prog)-[:BELONGS_TO]->(dx)
            """, **record)

    # ── Cex node (derived from address labels) ──────────────
    for addr_label in [record.get("from_label"), record.get("to_label")]:
        cex_name = detect_cex(addr_label)
        if cex_name:
            tx_neo.run("MERGE (cx:Cex {name: $name})", name=cex_name)
            # Link the address to its CEX
            if addr_label == record.get("from_label"):
                tx_neo.run(f"""
                    MATCH (a:{from_label} {{address: $from_address}})
                    MATCH (cx:Cex {{name: $cex_name}})
                    MERGE (a)-[:OPERATES_FOR]->(cx)
                """, from_address=record["from_address"], cex_name=cex_name)
            if addr_label == record.get("to_label"):
                tx_neo.run(f"""
                    MATCH (a:{to_label} {{address: $to_address}})
                    MATCH (cx:Cex {{name: $cex_name}})
                    MERGE (a)-[:OPERATES_FOR]->(cx)
                """, to_address=record["to_address"], cex_name=cex_name)


def flush_batch(driver, batch):
    """Write a batch of edges to Neo4j in a single transaction."""
    with driver.session() as session:
        with session.begin_transaction() as tx_neo:
            for record in batch:
                load_record(tx_neo, record)
            tx_neo.commit()


# ── Main loop ───────────────────────────────────────────────

running = True

def shutdown(sig, frame):
    global running
    print("\nShutting down...")
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def main():
    print(f"Connecting to Neo4j {NEO4J_URI}...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    print("  Neo4j connected.")

    print(f"Subscribing to Kafka topic '{TOPIC}'...")
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])
    print("  Kafka subscribed. Waiting for messages...")

    total_loaded = 0
    batch = []

    try:
        while running:
            msg = consumer.poll(timeout=2.0)

            if msg is None:
                if batch:
                    flush_batch(driver, batch)
                    consumer.commit()
                    total_loaded += len(batch)
                    print(f"  Flushed {len(batch)} edges ({total_loaded} total)")
                    batch = []
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"  Kafka error: {msg.error()}")
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))
                record = cast_numerics(record)
                batch.append(record)
            except json.JSONDecodeError as e:
                print(f"  Bad JSON (offset {msg.offset()}): {e}")
                continue

            if len(batch) >= BATCH_SIZE:
                flush_batch(driver, batch)
                consumer.commit()
                total_loaded += len(batch)
                print(f"  Loaded {len(batch)} edges ({total_loaded} total)")
                batch = []

    finally:
        consumer.close()
        driver.close()
        print(f"\nDone. {total_loaded} edges loaded into Neo4j.")


if __name__ == "__main__":
    main()
