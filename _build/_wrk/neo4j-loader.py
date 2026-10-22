#!/usr/bin/env python3
"""Load full tx_guide into Neo4j with per-transaction granularity.

Run on EC2:  python3 neo4j-loader.py
"""
import json, urllib.request, base64, sys, time, subprocess

NEO4J_URL = "http://localhost:7474/db/neo4j/tx/commit"
CREDS = base64.b64encode(b"neo4j:t16o_graph").decode()

MYSQL_CMD = [
    "docker", "exec", "t16o_v1_services_mysql",
    "mysql", "-u", "root", "-prootpassword", "t16o_db",
    "--batch", "--raw", "-e"
]


def cypher(statement, params=None):
    body = {"statements": [{"statement": statement}]}
    if params:
        body["statements"][0]["parameters"] = params
    data = json.dumps(body).encode()
    req = urllib.request.Request(NEO4J_URL, data=data, headers={
        "Content-Type": "application/json",
        "Authorization": "Basic " + CREDS
    })
    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read())
        errs = result.get("errors", [])
        if errs:
            print("  ERR: " + errs[0].get("message", "")[:120], file=sys.stderr)
        return result


def mysql_query(sql):
    proc = subprocess.run(MYSQL_CMD + [sql], capture_output=True, text=True)
    if proc.returncode != 0:
        print("  MYSQL ERR: " + proc.stderr[:200], file=sys.stderr)
        return []
    lines = proc.stdout.strip().split("\n")
    if len(lines) < 2:
        return []
    headers = lines[0].split("\t")
    rows = []
    for line in lines[1:]:
        vals = line.split("\t")
        row = {}
        for i, h in enumerate(headers):
            v = vals[i] if i < len(vals) else None
            if v in ("NULL", "\\N", ""):
                v = None
            row[h] = v
        rows.append(row)
    return rows


def main():
    t0 = time.time()

    # ── Clear (in batches to avoid timeout) ──
    print("Clearing existing data...")
    while True:
        r = cypher("MATCH ()-[t]->() WITH t LIMIT 50000 DELETE t RETURN count(t) AS d")
        d = r["results"][0]["data"][0]["row"][0]
        if d == 0:
            break
        print("  Deleted {} relationships...".format(d))
    while True:
        r = cypher("MATCH (n) WITH n LIMIT 50000 DELETE n RETURN count(n) AS d")
        d = r["results"][0]["data"][0]["row"][0]
        if d == 0:
            break
        print("  Deleted {} nodes...".format(d))
    print("  Done")

    # ── Indexes ──
    print("Creating indexes...")
    cypher("CREATE CONSTRAINT addr_unique IF NOT EXISTS FOR (a:Address) REQUIRE a.address IS UNIQUE")
    cypher("CREATE INDEX addr_type IF NOT EXISTS FOR (a:Address) ON (a.type)")
    cypher("CREATE INDEX addr_label IF NOT EXISTS FOR (a:Address) ON (a.label)")
    print("  Done")

    # ── Load addresses ──
    print("Loading addresses...")
    addr_sql = (
        "SELECT DISTINCT a.address, a.address_type, a.label, fb.address AS funded_by "
        "FROM ("
        "  SELECT from_address_id AS aid FROM tx_guide "
        "  UNION "
        "  SELECT to_address_id FROM tx_guide"
        ") ids "
        "JOIN tx_address a ON a.id = ids.aid "
        "LEFT JOIN tx_address fb ON fb.id = a.funded_by_address_id"
    )
    addrs = mysql_query(addr_sql)
    print("  Fetched {} addresses from MySQL".format(len(addrs)))

    addr_stmt = (
        "UNWIND $rows AS row "
        "MERGE (a:Address {address: row.address}) "
        "SET a.type = row.address_type, "
        "    a.label = row.label, "
        "    a.funded_by = row.funded_by"
    )
    batch = []
    total = 0
    for row in addrs:
        batch.append(row)
        if len(batch) >= 1000:
            cypher(addr_stmt, {"rows": batch})
            total += len(batch)
            batch = []
    if batch:
        cypher(addr_stmt, {"rows": batch})
        total += len(batch)
    print("  Loaded {} address nodes ({:.0f}s)".format(total, time.time() - t0))

    # ── Load edges ──
    print("Loading tx_guide edges...")
    cnt_rows = mysql_query("SELECT COUNT(*) as cnt FROM tx_guide")
    total_rows = int(cnt_rows[0]["cnt"]) if cnt_rows else 0
    print("  Total tx_guide rows: {:,}".format(total_rows))

    edge_sql = (
        "SELECT g.id AS guide_id, t.signature, g.block_time, "
        "fa.address AS from_address, ta.address AS to_address, "
        "IFNULL(gt.type_code, 'UNKNOWN') AS edge_type, "
        "IFNULL(gt.category, 'other') AS edge_category, "
        "IFNULL(gt.direction, 'neutral') AS edge_direction, "
        "IFNULL(gt.risk_weight, 0) AS risk_weight, "
        "IFNULL(g.swap_direction, '') AS swap_direction, "
        "IFNULL(tok.token_symbol, 'SOL') AS token_symbol, "
        "IFNULL(tm.address, 'SOL') AS token_mint, "
        "IFNULL(g.decimals, IFNULL(tok.decimals, 9)) AS decimals, "
        "IFNULL(g.amount, 0) AS amount, "
        "g.from_token_pre_balance, g.from_token_post_balance, "
        "g.to_token_pre_balance, g.to_token_post_balance, "
        "g.from_sol_pre_balance, g.from_sol_post_balance, "
        "g.to_sol_pre_balance, g.to_sol_post_balance, "
        "IFNULL(g.fee, 0) AS fee, IFNULL(g.priority_fee, 0) AS priority_fee, "
        "g.tax_bps, g.tax_amount, "
        "pa.address AS pool_address, p.pool_label, p.dex, p.token_pair, "
        "prg.name AS program_name, prg.program_type "
        "FROM tx_guide g "
        "JOIN tx t ON t.id = g.tx_id "
        "JOIN tx_address fa ON fa.id = g.from_address_id "
        "JOIN tx_address ta ON ta.id = g.to_address_id "
        "LEFT JOIN tx_guide_type gt ON gt.id = g.edge_type_id "
        "LEFT JOIN tx_token tok ON tok.id = g.token_id AND g.token_id > 0 "
        "LEFT JOIN tx_address tm ON tm.id = tok.mint_address_id "
        "LEFT JOIN tx_pool p ON p.id = g.pool_id "
        "LEFT JOIN tx_address pa ON pa.id = p.pool_address_id "
        "LEFT JOIN tx_program prg ON prg.id = COALESCE(p.program_id, fa.program_id) "
        "WHERE g.id > {offset} ORDER BY g.id LIMIT {chunk}"
    )

    edge_stmt = (
        "UNWIND $rows AS r "
        "MATCH (fr:Address {address: r.from_address}) "
        "MATCH (to:Address {address: r.to_address}) "
        "CREATE (fr)-[t:TRANSFER]->(to) "
        "SET t.guide_id = toInteger(r.guide_id), "
        "t.signature = r.signature, "
        "t.block_time = toInteger(r.block_time), "
        "t.edge_type = r.edge_type, "
        "t.edge_category = r.edge_category, "
        "t.edge_direction = r.edge_direction, "
        "t.risk_weight = toInteger(r.risk_weight), "
        "t.swap_direction = CASE WHEN r.swap_direction = '' THEN null ELSE r.swap_direction END, "
        "t.token_symbol = r.token_symbol, "
        "t.token_mint = r.token_mint, "
        "t.decimals = toInteger(r.decimals), "
        "t.amount = r.amount, "
        "t.from_token_pre_balance = r.from_token_pre_balance, "
        "t.from_token_post_balance = r.from_token_post_balance, "
        "t.to_token_pre_balance = r.to_token_pre_balance, "
        "t.to_token_post_balance = r.to_token_post_balance, "
        "t.from_sol_pre_balance = r.from_sol_pre_balance, "
        "t.from_sol_post_balance = r.from_sol_post_balance, "
        "t.to_sol_pre_balance = r.to_sol_pre_balance, "
        "t.to_sol_post_balance = r.to_sol_post_balance, "
        "t.fee = toInteger(r.fee), "
        "t.priority_fee = toInteger(r.priority_fee), "
        "t.tax_bps = CASE WHEN r.tax_bps IS NOT NULL THEN toInteger(r.tax_bps) END, "
        "t.tax_amount = r.tax_amount, "
        "t.pool_address = r.pool_address, "
        "t.pool_label = r.pool_label, "
        "t.dex = r.dex, "
        "t.token_pair = r.token_pair, "
        "t.program_name = r.program_name, "
        "t.program_type = r.program_type"
    )

    CHUNK_SIZE = 5000
    NEO_BATCH = 500
    offset = 0
    total_loaded = 0
    load_errors = 0

    while True:
        sql = edge_sql.format(offset=offset, chunk=CHUNK_SIZE)
        rows = mysql_query(sql)
        if not rows:
            break

        for i in range(0, len(rows), NEO_BATCH):
            b = rows[i:i + NEO_BATCH]
            try:
                cypher(edge_stmt, {"rows": b})
            except Exception as e:
                load_errors += 1
                if load_errors <= 5:
                    print("  WARN: batch error at {}: {}".format(total_loaded, e),
                          file=sys.stderr)

        total_loaded += len(rows)
        offset = int(rows[-1]["guide_id"])
        elapsed = time.time() - t0
        rate = total_loaded / max(1, elapsed)
        pct = total_loaded / max(1, total_rows) * 100
        print("  {:>8,} / {:,} ({:.1f}%) -- {:.0f}/s -- {:.0f}s".format(
            total_loaded, total_rows, pct, rate, elapsed))

        if len(rows) < CHUNK_SIZE:
            break

    elapsed = time.time() - t0
    print("  Complete: {:,} edges in {:.1f}s".format(total_loaded, elapsed))
    if load_errors:
        print("  Errors: {}".format(load_errors))

    # ── Secondary labels ──
    print("\nAdding secondary labels...")
    for atype, lbl in [("pool", "Pool"), ("vault", "Vault"), ("mint", "Mint"),
                        ("dex_wallet", "DexWallet"), ("market", "Market")]:
        r = cypher(
            "MATCH (a:Address) WHERE a.type = '{}' SET a:{} RETURN count(a) AS cnt".format(
                atype, lbl))
        cnt = r["results"][0]["data"][0]["row"][0]
        if cnt:
            print("  :{} -- {} nodes".format(lbl, cnt))

    # ── Verify ──
    print("\n=== Verification ===")
    r = cypher("MATCH (a:Address) RETURN count(a) AS nodes")
    print("  Nodes: {:,}".format(r["results"][0]["data"][0]["row"][0]))
    r = cypher("MATCH ()-[t:TRANSFER]->() RETURN count(t) AS edges")
    print("  Edges: {:,}".format(r["results"][0]["data"][0]["row"][0]))
    r = cypher("MATCH (a:Address) WHERE a.label IS NOT NULL RETURN count(a) AS labeled")
    print("  Labeled: {:,}".format(r["results"][0]["data"][0]["row"][0]))

    print("\n  By edge type:")
    r = cypher(
        "MATCH ()-[t:TRANSFER]->() RETURN t.edge_type AS type, count(t) AS cnt "
        "ORDER BY cnt DESC")
    for d in r["results"][0]["data"]:
        print("    {:18s} {:>8,}".format(str(d["row"][0]), d["row"][1]))

    print("\n  By token (top 10):")
    r = cypher(
        "MATCH ()-[t:TRANSFER]->() RETURN t.token_symbol AS tok, count(t) AS cnt "
        "ORDER BY cnt DESC LIMIT 10")
    for d in r["results"][0]["data"]:
        print("    {:12s} {:>8,}".format(str(d["row"][0]), d["row"][1]))

    # Storage
    print("\n=== Storage ===")
    proc = subprocess.run(
        ["docker", "exec", "neo4j", "du", "-sh", "/data/databases/", "/data/"],
        capture_output=True, text=True)
    for line in proc.stdout.strip().split("\n"):
        print("  " + line)

    total_elapsed = time.time() - t0
    print("\nTotal time: {:.1f}s".format(total_elapsed))


if __name__ == "__main__":
    main()
