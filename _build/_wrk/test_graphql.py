#!/usr/bin/env python3
"""
GraphQL interface tests for guide_graphql.py

Run:
    python test_graphql.py              # Schema-only tests (no DB needed)
    python test_graphql.py --live       # Full tests against local MySQL (port 3396)
"""

import sys
import json
import argparse

# ── Helpers ──────────────────────────────────────────────────────────────────

PASS = 0
FAIL = 0

def ok(msg):
    global PASS
    PASS += 1
    print(f"  [PASS] {msg}")

def fail(msg, detail=""):
    global FAIL
    FAIL += 1
    print(f"  [FAIL] {msg}")
    if detail:
        print(f"         {detail}")

def section(title):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")


# ── 1. Schema compilation ───────────────────────────────────────────────────

def test_schema_compiles():
    section("Schema Compilation")
    try:
        from guide_graphql import schema
        ok("schema object created")
    except Exception as e:
        fail("schema import", str(e))
        return None

    # Verify it's a valid strawberry schema
    try:
        sdl = schema.as_str()
        assert len(sdl) > 100, f"SDL too short ({len(sdl)} chars)"
        ok(f"schema SDL generated ({len(sdl)} chars)")
    except Exception as e:
        fail("schema SDL generation", str(e))
        return None

    return schema


# ── 2. Schema introspection ─────────────────────────────────────────────────

def test_introspection(schema):
    section("Schema Introspection")
    if not schema:
        fail("skipped — no schema")
        return

    result = schema.execute_sync("{ __schema { queryType { name } } }")
    if result.errors:
        fail("introspection query", str(result.errors))
        return

    query_type = result.data['__schema']['queryType']['name']
    assert query_type == 'Query', f"Expected 'Query', got '{query_type}'"
    ok(f"queryType = {query_type}")

    # Check all expected top-level queries exist
    result = schema.execute_sync("""
        { __schema { queryType { fields { name } } } }
    """)
    field_names = {f['name'] for f in result.data['__schema']['queryType']['fields']}
    expected = {'walletTransactions', 'transaction', 'address', 'token', 'edgeTypes'}
    missing = expected - field_names
    extra = field_names - expected

    if missing:
        fail(f"missing queries: {missing}")
    else:
        ok(f"all top-level queries present: {sorted(expected)}")
    if extra:
        print(f"         (extra queries: {sorted(extra)})")


# ── 3. Type structure ───────────────────────────────────────────────────────

def test_type_structure(schema):
    section("Type Structure")
    if not schema:
        fail("skipped — no schema")
        return

    # Check WalletTransactions fields
    result = schema.execute_sync("""
        { __type(name: "WalletTransactions") {
            fields { name type { name kind ofType { name kind } } }
        }}
    """)
    if result.errors:
        fail("WalletTransactions type query", str(result.errors))
        return

    fields = {f['name'] for f in result.data['__type']['fields']}
    expected_wt = {'address', 'count', 'edgeCount', 'mint', 'transactions'}
    missing = expected_wt - fields
    if missing:
        fail(f"WalletTransactions missing fields: {missing}")
    else:
        ok(f"WalletTransactions has all fields: {sorted(expected_wt)}")

    # Check GuideEdge has lazy resolver fields
    result = schema.execute_sync("""
        { __type(name: "GuideEdge") {
            fields { name }
        }}
    """)
    edge_fields = {f['name'] for f in result.data['__type']['fields']}
    expected_resolvers = {'fromAddress', 'toAddress', 'token', 'pool', 'edgeType'}
    missing_resolvers = expected_resolvers - edge_fields
    if missing_resolvers:
        fail(f"GuideEdge missing resolver fields: {missing_resolvers}")
    else:
        ok(f"GuideEdge has lazy resolver fields: {sorted(expected_resolvers)}")

    expected_data = {'amount', 'decimals', 'dex', 'swapDirection', 'fee', 'taxBps', 'poolLabel', 'insIndex'}
    missing_data = expected_data - edge_fields
    if missing_data:
        fail(f"GuideEdge missing data fields: {missing_data}")
    else:
        ok(f"GuideEdge has data fields: {sorted(expected_data)}")

    # Check Address.fundedBy recursive resolver
    result = schema.execute_sync("""
        { __type(name: "Address") {
            fields { name type { name kind ofType { name } } }
        }}
    """)
    addr_fields = {f['name'] for f in result.data['__type']['fields']}
    if 'fundedBy' in addr_fields:
        ok("Address.fundedBy recursive resolver present")
    else:
        fail("Address.fundedBy missing")

    # Check Pool has token1/token2 resolvers
    result = schema.execute_sync("""
        { __type(name: "Pool") { fields { name } } }
    """)
    pool_fields = {f['name'] for f in result.data['__type']['fields']}
    if {'token1', 'token2'}.issubset(pool_fields):
        ok("Pool.token1 / Pool.token2 resolvers present")
    else:
        fail(f"Pool missing token resolvers, has: {pool_fields}")


# ── 4. Query validation (no execution) ─────────────────────────────────────

def test_query_validation(schema):
    section("Query Validation (parse + validate, no execution)")
    if not schema:
        fail("skipped — no schema")
        return

    from graphql import parse, validate as gql_validate

    queries = {
        "minimal wallet txs": """
            query { walletTransactions(address: "test") {
                count transactions { signature }
            }}
        """,
        "wallet txs with all params": """
            query { walletTransactions(
                address: "test", mint: "abc", limit: 10,
                after: 100, before: 200, typeMask: 3
            ) { address count edgeCount mint transactions {
                signature blockTime blockTimeUtc
                edges { amount dex swapDirection edgeType { code category } }
            }}}
        """,
        "deep nested — edge resolvers": """
            query { transaction(signature: "abc") {
                signature blockTime
                edges {
                    amount
                    edgeType { code category direction }
                    fromAddress { address label addressType fundedBy { address } }
                    toAddress { address label }
                    token { tokenSymbol mintAddress decimals }
                    pool { poolLabel token1 { tokenSymbol } token2 { tokenSymbol } }
                }
            }}
        """,
        "address lookup": """
            query { address(address: "xyz") { address label addressType fundedBy { address } } }
        """,
        "token lookup": """
            query { token(mintAddress: "So1...") { tokenSymbol tokenName decimals supply } }
        """,
        "edge types": """
            query { edgeTypes { id code name category direction } }
        """,
    }

    for name, query_str in queries.items():
        try:
            doc = parse(query_str)
            errors = gql_validate(schema._schema, doc)
            if errors:
                fail(f"{name}", "; ".join(str(e) for e in errors))
            else:
                ok(f"{name}")
        except Exception as e:
            fail(f"{name}", str(e))

    # Negative test — should fail validation
    bad_query = "query { walletTransactions(address: \"x\") { nonExistentField } }"
    try:
        doc = parse(bad_query)
        errors = gql_validate(schema._schema, doc)
        if errors:
            ok("bad field correctly rejected")
        else:
            fail("bad field was NOT rejected")
    except Exception as e:
        fail("bad query parse error", str(e))


# ── 5. Flask integration ───────────────────────────────────────────────────

def test_flask_mount():
    section("Flask Integration")
    try:
        from flask import Flask
        from guide_graphql import mount_graphql

        app = Flask(__name__)

        # Stub auth functions
        def fake_validate(key):
            return {'id': 1, 'name': 'test', 'rate_limit': 0}
        def fake_rate_limit(kid, rl):
            return True, None
        def fake_db():
            raise RuntimeError("no DB in test")

        mount_graphql(app, fake_db, fake_validate, fake_rate_limit)

        # Check that /api/graphql route exists
        rules = [r.rule for r in app.url_map.iter_rules()]
        if '/api/graphql' in rules:
            ok("/api/graphql route registered")
        else:
            fail(f"/api/graphql not found in routes: {rules}")

    except Exception as e:
        fail("Flask mount", str(e))


# ── 6. Live DB tests ───────────────────────────────────────────────────────

def test_live(schema):
    section("Live DB Tests (MySQL)")

    try:
        import mysql.connector
    except ImportError:
        fail("mysql-connector-python not installed")
        return

    DB_CONFIG = {
        'host': '127.0.0.1',
        'port': 3396,
        'user': 'root',
        'password': 'rootpassword',
        'database': 't16o_db',
    }

    # Test DB connectivity
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        conn.close()
        ok("MySQL connection successful")
    except Exception as e:
        fail(f"MySQL connection failed — is Docker running? ({e})")
        return

    from guide_graphql import GQLContext

    def get_db():
        return mysql.connector.connect(**DB_CONFIG)

    ctx = GQLContext(get_db=get_db, api_key_info={'id': 0, 'name': 'test'})

    # edgeTypes — simple, always works if DB is up
    result = schema.execute_sync(
        "{ edgeTypes { id code name category } }",
        context_value=ctx,
    )
    if result.errors:
        fail("edgeTypes query", str(result.errors))
    elif not result.data['edgeTypes']:
        fail("edgeTypes returned empty (is tx_guide_type populated?)")
    else:
        count = len(result.data['edgeTypes'])
        first = result.data['edgeTypes'][0]
        ok(f"edgeTypes returned {count} types (first: {first['code']})")

    # Pick a wallet address that has guide data
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT a.address FROM tx_guide g
        JOIN tx_address a ON a.id = g.from_address_id
        LIMIT 1
    """)
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        print("  [SKIP] No tx_guide data — skipping walletTransactions live test")
        return

    test_address = row['address']
    print(f"  [INFO] Testing with wallet: {test_address[:12]}...")

    # walletTransactions — direct SQL
    result = schema.execute_sync(
        """
        query($addr: String!) {
            walletTransactions(address: $addr, limit: 5) {
                address count edgeCount
                transactions {
                    signature blockTime
                    edges {
                        amount dex swapDirection
                        edgeType { code category }
                    }
                }
            }
        }
        """,
        variable_values={"addr": test_address},
        context_value=ctx,
    )
    if result.errors:
        fail("walletTransactions query", str(result.errors))
    else:
        wt = result.data['walletTransactions']
        ok(f"walletTransactions: {wt['count']} txs, {wt['edgeCount']} edges")
        if wt['transactions']:
            tx0 = wt['transactions'][0]
            ok(f"  first tx: {tx0['signature'][:16]}... ({len(tx0['edges'])} edges)")

    # transaction with DataLoader-resolved nested objects
    if result.data and result.data['walletTransactions']['transactions']:
        sig = result.data['walletTransactions']['transactions'][0]['signature']
        ctx2 = GQLContext(get_db=get_db, api_key_info={'id': 0, 'name': 'test'})
        result2 = schema.execute_sync(
            """
            query($sig: String!) {
                transaction(signature: $sig) {
                    signature blockTime
                    edges {
                        amount dex
                        edgeType { code category }
                        fromAddress { address label addressType }
                        toAddress { address label }
                        token { tokenSymbol mintAddress }
                        pool { poolLabel }
                    }
                }
            }
            """,
            variable_values={"sig": sig},
            context_value=ctx2,
        )
        if result2.errors:
            fail("transaction query with resolvers", str(result2.errors))
        else:
            tx = result2.data['transaction']
            edges = tx['edges']
            ok(f"transaction resolver: {len(edges)} edges")
            # Check that DataLoaders actually resolved
            resolved_tokens = sum(1 for e in edges if e.get('token'))
            resolved_addrs = sum(1 for e in edges if e.get('fromAddress'))
            ok(f"  DataLoader resolved: {resolved_addrs} addresses, {resolved_tokens} tokens")


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Test GraphQL schema for theGuide")
    parser.add_argument('--live', action='store_true', help='Run live DB tests (needs MySQL on port 3396)')
    args = parser.parse_args()

    print("\n  theGuide GraphQL — Test Suite")
    print("  " + "-" * 40)

    schema = test_schema_compiles()
    test_introspection(schema)
    test_type_structure(schema)
    test_query_validation(schema)
    test_flask_mount()

    if args.live:
        test_live(schema)
    elif schema:
        print(f"\n  [SKIP] Live DB tests — run with --live when MySQL is up")

    # Summary
    print(f"\n{'='*60}")
    print(f"  Results: {PASS} passed, {FAIL} failed")
    print(f"{'='*60}\n")
    return 1 if FAIL else 0


if __name__ == '__main__':
    exit(main())
