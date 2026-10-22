"""
GraphQL interface for tx_guide data.

Direct SQL resolvers over tx_guide with DataLoader batching for FK lookups.
No stored procedures — GraphQL controls the query shape.

Usage:
    from guide_graphql import mount_graphql
    mount_graphql(app, get_db_connection, validate_api_key, check_rate_limit)

Dependencies:
    pip install strawberry-graphql[flask]
"""

import json
import enum
import os
from collections import OrderedDict
from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any

import strawberry
from strawberry.flask.views import GraphQLView
from strawberry.types import Info
from strawberry.schema.config import StrawberryConfig

# Custom scalar: serializes as string to avoid float scientific notation (1e-9).
BigDecimal = strawberry.scalar(
    str,
    name="BigDecimal",
    description="Decimal number serialized as string to preserve precision (e.g. '0.000000001' not 1e-9)",
    serialize=lambda v: str(v) if v is not None else None,
    parse_value=lambda v: str(v) if v is not None else None,
)


# ---------------------------------------------------------------------------
# Context — per-request, carries DB factory + auth + DataLoaders
# ---------------------------------------------------------------------------

class GQLContext:
    __slots__ = ('get_db', 'api_key_info', '_loaders')

    def __init__(self, get_db, api_key_info: dict):
        self.get_db = get_db
        self.api_key_info = api_key_info
        self._loaders: Dict[str, Any] = {}

    def address_loader(self) -> 'BatchLoader':
        if 'address' not in self._loaders:
            self._loaders['address'] = BatchLoader(
                self.get_db,
                "SELECT id, address, address_type, label, funded_by_address_id, "
                "first_seen_block_time FROM tx_address WHERE id IN ({ids})"
            )
        return self._loaders['address']

    def token_loader(self) -> 'BatchLoader':
        if 'token' not in self._loaders:
            self._loaders['token'] = BatchLoader(
                self.get_db,
                "SELECT t.id, a.address AS mint_address, t.token_name, t.token_symbol, "
                "t.decimals, t.supply, t.token_type "
                "FROM tx_token t JOIN tx_address a ON a.id = t.mint_address_id "
                "WHERE t.id IN ({ids})"
            )
        return self._loaders['token']

    def pool_loader(self) -> 'BatchLoader':
        if 'pool' not in self._loaders:
            self._loaders['pool'] = BatchLoader(
                self.get_db,
                "SELECT p.id, a.address AS pool_address, p.pool_label, "
                "p.token1_id, p.token2_id "
                "FROM tx_pool p JOIN tx_address a ON a.id = p.pool_address_id "
                "WHERE p.pool_address_id IN ({ids})",
                id_column='pool_address_id'
            )
        return self._loaders['pool']

    def guide_type_loader(self) -> 'BatchLoader':
        if 'guide_type' not in self._loaders:
            self._loaders['guide_type'] = BatchLoader(
                self.get_db,
                "SELECT id, type_code, type_name, category, direction "
                "FROM tx_guide_type WHERE id IN ({ids})"
            )
        return self._loaders['guide_type']


# ---------------------------------------------------------------------------
# BatchLoader — lightweight sync DataLoader for FK resolution
# ---------------------------------------------------------------------------

class BatchLoader:
    def __init__(self, get_db, query_template: str, id_column: str = 'id'):
        self._get_db = get_db
        self._query_template = query_template
        self._id_column = id_column
        self._cache: Dict[int, dict] = {}

    def load(self, id_val: int) -> Optional[dict]:
        if id_val in self._cache:
            return self._cache[id_val]
        self._fetch([id_val])
        return self._cache.get(id_val)

    def load_many(self, ids: List[int]) -> Dict[int, dict]:
        missing = [i for i in ids if i not in self._cache]
        if missing:
            self._fetch(missing)
        return {i: self._cache[i] for i in ids if i in self._cache}

    def _fetch(self, ids: List[int]) -> None:
        if not ids:
            return
        placeholders = ','.join(['%s'] * len(ids))
        sql = self._query_template.replace('{ids}', placeholders)
        conn = None
        try:
            conn = self._get_db()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(sql, ids)
            for row in cursor.fetchall():
                self._cache[row[self._id_column]] = row
            cursor.close()
        finally:
            if conn:
                conn.close()


# ---------------------------------------------------------------------------
# GraphQL Types
# ---------------------------------------------------------------------------

@strawberry.enum
class AddressType(enum.Enum):
    PROGRAM = "program"
    POOL = "pool"
    MINT = "mint"
    VAULT = "vault"
    WALLET = "wallet"
    ATA = "ata"
    UNKNOWN = "unknown"


@strawberry.enum
class EdgeCategory(enum.Enum):
    TRANSFER = "transfer"
    SWAP = "swap"
    FEE = "fee"
    ACCOUNT = "account"
    LENDING = "lending"
    STAKING = "staking"
    LIQUIDITY = "liquidity"
    BRIDGE = "bridge"
    PERP = "perp"
    NFT = "nft"
    OTHER = "other"


@strawberry.enum
class SwapDirection(enum.Enum):
    IN = "in"
    OUT = "out"


@strawberry.type
class Address:
    id: int
    address: str
    address_type: Optional[str] = None
    label: Optional[str] = None
    funded_by_address_id: Optional[int] = None
    first_seen_block_time: Optional[int] = None

    @strawberry.field
    def funded_by(self, info: Info) -> Optional['Address']:
        if not self.funded_by_address_id:
            return None
        ctx: GQLContext = info.context
        row = ctx.address_loader().load(self.funded_by_address_id)
        return _row_to_address(row) if row else None


@strawberry.type
class Token:
    id: int
    mint_address: str
    token_name: Optional[str] = None
    token_symbol: Optional[str] = None
    decimals: Optional[int] = None
    supply: Optional[int] = None
    token_type: Optional[str] = None


@strawberry.type
class Pool:
    pool_address: str
    pool_label: Optional[str] = None
    token1_id: Optional[int] = None
    token2_id: Optional[int] = None

    @strawberry.field
    def token1(self, info: Info) -> Optional[Token]:
        if not self.token1_id:
            return None
        ctx: GQLContext = info.context
        row = ctx.token_loader().load(self.token1_id)
        return _row_to_token(row) if row else None

    @strawberry.field
    def token2(self, info: Info) -> Optional[Token]:
        if not self.token2_id:
            return None
        ctx: GQLContext = info.context
        row = ctx.token_loader().load(self.token2_id)
        return _row_to_token(row) if row else None


@strawberry.type
class EdgeType:
    id: int
    code: str
    name: Optional[str] = None
    category: Optional[str] = None
    direction: Optional[str] = None


@strawberry.type
class GuideEdge:
    """A single tx_guide edge — one balance movement within a transaction."""
    id: int
    amount: Optional[BigDecimal] = None
    decimals: Optional[int] = None
    dex: Optional[str] = None
    pool_label: Optional[str] = None
    swap_direction: Optional[str] = None
    fee: Optional[int] = None
    priority_fee: Optional[int] = None
    tax_bps: Optional[int] = None
    tax_amount: Optional[int] = None
    ins_index: Optional[int] = None

    # Balance snapshots
    from_token_pre_balance: Optional[BigDecimal] = None
    from_token_post_balance: Optional[BigDecimal] = None
    to_token_pre_balance: Optional[BigDecimal] = None
    to_token_post_balance: Optional[BigDecimal] = None
    from_sol_pre_balance: Optional[BigDecimal] = None
    from_sol_post_balance: Optional[BigDecimal] = None
    to_sol_pre_balance: Optional[BigDecimal] = None
    to_sol_post_balance: Optional[BigDecimal] = None

    # FK IDs for lazy resolution via DataLoaders
    _from_address_id: strawberry.Private[Optional[int]] = None
    _to_address_id: strawberry.Private[Optional[int]] = None
    _token_id: strawberry.Private[Optional[int]] = None
    _pool_address_id: strawberry.Private[Optional[int]] = None
    _edge_type_id: strawberry.Private[Optional[int]] = None

    @strawberry.field
    def from_address(self, info: Info) -> Optional[Address]:
        if not self._from_address_id:
            return None
        ctx: GQLContext = info.context
        row = ctx.address_loader().load(self._from_address_id)
        return _row_to_address(row) if row else None

    @strawberry.field
    def to_address(self, info: Info) -> Optional[Address]:
        if not self._to_address_id:
            return None
        ctx: GQLContext = info.context
        row = ctx.address_loader().load(self._to_address_id)
        return _row_to_address(row) if row else None

    @strawberry.field
    def token(self, info: Info) -> Optional[Token]:
        if not self._token_id:
            return None
        ctx: GQLContext = info.context
        row = ctx.token_loader().load(self._token_id)
        return _row_to_token(row) if row else None

    @strawberry.field
    def pool(self, info: Info) -> Optional[Pool]:
        if not self._pool_address_id:
            return None
        ctx: GQLContext = info.context
        row = ctx.pool_loader().load(self._pool_address_id)
        return _row_to_pool(row) if row else None

    @strawberry.field
    def edge_type(self, info: Info) -> Optional[EdgeType]:
        if not self._edge_type_id:
            return None
        ctx: GQLContext = info.context
        row = ctx.guide_type_loader().load(self._edge_type_id)
        if not row:
            return None
        return EdgeType(
            id=row['id'], code=row['type_code'], name=row.get('type_name'),
            category=row.get('category'), direction=row.get('direction'),
        )


@strawberry.type
class Transaction:
    """A transaction with its guide edges."""
    signature: str
    block_time: Optional[int] = None
    block_time_utc: Optional[str] = None
    edges: List[GuideEdge] = strawberry.field(default_factory=list)


@strawberry.type
class WalletTransactions:
    """Top-level response for wallet transaction history."""
    address: str
    count: int = 0
    edge_count: int = 0
    mint: Optional[str] = None
    transactions: List[Transaction] = strawberry.field(default_factory=list)


# ---------------------------------------------------------------------------
# Row -> Type converters
# ---------------------------------------------------------------------------

def _row_to_address(row: dict) -> Address:
    return Address(
        id=row['id'],
        address=row['address'],
        address_type=row.get('address_type'),
        label=row.get('label'),
        funded_by_address_id=row.get('funded_by_address_id'),
        first_seen_block_time=_int_or_none(row.get('first_seen_block_time')),
    )

def _row_to_token(row: dict) -> Token:
    return Token(
        id=row['id'],
        mint_address=row['mint_address'],
        token_name=row.get('token_name'),
        token_symbol=row.get('token_symbol'),
        decimals=row.get('decimals'),
        supply=_int_or_none(row.get('supply')),
        token_type=row.get('token_type'),
    )

def _row_to_pool(row: dict) -> Pool:
    return Pool(
        pool_address=row['pool_address'],
        pool_label=row.get('pool_label'),
        token1_id=row.get('token1_id'),
        token2_id=row.get('token2_id'),
    )

def _row_to_guide_edge(r: dict) -> GuideEdge:
    """Convert a raw tx_guide row to GuideEdge with human-readable amount."""
    decimals = r.get('decimals') or 0
    raw_amount = r.get('amount')
    if raw_amount is not None and decimals:
        amount = str(Decimal(str(raw_amount)) / Decimal(10 ** decimals))
    else:
        amount = _str_or_none(raw_amount)

    return GuideEdge(
        id=r['id'],
        amount=amount,
        decimals=decimals,
        dex=r.get('dex'),
        pool_label=r.get('pool_label'),
        swap_direction=r.get('swap_direction'),
        fee=_int_or_none(r.get('fee')),
        priority_fee=_int_or_none(r.get('priority_fee')),
        tax_bps=_int_or_none(r.get('tax_bps')),
        tax_amount=_int_or_none(r.get('tax_amount')),
        ins_index=_int_or_none(r.get('ins_index')),
        from_token_pre_balance=_bal_str(r.get('from_token_pre_balance'), decimals),
        from_token_post_balance=_bal_str(r.get('from_token_post_balance'), decimals),
        to_token_pre_balance=_bal_str(r.get('to_token_pre_balance'), decimals),
        to_token_post_balance=_bal_str(r.get('to_token_post_balance'), decimals),
        from_sol_pre_balance=_bal_str(r.get('from_sol_pre_balance'), 9),
        from_sol_post_balance=_bal_str(r.get('from_sol_post_balance'), 9),
        to_sol_pre_balance=_bal_str(r.get('to_sol_pre_balance'), 9),
        to_sol_post_balance=_bal_str(r.get('to_sol_post_balance'), 9),
        _from_address_id=r.get('from_address_id'),
        _to_address_id=r.get('to_address_id'),
        _token_id=r.get('token_id'),
        _pool_address_id=r.get('pool_address_id'),
        _edge_type_id=r.get('edge_type_id'),
    )

def _int_or_none(v):
    if v is None:
        return None
    return int(v)

def _str_or_none(v):
    if v is None:
        return None
    return str(v)

def _bal_str(v, decimals: int) -> Optional[str]:
    """Convert raw balance (lamports/smallest unit) to human-readable decimal string."""
    if v is None:
        return None
    return str(Decimal(str(v)) / Decimal(10 ** decimals))

def _format_utc(block_time) -> Optional[str]:
    if block_time is None:
        return None
    try:
        return datetime.utcfromtimestamp(int(block_time)).strftime('%Y-%m-%dT%H:%M:%SZ')
    except (ValueError, OSError):
        return None


# ---------------------------------------------------------------------------
# Shared query: fetch tx_guide rows with filters, return grouped Transactions
# ---------------------------------------------------------------------------

GUIDE_SELECT = """
    SELECT g.id, g.tx_id, g.block_time,
           g.from_address_id, g.to_address_id,
           g.token_id, g.amount, g.decimals,
           g.edge_type_id, g.dex, g.pool_address_id, g.pool_label,
           g.swap_direction, g.fee, g.priority_fee,
           g.tax_bps, g.tax_amount, g.ins_index,
           g.from_token_pre_balance, g.from_token_post_balance,
           g.to_token_pre_balance, g.to_token_post_balance,
           g.from_sol_pre_balance, g.from_sol_post_balance,
           g.to_sol_pre_balance, g.to_sol_post_balance,
           t.signature
    FROM tx_guide g
    JOIN tx t ON t.id = g.tx_id
"""

def _query_guide_edges(ctx: GQLContext, where_clauses: List[str], params: list,
                       order_by: str = "g.block_time DESC, g.tx_id, g.ins_index",
                       limit: int = 200) -> List[dict]:
    """Run a filtered query against tx_guide and return raw rows."""
    sql = GUIDE_SELECT
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)
    sql += f" ORDER BY {order_by} LIMIT %s"
    params.append(limit)

    conn = None
    try:
        conn = ctx.get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        cursor.close()
        return rows
    finally:
        if conn:
            conn.close()


def _group_rows_to_transactions(rows: List[dict]) -> List[Transaction]:
    """Group flat tx_guide rows by signature into Transaction objects."""
    grouped: OrderedDict[str, list] = OrderedDict()
    for row in rows:
        sig = row['signature']
        grouped.setdefault(sig, []).append(row)

    transactions = []
    for sig, sig_rows in grouped.items():
        edges = [_row_to_guide_edge(r) for r in sig_rows]
        block_time = sig_rows[0].get('block_time')
        transactions.append(Transaction(
            signature=sig,
            block_time=_int_or_none(block_time),
            block_time_utc=_format_utc(block_time),
            edges=edges,
        ))
    return transactions


# ---------------------------------------------------------------------------
# Query resolvers
# ---------------------------------------------------------------------------

@strawberry.type
class Query:

    @strawberry.field(description="Get a wallet's transaction history from tx_guide")
    def wallet_transactions(
        self,
        info: Info,
        address: str,
        mint: Optional[str] = None,
        limit: int = 50,
        after: Optional[int] = None,
        before: Optional[int] = None,
        type_mask: Optional[int] = None,
    ) -> WalletTransactions:
        ctx: GQLContext = info.context
        limit = min(max(limit, 1), 500)

        # Resolve wallet address -> id
        conn = None
        try:
            conn = ctx.get_db()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT id FROM tx_address WHERE address = %s", (address,)
            )
            row = cursor.fetchone()
            cursor.close()
        finally:
            if conn:
                conn.close()

        if not row:
            return WalletTransactions(address=address, mint=mint)

        wallet_id = row['id']

        # Build WHERE clauses
        where = ["(g.from_address_id = %s OR g.to_address_id = %s)"]
        params: list = [wallet_id, wallet_id]

        if mint:
            # Resolve mint -> token_id
            conn2 = None
            try:
                conn2 = ctx.get_db()
                cursor2 = conn2.cursor(dictionary=True)
                cursor2.execute(
                    "SELECT t.id FROM tx_token t "
                    "JOIN tx_address a ON a.id = t.mint_address_id "
                    "WHERE a.address = %s", (mint,)
                )
                token_row = cursor2.fetchone()
                cursor2.close()
            finally:
                if conn2:
                    conn2.close()
            if not token_row:
                return WalletTransactions(address=address, mint=mint)
            where.append("g.token_id = %s")
            params.append(token_row['id'])

        if after is not None:
            where.append("g.block_time > %s")
            params.append(after)

        if before is not None:
            where.append("g.block_time < %s")
            params.append(before)

        if type_mask is not None:
            where.append(
                "g.edge_type_id IN ("
                "  SELECT id FROM tx_guide_type WHERE indicator & %s != 0"
                ")"
            )
            params.append(type_mask)

        # Query — fetch more edges than limit to account for grouping
        # (limit is on transactions, not edges)
        edge_limit = limit * 10  # generous: avg ~3-5 edges per tx
        rows = _query_guide_edges(ctx, where, params, limit=edge_limit)

        # Group by signature, then trim to transaction limit
        all_txs = _group_rows_to_transactions(rows)
        txs = all_txs[:limit]
        edge_count = sum(len(tx.edges) for tx in txs)

        return WalletTransactions(
            address=address,
            count=len(txs),
            edge_count=edge_count,
            mint=mint,
            transactions=txs,
        )

    @strawberry.field(description="Look up a single transaction's guide edges by signature")
    def transaction(self, info: Info, signature: str) -> Optional[Transaction]:
        ctx: GQLContext = info.context
        rows = _query_guide_edges(
            ctx,
            where_clauses=["t.signature = %s"],
            params=[signature],
            order_by="g.ins_index",
            limit=1000,
        )
        if not rows:
            return None
        txs = _group_rows_to_transactions(rows)
        return txs[0] if txs else None

    @strawberry.field(description="Look up an address by Solana address string")
    def address(self, info: Info, address: str) -> Optional[Address]:
        ctx: GQLContext = info.context
        conn = None
        try:
            conn = ctx.get_db()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT id, address, address_type, label, funded_by_address_id, "
                "first_seen_block_time FROM tx_address WHERE address = %s",
                (address,)
            )
            row = cursor.fetchone()
            cursor.close()
            return _row_to_address(row) if row else None
        finally:
            if conn:
                conn.close()

    @strawberry.field(description="Look up a token by mint address")
    def token(self, info: Info, mint_address: str) -> Optional[Token]:
        ctx: GQLContext = info.context
        conn = None
        try:
            conn = ctx.get_db()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT t.id, a.address AS mint_address, t.token_name, t.token_symbol, "
                "t.decimals, t.supply, t.token_type "
                "FROM tx_token t JOIN tx_address a ON a.id = t.mint_address_id "
                "WHERE a.address = %s",
                (mint_address,)
            )
            row = cursor.fetchone()
            cursor.close()
            return _row_to_token(row) if row else None
        finally:
            if conn:
                conn.close()

    @strawberry.field(description="Search tokens. No filter returns all tokens (paginated).")
    def tokens(
        self,
        info: Info,
        symbol: Optional[str] = None,
        name: Optional[str] = None,
        token_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Token]:
        ctx: GQLContext = info.context
        limit = min(max(limit, 1), 500)

        where: List[str] = []
        params: list = []

        if symbol:
            where.append("t.token_symbol LIKE %s")
            params.append(f"%{symbol}%")
        if name:
            where.append("t.token_name LIKE %s")
            params.append(f"%{name}%")
        if token_type:
            where.append("t.token_type = %s")
            params.append(token_type)

        sql = (
            "SELECT t.id, a.address AS mint_address, t.token_name, t.token_symbol, "
            "t.decimals, t.supply, t.token_type "
            "FROM tx_token t JOIN tx_address a ON a.id = t.mint_address_id"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY t.id DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        conn = None
        try:
            conn = ctx.get_db()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            cursor.close()
            return [_row_to_token(r) for r in rows]
        finally:
            if conn:
                conn.close()

    @strawberry.field(description="List all edge types")
    def edge_types(self, info: Info) -> List[EdgeType]:
        ctx: GQLContext = info.context
        conn = None
        try:
            conn = ctx.get_db()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT id, type_code, type_name, category, direction "
                "FROM tx_guide_type WHERE is_active = 1 ORDER BY id"
            )
            rows = cursor.fetchall()
            cursor.close()
            return [
                EdgeType(
                    id=r['id'], code=r['type_code'], name=r.get('type_name'),
                    category=r.get('category'), direction=r.get('direction'),
                )
                for r in rows
            ]
        finally:
            if conn:
                conn.close()


# ---------------------------------------------------------------------------
# Schema + Flask mount
# ---------------------------------------------------------------------------

schema = strawberry.Schema(
    query=Query,
    config=StrawberryConfig(auto_camel_case=True),
)


class AuthenticatedGraphQLView(GraphQLView):
    """GraphQL view with X-API-Key authentication and rate limiting."""

    def __init__(self, validate_api_key_fn, check_rate_limit_fn, get_db_fn, **kwargs):
        self._validate_api_key = validate_api_key_fn
        self._check_rate_limit = check_rate_limit_fn
        self._get_db = get_db_fn
        super().__init__(**kwargs)

    def get_context(self, request, response) -> GQLContext:
        from flask import request as flask_request

        api_key = flask_request.headers.get('X-API-Key')

        if api_key:
            key_info = self._validate_api_key(api_key)
            if not key_info:
                raise PermissionError("Invalid or inactive API key")
            rate_limit = key_info.get('rate_limit', 0)
            allowed, retry_after = self._check_rate_limit(key_info['id'], rate_limit)
            if not allowed:
                raise PermissionError(f"Rate limit exceeded. Retry after {retry_after}s")
        else:
            # Anonymous access for GraphiQL playground / dev use.
            # Set GRAPHQL_REQUIRE_AUTH=1 to enforce API keys.
            if os.environ.get('GRAPHQL_REQUIRE_AUTH', '').strip() in ('1', 'true', 'yes'):
                raise PermissionError("Missing X-API-Key header")
            key_info = {'id': 0, 'name': 'anonymous', 'rate_limit': 60}
            allowed, retry_after = self._check_rate_limit(key_info['id'], key_info['rate_limit'])
            if not allowed:
                raise PermissionError(f"Rate limit exceeded. Retry after {retry_after}s")

        return GQLContext(get_db=self._get_db, api_key_info=key_info)


def mount_graphql(app, get_db_connection, validate_api_key, check_rate_limit):
    """Mount the GraphQL endpoint onto an existing Flask app."""
    view = AuthenticatedGraphQLView.as_view(
        'graphql_view',
        schema=schema,
        validate_api_key_fn=validate_api_key,
        check_rate_limit_fn=check_rate_limit,
        get_db_fn=get_db_connection,
    )
    app.add_url_rule('/api/graphql', view_func=view, methods=['GET', 'POST'])
    print("[GraphQL] Mounted at /api/graphql")
