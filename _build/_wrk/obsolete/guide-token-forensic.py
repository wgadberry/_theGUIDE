#!/usr/bin/env python3
"""
Guide Token Forensic - Comprehensive Manipulation Detection for Solana Tokens
==============================================================================

This tool performs deep forensic analysis on token trading activity to detect
coordinated attacks, bot manipulation, pump-and-dump schemes, and wash trading.

The analysis produces both a detailed JSON document with all findings and a
professional Markdown report suitable for sharing with stakeholders.

Detection Capabilities:
-----------------------
1. BOT DETECTION: Identifies wallets exhibiting machine-like trading patterns
   - Sub-second reaction times between buy and sell
   - Suspiciously consistent timing intervals
   - Uniform trade sizes suggesting automated execution
   - Abnormally high transaction rates

2. COORDINATED DUMP DETECTION: Finds synchronized sell pressure
   - Multiple wallets selling within tight time windows
   - Volume clustering analysis
   - Identifies the "trigger" wallet and followers

3. SYBIL CLUSTER DETECTION: Traces wallet relationships
   - Common funding source analysis
   - Wallet creation timing correlation
   - Behavioral fingerprinting

4. WASH TRADING DETECTION: Exposes fake volume
   - Bidirectional trading between wallet pairs
   - Self-referential transaction loops
   - Volume inflation patterns

5. TIMELINE ANALYSIS: Reconstructs the attack narrative
   - Phase identification (accumulation, pump, dump)
   - Anomalous activity periods
   - Critical event timestamps

Usage:
------
    python token-forensic.py <token_mint>
    python token-forensic.py <token_mint> --deep --json report.json --markdown report.md

Author: T16O Forensics Team
"""

import argparse
import json
import sys
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Optional, Any
from dataclasses import dataclass, field

try:
    import mysql.connector
except ImportError:
    print("ERROR: mysql-connector-python not installed")
    print("Install with: pip install mysql-connector-python")
    sys.exit(1)

try:
    import networkx as nx
except ImportError:
    print("ERROR: networkx not installed")
    print("Install with: pip install networkx")
    sys.exit(1)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Detection thresholds - tuned for Solana memecoin manipulation patterns
BOT_SCORE_THRESHOLD = 30          # Minimum score to flag as bot
WASH_TRADE_MIN_ROUNDS = 2         # Minimum round-trips to flag wash trading
DUMP_WINDOW_DEFAULT = 60          # Default window for coordinated dump detection (seconds)
SYBIL_MIN_CLUSTER_SIZE = 3        # Minimum wallets from same funder to flag
RAPID_TRADE_THRESHOLD = 2         # Seconds - trades faster than this are suspicious
HIGH_FREQUENCY_THRESHOLD = 10     # Trades per hour threshold for bot detection
FUNDING_LOOKBACK_DAYS = 30        # Days to look back when investigating funding wallets
MAX_FUNDING_DEEP_DIVES = 10       # Maximum number of funding wallets to deep dive


# =============================================================================
# COMMENTARY ENGINE - The "Al Michaels" of Forensic Analysis
# =============================================================================

class ForensicCommentator:
    """
    Generates professional, detailed commentary throughout the analysis.
    Think of this as your play-by-play analyst walking through the evidence.
    """

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.commentary_log: List[Dict] = []
        self.section_number = 0

    def section(self, title: str) -> str:
        """Start a new section with a header"""
        self.section_number += 1
        header = f"\n{'='*70}\n[{self.section_number}] {title.upper()}\n{'='*70}"
        if self.verbose:
            print(header)
        return header

    def narrate(self, message: str, level: str = "info", data: Any = None) -> Dict:
        """
        Add commentary to the analysis log.

        Levels:
        - info: General observations
        - finding: Important discovery
        - alert: Suspicious activity detected
        - critical: Strong evidence of manipulation
        """
        timestamp = datetime.now(timezone.utc).isoformat()
        symbols = {
            "info": "ℹ️ ",
            "finding": "🔍",
            "alert": "⚠️ ",
            "critical": "🚨"
        }
        symbol = symbols.get(level, "  ")

        entry = {
            "timestamp": timestamp,
            "level": level,
            "message": message,
            "data": data
        }
        self.commentary_log.append(entry)

        if self.verbose:
            prefix = f"  {symbol} "
            print(f"{prefix}{message}")
            if data and level in ["alert", "critical"]:
                if isinstance(data, dict):
                    for k, v in data.items():
                        print(f"      → {k}: {v}")

        return entry

    def finding(self, message: str, data: Any = None) -> Dict:
        return self.narrate(message, "finding", data)

    def alert(self, message: str, data: Any = None) -> Dict:
        return self.narrate(message, "alert", data)

    def critical(self, message: str, data: Any = None) -> Dict:
        return self.narrate(message, "critical", data)

    def summarize_detection(self, detection_type: str, count: int, severity: str) -> str:
        """Generate a summary statement for a detection category"""
        if count == 0:
            summary = f"No {detection_type} detected - this vector appears clean."
        elif severity == "low":
            summary = f"Detected {count} minor {detection_type} instance(s) - warrants monitoring but not conclusive."
        elif severity == "medium":
            summary = f"Found {count} {detection_type} instance(s) - this is concerning and suggests possible coordination."
        elif severity == "high":
            summary = f"SIGNIFICANT: {count} {detection_type} instance(s) identified - strong evidence of manipulation."
        else:
            summary = f"CRITICAL: {count} {detection_type} instance(s) - this is a major red flag."

        self.narrate(summary, "finding" if count == 0 else "alert")
        return summary


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class WalletProfile:
    """
    Comprehensive behavioral profile for a wallet address.

    This profile captures trading patterns, timing signatures, and relationship
    data that helps identify whether a wallet is operated by a human or bot,
    and whether it's part of a coordinated operation.
    """
    address: str
    first_seen: int = 0                           # Unix timestamp of first activity
    last_seen: int = 0                            # Unix timestamp of last activity
    total_buys: int = 0                           # Number of buy transactions
    total_sells: int = 0                          # Number of sell transactions
    buy_volume: float = 0.0                       # Total volume purchased
    sell_volume: float = 0.0                      # Total volume sold
    unique_counterparties: Set[str] = field(default_factory=set)
    transaction_times: List[int] = field(default_factory=list)
    trade_amounts: List[float] = field(default_factory=list)
    avg_hold_time: float = 0.0                    # Average time between buy and sell
    is_bot_suspect: bool = False
    bot_score: float = 0.0
    bot_evidence: Dict = field(default_factory=dict)
    funding_sources: List[Dict] = field(default_factory=list)
    behavioral_flags: List[str] = field(default_factory=list)

    def net_position(self) -> float:
        """Calculate net token position (positive = holding, negative = sold more than bought)"""
        return self.buy_volume - self.sell_volume

    def activity_duration_hours(self) -> float:
        """How long has this wallet been active?"""
        if self.last_seen > self.first_seen:
            return (self.last_seen - self.first_seen) / 3600
        return 0

    def trades_per_hour(self) -> float:
        """Calculate trading frequency"""
        duration = self.activity_duration_hours()
        if duration > 0:
            return (self.total_buys + self.total_sells) / duration
        return 0

    def sell_buy_ratio(self) -> float:
        """Ratio of sells to buys - high ratio suggests dumping"""
        if self.total_buys == 0:
            return float('inf') if self.total_sells > 0 else 0
        return self.total_sells / self.total_buys


@dataclass
class ManipulationEvent:
    """
    A detected manipulation event with full context and evidence.

    Each event represents a specific instance of suspicious activity
    with enough detail to understand what happened and who was involved.
    """
    event_id: str                                 # Unique identifier
    event_type: str                               # Category of manipulation
    severity: str                                 # low, medium, high, critical
    timestamp: int                                # When it occurred
    timestamp_utc: str                            # Human-readable timestamp
    title: str                                    # Brief description
    narrative: str                                # Detailed explanation
    wallets_involved: List[str]                   # Addresses involved
    evidence: Dict                                # Supporting data
    score: float                                  # Confidence score
    recommendations: List[str]                    # Suggested actions


# =============================================================================
# DATABASE INTERFACE
# =============================================================================

def get_token_info(cursor, token_mint: str, commentator: ForensicCommentator) -> Dict:
    """
    Retrieve token metadata from the database.

    This gives us the token's identity - name, symbol, decimals - which is
    essential for properly interpreting amounts and contextualizing the analysis.
    """
    commentator.narrate(f"Looking up token metadata for {token_mint[:16]}...")

    cursor.execute("""
        SELECT t.id, t.token_name, t.token_symbol, t.decimals, a.address
        FROM tx_token t
        JOIN tx_address a ON a.id = t.mint_address_id
        WHERE a.address = %s
    """, (token_mint,))

    row = cursor.fetchone()
    if row:
        info = {
            'id': row[0],
            'name': row[1] or 'Unknown',
            'symbol': row[2] or 'UNK',
            'decimals': row[3] or 9,
            'mint': row[4]
        }
        commentator.finding(f"Token identified: {info['symbol']} ({info['name']})")
        return info

    commentator.narrate("Token not found in metadata cache - using defaults", "alert")
    return {'id': None, 'name': 'Unknown', 'symbol': 'UNK', 'decimals': 9, 'mint': token_mint}


def get_all_token_activity(cursor, token_mint: str, start_time: int = None,
                           end_time: int = None, commentator: ForensicCommentator = None) -> List[Dict]:
    """
    Fetch ALL transaction activity for the token from tx_guide.

    This is our raw evidence - every transfer, swap, and movement of the token.
    We cast a wide net here to ensure we don't miss any relevant activity.
    """
    if commentator:
        commentator.narrate("Querying transaction database for all token activity...")

    query = """
        SELECT
            g.id,
            g.tx_id,
            t.signature,
            g.block_time,
            gt.type_code as edge_type,
            gt.category,
            fa.address as from_address,
            ta.address as to_address,
            g.amount,
            g.decimals,
            tk.token_symbol,
            mint.address as token_mint
        FROM tx_guide g
        JOIN tx t ON t.id = g.tx_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        JOIN tx_address fa ON fa.id = g.from_address_id
        JOIN tx_address ta ON ta.id = g.to_address_id
        LEFT JOIN tx_token tk ON tk.id = g.token_id
        LEFT JOIN tx_address mint ON mint.id = tk.mint_address_id
        WHERE mint.address = %s
    """
    params = [token_mint]

    if start_time:
        query += " AND g.block_time >= %s"
        params.append(start_time)

    if end_time:
        query += " AND g.block_time <= %s"
        params.append(end_time)

    query += " ORDER BY g.block_time ASC, g.id ASC"

    cursor.execute(query, params)

    activities = []
    for row in cursor.fetchall():
        (edge_id, tx_id, signature, block_time, edge_type, category,
         from_addr, to_addr, amount, decimals, token_symbol, token_mint_addr) = row

        # Convert raw amount to human-readable using token decimals
        human_amount = float(amount) / (10 ** decimals) if amount and decimals else 0

        activities.append({
            'id': edge_id,
            'tx_id': tx_id,
            'signature': signature,
            'block_time': block_time,
            'block_time_utc': datetime.fromtimestamp(block_time, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if block_time else '',
            'edge_type': edge_type,
            'category': category,
            'from_address': from_addr,
            'to_address': to_addr,
            'amount': human_amount,
            'amount_raw': amount,
            'token_symbol': token_symbol,
            'token_mint': token_mint_addr
        })

    if commentator:
        if activities:
            first_time = activities[0]['block_time_utc']
            last_time = activities[-1]['block_time_utc']
            commentator.finding(f"Retrieved {len(activities):,} transactions spanning {first_time} to {last_time}")
        else:
            commentator.alert("No transaction data found for this token")

    return activities


def get_swap_activity(cursor, token_mint: str, start_time: int = None,
                      end_time: int = None, commentator: ForensicCommentator = None) -> List[Dict]:
    """
    Extract swap transactions specifically - these are our buys and sells.

    Swaps are the most important transactions for manipulation detection because
    they represent actual market trades where the token changes hands for value.
    """
    if commentator:
        commentator.narrate("Extracting swap transactions (buys and sells)...")

    query = """
        SELECT
            g.id,
            g.tx_id,
            t.signature,
            g.block_time,
            gt.type_code as edge_type,
            fa.address as from_address,
            fa.address_type as from_type,
            ta.address as to_address,
            ta.address_type as to_type,
            g.amount,
            g.decimals,
            tk.token_symbol,
            mint.address as token_mint
        FROM tx_guide g
        JOIN tx t ON t.id = g.tx_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        JOIN tx_address fa ON fa.id = g.from_address_id
        JOIN tx_address ta ON ta.id = g.to_address_id
        LEFT JOIN tx_token tk ON tk.id = g.token_id
        LEFT JOIN tx_address mint ON mint.id = tk.mint_address_id
        WHERE mint.address = %s
          AND gt.type_code IN ('swap_in', 'swap_out')
    """
    params = [token_mint]

    if start_time:
        query += " AND g.block_time >= %s"
        params.append(start_time)

    if end_time:
        query += " AND g.block_time <= %s"
        params.append(end_time)

    query += " ORDER BY g.block_time ASC"

    cursor.execute(query, params)

    swaps = []
    buy_count = 0
    sell_count = 0

    # Address types to exclude from trader analysis (these are not real traders)
    EXCLUDED_TYPES = ('pool', 'program', 'mint', 'vault')

    for row in cursor.fetchall():
        (edge_id, tx_id, signature, block_time, edge_type,
         from_addr, from_type, to_addr, to_type, amount, decimals, token_symbol, token_mint_addr) = row

        human_amount = float(amount) / (10 ** decimals) if amount and decimals else 0

        # Classify the trade direction:
        # swap_in = wallet is GIVING token to pool = SELL
        # swap_out = wallet is RECEIVING token from pool = BUY
        if edge_type == 'swap_in':
            trade_type = 'SELL'
            trader = from_addr
            trader_type = from_type
            sell_count += 1
        else:
            trade_type = 'BUY'
            trader = to_addr
            trader_type = to_type
            buy_count += 1

        # Skip if the "trader" is actually a pool, program, or other non-wallet entity
        if trader_type in EXCLUDED_TYPES:
            continue

        swaps.append({
            'id': edge_id,
            'tx_id': tx_id,
            'signature': signature,
            'block_time': block_time,
            'block_time_utc': datetime.fromtimestamp(block_time, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if block_time else '',
            'trade_type': trade_type,
            'trader': trader,
            'counterparty': to_addr if trade_type == 'SELL' else from_addr,
            'amount': human_amount,
            'amount_raw': amount,
            'token_symbol': token_symbol
        })

    if commentator:
        commentator.finding(f"Found {len(swaps):,} swap transactions: {buy_count:,} BUYS, {sell_count:,} SELLS")
        if sell_count > buy_count * 1.5:
            commentator.alert(f"Notable sell pressure: {sell_count} sells vs {buy_count} buys (ratio: {sell_count/max(buy_count,1):.2f})")

    return swaps


def get_wallet_funding_sources(cursor, wallet_address: str, limit: int = 10) -> List[Dict]:
    """
    Trace the funding origins of a wallet.

    This is crucial for Sybil detection - if multiple wallets received their
    initial funding from the same source, they're likely controlled by the
    same entity.

    Note: Only returns funders that are wallets - pools, vaults, programs etc.
    are filtered out as they represent normal trading, not sybil funding.
    """
    cursor.execute("""
        SELECT
            fa.address as funder,
            fa.address_type as funder_type,
            g.block_time,
            FROM_UNIXTIME(g.block_time) as block_time_utc,
            g.amount,
            g.decimals,
            gt.type_code,
            tk.token_symbol
        FROM tx_guide g
        JOIN tx_address fa ON fa.id = g.from_address_id
        JOIN tx_address ta ON ta.id = g.to_address_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        LEFT JOIN tx_token tk ON tk.id = g.token_id
        WHERE ta.address = %s
          AND gt.type_code IN ('sol_transfer', 'spl_transfer')
          AND fa.address_type IN ('wallet', 'unknown')  -- Only real wallets can be sybil funders
        ORDER BY g.block_time ASC
        LIMIT %s
    """, (wallet_address, limit))

    sources = []
    for row in cursor.fetchall():
        human_amount = float(row[4]) / (10 ** row[5]) if row[4] and row[5] else 0
        sources.append({
            'funder': row[0],
            'funder_type': row[1],
            'block_time': row[2],
            'block_time_utc': str(row[3]),
            'amount': human_amount,
            'type': row[6],
            'token': row[7] or 'SOL'
        })

    return sources


# =============================================================================
# FUNDING WALLET DEEP DIVE - The "Joe Buck" Investigation
# =============================================================================

def get_funding_wallet_full_activity(cursor, funder_address: str, days_back: int = 30) -> Dict:
    """
    Get comprehensive activity history for a funding wallet.

    This is the deep dive - we want to know EVERYTHING this wallet has been
    doing over the past month to understand if it's a serial bot deployer.
    """
    cutoff_time = int((datetime.now(timezone.utc) - timedelta(days=days_back)).timestamp())

    # Get all outbound transfers (who did this wallet fund?)
    cursor.execute("""
        SELECT
            ta.address as recipient,
            ta.address_type as recipient_type,
            g.block_time,
            g.amount,
            g.decimals,
            gt.type_code,
            tk.token_symbol,
            mint.address as token_mint
        FROM tx_guide g
        JOIN tx_address fa ON fa.id = g.from_address_id
        JOIN tx_address ta ON ta.id = g.to_address_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        LEFT JOIN tx_token tk ON tk.id = g.token_id
        LEFT JOIN tx_address mint ON mint.id = tk.mint_address_id
        WHERE fa.address = %s
          AND g.block_time >= %s
        ORDER BY g.block_time ASC
    """, (funder_address, cutoff_time))

    outbound = []
    funded_wallets = set()
    tokens_touched = set()
    total_sol_out = 0.0

    for row in cursor.fetchall():
        recipient, recipient_type, block_time, amount, decimals, type_code, token_symbol, token_mint = row
        human_amount = float(amount) / (10 ** decimals) if amount and decimals else 0

        outbound.append({
            'recipient': recipient,
            'recipient_type': recipient_type,
            'block_time': block_time,
            'block_time_utc': datetime.fromtimestamp(block_time, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'amount': human_amount,
            'type': type_code,
            'token': token_symbol or 'SOL',
            'token_mint': token_mint
        })

        # Only count wallets as funded (not pools, vaults, programs)
        if type_code in ['sol_transfer', 'spl_transfer'] and recipient_type in ('wallet', 'unknown'):
            funded_wallets.add(recipient)
            if type_code == 'sol_transfer' or token_symbol == 'SOL':
                total_sol_out += human_amount

        if token_mint:
            tokens_touched.add(token_mint)

    # Get all inbound transfers (where did this wallet get its funds?)
    # Only consider wallets as funding sources, not pools/vaults/programs
    cursor.execute("""
        SELECT
            fa.address as sender,
            fa.address_type as sender_type,
            g.block_time,
            g.amount,
            g.decimals,
            gt.type_code,
            tk.token_symbol
        FROM tx_guide g
        JOIN tx_address fa ON fa.id = g.from_address_id
        JOIN tx_address ta ON ta.id = g.to_address_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        LEFT JOIN tx_token tk ON tk.id = g.token_id
        WHERE ta.address = %s
          AND g.block_time >= %s
        ORDER BY g.block_time ASC
        LIMIT 100
    """, (funder_address, cutoff_time))

    inbound = []
    funding_sources = set()
    total_sol_in = 0.0

    for row in cursor.fetchall():
        sender, sender_type, block_time, amount, decimals, type_code, token_symbol = row
        human_amount = float(amount) / (10 ** decimals) if amount and decimals else 0

        inbound.append({
            'sender': sender,
            'sender_type': sender_type,
            'block_time': block_time,
            'block_time_utc': datetime.fromtimestamp(block_time, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'amount': human_amount,
            'type': type_code,
            'token': token_symbol or 'SOL'
        })

        # Only count wallets as funding sources (not pools, vaults, programs)
        if type_code in ['sol_transfer', 'spl_transfer'] and sender_type in ('wallet', 'unknown'):
            funding_sources.add(sender)
            if type_code == 'sol_transfer' or token_symbol == 'SOL':
                total_sol_in += human_amount

    # Get swap activity (was this wallet trading?)
    cursor.execute("""
        SELECT
            g.block_time,
            gt.type_code,
            g.amount,
            g.decimals,
            tk.token_symbol,
            mint.address as token_mint
        FROM tx_guide g
        JOIN tx_address fa ON fa.id = g.from_address_id
        JOIN tx_address ta ON ta.id = g.to_address_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        LEFT JOIN tx_token tk ON tk.id = g.token_id
        LEFT JOIN tx_address mint ON mint.id = tk.mint_address_id
        WHERE (fa.address = %s OR ta.address = %s)
          AND gt.type_code IN ('swap_in', 'swap_out')
          AND g.block_time >= %s
        ORDER BY g.block_time ASC
    """, (funder_address, funder_address, cutoff_time))

    swaps = []
    swap_tokens = set()

    for row in cursor.fetchall():
        block_time, type_code, amount, decimals, token_symbol, token_mint = row
        human_amount = float(amount) / (10 ** decimals) if amount and decimals else 0

        swaps.append({
            'block_time': block_time,
            'block_time_utc': datetime.fromtimestamp(block_time, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'type': 'SELL' if type_code == 'swap_in' else 'BUY',
            'amount': human_amount,
            'token': token_symbol,
            'token_mint': token_mint
        })

        if token_mint:
            swap_tokens.add(token_mint)
            tokens_touched.add(token_mint)

    return {
        'address': funder_address,
        'analysis_period_days': days_back,
        'outbound_transactions': outbound,
        'inbound_transactions': inbound,
        'swap_activity': swaps,
        'summary': {
            'total_wallets_funded': len(funded_wallets),
            'funded_wallet_addresses': list(funded_wallets),
            'total_sol_out': round(total_sol_out, 4),
            'total_sol_in': round(total_sol_in, 4),
            'net_sol_flow': round(total_sol_in - total_sol_out, 4),
            'unique_tokens_touched': len(tokens_touched),
            'tokens_touched': list(tokens_touched)[:20],  # Cap for readability
            'total_outbound_txs': len(outbound),
            'total_inbound_txs': len(inbound),
            'total_swaps': len(swaps),
            'swap_tokens': list(swap_tokens),
            'funding_sources': list(funding_sources)[:10]
        }
    }


def deep_dive_bot_funders(bot_suspects: List[Dict], cursor,
                          commentator: ForensicCommentator) -> List[Dict]:
    """
    THE JOE BUCK SPECIAL: Deep investigation of funding wallets behind detected bots.

    "And HERE'S the pitch... This funding wallet has been BUSY folks!
    We're looking at what could be a serial bot operator. Let's break down
    exactly what this puppet master has been doing over the past month..."

    For each detected bot, we trace back to the funding source and compile
    a comprehensive dossier on that funding wallet's activities.
    """
    commentator.section("FUNDING WALLET DEEP DIVE")
    commentator.narrate("🎙️ Time for the Joe Buck special - let's investigate who's pulling the strings...")

    if not bot_suspects:
        commentator.finding("No bot suspects to trace - skipping funding investigation")
        return []

    # Collect unique funding wallets from all bot suspects
    funder_to_bots: Dict[str, List[str]] = defaultdict(list)

    commentator.narrate(f"Tracing funding sources for {len(bot_suspects)} bot suspect(s)...")

    for bot in bot_suspects[:20]:  # Limit to top 20 bots
        bot_wallet = bot['wallet']
        sources = get_wallet_funding_sources(cursor, bot_wallet, limit=5)

        for source in sources:
            funder = source['funder']
            funder_to_bots[funder].append(bot_wallet)

    commentator.finding(f"Identified {len(funder_to_bots)} unique funding wallet(s) behind the bots")

    # Deep dive into each funding wallet
    funding_investigations = []

    # Sort funders by number of bots they funded (most suspicious first)
    sorted_funders = sorted(funder_to_bots.items(), key=lambda x: len(x[1]), reverse=True)

    for i, (funder, bots_funded) in enumerate(sorted_funders[:MAX_FUNDING_DEEP_DIVES]):
        commentator.narrate("")
        commentator.narrate(f"{'='*60}")
        commentator.narrate(f"🎙️ DEEP DIVE #{i+1}: Funding Wallet {funder}")
        commentator.narrate(f"{'='*60}")

        # Get comprehensive activity
        activity = get_funding_wallet_full_activity(cursor, funder, FUNDING_LOOKBACK_DAYS)
        summary = activity['summary']

        # Joe Buck style commentary
        commentator.narrate("")
        commentator.critical(f"AND HERE WE GO! Let's look at what this wallet has been up to...")

        # Opening analysis
        if len(bots_funded) >= 3:
            commentator.critical(
                f"MAJOR RED FLAG: This wallet funded {len(bots_funded)} of our detected bots!",
                {'bots_funded': bots_funded[:5]}
            )
        else:
            commentator.alert(
                f"This wallet funded {len(bots_funded)} bot(s) in our investigation",
                {'bots_funded': bots_funded}
            )

        # Funding operation scale
        if summary['total_wallets_funded'] >= 10:
            commentator.critical(
                f"WHOA! This is a PROLIFIC funder - {summary['total_wallets_funded']} wallets funded in {FUNDING_LOOKBACK_DAYS} days!",
                {'total_wallets': summary['total_wallets_funded'], 'sol_distributed': summary['total_sol_out']}
            )
            commentator.narrate(
                f"   That's an average of {summary['total_wallets_funded']/FUNDING_LOOKBACK_DAYS:.1f} new wallets PER DAY. "
                f"This looks like a wallet factory operation."
            )
        elif summary['total_wallets_funded'] >= 5:
            commentator.alert(
                f"Notable activity: {summary['total_wallets_funded']} wallets funded, "
                f"{summary['total_sol_out']:.2f} SOL distributed"
            )
        else:
            commentator.finding(
                f"Funded {summary['total_wallets_funded']} wallet(s) with {summary['total_sol_out']:.2f} SOL total"
            )

        # Token involvement
        if summary['unique_tokens_touched'] >= 5:
            commentator.alert(
                f"DIVERSIFIED OPERATOR: Touched {summary['unique_tokens_touched']} different tokens!",
                {'sample_tokens': summary['tokens_touched'][:5]}
            )
            commentator.narrate(
                f"   This wallet isn't just targeting one token - they're running operations across multiple tokens."
            )
        elif summary['unique_tokens_touched'] >= 2:
            commentator.finding(f"Active in {summary['unique_tokens_touched']} token(s)")

        # Swap activity analysis
        if summary['total_swaps'] >= 20:
            commentator.alert(
                f"ACTIVE TRADER: {summary['total_swaps']} swaps in the past {FUNDING_LOOKBACK_DAYS} days",
                {'swap_tokens': summary['swap_tokens'][:5]}
            )

            # Analyze swap patterns
            sells = [s for s in activity['swap_activity'] if s['type'] == 'SELL']
            buys = [s for s in activity['swap_activity'] if s['type'] == 'BUY']

            if len(sells) > len(buys) * 2:
                commentator.critical(
                    f"DUMPER ALERT: {len(sells)} sells vs {len(buys)} buys - this wallet is primarily SELLING"
                )
            elif len(buys) > len(sells) * 2:
                commentator.finding(f"Accumulator pattern: {len(buys)} buys vs {len(sells)} sells")
        elif summary['total_swaps'] > 0:
            commentator.finding(f"Light trading activity: {summary['total_swaps']} swap(s)")
        else:
            commentator.finding("No direct swap activity - appears to be purely a funding/distribution wallet")

        # Funding sources (who funds the funder?)
        if summary['funding_sources']:
            commentator.narrate("")
            commentator.finding(f"UPSTREAM SOURCES: This wallet received funds from {len(summary['funding_sources'])} address(es):")
            for upstream in summary['funding_sources'][:5]:
                commentator.narrate(f"   └─ {upstream}")

            if summary['net_sol_flow'] < -10:
                commentator.alert(
                    f"NET OUTFLOW: {abs(summary['net_sol_flow']):.2f} SOL more OUT than IN - "
                    f"this wallet is distributing funds, not accumulating"
                )

        # Timeline analysis
        if activity['outbound_transactions']:
            first_tx = activity['outbound_transactions'][0]
            last_tx = activity['outbound_transactions'][-1]
            commentator.narrate("")
            commentator.finding(f"Activity window: {first_tx['block_time_utc']} to {last_tx['block_time_utc']}")

            # Check for burst activity
            if len(activity['outbound_transactions']) >= 10:
                # Group by day
                daily_counts = defaultdict(int)
                for tx in activity['outbound_transactions']:
                    day = tx['block_time_utc'][:10]
                    daily_counts[day] += 1

                peak_day = max(daily_counts.items(), key=lambda x: x[1])
                if peak_day[1] >= 10:
                    commentator.alert(
                        f"BURST ACTIVITY: {peak_day[1]} transactions on {peak_day[0]} alone!",
                        {'peak_day': peak_day[0], 'tx_count': peak_day[1]}
                    )

        # Build investigation record
        investigation = {
            'funder_address': funder,
            'bots_funded_in_this_investigation': bots_funded,
            'bots_funded_count': len(bots_funded),
            'investigation_period_days': FUNDING_LOOKBACK_DAYS,
            'activity_summary': summary,
            'risk_assessment': {
                'is_serial_funder': summary['total_wallets_funded'] >= 10,
                'is_multi_token_operator': summary['unique_tokens_touched'] >= 3,
                'is_active_trader': summary['total_swaps'] >= 10,
                'is_net_distributor': summary['net_sol_flow'] < -5,
                'funded_multiple_bots': len(bots_funded) >= 2
            },
            'detailed_outbound': activity['outbound_transactions'][:50],  # Cap for report size
            'detailed_swaps': activity['swap_activity'][:50],
            'upstream_funders': summary['funding_sources']
        }

        # Calculate threat score for this funder
        threat_score = 0
        if summary['total_wallets_funded'] >= 10:
            threat_score += 30
        elif summary['total_wallets_funded'] >= 5:
            threat_score += 15

        if len(bots_funded) >= 3:
            threat_score += 30
        elif len(bots_funded) >= 2:
            threat_score += 15

        if summary['unique_tokens_touched'] >= 5:
            threat_score += 20
        elif summary['unique_tokens_touched'] >= 2:
            threat_score += 10

        if summary['total_swaps'] >= 20:
            threat_score += 10

        if summary['net_sol_flow'] < -10:
            threat_score += 10

        investigation['threat_score'] = threat_score

        if threat_score >= 50:
            investigation['threat_level'] = 'CRITICAL'
            commentator.critical(f"THREAT ASSESSMENT: CRITICAL (score: {threat_score})")
        elif threat_score >= 30:
            investigation['threat_level'] = 'HIGH'
            commentator.alert(f"THREAT ASSESSMENT: HIGH (score: {threat_score})")
        elif threat_score >= 15:
            investigation['threat_level'] = 'MEDIUM'
            commentator.finding(f"THREAT ASSESSMENT: MEDIUM (score: {threat_score})")
        else:
            investigation['threat_level'] = 'LOW'
            commentator.finding(f"THREAT ASSESSMENT: LOW (score: {threat_score})")

        # Generate narrative summary
        investigation['narrative'] = generate_funder_narrative(investigation)

        funding_investigations.append(investigation)

    # Final summary
    commentator.narrate("")
    commentator.section("FUNDING INVESTIGATION SUMMARY")

    critical_funders = [f for f in funding_investigations if f['threat_level'] == 'CRITICAL']
    high_funders = [f for f in funding_investigations if f['threat_level'] == 'HIGH']

    if critical_funders:
        commentator.critical(
            f"🚨 {len(critical_funders)} CRITICAL threat funding wallet(s) identified!",
            {'addresses': [f['funder_address'] for f in critical_funders]}
        )

    if high_funders:
        commentator.alert(f"⚠️ {len(high_funders)} HIGH threat funding wallet(s) identified")

    total_wallets_funded = sum(f['activity_summary']['total_wallets_funded'] for f in funding_investigations)
    commentator.finding(f"Combined reach: {total_wallets_funded} wallets funded by investigated addresses")

    return funding_investigations


def generate_funder_narrative(investigation: Dict) -> str:
    """Generate a prose narrative summarizing the funding wallet investigation."""

    funder = investigation['funder_address']
    summary = investigation['activity_summary']
    risk = investigation['risk_assessment']

    narrative_parts = []

    # Opening
    narrative_parts.append(
        f"Funding wallet `{funder}` was investigated as part of this forensic analysis "
        f"after being identified as the funding source for {investigation['bots_funded_count']} "
        f"bot wallet(s) involved in the manipulation."
    )

    # Scale of operation
    if risk['is_serial_funder']:
        narrative_parts.append(
            f"This wallet displays characteristics of a SERIAL FUNDING OPERATION, having funded "
            f"{summary['total_wallets_funded']} different wallets over the past {investigation['investigation_period_days']} days "
            f"with a total outflow of {summary['total_sol_out']:.2f} SOL."
        )
    else:
        narrative_parts.append(
            f"The wallet funded {summary['total_wallets_funded']} wallet(s) with {summary['total_sol_out']:.2f} SOL "
            f"during the investigation period."
        )

    # Multi-token activity
    if risk['is_multi_token_operator']:
        narrative_parts.append(
            f"Notably, this funding wallet has touched {summary['unique_tokens_touched']} different tokens, "
            f"suggesting it may be running manipulation operations across multiple token markets simultaneously."
        )

    # Trading activity
    if risk['is_active_trader'] and summary['total_swaps'] > 0:
        narrative_parts.append(
            f"The wallet also engaged in direct trading activity with {summary['total_swaps']} swap transactions, "
            f"indicating the operator may be taking profits directly through this wallet as well."
        )

    # Upstream funding
    if summary['funding_sources']:
        narrative_parts.append(
            f"Funds flowed into this wallet from {len(summary['funding_sources'])} upstream source(s), "
            f"which may warrant further investigation to identify the ultimate origin of the manipulation capital."
        )

    # Conclusion
    threat_level = investigation['threat_level']
    if threat_level == 'CRITICAL':
        narrative_parts.append(
            f"Based on the evidence, this funding wallet represents a CRITICAL threat and should be considered "
            f"a primary target for any enforcement or community warning actions."
        )
    elif threat_level == 'HIGH':
        narrative_parts.append(
            f"The activity pattern suggests this is a HIGH risk funding source that warrants close monitoring "
            f"and potential inclusion in watchlists."
        )

    return " ".join(narrative_parts)


# =============================================================================
# CROSS-REFERENCE ENGINE - "theGuide" Intelligence
# =============================================================================

def get_wallet_other_token_activity(cursor, wallet_address: str, exclude_token_mint: str = None) -> Dict:
    """
    Find all OTHER token activity for a wallet in theGuide.

    This shows what else this wallet has been doing - are they a serial
    manipulator hitting multiple tokens?
    """
    query = """
        SELECT
            mint.address as token_mint,
            tk.token_symbol,
            tk.token_name,
            gt.type_code,
            COUNT(*) as tx_count,
            SUM(g.amount / POW(10, g.decimals)) as total_volume,
            MIN(g.block_time) as first_seen,
            MAX(g.block_time) as last_seen
        FROM tx_guide g
        JOIN tx_address a ON (a.id = g.from_address_id OR a.id = g.to_address_id)
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        LEFT JOIN tx_token tk ON tk.id = g.token_id
        LEFT JOIN tx_address mint ON mint.id = tk.mint_address_id
        WHERE a.address = %s
          AND gt.type_code IN ('swap_in', 'swap_out', 'spl_transfer')
    """
    params = [wallet_address]

    if exclude_token_mint:
        query += " AND (mint.address != %s OR mint.address IS NULL)"
        params.append(exclude_token_mint)

    query += """
        GROUP BY mint.address, tk.token_symbol, tk.token_name, gt.type_code
        ORDER BY tx_count DESC
        LIMIT 50
    """

    cursor.execute(query, params)

    tokens = defaultdict(lambda: {
        'token_mint': None,
        'token_symbol': None,
        'token_name': None,
        'swaps_in': 0,
        'swaps_out': 0,
        'transfers': 0,
        'total_volume': 0.0,
        'first_seen': None,
        'last_seen': None
    })

    for row in cursor.fetchall():
        token_mint, symbol, name, type_code, tx_count, volume, first_seen, last_seen = row
        key = token_mint or 'SOL'

        tokens[key]['token_mint'] = token_mint
        tokens[key]['token_symbol'] = symbol or ('SOL' if not token_mint else 'UNK')
        tokens[key]['token_name'] = name

        if type_code == 'swap_in':
            tokens[key]['swaps_in'] += tx_count
        elif type_code == 'swap_out':
            tokens[key]['swaps_out'] += tx_count
        else:
            tokens[key]['transfers'] += tx_count

        if volume:
            tokens[key]['total_volume'] += float(volume)

        if first_seen:
            if not tokens[key]['first_seen'] or first_seen < tokens[key]['first_seen']:
                tokens[key]['first_seen'] = first_seen
        if last_seen:
            if not tokens[key]['last_seen'] or last_seen > tokens[key]['last_seen']:
                tokens[key]['last_seen'] = last_seen

    return dict(tokens)


def cross_reference_bad_actors(cursor, bad_actor_wallets: List[str],
                                current_token_mint: str,
                                commentator: ForensicCommentator) -> Dict:
    """
    Cross-reference identified bad actors against ALL data in theGuide.

    This is where we show off our intelligence - finding connections
    between manipulators and other tokens they've attacked.
    """
    commentator.section("CROSS-REFERENCE ANALYSIS - theGuide Intelligence")
    commentator.narrate("🔗 Cross-referencing bad actors against theGuide database...")
    commentator.narrate(f"Analyzing {len(bad_actor_wallets)} wallet(s) for activity on other tokens...")

    cross_ref_results = {
        'wallets_analyzed': len(bad_actor_wallets),
        'wallets_with_other_activity': 0,
        'unique_other_tokens': set(),
        'serial_offenders': [],
        'wallet_details': []
    }

    for wallet in bad_actor_wallets:
        other_activity = get_wallet_other_token_activity(cursor, wallet, current_token_mint)

        if other_activity:
            cross_ref_results['wallets_with_other_activity'] += 1

            # Track unique tokens
            for token_key, data in other_activity.items():
                if data['token_mint']:
                    cross_ref_results['unique_other_tokens'].add(data['token_mint'])

            # Calculate activity score
            total_other_txs = sum(
                d['swaps_in'] + d['swaps_out'] + d['transfers']
                for d in other_activity.values()
            )
            unique_tokens = len([k for k in other_activity.keys() if k != 'SOL'])

            wallet_summary = {
                'wallet': wallet,
                'other_tokens_count': unique_tokens,
                'total_other_transactions': total_other_txs,
                'tokens': []
            }

            # Sort tokens by transaction count
            sorted_tokens = sorted(
                other_activity.items(),
                key=lambda x: x[1]['swaps_in'] + x[1]['swaps_out'] + x[1]['transfers'],
                reverse=True
            )

            for token_key, data in sorted_tokens:
                if data['token_mint']:  # Skip SOL-only entries
                    wallet_summary['tokens'].append({
                        'token_mint': data['token_mint'],
                        'token_symbol': data['token_symbol'],
                        'token_name': data['token_name'],
                        'sells': data['swaps_in'],
                        'buys': data['swaps_out'],
                        'transfers': data['transfers'],
                        'total_volume': round(data['total_volume'], 4),
                        'first_seen': datetime.fromtimestamp(data['first_seen'], timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if data['first_seen'] else None,
                        'last_seen': datetime.fromtimestamp(data['last_seen'], timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if data['last_seen'] else None
                    })

            cross_ref_results['wallet_details'].append(wallet_summary)

            # Flag serial offenders (active in 3+ other tokens)
            if unique_tokens >= 3:
                cross_ref_results['serial_offenders'].append({
                    'wallet': wallet,
                    'token_count': unique_tokens,
                    'total_txs': total_other_txs
                })
                commentator.alert(
                    f"SERIAL OFFENDER: {wallet} active in {unique_tokens} OTHER tokens!",
                    {'tokens': [t['token_symbol'] for t in wallet_summary['tokens'][:5]]}
                )

    cross_ref_results['unique_other_tokens'] = list(cross_ref_results['unique_other_tokens'])

    # Summary commentary
    commentator.narrate("")
    if cross_ref_results['wallets_with_other_activity'] > 0:
        commentator.finding(
            f"{cross_ref_results['wallets_with_other_activity']} of {len(bad_actor_wallets)} "
            f"bad actors found in theGuide with activity on OTHER tokens"
        )
        commentator.finding(
            f"Combined footprint spans {len(cross_ref_results['unique_other_tokens'])} unique tokens"
        )

        if cross_ref_results['serial_offenders']:
            commentator.critical(
                f"🚨 {len(cross_ref_results['serial_offenders'])} SERIAL OFFENDER(s) identified "
                f"(active in 3+ tokens)"
            )
    else:
        commentator.finding("No additional token activity found for these wallets in theGuide")

    return cross_ref_results


# =============================================================================
# ANALYSIS ENGINES
# =============================================================================

def build_wallet_profiles(activities: List[Dict], commentator: ForensicCommentator) -> Dict[str, WalletProfile]:
    """
    Construct behavioral profiles for every wallet that touched this token.

    These profiles are the foundation of our analysis - by understanding how
    each wallet behaves, we can identify anomalies that suggest bot activity
    or coordinated manipulation.
    """
    commentator.section("WALLET PROFILING")
    commentator.narrate("Building behavioral profiles for all participating wallets...")

    profiles: Dict[str, WalletProfile] = {}

    for act in activities:
        from_addr = act['from_address']
        to_addr = act['to_address']
        block_time = act['block_time']
        amount = act['amount']
        edge_type = act['edge_type']

        # Initialize profiles for new wallets
        for addr in [from_addr, to_addr]:
            if addr and addr not in profiles:
                profiles[addr] = WalletProfile(address=addr)

        # Update sender profile
        if from_addr:
            p = profiles[from_addr]
            if p.first_seen == 0 or block_time < p.first_seen:
                p.first_seen = block_time
            if block_time > p.last_seen:
                p.last_seen = block_time
            p.transaction_times.append(block_time)

            # Classify outbound activity
            if edge_type in ['swap_in']:
                p.total_sells += 1
                p.sell_volume += amount
                p.trade_amounts.append(amount)
            elif edge_type in ['spl_transfer']:
                p.total_sells += 1
                p.sell_volume += amount

            if to_addr:
                p.unique_counterparties.add(to_addr)

        # Update receiver profile
        if to_addr:
            p = profiles[to_addr]
            if p.first_seen == 0 or block_time < p.first_seen:
                p.first_seen = block_time
            if block_time > p.last_seen:
                p.last_seen = block_time
            p.transaction_times.append(block_time)

            # Classify inbound activity
            if edge_type in ['swap_out']:
                p.total_buys += 1
                p.buy_volume += amount
                p.trade_amounts.append(amount)

            if from_addr:
                p.unique_counterparties.add(from_addr)

    # Generate profile statistics
    total_wallets = len(profiles)
    active_traders = sum(1 for p in profiles.values() if p.total_buys > 0 or p.total_sells > 0)
    heavy_sellers = sum(1 for p in profiles.values() if p.sell_volume > p.buy_volume * 2)

    commentator.finding(f"Profiled {total_wallets:,} unique wallet addresses")
    commentator.finding(f"Active traders (at least one buy or sell): {active_traders:,}")

    if heavy_sellers > 0:
        commentator.alert(f"Heavy sellers (sold 2x+ more than bought): {heavy_sellers:,}")

    return profiles


def detect_bot_signatures(profiles: Dict[str, WalletProfile], swaps: List[Dict],
                          commentator: ForensicCommentator) -> List[Dict]:
    """
    Identify wallets exhibiting bot-like trading behavior.

    Bots leave distinctive fingerprints in their trading patterns:
    - Unnaturally consistent timing between trades
    - Sub-second reaction times to market events
    - Identical or highly similar trade sizes
    - Extremely high trading frequency

    Each wallet is scored on multiple dimensions, and those exceeding
    the threshold are flagged for investigation.
    """
    commentator.section("BOT DETECTION ANALYSIS")
    commentator.narrate("Analyzing trading patterns for machine-like behavior signatures...")

    bot_suspects = []

    # Group all swaps by trader wallet
    wallet_txs = defaultdict(list)
    for swap in swaps:
        if swap['trader']:
            wallet_txs[swap['trader']].append(swap)

    wallets_analyzed = 0
    for wallet, txs in wallet_txs.items():
        wallets_analyzed += 1

        # Need minimum activity to establish patterns
        if len(txs) < 3:
            continue

        profile = profiles.get(wallet)
        if not profile:
            continue

        bot_score = 0.0
        evidence = {
            'analysis_notes': [],
            'timing_analysis': {},
            'amount_analysis': {},
            'frequency_analysis': {}
        }

        sorted_txs = sorted(txs, key=lambda x: x['block_time'])

        # =====================================================================
        # TEST 1: Timing Interval Consistency
        # Bots often operate on fixed intervals (e.g., every 30 seconds)
        # =====================================================================
        if len(sorted_txs) >= 3:
            intervals = []
            for i in range(1, len(sorted_txs)):
                interval = sorted_txs[i]['block_time'] - sorted_txs[i-1]['block_time']
                intervals.append(interval)

            if intervals:
                avg_interval = sum(intervals) / len(intervals)
                variance = sum((i - avg_interval) ** 2 for i in intervals) / len(intervals)
                std_dev = variance ** 0.5

                evidence['timing_analysis'] = {
                    'avg_interval_seconds': round(avg_interval, 2),
                    'std_deviation': round(std_dev, 2),
                    'variance': round(variance, 2),
                    'sample_size': len(intervals)
                }

                # Very low variance in timing = highly suspicious
                if variance < 100 and avg_interval < 300:
                    score_add = 30
                    bot_score += score_add
                    evidence['analysis_notes'].append(
                        f"SUSPICIOUS: Timing variance of {variance:.1f} is unusually consistent "
                        f"(avg interval: {avg_interval:.1f}s) - suggests automated execution"
                    )
                elif variance < 500 and avg_interval < 600:
                    score_add = 15
                    bot_score += score_add
                    evidence['analysis_notes'].append(
                        f"NOTABLE: Somewhat consistent timing patterns detected "
                        f"(variance: {variance:.1f}, avg: {avg_interval:.1f}s)"
                    )

        # =====================================================================
        # TEST 2: Rapid Reaction Analysis
        # Look for suspiciously fast buy-then-sell or sell-then-buy sequences
        # =====================================================================
        rapid_reactions = []
        for i in range(1, len(sorted_txs)):
            prev_tx = sorted_txs[i-1]
            curr_tx = sorted_txs[i]
            reaction_time = curr_tx['block_time'] - prev_tx['block_time']

            # Different trade types in rapid succession
            if prev_tx['trade_type'] != curr_tx['trade_type'] and reaction_time <= RAPID_TRADE_THRESHOLD:
                rapid_reactions.append({
                    'from': prev_tx['trade_type'],
                    'to': curr_tx['trade_type'],
                    'reaction_seconds': reaction_time,
                    'timestamp': curr_tx['block_time_utc']
                })

        if rapid_reactions:
            score_add = min(len(rapid_reactions) * 15, 40)
            bot_score += score_add
            evidence['rapid_reactions'] = rapid_reactions
            evidence['analysis_notes'].append(
                f"CRITICAL: {len(rapid_reactions)} sub-{RAPID_TRADE_THRESHOLD}s reaction(s) detected - "
                f"this speed is virtually impossible for human traders"
            )

        # =====================================================================
        # TEST 3: Trade Size Consistency
        # Bots often use identical or formula-based trade sizes
        # =====================================================================
        amounts = [tx['amount'] for tx in txs if tx['amount'] > 0]
        if len(amounts) >= 3:
            avg_amount = sum(amounts) / len(amounts)
            if avg_amount > 0:
                amount_variance = sum((a - avg_amount) ** 2 for a in amounts) / len(amounts)
                normalized_variance = amount_variance / (avg_amount ** 2)

                evidence['amount_analysis'] = {
                    'avg_trade_size': round(avg_amount, 4),
                    'normalized_variance': round(normalized_variance, 4),
                    'sample_size': len(amounts),
                    'min_amount': round(min(amounts), 4),
                    'max_amount': round(max(amounts), 4)
                }

                if normalized_variance < 0.05:
                    score_add = 25
                    bot_score += score_add
                    evidence['analysis_notes'].append(
                        f"SUSPICIOUS: Trade sizes are nearly identical (normalized variance: {normalized_variance:.4f}) - "
                        f"suggests pre-programmed amounts"
                    )
                elif normalized_variance < 0.15:
                    score_add = 10
                    bot_score += score_add
                    evidence['analysis_notes'].append(
                        f"NOTABLE: Trade sizes show unusual consistency (variance: {normalized_variance:.4f})"
                    )

        # =====================================================================
        # TEST 4: Trading Frequency Analysis
        # Sustained high-frequency trading suggests automation
        # =====================================================================
        tx_rate = profile.trades_per_hour()
        evidence['frequency_analysis'] = {
            'trades_per_hour': round(tx_rate, 2),
            'total_trades': len(txs),
            'activity_hours': round(profile.activity_duration_hours(), 2)
        }

        if tx_rate > HIGH_FREQUENCY_THRESHOLD:
            score_add = 25
            bot_score += score_add
            evidence['analysis_notes'].append(
                f"SUSPICIOUS: Trading at {tx_rate:.1f} trades/hour - this sustained rate suggests automation"
            )
        elif tx_rate > HIGH_FREQUENCY_THRESHOLD / 2:
            score_add = 10
            bot_score += score_add
            evidence['analysis_notes'].append(
                f"NOTABLE: Elevated trading frequency ({tx_rate:.1f} trades/hour)"
            )

        # =====================================================================
        # TEST 5: Sell Bias Analysis
        # Dump bots typically show heavy sell-side activity
        # =====================================================================
        if profile.total_sells > profile.total_buys * 2 and profile.total_sells >= 5:
            score_add = 15
            bot_score += score_add
            sell_ratio = profile.total_sells / max(profile.total_buys, 1)
            evidence['analysis_notes'].append(
                f"NOTABLE: Heavy sell bias ({profile.total_sells} sells vs {profile.total_buys} buys, "
                f"ratio: {sell_ratio:.1f}x) - consistent with dump bot behavior"
            )

        # =====================================================================
        # Compile Results
        # =====================================================================
        if bot_score >= BOT_SCORE_THRESHOLD:
            profile.is_bot_suspect = True
            profile.bot_score = bot_score
            profile.bot_evidence = evidence

            # Determine severity
            if bot_score >= 70:
                severity = 'critical'
            elif bot_score >= 50:
                severity = 'high'
            elif bot_score >= 40:
                severity = 'medium'
            else:
                severity = 'low'

            bot_suspects.append({
                'wallet': wallet,
                'bot_score': bot_score,
                'severity': severity,
                'total_trades': len(txs),
                'buys': profile.total_buys,
                'sells': profile.total_sells,
                'buy_volume': round(profile.buy_volume, 4),
                'sell_volume': round(profile.sell_volume, 4),
                'net_position': round(profile.net_position(), 4),
                'activity_hours': round(profile.activity_duration_hours(), 2),
                'first_seen': datetime.fromtimestamp(profile.first_seen, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if profile.first_seen else '',
                'last_seen': datetime.fromtimestamp(profile.last_seen, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if profile.last_seen else '',
                'evidence': evidence
            })

    # Sort by bot score descending
    bot_suspects.sort(key=lambda x: x['bot_score'], reverse=True)

    # Commentary on findings
    commentator.finding(f"Analyzed {wallets_analyzed:,} wallets with trading activity")

    if bot_suspects:
        critical_bots = [b for b in bot_suspects if b['severity'] == 'critical']
        high_bots = [b for b in bot_suspects if b['severity'] == 'high']

        commentator.alert(f"Identified {len(bot_suspects)} wallets with bot-like signatures")

        if critical_bots:
            commentator.critical(
                f"{len(critical_bots)} wallet(s) show CRITICAL bot indicators",
                {'top_suspect': critical_bots[0]['wallet'], 'score': critical_bots[0]['bot_score']}
            )

        if high_bots:
            commentator.alert(f"{len(high_bots)} additional wallet(s) with HIGH bot probability")

        # Highlight the worst offender
        top_bot = bot_suspects[0]
        commentator.narrate(
            f"Primary suspect: {top_bot['wallet']} with score {top_bot['bot_score']:.1f}",
            "critical" if top_bot['severity'] == 'critical' else "alert"
        )
    else:
        commentator.finding("No definitive bot signatures detected in trading patterns")

    return bot_suspects


def detect_wash_trading(swaps: List[Dict], profiles: Dict[str, WalletProfile],
                        commentator: ForensicCommentator) -> List[Dict]:
    """
    Identify wash trading - fake volume created by trading between related wallets.

    Wash trading is a classic manipulation technique where the same entity
    (or colluding entities) trade back and forth to create the illusion
    of market activity and liquidity.

    We detect this by looking for:
    - Bidirectional trading between wallet pairs
    - Circular trading patterns (A→B→C→A)
    - Volume that doesn't result in position changes
    """
    commentator.section("WASH TRADING DETECTION")
    commentator.narrate("Scanning for circular and bidirectional trading patterns...")

    wash_pairs = []

    # Build a matrix of trades between wallet pairs
    pair_trades = defaultdict(lambda: {
        'a_to_b_sells': 0,
        'b_to_a_sells': 0,
        'a_to_b_volume': 0.0,
        'b_to_a_volume': 0.0,
        'transactions': []
    })

    for swap in swaps:
        trader = swap['trader']
        counterparty = swap['counterparty']

        if not trader or not counterparty or trader == counterparty:
            continue

        # Normalize pair key (alphabetically sorted)
        pair_key = tuple(sorted([trader, counterparty]))
        is_a_to_b = trader == pair_key[0]

        if swap['trade_type'] == 'SELL':
            if is_a_to_b:
                pair_trades[pair_key]['a_to_b_sells'] += 1
                pair_trades[pair_key]['a_to_b_volume'] += swap['amount']
            else:
                pair_trades[pair_key]['b_to_a_sells'] += 1
                pair_trades[pair_key]['b_to_a_volume'] += swap['amount']

            pair_trades[pair_key]['transactions'].append({
                'direction': 'A→B' if is_a_to_b else 'B→A',
                'amount': swap['amount'],
                'time': swap['block_time_utc'],
                'signature': swap['signature']
            })

    # Identify suspicious pairs
    for (wallet_a, wallet_b), data in pair_trades.items():
        # Both directions must have meaningful activity
        if data['a_to_b_sells'] >= WASH_TRADE_MIN_ROUNDS and data['b_to_a_sells'] >= WASH_TRADE_MIN_ROUNDS:

            total_trades = data['a_to_b_sells'] + data['b_to_a_sells']
            total_volume = data['a_to_b_volume'] + data['b_to_a_volume']

            # Calculate wash score based on symmetry and volume
            symmetry = min(data['a_to_b_sells'], data['b_to_a_sells']) / max(data['a_to_b_sells'], data['b_to_a_sells'])
            wash_score = (
                min(data['a_to_b_sells'], data['b_to_a_sells']) * 15 +  # Reward bidirectionality
                symmetry * 20 +  # Reward symmetry
                min(total_volume / 1000, 20)  # Volume factor
            )

            # Determine severity
            if wash_score >= 50:
                severity = 'high'
            elif wash_score >= 30:
                severity = 'medium'
            else:
                severity = 'low'

            wash_pairs.append({
                'wallet_a': wallet_a,
                'wallet_b': wallet_b,
                'a_to_b_trades': data['a_to_b_sells'],
                'b_to_a_trades': data['b_to_a_sells'],
                'a_to_b_volume': round(data['a_to_b_volume'], 4),
                'b_to_a_volume': round(data['b_to_a_volume'], 4),
                'total_trades': total_trades,
                'total_volume': round(total_volume, 4),
                'symmetry_score': round(symmetry, 2),
                'wash_score': round(wash_score, 1),
                'severity': severity,
                'sample_transactions': data['transactions'][:10],  # First 10 for evidence
                'narrative': (
                    f"Wallet `{wallet_a}` and `{wallet_b}` engaged in {total_trades} "
                    f"bidirectional trades totaling {total_volume:,.2f} tokens. "
                    f"The symmetry score of {symmetry:.2f} suggests coordinated wash trading."
                )
            })

    wash_pairs.sort(key=lambda x: x['wash_score'], reverse=True)

    # Commentary
    if wash_pairs:
        high_severity = [w for w in wash_pairs if w['severity'] == 'high']
        total_wash_volume = sum(w['total_volume'] for w in wash_pairs)

        commentator.alert(f"Detected {len(wash_pairs)} potential wash trading pair(s)")
        commentator.narrate(f"Total suspected wash volume: {total_wash_volume:,.2f} tokens")

        if high_severity:
            commentator.critical(
                f"{len(high_severity)} pair(s) show HIGH confidence wash trading",
                {
                    'top_pair': f"{high_severity[0]['wallet_a']} ↔ {high_severity[0]['wallet_b']}",
                    'trades': high_severity[0]['total_trades'],
                    'volume': high_severity[0]['total_volume']
                }
            )
    else:
        commentator.finding("No definitive wash trading patterns detected")

    return wash_pairs


def detect_coordinated_dump(swaps: List[Dict], window_seconds: int,
                            commentator: ForensicCommentator) -> List[Dict]:
    """
    Detect coordinated sell events - multiple wallets dumping simultaneously.

    This is the signature of a planned attack: when multiple wallets that
    accumulated tokens suddenly sell in a tight time window, it creates
    massive downward pressure that legitimate holders can't escape.

    We look for:
    - Multiple unique sellers within a time window
    - High aggregate volume relative to normal trading
    - Timing that suggests coordination (too precise for coincidence)
    """
    commentator.section("COORDINATED DUMP DETECTION")
    commentator.narrate(f"Analyzing for synchronized sell events (window: {window_seconds}s)...")

    dump_events = []

    # Get all sells sorted chronologically
    sells = [s for s in swaps if s['trade_type'] == 'SELL']
    sells.sort(key=lambda x: x['block_time'])

    if len(sells) < 3:
        commentator.finding("Insufficient sell data for coordinated dump analysis")
        return dump_events

    commentator.narrate(f"Processing {len(sells):,} sell transactions...")

    # Sliding window analysis
    i = 0
    event_id = 0
    while i < len(sells):
        window_start = sells[i]['block_time']
        window_end = window_start + window_seconds

        # Collect all sells within this window
        window_sells = []
        j = i
        while j < len(sells) and sells[j]['block_time'] <= window_end:
            window_sells.append(sells[j])
            j += 1

        # Count unique sellers
        unique_sellers = list(set(s['trader'] for s in window_sells if s['trader']))

        # Flag if multiple wallets selling in tight window
        if len(unique_sellers) >= 3:
            event_id += 1
            total_volume = sum(s['amount'] for s in window_sells)
            actual_duration = window_sells[-1]['block_time'] - window_start if len(window_sells) > 1 else 0

            # Calculate coordination score
            # Higher score = more suspicious
            coordination_score = (
                len(unique_sellers) * 20 +              # More sellers = more suspicious
                len(window_sells) * 5 +                 # More transactions = more activity
                min(total_volume / 1000, 50) +          # Volume factor (capped)
                (1 - actual_duration / window_seconds) * 30 if actual_duration > 0 else 30  # Tighter = more suspicious
            )

            # Determine severity
            if coordination_score >= 80 or len(unique_sellers) >= 7:
                severity = 'critical'
            elif coordination_score >= 50 or len(unique_sellers) >= 5:
                severity = 'high'
            elif coordination_score >= 30:
                severity = 'medium'
            else:
                severity = 'low'

            # Build narrative
            narrative = (
                f"At {datetime.fromtimestamp(window_start, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC, "
                f"{len(unique_sellers)} different wallets executed {len(window_sells)} sell transactions "
                f"within {actual_duration} seconds, dumping a total of {total_volume:,.2f} tokens. "
            )

            if severity in ['critical', 'high']:
                narrative += (
                    f"The tight coordination of {len(unique_sellers)} sellers suggests this was a planned attack "
                    f"rather than organic market activity."
                )
            else:
                narrative += "This clustering warrants investigation but may represent normal market dynamics."

            dump_events.append({
                'event_id': f"DUMP-{event_id:03d}",
                'start_time': window_start,
                'start_time_utc': datetime.fromtimestamp(window_start, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                'end_time': window_sells[-1]['block_time'],
                'end_time_utc': datetime.fromtimestamp(window_sells[-1]['block_time'], timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                'duration_seconds': actual_duration,
                'unique_sellers': len(unique_sellers),
                'total_transactions': len(window_sells),
                'total_volume': round(total_volume, 4),
                'coordination_score': round(coordination_score, 1),
                'severity': severity,
                'participating_wallets': unique_sellers,
                'transactions': [
                    {
                        'wallet': s['trader'],
                        'amount': round(s['amount'], 4),
                        'time': s['block_time_utc'],
                        'signature': s['signature']
                    }
                    for s in window_sells
                ],
                'narrative': narrative
            })

            # Skip past this window to avoid overlapping events
            i = j
        else:
            i += 1

    dump_events.sort(key=lambda x: x['coordination_score'], reverse=True)

    # Commentary
    if dump_events:
        critical_dumps = [d for d in dump_events if d['severity'] == 'critical']
        high_dumps = [d for d in dump_events if d['severity'] == 'high']
        total_dump_volume = sum(d['total_volume'] for d in dump_events)

        commentator.alert(f"Identified {len(dump_events)} coordinated dump event(s)")
        commentator.narrate(f"Total volume in coordinated dumps: {total_dump_volume:,.2f} tokens")

        if critical_dumps:
            worst = critical_dumps[0]
            commentator.critical(
                f"MAJOR DUMP EVENT: {worst['unique_sellers']} wallets dumped {worst['total_volume']:,.2f} tokens in {worst['duration_seconds']}s",
                {
                    'event_id': worst['event_id'],
                    'time': worst['start_time_utc'],
                    'coordination_score': worst['coordination_score']
                }
            )

        if high_dumps:
            commentator.alert(f"{len(high_dumps)} additional HIGH severity dump event(s) detected")
    else:
        commentator.finding("No coordinated dump events detected within the analysis window")

    return dump_events


def detect_sybil_clusters(profiles: Dict[str, WalletProfile], cursor,
                          commentator: ForensicCommentator) -> List[Dict]:
    """
    Identify clusters of wallets likely controlled by the same entity.

    Sybil attacks use multiple wallets to disguise coordinated activity.
    We detect these by tracing funding sources - if multiple wallets
    received their initial SOL from the same funder, they're probably
    controlled by the same person/bot.

    Note: Only considers wallet-type funders. Pools, vaults, and programs
    are filtered out since receiving tokens from a pool is normal trading.
    """
    commentator.section("SYBIL CLUSTER DETECTION")
    commentator.narrate("Tracing wallet funding sources to identify common operators (excluding pools)...")

    clusters = []

    # Focus on wallets with meaningful activity
    suspect_wallets = [
        addr for addr, p in profiles.items()
        if p.total_sells >= 2 or p.is_bot_suspect or p.total_buys >= 3
    ]

    commentator.narrate(f"Analyzing funding sources for {len(suspect_wallets)} active wallets...")

    if len(suspect_wallets) < 2:
        commentator.finding("Insufficient wallet activity for Sybil analysis")
        return clusters

    # Trace funding for each wallet (limit queries for performance)
    funding_map: Dict[str, List[Dict]] = {}
    for wallet in suspect_wallets[:150]:  # Limit to avoid excessive queries
        sources = get_wallet_funding_sources(cursor, wallet, limit=5)
        if sources:
            funding_map[wallet] = sources

    commentator.narrate(f"Successfully traced funding for {len(funding_map)} wallets")

    # Invert: map funders to the wallets they funded
    funder_to_wallets: Dict[str, Set[str]] = defaultdict(set)
    for wallet, sources in funding_map.items():
        for source in sources:
            funder_to_wallets[source['funder']].add(wallet)

    # Identify clusters (funders with multiple recipients)
    cluster_id = 0
    for funder, funded_wallets in funder_to_wallets.items():
        if len(funded_wallets) >= SYBIL_MIN_CLUSTER_SIZE:
            cluster_id += 1

            # Calculate cluster statistics
            wallets_list = list(funded_wallets)
            total_sells = sum(profiles[w].total_sells for w in wallets_list if w in profiles)
            total_buys = sum(profiles[w].total_buys for w in wallets_list if w in profiles)
            total_sell_volume = sum(profiles[w].sell_volume for w in wallets_list if w in profiles)
            total_buy_volume = sum(profiles[w].buy_volume for w in wallets_list if w in profiles)
            bot_count = sum(1 for w in wallets_list if w in profiles and profiles[w].is_bot_suspect)

            # Score the cluster
            sybil_score = (
                len(funded_wallets) * 15 +
                total_sells * 3 +
                bot_count * 20 +
                min(total_sell_volume / 1000, 30)
            )

            # Severity
            if sybil_score >= 80 or len(funded_wallets) >= 8:
                severity = 'critical'
            elif sybil_score >= 50 or len(funded_wallets) >= 5:
                severity = 'high'
            elif sybil_score >= 30:
                severity = 'medium'
            else:
                severity = 'low'

            # Build narrative
            narrative = (
                f"Wallet `{funder}` funded {len(funded_wallets)} wallets that participated in trading. "
                f"Collectively, these wallets executed {total_sells} sells and {total_buys} buys. "
            )
            if bot_count > 0:
                narrative += f"Notably, {bot_count} of these wallets exhibit bot-like behavior. "
            if total_sell_volume > total_buy_volume * 1.5:
                narrative += f"The cluster shows net selling behavior, consistent with a coordinated dump operation."

            # Get wallet details
            wallet_details = []
            for w in wallets_list:
                if w in profiles:
                    p = profiles[w]
                    wallet_details.append({
                        'address': w,
                        'buys': p.total_buys,
                        'sells': p.total_sells,
                        'buy_volume': round(p.buy_volume, 4),
                        'sell_volume': round(p.sell_volume, 4),
                        'is_bot': p.is_bot_suspect,
                        'bot_score': p.bot_score
                    })

            clusters.append({
                'cluster_id': f"SYBIL-{cluster_id:03d}",
                'funder': funder,
                'wallet_count': len(funded_wallets),
                'wallets': wallets_list,
                'wallet_details': wallet_details,
                'total_buys': total_buys,
                'total_sells': total_sells,
                'total_buy_volume': round(total_buy_volume, 4),
                'total_sell_volume': round(total_sell_volume, 4),
                'net_position': round(total_buy_volume - total_sell_volume, 4),
                'bot_count': bot_count,
                'sybil_score': round(sybil_score, 1),
                'severity': severity,
                'narrative': narrative
            })

    clusters.sort(key=lambda x: x['sybil_score'], reverse=True)

    # Commentary
    if clusters:
        critical_clusters = [c for c in clusters if c['severity'] == 'critical']
        total_sybil_wallets = sum(c['wallet_count'] for c in clusters)

        commentator.alert(f"Identified {len(clusters)} Sybil cluster(s) involving {total_sybil_wallets} wallets")

        if critical_clusters:
            worst = critical_clusters[0]
            commentator.critical(
                f"MAJOR SYBIL OPERATION: {worst['wallet_count']} wallets funded by single source",
                {
                    'funder': worst['funder'],
                    'total_sells': worst['total_sells'],
                    'sell_volume': worst['total_sell_volume'],
                    'bot_count': worst['bot_count']
                }
            )
    else:
        commentator.finding("No Sybil clusters detected - wallets appear to have independent funding sources")

    return clusters


def analyze_timeline(swaps: List[Dict], commentator: ForensicCommentator) -> Dict:
    """
    Construct a timeline analysis of trading activity.

    This helps identify phases of manipulation:
    - Accumulation phase (quiet buying)
    - Pump phase (price increase, often with wash trading)
    - Dump phase (coordinated selling)
    """
    commentator.section("TIMELINE ANALYSIS")
    commentator.narrate("Reconstructing chronological trading patterns...")

    if not swaps:
        commentator.finding("No swap data available for timeline analysis")
        return {}

    # Group activity by hour
    hourly = defaultdict(lambda: {
        'buys': 0,
        'sells': 0,
        'buy_volume': 0.0,
        'sell_volume': 0.0,
        'unique_buyers': set(),
        'unique_sellers': set()
    })

    for swap in swaps:
        hour = datetime.fromtimestamp(swap['block_time'], timezone.utc).strftime('%Y-%m-%d %H:00')

        if swap['trade_type'] == 'BUY':
            hourly[hour]['buys'] += 1
            hourly[hour]['buy_volume'] += swap['amount']
            hourly[hour]['unique_buyers'].add(swap['trader'])
        else:
            hourly[hour]['sells'] += 1
            hourly[hour]['sell_volume'] += swap['amount']
            hourly[hour]['unique_sellers'].add(swap['trader'])

    # Convert to serializable format and identify anomalies
    hourly_data = []
    anomalies = []

    for hour in sorted(hourly.keys()):
        data = hourly[hour]
        sell_buy_ratio = data['sells'] / max(data['buys'], 1)
        volume_ratio = data['sell_volume'] / max(data['buy_volume'], 0.0001)

        entry = {
            'hour': hour,
            'buys': data['buys'],
            'sells': data['sells'],
            'buy_volume': round(data['buy_volume'], 4),
            'sell_volume': round(data['sell_volume'], 4),
            'unique_buyers': len(data['unique_buyers']),
            'unique_sellers': len(data['unique_sellers']),
            'sell_buy_ratio': round(sell_buy_ratio, 2),
            'volume_ratio': round(volume_ratio, 2)
        }
        hourly_data.append(entry)

        # Flag anomalous hours
        if data['sells'] > data['buys'] * 2 and data['sells'] >= 5:
            anomaly_severity = 'high' if data['sells'] >= 10 else 'medium'
            anomalies.append({
                'hour': hour,
                'type': 'heavy_selling',
                'severity': anomaly_severity,
                'details': entry,
                'narrative': (
                    f"Hour {hour}: Abnormal sell pressure with {data['sells']} sells vs {data['buys']} buys "
                    f"(ratio: {sell_buy_ratio:.1f}x). {len(data['unique_sellers'])} unique sellers dumped "
                    f"{data['sell_volume']:,.2f} tokens."
                )
            })

    # Identify phases
    phases = []
    if len(hourly_data) >= 3:
        # Simple phase detection based on net flow
        current_phase = None
        phase_start = None

        for entry in hourly_data:
            net_flow = entry['buy_volume'] - entry['sell_volume']

            if net_flow > 0 and entry['buys'] > entry['sells']:
                new_phase = 'accumulation'
            elif net_flow < 0 and entry['sells'] > entry['buys'] * 1.5:
                new_phase = 'distribution'
            else:
                new_phase = 'neutral'

            if new_phase != current_phase:
                if current_phase and phase_start:
                    phases.append({
                        'phase': current_phase,
                        'start': phase_start,
                        'end': entry['hour']
                    })
                current_phase = new_phase
                phase_start = entry['hour']

        # Close last phase
        if current_phase and phase_start:
            phases.append({
                'phase': current_phase,
                'start': phase_start,
                'end': hourly_data[-1]['hour']
            })

    timeline = {
        'first_trade': swaps[0]['block_time_utc'] if swaps else None,
        'last_trade': swaps[-1]['block_time_utc'] if swaps else None,
        'total_hours': len(hourly_data),
        'hourly_breakdown': hourly_data,
        'anomalous_periods': anomalies,
        'detected_phases': phases
    }

    # Commentary
    commentator.finding(f"Timeline spans {len(hourly_data)} hours of activity")
    commentator.finding(f"First trade: {timeline['first_trade']}, Last trade: {timeline['last_trade']}")

    if anomalies:
        high_anomalies = [a for a in anomalies if a['severity'] == 'high']
        commentator.alert(f"Identified {len(anomalies)} anomalous period(s)")
        if high_anomalies:
            commentator.critical(f"{len(high_anomalies)} period(s) show severe sell pressure anomalies")

    if phases:
        distribution_phases = [p for p in phases if p['phase'] == 'distribution']
        if distribution_phases:
            commentator.alert(f"Detected {len(distribution_phases)} distribution (dump) phase(s)")

    return timeline


# =============================================================================
# REPORT GENERATION
# =============================================================================

def generate_executive_summary(
    token_info: Dict,
    total_txs: int,
    total_wallets: int,
    bot_suspects: List[Dict],
    wash_pairs: List[Dict],
    dump_events: List[Dict],
    sybil_clusters: List[Dict],
    commentator: ForensicCommentator
) -> Dict:
    """
    Generate an executive summary with overall manipulation assessment.
    """
    commentator.section("EXECUTIVE SUMMARY")

    # Calculate overall manipulation score
    manipulation_score = 0.0
    findings = []
    recommendations = []

    # Bot activity contribution
    if bot_suspects:
        critical_bots = [b for b in bot_suspects if b['severity'] == 'critical']
        high_bots = [b for b in bot_suspects if b['severity'] == 'high']
        bot_contribution = min(len(critical_bots) * 15 + len(high_bots) * 8 + len(bot_suspects) * 2, 30)
        manipulation_score += bot_contribution
        findings.append(f"Detected {len(bot_suspects)} bot wallet(s), including {len(critical_bots)} critical")

    # Coordinated dump contribution
    if dump_events:
        critical_dumps = [d for d in dump_events if d['severity'] == 'critical']
        high_dumps = [d for d in dump_events if d['severity'] == 'high']
        dump_contribution = min(len(critical_dumps) * 20 + len(high_dumps) * 12 + len(dump_events) * 3, 35)
        manipulation_score += dump_contribution
        findings.append(f"Identified {len(dump_events)} coordinated dump event(s)")

    # Sybil cluster contribution
    if sybil_clusters:
        critical_clusters = [c for c in sybil_clusters if c['severity'] == 'critical']
        cluster_contribution = min(len(critical_clusters) * 15 + len(sybil_clusters) * 8, 20)
        manipulation_score += cluster_contribution
        total_sybil_wallets = sum(c['wallet_count'] for c in sybil_clusters)
        findings.append(f"Found {len(sybil_clusters)} Sybil cluster(s) with {total_sybil_wallets} wallets")

    # Wash trading contribution
    if wash_pairs:
        high_wash = [w for w in wash_pairs if w['severity'] == 'high']
        wash_contribution = min(len(high_wash) * 10 + len(wash_pairs) * 3, 15)
        manipulation_score += wash_contribution
        findings.append(f"Detected {len(wash_pairs)} wash trading pair(s)")

    manipulation_score = min(manipulation_score, 100)

    # Determine overall assessment
    if manipulation_score >= 70:
        assessment = "HIGH PROBABILITY OF COORDINATED MANIPULATION"
        assessment_detail = (
            "The evidence strongly suggests this token was subjected to a coordinated attack. "
            "Multiple indicators including bot activity, synchronized dumps, and Sybil clusters "
            "point to deliberate manipulation by one or more bad actors."
        )
        confidence = "high"
        recommendations = [
            "Document all evidence for potential legal/regulatory action",
            "Consider reporting to relevant authorities if applicable",
            "Warn community members about identified malicious wallets",
            "Implement monitoring for identified Sybil cluster addresses"
        ]
    elif manipulation_score >= 40:
        assessment = "MODERATE MANIPULATION INDICATORS"
        assessment_detail = (
            "Several suspicious patterns were detected that warrant concern. "
            "While not conclusive, the evidence suggests possible coordination "
            "among some market participants."
        )
        confidence = "medium"
        recommendations = [
            "Continue monitoring identified suspicious wallets",
            "Gather additional data to strengthen or refute findings",
            "Consider enhanced due diligence for future token interactions"
        ]
    elif manipulation_score >= 20:
        assessment = "MINOR ANOMALIES DETECTED"
        assessment_detail = (
            "Some unusual patterns were observed, but they may represent normal market "
            "dynamics or isolated incidents rather than coordinated manipulation."
        )
        confidence = "low"
        recommendations = [
            "Monitor for escalation of suspicious activity",
            "No immediate action required"
        ]
    else:
        assessment = "NO SIGNIFICANT MANIPULATION DETECTED"
        assessment_detail = (
            "The analysis did not reveal strong evidence of coordinated manipulation. "
            "Trading patterns appear consistent with organic market activity."
        )
        confidence = "low"
        recommendations = [
            "Routine monitoring recommended",
            "Re-analyze if market conditions change significantly"
        ]

    summary = {
        'token': token_info.get('symbol', 'Unknown'),
        'token_name': token_info.get('name', 'Unknown'),
        'token_mint': token_info.get('mint'),
        'assessment': assessment,
        'assessment_detail': assessment_detail,
        'manipulation_score': round(manipulation_score, 1),
        'confidence': confidence,
        'total_transactions_analyzed': total_txs,
        'total_wallets_analyzed': total_wallets,
        'key_findings': findings,
        'recommendations': recommendations,
        'analysis_timestamp': datetime.now(timezone.utc).isoformat()
    }

    # Commentary
    commentator.narrate(f"MANIPULATION SCORE: {manipulation_score:.1f}/100", "critical" if manipulation_score >= 70 else "finding")
    commentator.narrate(assessment, "critical" if manipulation_score >= 70 else ("alert" if manipulation_score >= 40 else "finding"))

    for finding in findings:
        commentator.finding(finding)

    return summary


def build_full_report(
    token_info: Dict,
    activities: List[Dict],
    swaps: List[Dict],
    profiles: Dict[str, WalletProfile],
    bot_suspects: List[Dict],
    wash_pairs: List[Dict],
    dump_events: List[Dict],
    sybil_clusters: List[Dict],
    timeline: Dict,
    commentator: ForensicCommentator,
    funding_investigations: List[Dict] = None,
    cross_ref_results: Dict = None
) -> Dict:
    """
    Assemble the complete forensic report with all findings and commentary.
    """
    funding_investigations = funding_investigations or []
    cross_ref_results = cross_ref_results or {}
    # Generate executive summary
    summary = generate_executive_summary(
        token_info, len(activities), len(profiles),
        bot_suspects, wash_pairs, dump_events, sybil_clusters, commentator
    )

    # Compile top sellers for quick reference
    top_sellers = []
    sellers = [(addr, p) for addr, p in profiles.items() if p.total_sells > 0]
    sellers.sort(key=lambda x: x[1].sell_volume, reverse=True)

    for addr, p in sellers[:25]:
        top_sellers.append({
            'wallet': addr,
            'sells': p.total_sells,
            'sell_volume': round(p.sell_volume, 4),
            'buys': p.total_buys,
            'buy_volume': round(p.buy_volume, 4),
            'net_position': round(p.net_position(), 4),
            'is_bot_suspect': p.is_bot_suspect,
            'bot_score': p.bot_score,
            'first_seen': datetime.fromtimestamp(p.first_seen, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if p.first_seen else '',
            'last_seen': datetime.fromtimestamp(p.last_seen, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if p.last_seen else ''
        })

    # Build full report
    report = {
        'report_metadata': {
            'report_type': 'Token Forensic Analysis',
            'version': '2.0',
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'generator': 'T16O Token Forensic Analyzer'
        },
        'executive_summary': summary,
        'token_information': token_info,
        'analysis_scope': {
            'total_transactions': len(activities),
            'total_swaps': len(swaps),
            'total_wallets': len(profiles),
            'time_range': {
                'start': timeline.get('first_trade'),
                'end': timeline.get('last_trade')
            }
        },
        'detection_results': {
            'bot_detection': {
                'total_suspects': len(bot_suspects),
                'critical_count': len([b for b in bot_suspects if b['severity'] == 'critical']),
                'high_count': len([b for b in bot_suspects if b['severity'] == 'high']),
                'suspects': bot_suspects
            },
            'coordinated_dumps': {
                'total_events': len(dump_events),
                'critical_count': len([d for d in dump_events if d['severity'] == 'critical']),
                'high_count': len([d for d in dump_events if d['severity'] == 'high']),
                'events': dump_events
            },
            'sybil_clusters': {
                'total_clusters': len(sybil_clusters),
                'total_wallets_in_clusters': sum(c['wallet_count'] for c in sybil_clusters),
                'clusters': sybil_clusters
            },
            'wash_trading': {
                'total_pairs': len(wash_pairs),
                'total_volume': sum(w['total_volume'] for w in wash_pairs),
                'pairs': wash_pairs
            },
            'funding_investigations': {
                'total_investigated': len(funding_investigations),
                'critical_count': len([f for f in funding_investigations if f.get('threat_level') == 'CRITICAL']),
                'high_count': len([f for f in funding_investigations if f.get('threat_level') == 'HIGH']),
                'investigations': funding_investigations
            }
        },
        'timeline_analysis': timeline,
        'top_sellers': top_sellers,
        'cross_reference_intelligence': cross_ref_results,
        'commentary_log': commentator.commentary_log
    }

    return report


def generate_markdown_report(report: Dict) -> str:
    """
    Generate a professional Markdown report suitable for sharing with stakeholders.
    """
    summary = report['executive_summary']
    detection = report['detection_results']
    timeline = report.get('timeline_analysis', {})

    md = []

    # Header
    md.append(f"# Token Forensic Analysis Report")
    md.append(f"## {summary['token']} ({summary['token_name']})")
    md.append("")
    md.append(f"**Token Address:** `{summary['token_mint']}`")
    md.append(f"**Analysis Date:** {summary['analysis_timestamp'][:10]}")
    md.append(f"**Report Generated:** {report['report_metadata']['generated_at']}")
    md.append("")

    # Executive Summary Box
    md.append("---")
    md.append("")
    md.append("## Executive Summary")
    md.append("")

    # Assessment with visual indicator
    score = summary['manipulation_score']
    if score >= 70:
        indicator = "🔴 **CRITICAL**"
    elif score >= 40:
        indicator = "🟠 **WARNING**"
    elif score >= 20:
        indicator = "🟡 **CAUTION**"
    else:
        indicator = "🟢 **CLEAR**"

    md.append(f"### {indicator}")
    md.append(f"### {summary['assessment']}")
    md.append("")
    md.append(f"**Manipulation Score:** {score}/100")
    md.append(f"**Confidence Level:** {summary['confidence'].upper()}")
    md.append("")
    md.append(summary['assessment_detail'])
    md.append("")

    # Key Statistics
    md.append("### Analysis Scope")
    md.append("")
    md.append(f"| Metric | Value |")
    md.append(f"|--------|-------|")
    md.append(f"| Total Transactions | {summary['total_transactions_analyzed']:,} |")
    md.append(f"| Total Wallets | {summary['total_wallets_analyzed']:,} |")
    md.append(f"| Bot Suspects | {detection['bot_detection']['total_suspects']} |")
    md.append(f"| Coordinated Dumps | {detection['coordinated_dumps']['total_events']} |")
    md.append(f"| Sybil Clusters | {detection['sybil_clusters']['total_clusters']} |")
    md.append(f"| Wash Trading Pairs | {detection['wash_trading']['total_pairs']} |")
    md.append(f"| Funding Wallets Investigated | {detection.get('funding_investigations', {}).get('total_investigated', 0)} |")
    md.append("")

    # Key Findings
    if summary['key_findings']:
        md.append("### Key Findings")
        md.append("")
        for finding in summary['key_findings']:
            md.append(f"- {finding}")
        md.append("")

    # Recommendations
    if summary['recommendations']:
        md.append("### Recommendations")
        md.append("")
        for i, rec in enumerate(summary['recommendations'], 1):
            md.append(f"{i}. {rec}")
        md.append("")

    md.append("---")
    md.append("")

    # Detailed Findings
    md.append("## Detailed Findings")
    md.append("")

    # Bot Detection
    md.append("### 1. Bot Activity Detection")
    md.append("")
    bot_data = detection['bot_detection']
    if bot_data['suspects']:
        md.append(f"**{bot_data['total_suspects']} wallet(s)** exhibit bot-like trading behavior.")
        md.append("")
        if bot_data['critical_count'] > 0:
            md.append(f"- 🔴 **Critical:** {bot_data['critical_count']} wallet(s)")
        if bot_data['high_count'] > 0:
            md.append(f"- 🟠 **High:** {bot_data['high_count']} wallet(s)")
        md.append("")

        md.append("#### Top Bot Suspects")
        md.append("")
        md.append("| Wallet | Bot Score | Trades | Sells | Sell Volume | Evidence |")
        md.append("|--------|-----------|--------|-------|-------------|----------|")

        for bot in bot_data['suspects'][:10]:
            wallet_short = f"`{bot['wallet']}`"
            evidence_summary = []
            if bot['evidence'].get('rapid_reactions'):
                evidence_summary.append(f"{len(bot['evidence']['rapid_reactions'])} fast reactions")
            if bot['evidence'].get('timing_analysis', {}).get('variance', 999) < 100:
                evidence_summary.append("consistent timing")
            if bot['evidence'].get('amount_analysis', {}).get('normalized_variance', 1) < 0.1:
                evidence_summary.append("uniform amounts")
            evidence_str = ", ".join(evidence_summary) if evidence_summary else "multiple signals"

            md.append(f"| {wallet_short} | {bot['bot_score']:.1f} | {bot['total_trades']} | {bot['sells']} | {bot['sell_volume']:,.2f} | {evidence_str} |")

        md.append("")

        # Detailed bot evidence for top suspect
        if bot_data['suspects']:
            top_bot = bot_data['suspects'][0]
            md.append(f"#### Primary Suspect Analysis: `{top_bot['wallet']}`")
            md.append("")
            md.append(f"**Bot Score:** {top_bot['bot_score']:.1f} ({top_bot['severity'].upper()})")
            md.append("")
            if top_bot['evidence'].get('analysis_notes'):
                md.append("**Evidence:**")
                md.append("")
                for note in top_bot['evidence']['analysis_notes']:
                    md.append(f"- {note}")
                md.append("")
    else:
        md.append("No definitive bot signatures were detected in the trading patterns.")
        md.append("")

    # Coordinated Dumps
    md.append("### 2. Coordinated Dump Events")
    md.append("")
    dump_data = detection['coordinated_dumps']
    if dump_data['events']:
        md.append(f"**{dump_data['total_events']} coordinated selling event(s)** were detected.")
        md.append("")

        md.append("| Event ID | Time (UTC) | Sellers | Transactions | Volume | Score | Severity |")
        md.append("|----------|------------|---------|--------------|--------|-------|----------|")

        for dump in dump_data['events'][:10]:
            severity_icon = "🔴" if dump['severity'] == 'critical' else ("🟠" if dump['severity'] == 'high' else "🟡")
            md.append(f"| {dump['event_id']} | {dump['start_time_utc']} | {dump['unique_sellers']} | {dump['total_transactions']} | {dump['total_volume']:,.2f} | {dump['coordination_score']:.1f} | {severity_icon} {dump['severity'].upper()} |")

        md.append("")

        # Detail the worst dump
        if dump_data['events']:
            worst_dump = dump_data['events'][0]
            md.append(f"#### Critical Event: {worst_dump['event_id']}")
            md.append("")
            md.append(worst_dump['narrative'])
            md.append("")
            md.append("**Participating Wallets:**")
            md.append("")
            for wallet in worst_dump['participating_wallets']:
                md.append(f"- `{wallet}`")
            md.append("")
    else:
        md.append("No coordinated dump events were detected.")
        md.append("")

    # Sybil Clusters
    md.append("### 3. Sybil Cluster Analysis")
    md.append("")
    sybil_data = detection['sybil_clusters']
    if sybil_data['clusters']:
        md.append(f"**{sybil_data['total_clusters']} cluster(s)** of related wallets were identified, ")
        md.append(f"comprising **{sybil_data['total_wallets_in_clusters']} wallets** total.")
        md.append("")

        for cluster in sybil_data['clusters'][:5]:
            severity_icon = "🔴" if cluster['severity'] == 'critical' else ("🟠" if cluster['severity'] == 'high' else "🟡")
            md.append(f"#### {severity_icon} {cluster['cluster_id']}")
            md.append("")
            md.append(f"**Funding Source:** `{cluster['funder']}`")
            md.append(f"**Wallets in Cluster:** {cluster['wallet_count']}")
            md.append(f"**Combined Sells:** {cluster['total_sells']} ({cluster['total_sell_volume']:,.2f} tokens)")
            md.append(f"**Bot Wallets in Cluster:** {cluster['bot_count']}")
            md.append("")
            md.append(cluster['narrative'])
            md.append("")
            md.append("**Member Wallets:**")
            md.append("")
            for w in cluster['wallet_details']:
                bot_flag = " 🤖" if w['is_bot'] else ""
                md.append(f"- `{w['address']}` - {w['sells']} sells ({w['sell_volume']:,.2f}){bot_flag}")
            md.append("")
    else:
        md.append("No Sybil clusters were identified. Wallets appear to have independent funding sources.")
        md.append("")

    # Wash Trading
    md.append("### 4. Wash Trading Detection")
    md.append("")
    wash_data = detection['wash_trading']
    if wash_data['pairs']:
        md.append(f"**{wash_data['total_pairs']} wallet pair(s)** show bidirectional trading patterns ")
        md.append(f"with estimated wash volume of **{wash_data['total_volume']:,.2f} tokens**.")
        md.append("")

        md.append("| Wallet A | Wallet B | A→B Trades | B→A Trades | Total Volume | Score |")
        md.append("|----------|----------|------------|------------|--------------|-------|")

        for pair in wash_data['pairs'][:10]:
            md.append(f"| `{pair['wallet_a']}` | `{pair['wallet_b']}` | {pair['a_to_b_trades']} | {pair['b_to_a_trades']} | {pair['total_volume']:,.2f} | {pair['wash_score']:.1f} |")

        md.append("")
    else:
        md.append("No significant wash trading patterns were detected.")
        md.append("")

    # Funding Wallet Investigations (Joe Buck Special)
    md.append("### 5. Funding Wallet Deep Dive")
    md.append("")
    funding_data = detection.get('funding_investigations', {})
    investigations = funding_data.get('investigations', [])

    if investigations:
        md.append(f"**{len(investigations)} funding wallet(s)** behind detected bots were investigated.")
        md.append("")

        if funding_data.get('critical_count', 0) > 0:
            md.append(f"- 🔴 **Critical Threat:** {funding_data['critical_count']} funder(s)")
        if funding_data.get('high_count', 0) > 0:
            md.append(f"- 🟠 **High Threat:** {funding_data['high_count']} funder(s)")
        md.append("")

        for inv in investigations[:5]:
            threat_icon = "🔴" if inv.get('threat_level') == 'CRITICAL' else ("🟠" if inv.get('threat_level') == 'HIGH' else "🟡")
            md.append(f"#### {threat_icon} Funding Wallet: `{inv['funder_address']}`")
            md.append("")
            md.append(f"**Threat Level:** {inv.get('threat_level', 'UNKNOWN')} (Score: {inv.get('threat_score', 0)})")
            md.append(f"**Bots Funded in This Investigation:** {inv.get('bots_funded_count', 0)}")
            md.append("")

            summary = inv.get('activity_summary', {})
            md.append(f"| Metric | Value |")
            md.append(f"|--------|-------|")
            md.append(f"| Total Wallets Funded (30 days) | {summary.get('total_wallets_funded', 0)} |")
            md.append(f"| SOL Distributed | {summary.get('total_sol_out', 0):.2f} |")
            md.append(f"| Tokens Touched | {summary.get('unique_tokens_touched', 0)} |")
            md.append(f"| Direct Swaps | {summary.get('total_swaps', 0)} |")
            md.append(f"| Net SOL Flow | {summary.get('net_sol_flow', 0):.2f} |")
            md.append("")

            # Risk flags
            risk = inv.get('risk_assessment', {})
            flags = []
            if risk.get('is_serial_funder'):
                flags.append("Serial Funder (10+ wallets)")
            if risk.get('is_multi_token_operator'):
                flags.append("Multi-Token Operator")
            if risk.get('is_active_trader'):
                flags.append("Active Trader")
            if risk.get('is_net_distributor'):
                flags.append("Net Fund Distributor")
            if risk.get('funded_multiple_bots'):
                flags.append("Funded Multiple Bots")

            if flags:
                md.append("**Risk Flags:**")
                for flag in flags:
                    md.append(f"- ⚠️ {flag}")
                md.append("")

            # Narrative
            if inv.get('narrative'):
                md.append("**Analysis:**")
                md.append("")
                md.append(f"> {inv['narrative']}")
                md.append("")

            # Upstream funders
            upstream = summary.get('funding_sources', [])
            if upstream:
                md.append("**Upstream Funding Sources:**")
                for src in upstream:
                    md.append(f"- `{src}`")
                md.append("")

            # Bots funded
            bots = inv.get('bots_funded_in_this_investigation', [])
            if bots:
                md.append("**Bots Funded (in this investigation):**")
                for bot in bots:
                    md.append(f"- `{bot}`")
                md.append("")

    else:
        md.append("No funding wallet investigations were conducted (no bots detected or no funding sources found).")
        md.append("")

    # Timeline
    md.append("### 6. Timeline Analysis")
    md.append("")
    if timeline.get('anomalous_periods'):
        md.append(f"**{len(timeline['anomalous_periods'])} anomalous period(s)** identified:")
        md.append("")
        for anomaly in timeline['anomalous_periods'][:5]:
            severity_icon = "🔴" if anomaly['severity'] == 'high' else "🟠"
            md.append(f"- {severity_icon} **{anomaly['hour']}**: {anomaly['narrative']}")
        md.append("")
    else:
        md.append("No significant timeline anomalies detected.")
        md.append("")

    # Top Sellers Table
    md.append("---")
    md.append("")
    md.append("## Top Sellers")
    md.append("")
    md.append("The following wallets had the highest sell volume:")
    md.append("")
    md.append("| Rank | Wallet | Sells | Sell Volume | Buys | Net Position | Bot? |")
    md.append("|------|--------|-------|-------------|------|--------------|------|")

    for i, seller in enumerate(report['top_sellers'][:15], 1):
        wallet_short = f"`{seller['wallet']}`"
        bot_flag = "🤖" if seller['is_bot_suspect'] else ""
        net = seller['net_position']
        net_str = f"+{net:,.2f}" if net > 0 else f"{net:,.2f}"
        md.append(f"| {i} | {wallet_short} | {seller['sells']} | {seller['sell_volume']:,.2f} | {seller['buys']} | {net_str} | {bot_flag} |")

    md.append("")

    # Cross-Reference Intelligence Section
    md.append("---")
    md.append("")
    md.append("## theGuide Cross-Reference Intelligence")
    md.append("")
    md.append("This section shows what OTHER tokens the identified bad actors have been involved with,")
    md.append("demonstrating the power of theGuide's comprehensive blockchain intelligence.")
    md.append("")

    cross_ref = report.get('cross_reference_intelligence', {})

    if cross_ref.get('wallet_details'):
        md.append(f"**{cross_ref.get('wallets_analyzed', 0)} bad actor wallet(s)** were cross-referenced against theGuide.")
        md.append(f"**{cross_ref.get('wallets_with_other_activity', 0)}** showed activity on **other tokens**.")
        md.append(f"Combined footprint spans **{len(cross_ref.get('unique_other_tokens', []))} unique tokens**.")
        md.append("")

        # Serial offenders callout
        serial = cross_ref.get('serial_offenders', [])
        if serial:
            md.append("### 🚨 Serial Offenders (Active in 3+ Tokens)")
            md.append("")
            md.append("These wallets show a pattern of activity across MULTIPLE tokens - potential repeat offenders:")
            md.append("")
            md.append("| Wallet | Other Tokens | Total Txs |")
            md.append("|--------|--------------|-----------|")
            for offender in serial:
                md.append(f"| `{offender['wallet']}` | {offender['token_count']} | {offender['total_txs']} |")
            md.append("")

        # Detailed wallet breakdowns
        md.append("### Wallet Activity on Other Tokens")
        md.append("")

        for wallet_data in cross_ref.get('wallet_details', []):
            if wallet_data.get('tokens'):
                md.append(f"#### `{wallet_data['wallet']}`")
                md.append("")
                md.append(f"**Active in {wallet_data['other_tokens_count']} other token(s)** with {wallet_data['total_other_transactions']} total transactions")
                md.append("")

                md.append("| Token | Symbol | Sells | Buys | Transfers | Volume | First Seen | Last Seen |")
                md.append("|-------|--------|-------|------|-----------|--------|------------|-----------|")

                for token in wallet_data['tokens']:
                    md.append(
                        f"| `{token['token_mint']}` | "
                        f"{token['token_symbol'] or 'UNK'} | "
                        f"{token['sells']} | "
                        f"{token['buys']} | "
                        f"{token['transfers']} | "
                        f"{token['total_volume']:,.2f} | "
                        f"{token['first_seen'] or 'N/A'} | "
                        f"{token['last_seen'] or 'N/A'} |"
                    )
                md.append("")
    else:
        md.append("No additional token activity found for the identified bad actors in theGuide database.")
        md.append("")
        md.append("This could mean:")
        md.append("- These wallets were created specifically for this attack")
        md.append("- Other token activity hasn't been loaded into theGuide yet")
        md.append("- The wallets are relatively new to the ecosystem")
        md.append("")

    # Footer
    md.append("---")
    md.append("")
    md.append("## Appendix")
    md.append("")
    md.append("### Methodology")
    md.append("")
    md.append("This analysis employed the following detection techniques:")
    md.append("")
    md.append("1. **Bot Detection**: Statistical analysis of trading timing, amounts, and frequency")
    md.append("2. **Coordinated Dump Detection**: Sliding window analysis of sell clustering")
    md.append("3. **Sybil Detection**: Funding source tracing to identify common wallet operators")
    md.append("4. **Wash Trading Detection**: Bidirectional trade flow analysis")
    md.append("5. **Timeline Analysis**: Chronological pattern identification")
    md.append("")
    md.append("### Confidence Levels")
    md.append("")
    md.append("- **High**: >200 transactions analyzed, multiple detection methods confirm findings")
    md.append("- **Medium**: 50-200 transactions, patterns detected but sample size limits certainty")
    md.append("- **Low**: <50 transactions, insufficient data for high-confidence conclusions")
    md.append("")
    md.append("### Disclaimer")
    md.append("")
    md.append("This report represents an automated analysis of on-chain data. While the detection ")
    md.append("algorithms are designed to identify common manipulation patterns, false positives ")
    md.append("and false negatives are possible. This report should be considered as one input ")
    md.append("among many in any investigation and does not constitute legal or financial advice.")
    md.append("")
    md.append("---")
    md.append("")
    md.append(f"*Report generated by T16O Token Forensic Analyzer v2.0*")
    md.append(f"*Analysis timestamp: {report['report_metadata']['generated_at']}*")

    return "\n".join(md)


def generate_gexf_graph(
    token_info: Dict,
    activities: List[Dict],
    swaps: List[Dict],
    profiles: Dict[str, WalletProfile],
    bot_suspects: List[Dict],
    sybil_clusters: List[Dict],
    dump_events: List[Dict],
    funding_investigations: List[Dict],
    commentator: ForensicCommentator
) -> nx.DiGraph:
    """
    Build a NetworkX directed graph from the forensic data for GEXF export.

    Node Types:
    - wallet: Trading wallets with behavioral attributes
    - token: The token being analyzed (central node)
    - funder: Funding wallet nodes

    Edge Types:
    - swap_in: Wallet sold token
    - swap_out: Wallet bought token
    - transfer: Token transfer between wallets
    - funding: SOL/token funding relationship

    Node Attributes:
    - type: wallet/token/funder
    - label: Short identifier
    - is_bot: Boolean flag
    - bot_score: Numeric bot likelihood
    - total_buys/sells: Transaction counts
    - buy_volume/sell_volume: Token volumes
    - threat_level: For funders
    - in_sybil_cluster: Boolean
    - in_dump_event: Boolean

    Edge Attributes:
    - type: Transaction type
    - amount: Token amount
    - timestamp: Unix timestamp
    - timestamp_utc: Human readable
    """
    commentator.section("GEXF GRAPH GENERATION")
    commentator.narrate("Building transaction graph for Gephi visualization...")

    G = nx.DiGraph()

    # Create sets for quick lookups
    bot_wallets = {b['wallet'] for b in bot_suspects}
    bot_scores = {b['wallet']: b['bot_score'] for b in bot_suspects}

    sybil_wallets = set()
    for cluster in sybil_clusters:
        for w in cluster.get('wallets', []):
            sybil_wallets.add(w)

    dump_wallets = set()
    for dump in dump_events:
        for w in dump.get('participating_wallets', []):
            dump_wallets.add(w)

    funder_addresses = {inv['funder_address'] for inv in funding_investigations}
    funder_threat = {inv['funder_address']: inv.get('threat_level', 'UNKNOWN') for inv in funding_investigations}

    # Add token as central node
    token_mint = token_info.get('mint', 'unknown')
    token_symbol = token_info.get('symbol', 'UNK')

    G.add_node(
        token_mint,
        label=token_symbol,
        node_type='token',
        viz_size=50,
        viz_color_r=255,
        viz_color_g=215,
        viz_color_b=0
    )

    commentator.finding(f"Added token node: {token_symbol}")

    # Add wallet nodes from profiles
    wallets_added = 0
    for addr, profile in profiles.items():
        # Determine node color based on attributes
        if addr in bot_wallets:
            color = (255, 0, 0)  # Red for bots
            node_type = 'bot'
        elif addr in funder_addresses:
            color = (128, 0, 128)  # Purple for funders
            node_type = 'funder'
        elif addr in sybil_wallets:
            color = (255, 165, 0)  # Orange for sybil
            node_type = 'sybil'
        elif addr in dump_wallets:
            color = (255, 69, 0)  # Red-orange for dumpers
            node_type = 'dumper'
        elif profile.sell_volume > profile.buy_volume * 2:
            color = (255, 100, 100)  # Light red for heavy sellers
            node_type = 'seller'
        elif profile.buy_volume > profile.sell_volume * 2:
            color = (100, 255, 100)  # Light green for heavy buyers
            node_type = 'buyer'
        else:
            color = (200, 200, 200)  # Gray for neutral
            node_type = 'wallet'

        # Calculate node size based on activity
        activity_score = profile.total_buys + profile.total_sells
        viz_size = min(5 + activity_score * 2, 40)

        G.add_node(
            addr,
            label=addr[:8] + '...',
            full_address=addr,
            node_type=node_type,
            is_bot=addr in bot_wallets,
            bot_score=bot_scores.get(addr, 0),
            is_sybil=addr in sybil_wallets,
            is_dumper=addr in dump_wallets,
            is_funder=addr in funder_addresses,
            funder_threat=funder_threat.get(addr, ''),
            total_buys=profile.total_buys,
            total_sells=profile.total_sells,
            buy_volume=round(profile.buy_volume, 4),
            sell_volume=round(profile.sell_volume, 4),
            net_position=round(profile.net_position(), 4),
            first_seen=datetime.fromtimestamp(profile.first_seen, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if profile.first_seen else '',
            last_seen=datetime.fromtimestamp(profile.last_seen, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if profile.last_seen else '',
            viz_size=viz_size,
            viz_color_r=color[0],
            viz_color_g=color[1],
            viz_color_b=color[2]
        )
        wallets_added += 1

    commentator.finding(f"Added {wallets_added} wallet nodes")

    # Add edges from swaps (wallet <-> token relationships)
    swap_edges = 0
    for swap in swaps:
        trader = swap.get('trader')
        if not trader:
            continue

        edge_attrs = {
            'edge_type': swap['trade_type'].lower(),
            'amount': swap['amount'],
            'timestamp': swap['block_time'],
            'timestamp_utc': swap['block_time_utc'],
            'signature': swap.get('signature', ''),
            'weight': min(swap['amount'] / 1000, 10) if swap['amount'] else 1
        }

        if swap['trade_type'] == 'SELL':
            # Wallet -> Token (selling)
            G.add_edge(trader, token_mint, **edge_attrs)
        else:
            # Token -> Wallet (buying)
            G.add_edge(token_mint, trader, **edge_attrs)

        swap_edges += 1

    commentator.finding(f"Added {swap_edges} swap edges")

    # Add edges from activities (transfers between wallets)
    transfer_edges = 0
    for act in activities:
        if act['edge_type'] not in ['spl_transfer', 'sol_transfer']:
            continue

        from_addr = act.get('from_address')
        to_addr = act.get('to_address')

        if not from_addr or not to_addr:
            continue

        # Ensure both nodes exist
        if from_addr not in G.nodes:
            G.add_node(from_addr, label=from_addr[:8] + '...', full_address=from_addr, node_type='external')
        if to_addr not in G.nodes:
            G.add_node(to_addr, label=to_addr[:8] + '...', full_address=to_addr, node_type='external')

        edge_attrs = {
            'edge_type': act['edge_type'],
            'amount': act['amount'],
            'timestamp': act['block_time'],
            'timestamp_utc': act['block_time_utc'],
            'token': act.get('token_symbol', 'UNK'),
            'weight': 1
        }

        # Add edge (or update if exists)
        if G.has_edge(from_addr, to_addr):
            # Increment weight for multiple transfers
            G[from_addr][to_addr]['weight'] += 1
            G[from_addr][to_addr]['amount'] += act['amount']
        else:
            G.add_edge(from_addr, to_addr, **edge_attrs)
            transfer_edges += 1

    commentator.finding(f"Added {transfer_edges} transfer edges")

    # Add funding relationships from investigations
    funding_edges = 0
    for inv in funding_investigations:
        funder = inv['funder_address']

        # Ensure funder node exists with special attributes
        if funder not in G.nodes:
            G.add_node(
                funder,
                label=funder[:8] + '...',
                full_address=funder,
                node_type='funder',
                is_funder=True,
                funder_threat=inv.get('threat_level', 'UNKNOWN'),
                viz_size=30,
                viz_color_r=128,
                viz_color_g=0,
                viz_color_b=128
            )
        else:
            # Update existing node
            G.nodes[funder]['is_funder'] = True
            G.nodes[funder]['funder_threat'] = inv.get('threat_level', 'UNKNOWN')

        # Add edges to bots this funder funded
        for bot_addr in inv.get('bots_funded_in_this_investigation', []):
            if bot_addr in G.nodes:
                G.add_edge(
                    funder,
                    bot_addr,
                    edge_type='funding',
                    relationship='funded_bot',
                    weight=5  # Heavy weight for funding relationships
                )
                funding_edges += 1

    commentator.finding(f"Added {funding_edges} funding relationship edges")

    # Summary
    commentator.narrate("")
    commentator.finding(f"Graph complete: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    return G


def export_gexf(G: nx.DiGraph, filepath: str, commentator: ForensicCommentator):
    """Export the graph to GEXF format for Gephi."""
    commentator.narrate(f"Exporting graph to GEXF: {filepath}")

    try:
        nx.write_gexf(G, filepath)
        commentator.finding(f"GEXF graph exported successfully: {filepath}")
        commentator.narrate(f"  Nodes: {G.number_of_nodes()}")
        commentator.narrate(f"  Edges: {G.number_of_edges()}")
        commentator.narrate("")
        commentator.narrate("Gephi visualization tips:")
        commentator.narrate("  - Use 'ForceAtlas 2' layout for best results")
        commentator.narrate("  - Color nodes by 'node_type' or 'is_bot'")
        commentator.narrate("  - Size nodes by 'viz_size' or 'sell_volume'")
        commentator.narrate("  - Filter by 'is_bot=true' to isolate bot network")
    except Exception as e:
        commentator.alert(f"Failed to export GEXF: {e}")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Token Forensic Analyzer - Comprehensive manipulation detection',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python token-forensic.py J2UXqJ1HYRwjCviLEqkY9mhoEPJ5UnoHEnbeijCweREV
    python token-forensic.py <token> --json report.json --markdown report.md
    python token-forensic.py <token> --start-date 2025-01-01 --dump-window 30
        """
    )
    parser.add_argument('token', help='Token mint address to analyze')
    parser.add_argument('--deep', action='store_true', help='Enable deep analysis (more thorough, slower)')
    parser.add_argument('--start-date', help='Start date filter (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date filter (YYYY-MM-DD)')
    parser.add_argument('--dump-window', type=int, default=DUMP_WINDOW_DEFAULT,
                        help=f'Time window for coordinated dump detection in seconds (default: {DUMP_WINDOW_DEFAULT})')
    parser.add_argument('--json', help='Export full report to JSON file')
    parser.add_argument('--markdown', help='Export professional report to Markdown file')
    parser.add_argument('--gexf', help='Export transaction graph to GEXF file (for Gephi)')
    parser.add_argument('--quiet', action='store_true', help='Suppress detailed console output')
    parser.add_argument('--db-host', default='localhost')
    parser.add_argument('--db-port', type=int, default=3396)
    parser.add_argument('--db-user', default='root')
    parser.add_argument('--db-pass', default='rootpassword')
    parser.add_argument('--db-name', default='t16o_db')

    args = parser.parse_args()

    # Initialize commentator
    commentator = ForensicCommentator(verbose=not args.quiet)

    # Parse date filters
    start_time = None
    end_time = None

    if args.start_date:
        dt = datetime.strptime(args.start_date, '%Y-%m-%d')
        start_time = int(dt.replace(tzinfo=timezone.utc).timestamp())

    if args.end_date:
        dt = datetime.strptime(args.end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        end_time = int(dt.replace(tzinfo=timezone.utc).timestamp())

    # Header
    print("")
    print("=" * 70)
    print("  T16O TOKEN FORENSIC ANALYZER")
    print("  Comprehensive Manipulation Detection System")
    print("=" * 70)
    print(f"  Token: {args.token}")
    if args.start_date:
        print(f"  Date Range: {args.start_date} to {args.end_date or 'present'}")
    print(f"  Dump Window: {args.dump_window}s")
    print("=" * 70)
    print("")

    # Database connection
    commentator.section("INITIALIZATION")
    commentator.narrate("Establishing database connection...")

    try:
        conn = mysql.connector.connect(
            host=args.db_host,
            port=args.db_port,
            user=args.db_user,
            password=args.db_pass,
            database=args.db_name
        )
        cursor = conn.cursor()
        commentator.finding("Database connection established")
    except Exception as e:
        commentator.critical(f"Database connection failed: {e}")
        return 1

    # Get token info
    token_info = get_token_info(cursor, args.token, commentator)

    # Fetch transaction data
    commentator.section("DATA COLLECTION")
    activities = get_all_token_activity(cursor, args.token, start_time, end_time, commentator)

    if not activities:
        commentator.critical("No transaction data found for this token")
        print("\nTo load data, run:")
        print(f"  python guide-producer.py {args.token} --max-signatures 50000")
        print(f"  python shredder-guide.py")
        return 1

    swaps = get_swap_activity(cursor, args.token, start_time, end_time, commentator)

    # Build wallet profiles
    profiles = build_wallet_profiles(activities, commentator)

    # Run detection algorithms
    bot_suspects = detect_bot_signatures(profiles, swaps, commentator)

    # THE JOE BUCK SPECIAL: Deep dive into funding wallets behind detected bots
    funding_investigations = deep_dive_bot_funders(bot_suspects, cursor, commentator)

    wash_pairs = detect_wash_trading(swaps, profiles, commentator)
    dump_events = detect_coordinated_dump(swaps, args.dump_window, commentator)
    sybil_clusters = detect_sybil_clusters(profiles, cursor, commentator)
    timeline = analyze_timeline(swaps, commentator)

    # Collect all bad actor wallets for cross-reference
    bad_actor_wallets = set()

    # Add bot suspects
    for bot in bot_suspects:
        bad_actor_wallets.add(bot['wallet'])

    # Add top sellers (top 15)
    sellers = [(addr, p) for addr, p in profiles.items() if p.total_sells > 0]
    sellers.sort(key=lambda x: x[1].sell_volume, reverse=True)
    for addr, _ in sellers[:15]:
        bad_actor_wallets.add(addr)

    # Add Sybil cluster members
    for cluster in sybil_clusters:
        for w in cluster.get('wallets', []):
            bad_actor_wallets.add(w)

    # Add coordinated dump participants
    for dump in dump_events:
        for w in dump.get('participating_wallets', []):
            bad_actor_wallets.add(w)

    # Add funding wallet operators
    for inv in funding_investigations:
        bad_actor_wallets.add(inv['funder_address'])

    # Cross-reference against theGuide
    cross_ref_results = cross_reference_bad_actors(
        cursor, list(bad_actor_wallets), args.token, commentator
    )

    # Build complete report
    report = build_full_report(
        token_info, activities, swaps, profiles,
        bot_suspects, wash_pairs, dump_events, sybil_clusters,
        timeline, commentator, funding_investigations, cross_ref_results
    )

    # Export JSON if requested
    if args.json:
        commentator.section("EXPORT")
        with open(args.json, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        commentator.finding(f"Full JSON report exported to: {args.json}")

    # Export Markdown if requested
    if args.markdown:
        md_content = generate_markdown_report(report)
        with open(args.markdown, 'w', encoding='utf-8') as f:
            f.write(md_content)
        commentator.finding(f"Professional Markdown report exported to: {args.markdown}")

    # Export GEXF graph if requested
    if args.gexf:
        G = generate_gexf_graph(
            token_info, activities, swaps, profiles,
            bot_suspects, sybil_clusters, dump_events,
            funding_investigations, commentator
        )
        export_gexf(G, args.gexf, commentator)

    # Final summary
    print("")
    print("=" * 70)
    print("  ANALYSIS COMPLETE")
    print("=" * 70)
    summary = report['executive_summary']
    print(f"  Token: {summary['token']} ({summary['token_name']})")
    print(f"  Manipulation Score: {summary['manipulation_score']}/100")
    print(f"  Assessment: {summary['assessment']}")
    print("")
    if args.json:
        print(f"  JSON Report: {args.json}")
    if args.markdown:
        print(f"  Markdown Report: {args.markdown}")
    if args.gexf:
        print(f"  GEXF Graph: {args.gexf}")
    print("=" * 70)

    cursor.close()
    conn.close()

    return 0


if __name__ == '__main__':
    sys.exit(main())
