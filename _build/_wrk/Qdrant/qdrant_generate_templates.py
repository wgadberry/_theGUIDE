"""
qdrant_generate_templates.py
─────────────────────────────────────────────────────────────────────────────
Generate canonical narrative text for each edge pattern in tx_guide_template,
embed via OpenAI, and store the vectors back in MySQL.

Prerequisites:
    1. tx_guide_template table exists
    2. sp_tx_guide_template_discover has been run (patterns populated)

Steps:
    1. Read all patterns with use_template=1 from tx_guide_template
    2. Build a canonical narrative string for each pattern
    3. Embed all narratives in one OpenAI batch call
    4. UPDATE tx_guide_template SET narrative=..., embedding=... for each

Usage:
    python3 qdrant_generate_templates.py              # generate + embed
    python3 qdrant_generate_templates.py --dry-run    # show narratives only

Author : theGuide / T16O
─────────────────────────────────────────────────────────────────────────────
"""

import argparse
import json
import logging
import os
import sys

import pymysql
import requests

from qdrant_client_t16o import VECTOR_DIM

# ─── Logging ─────────────────────────────────────────────────────────────────

log = logging.getLogger("t16o.templates")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ─── Config ──────────────────────────────────────────────────────────────────

_config_candidates = [
    os.path.join(os.path.dirname(__file__), '..', 'guide-config.json'),
    os.path.join(os.path.dirname(__file__), 'guide-config.json'),
    os.path.join(os.getcwd(), 'guide-config.json'),
]
_cfg = {}
for _cp in _config_candidates:
    if os.path.exists(_cp):
        with open(_cp) as f:
            _cfg = json.load(f)
        break

MYSQL_HOST     = _cfg.get('DB_HOST', '34.196.15.116')
MYSQL_PORT     = _cfg.get('DB_PORT', 3397)
MYSQL_USER     = _cfg.get('DB_USER', 'root')
MYSQL_PASSWORD = _cfg.get('DB_PASSWORD', 'rootpassword')
MYSQL_DB       = _cfg.get('DB_NAME', 't16o_db')

OPENAI_API_KEY = _cfg.get('QDRANT_API', '')
OPENAI_MODEL   = "text-embedding-3-small"
OPENAI_DIM     = VECTOR_DIM  # 384


def get_connection():
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=MYSQL_DB, charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=30, read_timeout=120,
    )


# ─── Narrative Builder ──────────────────────────────────────────────────────

# Human-readable descriptions for address types
ADDRESS_TYPE_DESC = {
    "wallet":      "a user wallet",
    "pool":        "a liquidity pool",
    "vault":       "a vault (PDA or token account)",
    "ata":         "an associated token account",
    "mint":        "a mint authority",
    "lp_token":    "an LP token mint",
    "dex_wallet":  "a DEX-owned wallet",
    "program":     "a program account",
    "bot":         "a bot/MEV wallet",
    "market":      "a market account",
    "unknown":     "an unknown address",
}

# Human-readable descriptions for edge categories
CATEGORY_DESC = {
    "transfer":    "token transfer",
    "swap":        "token swap",
    "account":     "account lifecycle operation",
    "lending":     "lending protocol operation",
    "other":       "miscellaneous operation",
}

DIRECTION_DESC = {
    "outflow":  "outgoing",
    "inflow":   "incoming",
    "neutral":  "neutral",
}


BALANCE_SIGNAL_DESC = {
    "drained":  "Sender's token balance was fully drained to zero (wallet emptied). "
                "This is a high-significance forensic signal — may indicate exit, rug pull, or liquidation.",
    "funded":   "Receiver had zero token balance before this transfer (first deposit). "
                "May indicate new wallet funding, airdrop receipt, or fresh position entry.",
    "large":    "Transfer amount exceeds 90% of sender's pre-balance (near-total transfer). "
                "Significant movement — may indicate consolidation or withdrawal.",
    "dust":     "Transfer amount is extremely small (< 10,000 raw units). "
                "May indicate dust attack, test transaction, or token spam.",
    "normal":   "Standard partial transfer — sender retains significant balance after the transaction.",
}


def build_template_narrative(row: dict) -> str:
    """Build a canonical narrative string for an edge pattern.

    Captures the structural meaning of the pattern AND the financial
    significance (balance signal) without specific addresses, amounts,
    or token names (those live in the Qdrant payload).
    """
    etc = row["edge_type_code"]
    cat = row["edge_category"]
    direction = row["edge_direction"]
    fat = row["from_address_type"]
    tat = row["to_address_type"]
    has_pool = bool(row["has_pool"])
    has_tax = bool(row["has_tax"])
    swap_dir = row["swap_direction"]
    bal_signal = row.get("balance_signal", "normal")

    # Core description
    cat_desc = CATEGORY_DESC.get(cat, cat)
    dir_desc = DIRECTION_DESC.get(direction, direction)
    from_desc = ADDRESS_TYPE_DESC.get(fat, fat)
    to_desc = ADDRESS_TYPE_DESC.get(tat, tat)

    parts = [
        f"Edge type: {etc} [{cat}/{direction}]",
        f"This is an {dir_desc} {cat_desc} from {from_desc} ({fat}) to {to_desc} ({tat}).",
    ]

    amt_mag = row.get("amount_magnitude", "medium")
    lbl_signal = row.get("label_signal", "unlabeled")

    # Balance signal — the forensic dimension
    bal_desc = BALANCE_SIGNAL_DESC.get(bal_signal, "")
    if bal_desc:
        parts.append(f"Balance signal: {bal_signal}. {bal_desc}")

    # Amount magnitude
    mag_desc = {
        "micro":  "Extremely small amount (< 1M raw units). Likely dust, fees, or rounding.",
        "small":  "Small amount (1M–1B raw units). Minor transfer or partial position.",
        "medium": "Medium amount (1B–1T raw units). Significant but not extraordinary.",
        "whale":  "Very large amount (> 1T raw units). Whale-scale movement — high forensic interest.",
    }
    md = mag_desc.get(amt_mag, "")
    if md:
        parts.append(f"Amount magnitude: {amt_mag}. {md}")

    # Label signal — address identity context
    lbl_desc = {
        "dex_labeled":      "One or both addresses are labeled as a DEX entity (Raydium, Orca, Jupiter, Meteora, Pump.fun, OpenBook).",
        "cex_labeled":      "One or both addresses are labeled as a centralized exchange (Binance, Coinbase, Kraken, OKX, Bybit).",
        "burn_labeled":     "One or both addresses are labeled as a burn wallet. Tokens are being permanently destroyed.",
        "authority":        "One or both addresses are labeled as an authority account (vault authority, program authority).",
        "labeled":          "One or both addresses have a known label (not DEX/CEX/burn specific).",
        "unlabeled":        "Both addresses are unlabeled — unknown entities.",
    }
    ld = lbl_desc.get(lbl_signal, "")
    if ld:
        parts.append(f"Address context: {lbl_signal}. {ld}")

    # Swap details
    if swap_dir and swap_dir != "none":
        parts.append(f"Swap direction: {swap_dir} (token {'purchased' if swap_dir == 'out' else 'sold'} via DEX).")

    # Pool involvement
    if has_pool:
        parts.append("Involves a liquidity pool. Pool address, DEX name, and token pair are available.")
    else:
        parts.append("No pool involvement — direct transfer between addresses.")

    # Tax
    if has_tax:
        parts.append("Transaction includes a token tax (transfer fee). Tax basis points and amount are recorded.")

    # Pattern-specific context
    if etc == "close_ata":
        parts.append("ATA closure: associated token account closed, remaining balance returned to owner.")
    elif etc == "mint" and fat == "lp_token":
        parts.append("LP token minting: liquidity provider received LP tokens after depositing to pool.")
    elif etc == "burn" and tat == "lp_token":
        parts.append("LP token burning: liquidity provider withdrew from pool by burning LP tokens.")
    elif etc == "lend_deposit":
        parts.append("Lending deposit: tokens deposited into a lending protocol vault.")
    elif etc == "lend_withdraw":
        parts.append("Lending withdrawal: tokens withdrawn from a lending protocol vault.")
    elif cat == "transfer" and fat == "wallet" and tat == "wallet":
        parts.append("Peer-to-peer token transfer between two user wallets.")
    elif cat == "transfer" and tat == "vault":
        parts.append("Tokens sent to a vault — may indicate staking, protocol deposit, or custody.")
    elif cat == "swap" and fat == "wallet" and tat == "pool":
        parts.append("User-initiated swap: tokens sent from wallet to pool as swap input.")
    elif cat == "swap" and fat == "pool" and tat == "wallet":
        parts.append("Swap output: tokens received from pool to wallet after swap execution.")

    # Row count context
    cnt = row.get("row_count", 0)
    pct = row.get("pct_of_total", 0)
    parts.append(f"Pattern frequency: {cnt:,} edges ({pct}% of all tx_guide records).")

    return "\n".join(parts)


# ─── OpenAI Embedding ───────────────────────────────────────────────────────

def embed_texts(texts: list[str], chunk_size: int = 100) -> list[list[float]]:
    """Embed texts via OpenAI API in chunks to stay under TPM limits."""
    import time as _time
    if not texts:
        return []

    all_embeddings = []
    for i in range(0, len(texts), chunk_size):
        chunk = texts[i:i + chunk_size]
        for attempt in range(5):
            resp = requests.post(
                "https://api.openai.com/v1/embeddings",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": OPENAI_MODEL,
                    "input": chunk,
                    "dimensions": OPENAI_DIM,
                },
                timeout=60,
            )
            if resp.status_code == 429:
                wait = min(2 ** attempt * 2, 30)
                log.warning("Rate limited on chunk %d/%d, waiting %ds...",
                            i // chunk_size + 1, -(-len(texts) // chunk_size), wait)
                _time.sleep(wait)
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"OpenAI API error {resp.status_code}: {resp.text[:300]}")
            data = resp.json()
            embeddings = sorted(data["data"], key=lambda x: x["index"])
            all_embeddings.extend([e["embedding"] for e in embeddings])
            log.info("Embedded chunk %d/%d (%d texts)",
                     i // chunk_size + 1, -(-len(texts) // chunk_size), len(chunk))
            break
        else:
            raise RuntimeError("OpenAI API rate limit exceeded after 5 retries")

        # Brief pause between chunks to spread TPM usage
        if i + chunk_size < len(texts):
            _time.sleep(2)

    return all_embeddings


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate template narratives and embeddings")
    parser.add_argument("--dry-run", action="store_true", help="Show narratives only, don't embed or save")
    parser.add_argument("--all", action="store_true", help="Generate for all patterns, not just use_template=1")
    args = parser.parse_args()

    if not args.dry_run and not OPENAI_API_KEY:
        log.error("QDRANT_API key not found in guide-config.json")
        sys.exit(1)

    conn = get_connection()

    # Fetch patterns
    where = "" if args.all else "WHERE use_template = 1"
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM tx_guide_template {where} ORDER BY row_count DESC")
        patterns = cur.fetchall()

    log.info("Found %d patterns to process", len(patterns))

    if not patterns:
        log.info("No patterns found. Run: CALL sp_tx_guide_template_discover(100)")
        conn.close()
        return

    # Build narratives
    narratives = []
    for p in patterns:
        narrative = build_template_narrative(p)
        narratives.append(narrative)
        if args.dry_run:
            print(f"\n{'='*70}")
            print(f"Pattern #{p['id']} | {p['edge_type_code']} | "
                  f"{p['from_address_type']}→{p['to_address_type']} | "
                  f"rows={p['row_count']:,} ({p['pct_of_total']}%)")
            print(f"{'='*70}")
            print(narrative)

    if args.dry_run:
        print(f"\n{len(patterns)} narratives generated (dry run, not embedded)")
        conn.close()
        return

    # Embed all narratives (batch — ~89 texts, well within OpenAI limits)
    log.info("Embedding %d narratives via OpenAI %s...", len(narratives), OPENAI_MODEL)
    embeddings = embed_texts(narratives)
    log.info("Got %d embeddings (dim=%d)", len(embeddings), len(embeddings[0]) if embeddings else 0)

    # Save back to MySQL
    updated = 0
    with conn.cursor() as cur:
        for i, p in enumerate(patterns):
            cur.execute(
                "UPDATE tx_guide_template SET narrative = %s, embedding = %s WHERE id = %s",
                (narratives[i], json.dumps(embeddings[i]), p["id"])
            )
            updated += 1

    conn.commit()
    conn.close()

    log.info("Done: %d templates updated with narratives + embeddings", updated)


if __name__ == "__main__":
    main()
