# SOLTIT Token Forensic Analysis Report

**Generated:** 2025-12-19
**Token:** SOLTIT (J2UXqJ1HYRwjCviLEqkY9mhoEPJ5UnoHEnbeijCweREV)
**Period:** 2025-10-07 to 2025-12-19 (73 days)

---

## Executive Summary

SOLTIT shows clear evidence of **insider distribution** and **coordinated trading activity**. The top "profitable" wallet sold 714M tokens without ever purchasing or receiving tokens through normal transfers - indicating pre-loaded insider supply. Multiple wallet clusters received tokens from the same distributors within minutes of launch.

### Key Findings

| Metric | Value |
|--------|-------|
| Total Wallets | 456 |
| Profitable Wallets | 148 (32.5%) |
| Losing Wallets | 57 (12.5%) |
| Total Profits | +171.91 SOL |
| Total Losses | -33.81 SOL |
| Net to Traders | +138.10 SOL |
| Coordinated Groups | 6 identified |
| First-Hour Wallets | 146 |

---

## 1. Top Winner Analysis: BTmqi8V5...

**Wallet:** `BTmqi8V5WvMBiXhEDf7zhncFoxFczQREyQNcKLkhzEZS`

### Red Flags

1. **No Token Purchases**: 0 buys recorded - never purchased SOLTIT
2. **Massive Sales**: 376 sell transactions totaling 714,741,313 SOLTIT
3. **Pre-Loaded Supply**: Only received 10 tokens via transfer, yet sold 714M
4. **Immediate Activity**: Started selling 25 minutes after token launch
5. **Consistent Dumping**: Sold every single day from launch to Dec 6

### Activity Pattern

| Period | Sell Count | Tokens Sold |
|--------|------------|-------------|
| Launch Day (Oct 7) | 202 | 345M |
| Week 1 | 238 | ~430M |
| Remaining Period | 138 | ~285M |

**Conclusion**: This wallet appears to be an **insider/team wallet** that received initial token allocation and systematically dumped on retail buyers.

---

## 2. Coordinated Wallet Groups

### Group 1: 5LMtwSn45... Distribution Network (11 wallets)

**Hub Distributor:** `5LMtwSn45ttbcUfqnEbyT9PwEuTYbQ3LgHEHSTD4...`
**Total Distributed:** 48,901,376 SOLTIT

Recipients received between 1M-12M tokens each shortly after launch.

| Recipient | Amount |
|-----------|--------|
| BjuEs4J... | 12,000,000 |
| 9gsJ6f1... | 9,040,000 |
| CmmAmcF... | 7,150,000 |
| LCJ2bM8... | 5,000,000 |
| GJDzNW1... | 5,000,000 |
| *+6 more* | ~10,700,000 |

### Group 2: 6qZRYps... Initial Distribution (5 wallets)

**Hub Distributor:** `6qZRYpsJbx9WDtDfUkbCwopMGK6eh2yEP25Nzb2J...`
**Total Distributed:** 90,000,000 SOLTIT

This appears to be a **primary distribution wallet** - possibly team-controlled.

| Recipient | Amount | Time After Launch |
|-----------|--------|-------------------|
| 628Rzrm... | 20,000,000 | +6 min |
| BjuEs4J... | 20,000,000 | +6 min |
| 4c1D6iX... | 20,000,000 | +5 min |
| H9kufed... | 20,000,000 | +4 min |
| HGezrug... | 10,000,000 | +8 min |

**Note:** BjuEs4J... appears in BOTH Group 1 and Group 2, indicating potential sybil network overlap.

### Additional Groups (3-4 wallets each)

- **8LzQdFq...** distributed to 4 wallets
- **BPrNYyE...** distributed to 3 wallets
- **F4KHDgy...** distributed to 3 wallets
- **HLnpSz9...** distributed to 3 wallets

---

## 3. Coordinated Trading Events

### Highest Coordination Windows

| Time | Event Type | Unique Wallets |
|------|------------|----------------|
| 2025-10-06 21:05 | Buying Frenzy | 25 buyers |
| 2025-10-06 21:00 | Mixed | 24 buyers, 10 sellers |
| 2025-12-12 19:45 | Dump | 18 sellers |
| 2025-10-07 16:30 | Dump | 16 sellers |
| 2025-10-08 22:55 | Dump | 14 sellers |

**Pattern**: Large coordinated buying at launch, followed by coordinated selling dumps.

---

## 4. Launch Window Analysis

**Token Launch:** 2025-10-06 20:36:23 UTC

### First 20 Wallets to Interact

| Time | Wallet | Likely Role |
|------|--------|-------------|
| +0s | yudkpbXU7... | Deployer/Creator |
| +0s | 6qZRYpsJ... | Primary Distributor |
| +4-8 min | H9kufed, 4c1D6iX, BjuEs4J, 628Rzrm, HGezrug | Initial Recipients |
| +24 min | 10+ wallets | First Public Buyers |

---

## 5. Weekly Activity Breakdown

| Week | Buys | Sells | SOL In | SOL Out |
|------|------|-------|--------|---------|
| Oct 6 (Launch) | 565 | 578 | 9.95 | 7.42 |
| Oct 13 | 352 | 207 | 20.62 | 7.81 |
| Oct 20 | 96 | 82 | 8.61 | 15.22 |
| Oct 27 | 147 | 136 | 22.51 | 19.44 |
| ... | ... | ... | ... | ... |
| **Dec 8** | **109** | **200** | **18.66** | **131.36** |
| Dec 15 | 17 | 62 | 0.80 | 27.24 |

**Dec 8 Dump Event**: Massive coordinated exit - 131 SOL extracted by sellers.

---

## 6. Organic Winners Analysis

After excluding insiders, early wallets, and large token recipients, we analyzed who actually profited through legitimate trading.

### Filtering Criteria

| Filter | Wallets Excluded |
|--------|------------------|
| Known insiders | 3 |
| Large recipients (>5M tokens via transfer) | ~100 |
| Early wallets (first 10 min) | ~7 |
| Infrastructure (programs, pools) | ~17,000 |

### Results: Almost No Pure Organic Winners

| Category | Count | Total Profit |
|----------|-------|--------------|
| **Pure Organic** (>=95% bought) | **0** | **0 SOL** |
| Mostly Organic (50-95% bought) | 15 | 2.28 SOL |
| Mixed (<50% bought) | 17 | ~1 SOL |

**Key Finding**: There are ZERO wallets that profited purely from buying and selling SOLTIT on the open market. Every profitable trader either:
- Received tokens via direct transfer (from insiders/distributors)
- Was active in the first 10 minutes (insider timing)
- Received large allocations (>5M tokens)

### Top "Mostly Organic" Winners

| Wallet | Profit | Spent | Bought% |
|--------|--------|-------|---------|
| HbpeMcQY... | +0.91 SOL | 0.10 | 50% |
| 4epJ3qUB... | +0.66 SOL | 0.04 | 50% |
| JBFfY8KM... | +0.14 SOL | 0.19 | 50% |
| DzjssgqvU... | +0.11 SOL | 0.12 | 50% |
| B9hgwkgD... | +0.10 SOL | 0.01 | 50% |

Note: 50% bought means they received the other 50% via transfer - likely from insider connections.

### Conclusion

**SOLTIT appears to be a zero-sum (or negative-sum) game for retail traders.** All meaningful profits went to:
- Pre-loaded insider wallets
- Coordinated distribution recipients
- Early participants with insider timing

---

## 7. Risk Assessment

### High-Risk Indicators

1. **Insider Pre-Loading**: Top seller never bought tokens
2. **Rapid Distribution**: 90M tokens distributed in first 10 minutes
3. **Sybil Clusters**: Same wallets receiving from multiple distributors
4. **Coordinated Dumps**: Multiple 10+ wallet selling events
5. **Asymmetric P&L**: Insiders profitable, retail losses

### Wallet Classification

| Category | Count | Behavior |
|----------|-------|----------|
| Likely Insiders | ~15 | Pre-loaded, systematic selling |
| Coordinated Group | ~30 | Received from same sources |
| Retail Losers | ~57 | Bought, never sold |
| Profitable Traders | ~148 | Mixed (some likely insiders) |

---

## Files Generated

- `soltit---wallet_pnl.csv` - Per-wallet profit/loss breakdown
- `soltit---weekly_activity.csv` - Weekly aggregate activity
- `soltit---coordination_analysis.json` - Full coordination data
- `soltit---organic_winners.csv` - Organic winners (non-insider) analysis

---

*Analysis performed using theGuide transaction analytics system*
