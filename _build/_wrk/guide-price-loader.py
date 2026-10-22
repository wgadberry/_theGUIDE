#!/usr/bin/env python3
"""
Guide Price Loader - Load historical token prices from Solscan API

Queries tx_guide for distinct tokens by day, then fetches prices from Solscan
and stores them in tx_token_price table.

Usage:
    python guide-price-loader.py                     # Process all days (50+ activities)
    python guide-price-loader.py --days 7            # Process last 7 days only
    python guide-price-loader.py --date 20251210     # Process specific date
    python guide-price-loader.py --skip-existing     # Skip tokens that already have prices
    python guide-price-loader.py --force             # Re-fetch and overwrite existing prices
    python guide-price-loader.py --min-activities 100  # Only tokens with 100+ activities
"""

import argparse
import json
import time
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, List

# MySQL connector
try:
    import mysql.connector
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False


# Configuration
SOLSCAN_API = "https://pro-api.solscan.io/v2.0"
SOLSCAN_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjcmVhdGVkQXQiOjE3NTg0MzQzODI5MTEsImVtYWlsIjoid2dhZGJlcnJ5QGdtYWlsLmNvbSIsImFjdGlvbiI6InRva2VuLWFwaSIsImFwaVZlcnNpb24iOiJ2MiIsImlhdCI6MTc1ODQzNDM4Mn0.d_ZYyfZGaFdLfYXsuB_M2rT_Q6WkwpxVp0ZqNE_zUAk"

# Rate limiting
API_DELAY = 0.25  # seconds between API calls


class PriceLoader:
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        self.session = requests.Session()
        self.session.headers.update({"token": SOLSCAN_TOKEN})

        # Stats
        self.api_calls = 0
        self.prices_loaded = 0
        self.errors = 0
        self.skipped = 0

    def connect_db(self):
        """Connect to MySQL database"""
        self.conn = mysql.connector.connect(**self.db_config)
        self.cursor = self.conn.cursor(dictionary=True)

    def close_db(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def get_tokens_by_day(self, specific_date: Optional[int] = None,
                          days_limit: Optional[int] = None,
                          min_activities: int = 50) -> List[Dict]:
        """
        Get distinct tokens for each day from tx_guide.
        Only includes tokens with at least min_activities guide records.
        """

        # First, get tokens with sufficient activity
        activity_filter = ""
        if min_activities > 0:
            activity_filter = f"""
                AND g.token_id IN (
                    SELECT token_id FROM tx_guide
                    WHERE token_id IS NOT NULL
                    GROUP BY token_id
                    HAVING COUNT(*) >= {min_activities}
                )
            """

        query = f"""
            SELECT
                block_time_num,
                COUNT(*) AS token_count,
                JSON_ARRAYAGG(mint_address) AS mint_addresses
            FROM (
                SELECT DISTINCT
                    CAST(DATE_FORMAT(FROM_UNIXTIME(g.block_time), '%Y%m%d') AS UNSIGNED) AS block_time_num,
                    mint.address AS mint_address
                FROM tx_guide g
                JOIN tx_token tk ON g.token_id = tk.id
                JOIN tx_address mint ON tk.mint_address_id = mint.id
                WHERE g.token_id IS NOT NULL
                {activity_filter}
        """

        params = []

        if specific_date:
            query += " AND CAST(DATE_FORMAT(FROM_UNIXTIME(g.block_time), '%Y%m%d') AS UNSIGNED) = %s"
            params.append(specific_date)
        elif days_limit:
            cutoff = datetime.now() - timedelta(days=days_limit)
            cutoff_ts = int(cutoff.timestamp())
            query += " AND g.block_time >= %s"
            params.append(cutoff_ts)

        query += """
            ) AS distinct_tokens
            GROUP BY block_time_num
            ORDER BY block_time_num DESC
        """

        self.cursor.execute(query, params)
        results = []

        for row in self.cursor.fetchall():
            mint_addresses = row['mint_addresses']
            if isinstance(mint_addresses, str):
                mint_addresses = json.loads(mint_addresses)

            results.append({
                'date': row['block_time_num'],
                'token_count': row['token_count'],
                'mint_addresses': mint_addresses
            })

        return results

    def get_token_id(self, mint_address: str) -> Optional[int]:
        """Get token_id from tx_token for a mint address"""
        self.cursor.execute("""
            SELECT tk.id
            FROM tx_token tk
            JOIN tx_address mint ON tk.mint_address_id = mint.id
            WHERE mint.address = %s
        """, (mint_address,))

        row = self.cursor.fetchone()
        return row['id'] if row else None

    def price_exists(self, token_id: int, date_num: int) -> bool:
        """Check if price already exists for this token/date"""
        date_str = str(date_num)
        date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

        self.cursor.execute("""
            SELECT 1 FROM tx_token_price
            WHERE token_id = %s AND date = %s
        """, (token_id, date_formatted))

        return self.cursor.fetchone() is not None

    def fetch_price(self, mint_address: str, date_num: int) -> Optional[float]:
        """Fetch token price from Solscan API"""
        url = f"{SOLSCAN_API}/token/price"
        params = {
            "address": mint_address,
            "from_time": date_num,
            "to_time": date_num
        }

        try:
            self.api_calls += 1
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            result = response.json()

            if result.get("success") and result.get("data"):
                for price_data in result["data"]:
                    if price_data.get("date") == date_num:
                        return price_data.get("price")

            return None

        except Exception as e:
            print(f"    API error: {e}")
            self.errors += 1
            return None

    def save_price(self, token_id: int, date_num: int, price: float) -> bool:
        """Save price to tx_token_price table"""
        try:
            date_str = str(date_num)
            date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

            self.cursor.execute("""
                INSERT INTO tx_token_price (token_id, date, price)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE price = VALUES(price)
            """, (token_id, date_formatted, price))

            self.conn.commit()
            self.prices_loaded += 1
            return True

        except Exception as e:
            print(f"    DB error: {e}")
            self.conn.rollback()
            self.errors += 1
            return False

    def process_day(self, date_num: int, mint_addresses: List[str],
                    skip_existing: bool = False, force: bool = False):
        """Process all tokens for a single day"""
        date_str = str(date_num)
        date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

        print(f"\n{'='*60}")
        print(f"Processing date: {date_formatted} ({len(mint_addresses)} tokens)")
        print(f"{'='*60}")

        for i, mint in enumerate(mint_addresses):
            # Get token_id
            token_id = self.get_token_id(mint)
            if not token_id:
                print(f"  [{i+1}/{len(mint_addresses)}] {mint[:20]}... - token not found")
                continue

            # Check if price exists (skip unless force)
            if not force and skip_existing and self.price_exists(token_id, date_num):
                self.skipped += 1
                continue

            # Fetch price from API
            price = self.fetch_price(mint, date_num)

            if price is not None:
                self.save_price(token_id, date_num, price)
                print(f"  [{i+1}/{len(mint_addresses)}] {mint[:20]}... = ${price:.10f}")
            else:
                print(f"  [{i+1}/{len(mint_addresses)}] {mint[:20]}... - no price data")

            time.sleep(API_DELAY)

    def print_summary(self):
        """Print processing summary"""
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        print(f"API calls:     {self.api_calls}")
        print(f"Prices loaded: {self.prices_loaded}")
        print(f"Skipped:       {self.skipped}")
        print(f"Errors:        {self.errors}")


def main():
    parser = argparse.ArgumentParser(description='Load token prices from Solscan API')
    parser.add_argument('--date', type=int, help='Process specific date (YYYYMMDD)')
    parser.add_argument('--days', type=int, help='Process last N days only')
    parser.add_argument('--skip-existing', action='store_true',
                        help='Skip tokens that already have prices for a given day')
    parser.add_argument('--force', action='store_true',
                        help='Re-fetch and overwrite prices even if they already exist')
    parser.add_argument('--min-activities', type=int, default=50,
                        help='Only process tokens with at least N guide activities (default: 50, 0=all)')
    parser.add_argument('--db-host', default='localhost', help='MySQL host')
    parser.add_argument('--db-port', type=int, default=3396, help='MySQL port')
    parser.add_argument('--db-user', default='root', help='MySQL user')
    parser.add_argument('--db-pass', default='rootpassword', help='MySQL password')
    parser.add_argument('--db-name', default='t16o_db', help='MySQL database')

    args = parser.parse_args()

    if not HAS_MYSQL:
        print("Error: mysql-connector-python not installed")
        return 1

    print("PRICE LOADER")
    print(f"{'='*60}")
    print(f"Min activities: {args.min_activities}")
    if args.days:
        print(f"Days limit: {args.days}")
    if args.date:
        print(f"Specific date: {args.date}")
    if args.skip_existing:
        print(f"Skip existing: Yes")
    if args.force:
        print(f"Force refresh: Yes")
    print()

    db_config = {
        'host': args.db_host,
        'port': args.db_port,
        'user': args.db_user,
        'password': args.db_pass,
        'database': args.db_name
    }

    loader = PriceLoader(db_config)
    loader.connect_db()

    try:
        # Get tokens by day
        print(f"Querying tokens by day (min {args.min_activities} activities)...")
        days_data = loader.get_tokens_by_day(
            specific_date=args.date,
            days_limit=args.days,
            min_activities=args.min_activities
        )

        total_tokens = sum(d['token_count'] for d in days_data)
        print(f"Found {len(days_data)} days with {total_tokens} total token/day combinations")

        if not days_data:
            print("No data to process")
            return 0

        # Process each day
        for day_data in days_data:
            loader.process_day(
                day_data['date'],
                day_data['mint_addresses'],
                skip_existing=args.skip_existing,
                force=args.force
            )

        loader.print_summary()

    finally:
        loader.close_db()

    return 0


if __name__ == '__main__':
    exit(main())
