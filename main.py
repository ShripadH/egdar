import argparse
import sys
import pandas as pd
from pymongo import MongoClient

# ----------------------------------------------------------------------
# Configuration & Constants
# ----------------------------------------------------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "sec_13f_db"
COLLECTION_NAME = "holdings"

# Simple Ticker-to-CUSIP mapping registry
# (Extend this dictionary or plug in OpenFIGI API)
TICKER_TO_CUSIP = {
    # Map ticker to a LIST of valid current and historical CUSIPs
    "AAPL": ["037833100"],
    "TSLA": ["88160R101"],
    "GOOGL": ["02079K305", "02079K107"], # Maps both Class A and Class C
}


def get_mongo_collection():
    """Connects to local MongoDB collection."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION_NAME]


def calculate_previous_quarter(year: int, quarter: int) -> tuple[int, int]:
    """Calculates the preceding (Year, Quarter) tuple."""
    if quarter == 1:
        return year - 1, 4
    return year, quarter - 1


def query_13f_report(ticker: str, year: int, quarter: int, top_n: int = 10):
    """
    Queries local MongoDB collection for target and previous quarter holdings,
    then calculates net position deltas for buyers, sellers, and top holders.
    """
    cusip = TICKER_TO_CUSIP.get(ticker.upper())
    if not cusip:
        raise ValueError(f"❌ CUSIP mapping for ticker '{ticker}' not found. Please add it to TICKER_TO_CUSIP.")

    prev_year, prev_q = calculate_previous_quarter(year, quarter)
    collection = get_mongo_collection()

    print(f"\n🔍 Querying MongoDB for {ticker.upper()} (CUSIP: {cusip})...")
    print(f"   Current Period:  {year} Q{quarter}")
    print(f"   Baseline Period: {prev_year} Q{prev_q}\n")

    # Accept either a single string or a list of CUSIPs
    cusip_input = TICKER_TO_CUSIP.get(ticker.upper())
    cusip_list = [cusip_input] if isinstance(cusip_input, str) else cusip_input

    # Fetch records matching ANY of the CUSIPs in the array
    curr_cursor = collection.find(
        {
            "cusip": {"$in": cusip_list},
            "year": year,
            "quarter": quarter
        },
        {"_id": 0, "cik": 1, "filer_name": 1, "shares": 1, "value": 1}
    )
    curr_docs = list(curr_cursor)

    # 2. Fetch Previous Quarter Records
    prev_cursor = collection.find(
        {"cusip": cusip, "year": prev_year, "quarter": prev_q},
        {"_id": 0, "cik": 1, "filer_name": 1, "shares": 1}
    )
    prev_docs = list(prev_cursor)

    if not curr_docs:
        print(f"⚠️ No documents found in MongoDB for {ticker.upper()} in {year} Q{quarter}.")
        print("   Make sure you have ingested this quarter's dataset.")
        return

    # Convert to DataFrames
    df_curr = pd.DataFrame(curr_docs)
    df_prev = pd.DataFrame(prev_docs) if prev_docs else pd.DataFrame(columns=['cik', 'filer_name', 'shares'])

    # Format numbers for output display
    df_curr['shares'] = pd.to_numeric(df_curr['shares'], errors='coerce').fillna(0).astype(int)
    df_curr['value'] = pd.to_numeric(df_curr['value'], errors='coerce').fillna(0).astype(int)

    # ------------------------------------------------------------------
    # 1. TOP INVESTORS (Quarter Q)
    # ------------------------------------------------------------------
    top_investors = (
        df_curr.sort_values(by='shares', ascending=False)
        .head(top_n)[['cik', 'filer_name', 'shares', 'value']]
    )

    # ------------------------------------------------------------------
    # 2. COMPUTING POSITION DELTAS (Quarter Q vs Quarter Q-1)
    # ------------------------------------------------------------------
    if df_prev.empty:
        print(
            f"⚠️ Baseline period {prev_year} Q{prev_q} not found in MongoDB. Buyer/Seller deltas will treat previous holdings as 0.")

    # Outer join to capture new positions, existing adjustments, and complete liquidations
    comparison = pd.merge(
        df_curr,
        df_prev,
        on='cik',
        how='outer',
        suffixes=('_curr', '_prev')
    )

    # Coalesce Filer Names & Fill missing share counts with 0
    comparison['filer_name'] = comparison['filer_name_curr'].combine_first(comparison['filer_name_prev'])
    comparison['shares_curr'] = comparison['shares_curr'].fillna(0).astype(int)
    comparison['shares_prev'] = comparison['shares_prev'].fillna(0).astype(int)

    # Calculate net share delta
    comparison['share_change'] = comparison['shares_curr'] - comparison['shares_prev']

    # Calculate percentage change
    comparison['pct_change'] = comparison.apply(
        lambda row: ((row['share_change'] / row['shares_prev']) * 100) if row['shares_prev'] > 0 else 100.0,
        axis=1
    )

    # ------------------------------------------------------------------
    # 3. TOP BUYERS (Positive Delta)
    # ------------------------------------------------------------------
    top_buyers = (
        comparison[comparison['share_change'] > 0]
        .sort_values(by='share_change', ascending=False)
        .head(top_n)[['cik', 'filer_name', 'shares_prev', 'shares_curr', 'share_change', 'pct_change']]
    )

    # ------------------------------------------------------------------
    # 4. TOP SELLERS (Negative Delta)
    # ------------------------------------------------------------------
    top_sellers = (
        comparison[comparison['share_change'] < 0]
        .sort_values(by='share_change', ascending=True)
        .head(top_n)[['cik', 'filer_name', 'shares_prev', 'shares_curr', 'share_change', 'pct_change']]
    )

    # Output formatted tables
    display_results(ticker.upper(), year, quarter, top_investors, top_buyers, top_sellers)


def display_results(ticker: str, year: int, quarter: int, top_inv, top_buy, top_sell):
    """Prints clean tabular output for the CLI."""
    print("=" * 80)
    print(f"📊 13F INSTITUTIONAL REPORT: {ticker} ({year} Q{quarter})")
    print("=" * 80)

    print("\n🏆 1. TOP INVESTORS (Largest Share Holders)")
    print("-" * 80)
    if not top_inv.empty:
        top_inv_display = top_inv.copy()
        top_inv_display['shares'] = top_inv_display['shares'].apply(lambda x: f"{x:,.0f}")
        top_inv_display['value ($K)'] = top_inv_display['value'].apply(lambda x: f"${x:,.0f}")
        print(top_inv_display[['cik', 'filer_name', 'shares', 'value ($K)']].to_string(index=False))
    else:
        print("No investor records found.")

    print("\n📈 2. TOP BUYERS (Largest Net Position Increases)")
    print("-" * 80)
    if not top_buy.empty:
        top_buy_display = top_buy.copy()
        top_buy_display['shares_prev'] = top_buy_display['shares_prev'].apply(lambda x: f"{x:,.0f}")
        top_buy_display['shares_curr'] = top_buy_display['shares_curr'].apply(lambda x: f"{x:,.0f}")
        top_buy_display['share_change'] = top_buy_display['share_change'].apply(lambda x: f"+{x:,.0f}")
        top_buy_display['pct_change'] = top_buy_display['pct_change'].apply(lambda x: f"+{x:.1f}%")
        print(top_buy_display[
                  ['cik', 'filer_name', 'shares_prev', 'shares_curr', 'share_change', 'pct_change']].to_string(
            index=False))
    else:
        print("No buying activity recorded.")

    print("\n📉 3. TOP SELLERS (Largest Net Position Decreases / Liquidations)")
    print("-" * 80)
    if not top_sell.empty:
        top_sell_display = top_sell.copy()
        top_sell_display['shares_prev'] = top_sell_display['shares_prev'].apply(lambda x: f"{x:,.0f}")
        top_sell_display['shares_curr'] = top_sell_display['shares_curr'].apply(lambda x: f"{x:,.0f}")
        top_sell_display['share_change'] = top_sell_display['share_change'].apply(lambda x: f"{x:,.0f}")
        top_sell_display['pct_change'] = top_sell_display['pct_change'].apply(lambda x: f"{x:.1f}%")
        print(top_sell_display[
                  ['cik', 'filer_name', 'shares_prev', 'shares_curr', 'share_change', 'pct_change']].to_string(
            index=False))
    else:
        print("No selling activity recorded.")
    print("\n" + "=" * 80 + "\n")


# ----------------------------------------------------------------------
# CLI Runner
# ----------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SEC 13F Local Query Tool")
    parser.add_argument("--ticker", type=str, required=True, help="Stock ticker symbol (e.g. AAPL)")
    parser.add_argument("--year", type=int, required=True, help="Target year (e.g. 2025)")
    parser.add_argument("--quarter", type=int, choices=[1, 2, 3, 4], required=True, help="Target quarter (1-4)")
    parser.add_argument("--top", type=int, default=10, help="Number of records to output (default: 10)")

    args = parser.parse_args()

    try:
        query_13f_report(args.ticker, args.year, args.quarter, top_n=args.top)
    except Exception as err:
        print(f"\n❌ Query failed: {err}", file=sys.stderr)
        sys.exit(1)


 # python main.py --ticker GOOGL --year 2025 --quarter 3
