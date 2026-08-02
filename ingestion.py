import io
import zipfile
import argparse
import sys
from typing import List, Tuple, Optional
import pandas as pd
import requests
from pymongo import MongoClient, ASCENDING

# ----------------------------------------------------------------------
# Configuration & Constants
# ----------------------------------------------------------------------
HEADERS = {
    'User-Agent': 'InstitutionalHoldingTracker admin@mycompany.com',
    'Accept-Encoding': 'gzip, deflate'
}

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "sec_13f_db"
COLLECTION_NAME = "holdings"


def get_mongo_collection():
    """Initializes MongoDB client, database, and performance indexes."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Compound indexes for fast querying
    collection.create_index([("cusip", ASCENDING), ("year", ASCENDING), ("quarter", ASCENDING)])
    collection.create_index([("cik", ASCENDING)])
    collection.create_index([("year", ASCENDING), ("quarter", ASCENDING)])

    return collection


def generate_sec_candidate_urls(year: int, quarter: int) -> List[str]:
    """Generates candidate URLs for SEC EDGAR 13F datasets."""
    candidate_urls = []

    # Legacy URL format (<= 2023)
    candidate_urls.append(
        f"https://www.sec.gov/files/structureddata/data/form-13f-data-sets/{year}q{quarter}_13f.zip"
    )

    # Modern URL format (2024+)
    modern_patterns = {
        1: [f"01mar{year}-31may{year}_form13f.zip"],
        2: [f"01jun{year}-31aug{year}_form13f.zip"],
        3: [f"01sep{year}-30nov{year}_form13f.zip"],
        4: [
            f"01dec{year}-28feb{year + 1}_form13f.zip",
            f"01dec{year}-29feb{year + 1}_form13f.zip"
        ]
    }

    for filename in modern_patterns.get(quarter, []):
        candidate_urls.append(
            f"https://www.sec.gov/files/structureddata/data/form-13f-data-sets/{filename}"
        )

    return candidate_urls


def download_13f_zip(year: int, quarter: int) -> bytes:
    """Downloads the target SEC 13F zip file."""
    urls = generate_sec_candidate_urls(year, quarter)

    for url in urls:
        print(f"🔎 Trying download from: {url}")
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            if response.status_code == 200:
                print(f"✅ Download successful! ({len(response.content) / (1024 * 1024):.2f} MB)")
                return response.content
        except requests.RequestException as e:
            print(f"⚠️ Warning: Connection error on {url}: {e}")

    raise RuntimeError(
        f"❌ Failed to fetch 13F dataset for {year} Q{quarter}. All candidate URLs returned non-200 responses.")


def parse_13f_zip(zip_bytes: bytes) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[pd.DataFrame]]:
    """Extracts TSV files from the zip archive using case-insensitive matching."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        filenames = z.namelist()

        # Case-insensitive filename matching using .lower()
        sub_file = next((f for f in filenames if f.lower().endswith('submission.tsv')), None)
        info_file = next((f for f in filenames if f.lower().endswith('infotable.tsv')), None)
        cover_file = next((f for f in filenames if f.lower().endswith('coverpage.tsv')), None)

        if not sub_file or not info_file:
            raise KeyError(f"Expected SUBMISSION.tsv and INFOTABLE.tsv in zip archive. Found: {filenames}")

        print("⚙️ Parsing TSV files into DataFrames...")
        sub_df = pd.read_csv(z.open(sub_file), sep='\t', low_memory=False)
        info_df = pd.read_csv(z.open(info_file), sep='\t', low_memory=False)
        cover_df = pd.read_csv(z.open(cover_file), sep='\t', low_memory=False) if cover_file else None

    return sub_df, info_df, cover_df


def process_and_aggregate(
        sub_df: pd.DataFrame,
        info_df: pd.DataFrame,
        cover_df: Optional[pd.DataFrame],
        year: int,
        quarter: int
) -> List[dict]:
    """Cleans, filters, and aggregates 13F holdings by (CIK, FILER_NAME, CUSIP)."""
    print("⚡ Cleaning and processing raw holdings data...")

    # 1. Filter for common stock ('SH')
    if 'SSHPRNAMT_TYPE' in info_df.columns:
        info_df = info_df[info_df['SSHPRNAMT_TYPE'].astype(str).str.strip().str.upper() == 'SH'].copy()

    # 2. Clean numeric values
    info_df['SSHPRNAMT'] = pd.to_numeric(info_df['SSHPRNAMT'], errors='coerce').fillna(0)
    info_df['VALUE'] = pd.to_numeric(info_df['VALUE'], errors='coerce').fillna(0)

    # 3. Clean CUSIP strings
    info_df['CUSIP'] = info_df['CUSIP'].astype(str).str.strip().str.upper()

    # 4. Standardize accession numbers
    sub_df['ACCESSION_NUMBER'] = sub_df['ACCESSION_NUMBER'].astype(str).str.strip()
    info_df['ACCESSION_NUMBER'] = info_df['ACCESSION_NUMBER'].astype(str).str.strip()

    # Fallback for FILER_NAME from COVERPAGE if absent in SUBMISSION
    if 'FILER_NAME' not in sub_df.columns and cover_df is not None:
        cover_df['ACCESSION_NUMBER'] = cover_df['ACCESSION_NUMBER'].astype(str).str.strip()
        name_col = next((c for c in ['FILER_NAME', 'FILING_MANAGER_NAME', 'NAMEOFISSUER'] if c in cover_df.columns),
                        None)
        if name_col:
            cover_subset = cover_df[['ACCESSION_NUMBER', name_col]].drop_duplicates('ACCESSION_NUMBER')
            sub_df = sub_df.merge(cover_subset, on='ACCESSION_NUMBER', how='left')
            sub_df.rename(columns={name_col: 'FILER_NAME'}, inplace=True)

    if 'FILER_NAME' not in sub_df.columns:
        sub_df['FILER_NAME'] = "CIK " + sub_df['CIK'].astype(str)

    # 5. Merge holdings with submission metadata
    merged = info_df.merge(
        sub_df[['ACCESSION_NUMBER', 'CIK', 'FILER_NAME']],
        on='ACCESSION_NUMBER',
        how='inner'
    )

    # 6. Aggregate positions per institution per security
    print("📊 Aggregating positions per Institution per Security...")
    aggregated = merged.groupby(['CIK', 'FILER_NAME', 'CUSIP'], as_index=False).agg({
        'SSHPRNAMT': 'sum',
        'VALUE': 'sum'
    })

    # 7. Mongo schema alignment
    aggregated['year'] = year
    aggregated['quarter'] = quarter

    aggregated.rename(columns={
        'CIK': 'cik',
        'FILER_NAME': 'filer_name',
        'CUSIP': 'cusip',
        'SSHPRNAMT': 'shares',
        'VALUE': 'value'
    }, inplace=True)

    aggregated['cik'] = aggregated['cik'].astype(str).str.zfill(10)

    return aggregated.to_dict('records')


def ingest_quarter(year: int, quarter: int):
    """Execution pipeline for target year and quarter."""
    print(f"\n==================================================")
    print(f"🚀 STARTING INGESTION FOR {year} Q{quarter}")
    print(f"==================================================")

    collection = get_mongo_collection()

    # Download zip file
    zip_bytes = download_13f_zip(year, quarter)

    # Parse zip files
    sub_df, info_df, cover_df = parse_13f_zip(zip_bytes)

    # Process & Aggregate
    records = process_and_aggregate(sub_df, info_df, cover_df, year, quarter)

    if not records:
        print(f"⚠️ No valid common stock records found for {year} Q{quarter}.")
        return

    # Clear old records for this quarter
    print(f"💾 Clearing any existing records for {year} Q{quarter} from MongoDB...")
    deleted_result = collection.delete_many({"year": year, "quarter": quarter})
    print(f"   (Removed {deleted_result.deleted_count:,} stale documents)")

    # Bulk insert in batches of 50,000
    print(f"📥 Batch inserting {len(records):,} records into MongoDB...")
    chunk_size = 50000
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]
        collection.insert_many(chunk)

    print(f"✨ COMPLETED: Successfully ingested {year} Q{quarter} ({len(records):,} holdings).\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SEC 13F Bulk Dataset Ingestion Script")
    parser.add_argument("--year", type=int, required=True, help="Target year (e.g. 2025)")
    parser.add_argument("--quarter", type=int, choices=[1, 2, 3, 4], required=True, help="Target quarter (1-4)")

    args = parser.parse_args()

    try:
        ingest_quarter(args.year, args.quarter)
    except Exception as err:
        print(f"\n❌ Pipeline failed: {err}", file=sys.stderr)
        sys.exit(1)

# python ingestion.py --year 2025 --quarter 3