"""
AUTOMATED DATA LOADING SCRIPT

This script demonstrates full automation for loading quarterly Credit Data snapshots
into DuckDB without any manual code changes for new quarters.
- Automatically detects all CSV files matching "Credit Data - *.csv" pattern
- Adding new quarters requires no code changes

Example:
    Add: Data/Raw/Credit Data - 01-01-2026.csv
    Run: python src/load_to_duckdb.py
    Result: Automatically creates table credit_data_20260101

The pipeline then automatically:
    1. dbt macro detects new table via information_schema
    2. Union includes new quarter
    3. All downstream models updated
"""

import duckdb
import pandas as pd
from pathlib import Path
import re


DATA_DIR = Path("Data/Raw")
DB_PATH = "duckdb/mophones.duckdb"

# Connect to DuckDB
con = duckdb.connect(DB_PATH)

def table_exists(table_name):
    """Check if a table exists in DuckDB"""
    result = con.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
    """, [table_name]).fetchone()
    return result[0] > 0

print("Loading data into DuckDB...")

# Load CSV files - Credit Data snapshots
for file in DATA_DIR.glob("Credit Data - *.csv"):

    date_match = re.search(r'(\d{2})-(\d{2})-(\d{4})', file.stem)
    if date_match:
        dd, mm, yyyy = date_match.groups()
        table_name = f"credit_data_{yyyy}{mm}{dd}"
    else:
        table_name = file.stem.lower().replace(" ", "_").replace("-", "_")

    if table_exists(table_name):
        print(f" [SKIP] Table `{table_name}` already exists, skipping {file.name}")
        continue

    df = pd.read_csv(file)

    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM df
    """)

    print(f" [OK] Loaded {file.name} -> table `{table_name}` ({len(df)} rows)")

# Load Excel files with multiple sheets
customer_file = DATA_DIR / "Sales and Customer Data.xlsx"
if customer_file.exists():
    # Load each sheet as a separate table
    sheet_mapping = {
        'Sales Details': 'customer_sales',
        'Gender': 'customer_gender',
        'DOB': 'customer_dob',
        'Income Level': 'customer_income'
    }

    for sheet_name, table_name in sheet_mapping.items():
        if table_exists(table_name):
            print(f"  [SKIP] Table `{table_name}` already exists, skipping sheet '{sheet_name}'")
            continue

        df = pd.read_excel(customer_file, sheet_name=sheet_name)

        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM df
        """)

        print(
            f"  [OK] Loaded sheet '{sheet_name}' -> table `{table_name}` ({len(df)} rows)")
else:
    print(f"  [WARN] {customer_file.name} not found")

print("\n3. Loading NPS Data from Excel...")
nps_file = DATA_DIR / "NPS Data.xlsx"
if nps_file.exists():
    if table_exists('nps_data'):
        print(f"  [SKIP] Table `nps_data` already exists, skipping {nps_file.name}")
    else:
        df_nps = pd.read_excel(nps_file, sheet_name=0)

        con.execute("""
            CREATE OR REPLACE TABLE nps_data AS
            SELECT * FROM df_nps
        """)

        print(
            f"  [OK] Loaded {nps_file.name} -> table `nps_data` ({len(df_nps)} rows)")
else:
    print(f"  [WARN] {nps_file.name} not found")

# Show all tables created
print("Summary of tables in DuckDB:")
tables = con.execute("SHOW TABLES").fetchall()
for table in tables:
    count = con.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
    print(f"  - {table[0]}: {count:,} rows")

con.close()

print("\n[SUCCESS] Data loading complete!")
print(f"Database saved at: {DB_PATH}")
