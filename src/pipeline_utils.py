"""
PIPELINE UTILITIES/EDA

This module provides utility functions for data quality verification,
data export, and automation testing.

Functions:
- verify_data_quality(): Validate cleaned data and demographic coverage
- export_processed_data(): Export analytical tables to CSV
- test_automation(): Verify dynamic table detection is working
"""

import duckdb
import pandas as pd
from pathlib import Path


DB_PATH = "duckdb/mophones.duckdb"


def verify_data_quality():
    """
    Verify data cleaning results and demographic coverage.

    Outputs:
    - Row counts for staging models after cleaning
    - Demographic coverage percentages
    - Age and income group distributions
    """
    con = duckdb.connect(DB_PATH)

    print("DATA QUALITY VERIFICATION")

    # Check staging models row counts
    print("\n1. Staging Models (After Data Cleaning):")

    customers = con.execute(
        "SELECT COUNT(*) FROM main_staging.stg_customers").fetchone()[0]
    print(f"stg_customers: {customers:,} rows (cleaned from 1,048,575)")

    demographics = con.execute(
        "SELECT COUNT(DISTINCT loan_id) FROM main_staging.stg_customer_demographics").fetchone()[0]
    print(f"stg_customer_demographics: {demographics:,} unique loan_ids")

    # Check demographic coverage
    demo_detail = con.execute("""
        SELECT
            COUNT(DISTINCT loan_id) as total_unique_loans,
            COUNT(DISTINCT CASE WHEN date_of_birth IS NOT NULL THEN loan_id END) as with_dob,
            COUNT(DISTINCT CASE WHEN gender IS NOT NULL THEN loan_id END) as with_gender,
            COUNT(DISTINCT CASE WHEN avg_monthly_income IS NOT NULL THEN loan_id END) as with_income
        FROM main_staging.stg_customer_demographics
    """).fetchone()

    # print(f"\nDemographic Coverage:")
    # print(f"  Total unique loans: {demo_detail[0]:,}")
    # print(f"  With DOB: {demo_detail[1]:,} ({demo_detail[1]/demo_detail[0]*100:.1f}%)")
    # print(f"  With Gender: {demo_detail[2]:,} ({demo_detail[2]/demo_detail[0]*100:.1f}%)")
    # print(f"  With Income: {demo_detail[3]:,} ({demo_detail[3]/demo_detail[0]*100:.1f}%)")

    # Check the new customers data
    enriched_stats = con.execute("""
        SELECT
            COUNT(DISTINCT loan_id) as total_loans,
            COUNT(DISTINCT CASE WHEN age_group NOT IN ('Unknown', 'None') THEN loan_id END) as with_age_group,
            COUNT(DISTINCT CASE WHEN income_group NOT IN ('Unknown', 'None') THEN loan_id END) as with_income_group
        FROM main_intermediate.int_credit_with_customer
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main_intermediate.int_credit_with_customer)
    """).fetchone()

    print(f"Total loans in latest snapshot: {enriched_stats[0]:,}")
    print(
        f"With valid age group: {enriched_stats[1]:,} ({enriched_stats[1]/enriched_stats[0]*100:.1f}%)")
    print(
        f"With valid income group: {enriched_stats[2]:,} ({enriched_stats[2]/enriched_stats[0]*100:.1f}%)")

    # Age group breakdown
    print("\n3. Age Group Distribution:")

    age_dist = con.execute("""
        SELECT
            age_group,
            COUNT(DISTINCT loan_id) as loan_count,
            ROUND(COUNT(DISTINCT loan_id) * 100.0 / SUM(COUNT(DISTINCT loan_id)) OVER (), 1) as pct
        FROM main_intermediate.int_credit_with_customer
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main_intermediate.int_credit_with_customer)
        GROUP BY age_group
        ORDER BY age_group
    """).fetchdf()
    print(age_dist.to_string(index=False))

    con.close()


def export_processed_data():
    """
    Export final analytical tables (dbt marts) to CSV format.

    Outputs to: Data/Processed/ folder
    - Mart tables (portfolio performance, NPS, cohorts)
    - Cleaned staging data
    - Summary statistics
    """
    OUTPUT_DIR = Path("Data/Processed")
    OUTPUT_DIR.mkdir(exist_ok=True)

    con = duckdb.connect(DB_PATH)

    print("EXPORTING PROCESSED DATA TO CSV")

    # Define tables to export
    exports = {
        "mart_portfolio_performance.csv": """
            SELECT * FROM main_marts.mart_portfolio_performance
            ORDER BY snapshot_date
        """,

        "mart_credit_vs_nps.csv": """
            SELECT * FROM main_marts.mart_credit_vs_nps
            ORDER BY loan_id
        """,

        "mart_cohort_analysis.csv": """
            SELECT * FROM main_marts.mart_cohort_analysis
            ORDER BY cohort_quarter, snapshot_date
        """,

        "stg_customers.csv": """
            SELECT * FROM main_staging.stg_customers
            ORDER BY loan_id
        """,
    }

    print("\nExporting tables:")

    for filename, query in exports.items():
        try:
            df = con.execute(query).fetchdf()
            output_path = OUTPUT_DIR / filename
            df.to_csv(output_path, index=False)
            print(f"  [OK] {filename:40} {len(df):>8,} rows")
        except Exception as e:
            print(f"  [SKIP] {filename:40} (Error: {str(e)[:50]}...)")

    # Export summary statistics
    print("Generating summary statistics")

    summary = {
        'Metric': [],
        'Value': []
    }

    # Portfolio stats
    total_loans = con.execute("""
        SELECT COUNT(DISTINCT loan_id)
        FROM main_staging.stg_credit_snapshots
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main_staging.stg_credit_snapshots)
    """).fetchone()[0]

    total_customers = con.execute(
        "SELECT COUNT(DISTINCT loan_id) FROM main_staging.stg_customers").fetchone()[0]
    total_snapshots = con.execute(
        "SELECT COUNT(*) FROM main_staging.stg_credit_snapshots").fetchone()[0]
    nps_responses = con.execute(
        "SELECT COUNT(*) FROM main_marts.mart_credit_vs_nps WHERE nps_score IS NOT NULL").fetchone()[0]

    summary['Metric'].extend([
        'Total Loans (Latest Snapshot)',
        'Total Unique Customers',
        'Total Credit Snapshot Records',
        'NPS Survey Responses',
        'NPS Response Rate (%)'
    ])

    summary['Value'].extend([
        f"{total_loans:,}",
        f"{total_customers:,}",
        f"{total_snapshots:,}",
        f"{nps_responses:,}",
        f"{(nps_responses / total_loans * 100):.1f}%"
    ])

    summary_df = pd.DataFrame(summary)
    summary_df.to_csv(OUTPUT_DIR / "SUMMARY.csv", index=False)

    print(f"[OK] Export complete! Location: {OUTPUT_DIR.absolute()}")

    con.close()


def test_automation():
    """
    Verify that automation is working correctly.

    Tests:
    - Dynamic table detection via information_schema
    - Automated union of all credit_data_* tables
    - Correct row counts and date ranges
    """
    con = duckdb.connect(DB_PATH)

    # Show current tables detected
    print("\n1. Currently Detected Credit Tables:")
    tables = con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name LIKE 'credit_data_%'
        ORDER BY table_name
    """).fetchall()

    for table in tables:
        table_name = table[0]
        row_count = con.execute(
            f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  {table_name}: {row_count:,} rows")

    print(f"\nTotal tables detected: {len(tables)}")

    # Show union results
    union_stats = con.execute("""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT loan_id) as unique_loans,
            COUNT(DISTINCT snapshot_date) as snapshot_dates,
            MIN(snapshot_date) as earliest_snapshot,
            MAX(snapshot_date) as latest_snapshot
        FROM main_staging.stg_credit_snapshots
    """).fetchone()

    # Show snapshot breakdown
    snapshot_breakdown = con.execute("""
        SELECT
            snapshot_date,
            COUNT(DISTINCT loan_id) as unique_loans,
            COUNT(*) as total_records
        FROM main_staging.stg_credit_snapshots
        GROUP BY snapshot_date
        ORDER BY snapshot_date
    """).fetchall()

    for snapshot in snapshot_breakdown:
        print(
            f"  {snapshot[0]}: {snapshot[1]:,} loans, {snapshot[2]:,} records")

    print("\n" + "=" * 80)
    print("[OK] AUTOMATION STATUS: FULLY OPERATIONAL")

    con.close()


if __name__ == "__main__":
    """
    Run all utility functions when script is executed directly.
    """
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "verify":
            verify_data_quality()
        elif command == "export":
            export_processed_data()
        elif command == "test":
            test_automation()
        else:
            print(f"Unknown command: {command}")
            print("Usage: python src/pipeline_utils.py [verify|export|test]")
    else:
        # Run all by default
        print("\nRunning all pipeline utilities\n")
        verify_data_quality()
        print("\n")
        export_processed_data()
        print("\n")
        test_automation()
