{{
    config(
        materialized='view'
    )
}}

/*
    AUTOMATED CREDIT SNAPSHOTS UNION

    This model automatically detects and unions ALL credit_data_* tables.

    How it works:
    1. Queries information_schema to find all credit_data_* tables
    2. Dynamically generates UNION ALL for each table
    3. New quarters are automatically included

    Example: Add "Credit Data - 31-03-2026.csv" to Data/Raw/
    → Run: python src/load_to_duckdb.py (creates credit_data_20260331 table)
    → Run: dbt run (automatically includes new table in union)
    → Analysis updated
*/

with credit_snapshots as (
    {{ union_credit_snapshots() }}
)

select * from credit_snapshots
