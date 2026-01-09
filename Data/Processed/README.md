This folder contains cleaned, transformed, and analysis-ready data exported from the dbt pipeline.

## Files Overview

These are the primary outputs of the pipeline

1. mart_portfolio_performance.csv- Portfolio health metrics over time | 5 | Track arrears rate, FPD, payment collection, DPD trends.
2. mart_credit_vs_nps.csv- Credit outcomes linked to NPS | 3,399 | Analyze customer satisfaction vs payment performance 
3. mart_cohort_analysis.csv -Loan performance by origination cohort | 50 | Track how loan vintages perform over time 

## Cleaned Staging Data
1. stg_customers.csv Clean customer/sales data (null Loan IDs filtered) | 20,696 | Customer lookup, sales analysis

## Summary Statistics
High-level portfolio metrics 

## Data Quality
All exported data has been:
- Cleaned (null/NA Loan IDs removed)
- Standardized (gender: Male/Female; citizenship: Kenyan)
- Validated (dbt tests passed)
- Enriched (demographics, age/income groupings added)
- Aggregated (ready for business analysis)

