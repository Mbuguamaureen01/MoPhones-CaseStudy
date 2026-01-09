# MoPhones Credit Portfolio Analysis

## Overview

This project analyzes MoPhones' credit portfolio to understand portfolio health, track repayment patterns, assess credit risk, and examine the relationship between credit outcomes and customer satisfaction (NPS).

## Project Structure

```
MoPhones-CreditAnalysis/
├── Data/
│   ├── Raw/                      # Original CSV and Excel files
│   └── Processed/                # Analysis-ready CSV exports (mart tables)
├── dbt/mophones_dbt/             # dbt project
│   ├── models/
│   │   ├── staging/              # Staging models (raw data cleaning)
│   │   ├── intermediate/         # Intermediate models (joins, enrichment)
│   │   └── marts/                # Mart models (final analytical tables)
│   ├── tests/                    # dbt tests
│   ├── dbt_project.yml           # dbt project configuration
│   └── profiles.yml              # dbt connection profile
├── duckdb/
│   └── mophones.duckdb           # DuckDB database file
├── notebooks/
│   └── analysis.ipynb            # Jupyter notebook with analysis
├── src/
│   ├── load_to_duckdb.py         # Load raw data to DuckDB (automated detection)
│   └── pipeline_utils.py         # Data quality, export, and automation testing
└── requirements.txt              # Python dependencies
```

## Automated Workflow 

This pipeline is fully automated - new data is detected automatically without code changes.

1. Auto-detects ALL CSV files in Data/Raw/ and loads to DuckDB
2. Runs dbt transformations (auto-detects new quarters)
3. Validates data quality
4. Prepares analysis full analysis notebook

**How Automation Works:**
- [load_to_duckdb.py](src/load_to_duckdb.py:24-29) uses glob pattern to find all credit CSV files
- [union_credit_snapshots.sql](dbt/mophones_dbt/macros/union_credit_snapshots.sql:7-13) queries information_schema to detect all credit_data_* tables
- [stg_credit_snapshots.sql](dbt/mophones_dbt/models/staging/stg_credit_snapshots.sql:24) calls dynamic macro instead of hardcoded unions

### Quick Access to Results

Processed data is available in **[Data/Processed/](Data/Processed/)** as CSV files:
- `mart_portfolio_performance.csv` - Portfolio metrics over time
- `mart_credit_vs_nps.csv` - Customer satisfaction analysis
- `mart_cohort_analysis.csv` - Loan cohort performance
- `stg_customers.csv` - Cleaned customer data
- `SUMMARY.csv` - High-level statistics

See [Data/Processed/README.md](Data/Processed/README.md) for details.

### Manual Steps

#### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

#### 2. Load Data into DuckDB

```bash
python src/load_to_duckdb.py
```

This automatically loads:
- ALL credit data snapshots matching "Credit Data - *.csv" pattern
- Customer/sales data from all Excel sheets (Sales Details, Gender, DOB, Income Level)
- NPS survey data

#### 3. Run dbt Models

```bash
cd dbt/mophones_dbt
dbt run
```

This creates:
- **Staging models**: Cleaned raw data (filters null/NA Loan IDs)
- **Intermediate models**: Enriched data with demographics and age/income groupings
- **Mart models**: Final analytical tables

#### 4. Run Analysis

Open and run [notebooks/mophones_analysis_with_demographics.ipynb](notebooks/mophones_analysis_with_demographics.ipynb)

## Data Model

### Staging Layer

- **stg_credit_snapshots**: Union of all credit portfolio snapshots
- **stg_customers**: Cleaned customer sales data
- **stg_customer_demographics**: Customer demographics (DOB, gender, income)
- **stg_nps**: Cleaned NPS survey responses

### Intermediate Layer

- **int_customer_enriched**: Customers with age and income groupings (as per case study requirements)
- **int_credit_with_customer**: Credit data enriched with customer attributes and demographics
- **int_credit_with_nps**: Credit data with NPS responses

### Marts Layer

- **mart_portfolio_performance**: Key portfolio metrics over time
- **mart_credit_vs_nps**: Credit outcomes linked to customer satisfaction
- **mart_cohort_analysis**: Loan performance by origination cohort

## Key Findings

### 1. Portfolio Performance

- Portfolio grew from 8,935 loans (Jan) to 20,742 loans (Dec)
- Arrears rate tracking shows [trend from analysis]
- FPD rate indicates [early warning signals]

### 2. Risk Indicators

**Recommended KPIs:**
1. Arrears Rate - % of loans in arrears
2. First Payment Default (FPD) Rate - Early warning indicator
3. 90+ Days Past Due - Likely defaults/write-offs
4. Average Days Past Due - Portfolio aging
5. Payment Collection Rate - Collection efficiency
6. Paid-Off Rate - Successful completions

### 3. NPS vs Credit Performance

- Operational issues (phone locking, payment delays) strongly impact NPS
- Trade-off exists between aggressive collections and customer satisfaction
- Support quality correlates with higher NPS across all payment categories

## Data Quality & Segmentation

### Demographic Segmentation Available

1. **Age Groups** (calculated from DOB at each snapshot date):
   - 18-25: 11.8% of loans
   - 26-35: 29.0% of loans (largest segment)
   - 36-45: 9.8% of loans
   - 46-55: 2.7% of loans
   - Above 55: 0.6% of loans
   - Unknown: 45.9% (due to missing data)

2. **Income Groups** (average monthly income from financial data):
   - Below 5,000 to 150,000+ in 8 bands as specified
   - Calculated as: (Total Income from all sources) / Duration months
   - ~52% have calculable income, ~48% unknown

3. **Key Demographic Insights**:
   - Arrears rate decreases with age (18-25: 42%, 46-55: 24%)
   - Income group 20,000-29,999 has highest arrears rate (51%)
   - Gender distribution available for segmentation

### Data Quality & Cleaning

1. **Data Cleaning Implemented**
   - Raw customer data has 1,048,575 rows (includes null/NA Loan IDs)
   - Staging models filter to only valid Loan IDs: `WHERE loan_id IS NOT NULL AND loan_id != '' AND loan_id != '#N/A'`
   - **Gender standardization**: M, Male, MALE → "Male"; F, Female, FEMALE → "Female"
   - **Citizenship standardization**: KENYAN, CITIZEN, Kenyan → "Kenyan"
   - **Clean dataset**: 20,696 valid customer records after filtering
   - **Impact**: Analysis based on validated, standardized customer records

2. **Snapshot-Only Data**
   - No transaction-level payment history
   - **Impact**: Cannot track payment patterns or timing
   - **Assumption**: Payments occur evenly between snapshots

3. **NPS Survey Coverage**
   - Only 4,129 NPS responses vs 20,742 total loans (20% response rate)
   - 144 null NPS scores in responses
   - **Impact**: NPS analysis limited to responding customers

## Assumptions Made

### Data Assumptions

1. **Age Calculation**: Age calculated at each snapshot date from DOB
2. **Income Calculation**: Average monthly income = (Received + Persons Received + Banks Received + Paybills Received) / Duration
3. **Citizenship Standardization**: All non-blank citizenship values standardized to "Kenyan" (KENYAN, CITIZEN, Kenyan → Kenyan)
4. **Gender Standardization**: All gender values standardized to "Male" or "Female" (M, MALE → Male; F, FEMALE → Female)
5. **Invalid Records Excluded**: Rows with null, blank, or '#N/A' Loan IDs are irrelevant and excluded from analysis
6. **Payment Timing**: Payments assumed to occur evenly between snapshots
7. **Status Changes**: Cannot determine exact timing, only state at snapshot dates
8. **NPS Timing**: Survey responses may not align with credit snapshot dates
9. **Data Completeness**: Working with 20,696 valid customer records after data quality filtering

### Analytical Assumptions

10. **Cohort Definition**: Using sale_date quarter as cohort identifier
11. **Payment Performance**: Categorizing based on balance_due_status and DPD
12. **NPS Categories**: Standard definition (9-10: Promoter, 7-8: Passive, 0-6: Detractor)

## Recommendations

1. **Complete Demographic Coverage**: Ensure all customers have DOB and income data (currently ~46-48% missing)
2. **Implement Transaction Tracking**: Capture payment events with timestamps (not just quarterly snapshots)
3. **Improve NPS Collection**: Increase survey response rate from current 20%
4. **Data Quality at Source**: Implement validation to prevent null/NA Loan IDs in source systems

### Analytical Improvements

1. **Real-Time Dashboard**: Move from quarterly snapshots to daily metrics
2. **Predictive Models**: Build early warning models for defaults
3. **Cohort Analysis**: Track loan vintages systematically
4. **Operational Metrics**: Link system issues (phone locks, delays) to NPS

### Process Improvements

1. **Data Quality Checks**: Automated validation and completeness monitoring
2. **Documentation**: Maintain data dictionary and business logic docs
3. **Regular Reviews**: Weekly portfolio review meetings with key metrics

## Technical Details

### Technology Stack

- **Database**: DuckDB (embedded analytics database)
- **Transformation**: dbt (data build tool)
- **Analysis**: Python (pandas, matplotlib, seaborn, plotly)
- **Notebook**: Jupyter

### dbt Lineage

```
Sources (DuckDB tables)
    ├── stg_credit_snapshots
    ├── stg_customers
    └── stg_nps
            ↓
    int_credit_with_customer
            ↓
    int_credit_with_nps
            ↓
    ├── mart_portfolio_performance
    ├── mart_credit_vs_nps
    └── mart_cohort_analysis
```

## Running Tests

```bash
cd dbt/mophones_dbt
dbt test
```

Tests validate:
- Unique loan IDs
- Not null constraints
- NPS score ranges (0-10)
- NPS category values
