# Source Code - Pipeline Scripts

This folder contains the core Python scripts for the automated data pipeline.

## Scripts

### 1. load_to_duckdb.py
**Purpose**: Load raw data into DuckDB database

**Features**:
- Automatically detects ALL CSV files matching `Credit Data - *.csv` pattern
- Loads all 4 sheets from Excel file (Sales, Gender, DOB, Income)
- No hardcoded file names
- Creates clean table names from file dates

**Usage**:
```bash
python src/load_to_duckdb.py
```

**Automation**:
- Add new quarter CSV → Run script → Table automatically created
- Zero code changes required for new data

---

### 2. pipeline_utils.py
**Purpose**: Data quality verification, export, and automation testing

**Functions**:

#### verify_data_quality()
- Validates cleaned data row counts
- Checks demographic coverage percentages
- Shows age/income group distributions

#### export_processed_data()
- Exports mart tables to CSV format
- Outputs to `Data/Processed/` folder
- Generates summary statistics

#### test_automation()
- Verifies dynamic table detection is working
- Shows all credit_data_* tables found
- Validates union results

**Usage**:
```bash
# Run all functions
python src/pipeline_utils.py

# Run specific function
python src/pipeline_utils.py verify
python src/pipeline_utils.py export
python src/pipeline_utils.py test
```

---

## Integration with dbt

```
Raw Data (Data/Raw/)
    ↓
load_to_duckdb.py → DuckDB tables
    ↓
dbt run → Staging → Intermediate → Marts
    ↓
pipeline_utils.py → Verification & Export
    ↓
Processed Data (Data/Processed/)
```

---
