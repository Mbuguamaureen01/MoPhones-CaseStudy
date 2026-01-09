"""
DATA CLEANING SUMMARY

This script documents all data cleaning transformations applied in the pipeline.
Run this to see before/after comparisons.
"""

import duckdb

con = duckdb.connect('duckdb/mophones.duckdb')

print("DATA CLEANING SUMMARY - MoPhones Credit Analysis")

print("\n1. LOAN ID FILTERING")

print("Raw customer_sales rows: 1,048,575")
raw_valid = con.execute(
    "SELECT COUNT(*) FROM customer_sales WHERE \"Loan Id\" IS NOT NULL AND \"Loan Id\" != '#N/A'").fetchone()[0]
print(f"After filtering null/NA IDs: {raw_valid:,}")
staging_count = con.execute(
    "SELECT COUNT(*) FROM main_staging.stg_customers").fetchone()[0]
print(f"Final staging table: {staging_count:,} valid records")
print(f"Records removed: {1048575 - staging_count:,}")

print("\n2. GENDER STANDARDIZATION")
print("Transformation: M, Male, MALE -> 'Male'; F, Female, FEMALE -> 'Female'")
print("\nFinal standardized values:")
staging_gender = con.execute("""
    SELECT gender, COUNT(DISTINCT loan_id) as loans
    FROM main_staging.stg_customer_demographics
    GROUP BY gender
    ORDER BY loans DESC
""").fetchall()
for row in staging_gender:
    gender_val = row[0] if row[0] else 'None'
    print(f"  {gender_val:15} {row[1]:>6,} unique loans")

print("\n3. CITIZENSHIP STANDARDIZATION")
print("Transformation: KENYAN, CITIZEN, Kenyan -> 'Kenyan'; blanks remain as is")
print("\nFinal standardized values:")
staging_citizenship = con.execute("""
    SELECT citizenship, COUNT(DISTINCT loan_id) as loans
    FROM main_staging.stg_customer_demographics
    GROUP BY citizenship
    ORDER BY loans DESC
""").fetchall()
for row in staging_citizenship:
    citizenship_val = row[0] if row[0] else 'None'
    print(f"  {citizenship_val:15} {row[1]:>6,} unique loans")

print("\n4. INCOME GROUPING")
print("Income calculation:")
print("  Total Income = Received + Persons Received + Banks + Paybills")
print("  Avg Monthly Income = Total Income / Duration")
print("\nIncome ranges (as per case study):")
ranges = [
    "Below 5,000", "5,000-9,999", "10,000-19,999", "20,000-29,999",
    "30,000-49,999", "50,000-99,999", "100,000-149,999", "150,000 and above"
]
for r in ranges:
    print(f"  - {r}")

income_dist = con.execute("""
    SELECT income_group, COUNT(DISTINCT loan_id) as loans
    FROM main_intermediate.int_credit_with_customer
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main_intermediate.int_credit_with_customer)
        AND income_group NOT IN ('Unknown', 'None')
    GROUP BY income_group
    ORDER BY
        CASE income_group
            WHEN 'Below 5,000' THEN 1
            WHEN '5,000-9,999' THEN 2
            WHEN '10,000-19,999' THEN 3
            WHEN '20,000-29,999' THEN 4
            WHEN '30,000-49,999' THEN 5
            WHEN '50,000-99,999' THEN 6
            WHEN '100,000-149,999' THEN 7
            WHEN '150,000 and above' THEN 8
        END
""").fetchdf()
print("\nDistribution:")
for _, row in income_dist.iterrows():
    pct = (row['loans'] / income_dist['loans'].sum()) * 100
    print(f"  {row['income_group']:25} {row['loans']:>6,} loans ({pct:>5.1f}%)")

print("\n5. AGE GROUPING")
print("Age calculation:")
print("  Age = Years between DOB and snapshot_date")
print("\nAge ranges (as per case study):")
age_ranges = ["18-25", "26-35", "36-45", "46-55", "Above 55"]
for r in age_ranges:
    print(f"  - {r}")

age_dist = con.execute("""
    SELECT age_group, COUNT(DISTINCT loan_id) as loans
    FROM main_intermediate.int_credit_with_customer
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main_intermediate.int_credit_with_customer)
        AND age_group NOT IN ('Unknown', 'None')
    GROUP BY age_group
    ORDER BY age_group
""").fetchdf()
print("\nDistribution:")
for _, row in age_dist.iterrows():
    pct = (row['loans'] / age_dist['loans'].sum()) * 100
    print(f"  {row['age_group']:15} {row['loans']:>6,} loans ({pct:>5.1f}%)")


print("DATA CLEANING COMPLETE")

con.close()
