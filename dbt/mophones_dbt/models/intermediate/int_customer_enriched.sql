{{
    config(
        materialized='view'
    )
}}

-- Enrich customer data with age and income groupings as per case study requirements
with customers as (
    select * from {{ ref('stg_customers') }}
),

demographics as (
    select * from {{ ref('stg_customer_demographics') }}
),

credit_snapshots as (
    select
        loan_id,
        snapshot_date,
        sale_date
    from {{ ref('stg_credit_snapshots') }}
),

customer_with_demographics as (
    select
        c.*,
        d.date_of_birth,
        d.citizenship,
        d.gender,
        d.duration,
        d.avg_monthly_income,
        d.total_income

    from customers c
    left join demographics d
        on c.loan_id = d.loan_id
),

-- Calculate age at each snapshot date and create age groups
customer_with_age_groups as (
    select
        cwd.*,
        cs.snapshot_date,

        -- Calculate age at snapshot date
        case
            when cwd.date_of_birth is not null and cs.snapshot_date is not null
            then datediff('year', cwd.date_of_birth, cs.snapshot_date)
            else null
        end as age_at_snapshot,

        -- Age groups as per case study: 18–25, 26–35, 36–45, 46-55, Above 55
        case
            when datediff('year', cwd.date_of_birth, cs.snapshot_date) between 18 and 25 then '18-25'
            when datediff('year', cwd.date_of_birth, cs.snapshot_date) between 26 and 35 then '26-35'
            when datediff('year', cwd.date_of_birth, cs.snapshot_date) between 36 and 45 then '36-45'
            when datediff('year', cwd.date_of_birth, cs.snapshot_date) between 46 and 55 then '46-55'
            when datediff('year', cwd.date_of_birth, cs.snapshot_date) > 55 then 'Above 55'
            else 'Unknown'
        end as age_group,

        -- Income groups as per case study
        -- Below 5,000, 5,000–9,999, 10,000–19,999, 20,000–29,999, 30,000–49,999,
        -- 50,000–99,999, 100,000–149,999, 150,000 and above
        case
            when cwd.avg_monthly_income < 5000 then 'Below 5,000'
            when cwd.avg_monthly_income between 5000 and 9999 then '5,000-9,999'
            when cwd.avg_monthly_income between 10000 and 19999 then '10,000-19,999'
            when cwd.avg_monthly_income between 20000 and 29999 then '20,000-29,999'
            when cwd.avg_monthly_income between 30000 and 49999 then '30,000-49,999'
            when cwd.avg_monthly_income between 50000 and 99999 then '50,000-99,999'
            when cwd.avg_monthly_income between 100000 and 149999 then '100,000-149,999'
            when cwd.avg_monthly_income >= 150000 then '150,000 and above'
            else 'Unknown'
        end as income_group

    from customer_with_demographics cwd
    cross join (select distinct snapshot_date from credit_snapshots) cs
)

select * from customer_with_age_groups
