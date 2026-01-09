{{
    config(
        materialized='table'
    )
}}

-- Cohort analysis: Track loan performance by origination quarter
with credit_with_customer as (
    select * from {{ ref('int_credit_with_customer') }}
),

cohorts as (
    select
        *,
        -- Define cohort by sale quarter
        date_trunc('quarter', sale_date) as cohort_quarter,
        -- Calculate cohort age (quarters since origination)
        datediff('quarter', date_trunc('quarter', sale_date), date_trunc('quarter', snapshot_date)) as cohort_age_quarters

    from credit_with_customer
),

cohort_metrics as (
    select
        cohort_quarter,
        cohort_age_quarters,
        snapshot_date,

        -- Cohort size and composition
        count(distinct loan_id) as cohort_size,

        -- Payment performance
        avg(total_paid) as avg_total_paid,
        avg(balance) as avg_balance,
        sum(total_paid) / nullif(sum(total_paid + balance), 0) * 100 as avg_repayment_rate_pct,

        -- Arrears metrics
        count(distinct case when balance_due_status = 'Arrears' then loan_id end) as arrears_count,
        count(distinct case when balance_due_status = 'Arrears' then loan_id end)::float / count(distinct loan_id)::float * 100 as arrears_rate_pct,
        avg(days_past_due) as avg_dpd,

        -- Status distribution
        count(distinct case when account_status_l2 = 'Active' then loan_id end) as active_count,
        count(distinct case when account_status_l2 = 'FPD' then loan_id end) as fpd_count,
        count(distinct case when account_status_l2 = 'Paid Off' then loan_id end) as paid_off_count,
        count(distinct case when account_status_l2 = 'Inactive' then loan_id end) as inactive_count,

        -- Paid off rate
        count(distinct case when account_status_l2 = 'Paid Off' then loan_id end)::float / count(distinct loan_id)::float * 100 as paid_off_rate_pct

    from cohorts
    where cohort_quarter is not null
    group by cohort_quarter, cohort_age_quarters, snapshot_date
)

select *
from cohort_metrics
order by cohort_quarter, cohort_age_quarters
