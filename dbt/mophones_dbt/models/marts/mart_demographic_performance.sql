{{
    config(
        materialized='table'
    )
}}

-- Portfolio performance by age and income segments as per case study requirements
with credit_with_customer as (
    select * from {{ ref('int_credit_with_customer') }}
),

by_age_group as (
    select
        snapshot_date,
        age_group,

        count(distinct loan_id) as total_loans,
        sum(balance) as total_balance,
        sum(total_paid) as total_payments,
        sum(arrears) as total_arrears,
        avg(days_past_due) as avg_days_past_due,
        avg(avg_monthly_income) as avg_income,

        -- Status distribution
        count(distinct case when account_status_l2 = 'Active' then loan_id end) as active_loans,
        count(distinct case when account_status_l2 = 'FPD' then loan_id end) as fpd_loans,
        count(distinct case when account_status_l2 = 'Paid Off' then loan_id end) as paid_off_loans,

        -- Arrears metrics
        count(distinct case when balance_due_status = 'Arrears' then loan_id end) as arrears_count,
        sum(case when balance_due_status = 'Arrears' then arrears else 0 end) as total_arrears_amount,

        -- Rates
        case when count(distinct loan_id) > 0
             then count(distinct case when balance_due_status = 'Arrears' then loan_id end)::float / count(distinct loan_id)::float * 100
             else 0 end as arrears_rate_pct,

        case when count(distinct loan_id) > 0
             then count(distinct case when account_status_l2 = 'FPD' then loan_id end)::float / count(distinct loan_id)::float * 100
             else 0 end as fpd_rate_pct

    from credit_with_customer
    where age_group is not null and age_group != 'Unknown'
    group by snapshot_date, age_group
),

by_income_group as (
    select
        snapshot_date,
        income_group,

        count(distinct loan_id) as total_loans,
        sum(balance) as total_balance,
        sum(total_paid) as total_payments,
        sum(arrears) as total_arrears,
        avg(days_past_due) as avg_days_past_due,
        avg(avg_monthly_income) as avg_income,

        -- Status distribution
        count(distinct case when account_status_l2 = 'Active' then loan_id end) as active_loans,
        count(distinct case when account_status_l2 = 'FPD' then loan_id end) as fpd_loans,
        count(distinct case when account_status_l2 = 'Paid Off' then loan_id end) as paid_off_loans,

        -- Arrears metrics
        count(distinct case when balance_due_status = 'Arrears' then loan_id end) as arrears_count,
        sum(case when balance_due_status = 'Arrears' then arrears else 0 end) as total_arrears_amount,

        -- Rates
        case when count(distinct loan_id) > 0
             then count(distinct case when balance_due_status = 'Arrears' then loan_id end)::float / count(distinct loan_id)::float * 100
             else 0 end as arrears_rate_pct,

        case when count(distinct loan_id) > 0
             then count(distinct case when account_status_l2 = 'FPD' then loan_id end)::float / count(distinct loan_id)::float * 100
             else 0 end as fpd_rate_pct

    from credit_with_customer
    where income_group is not null and income_group != 'Unknown'
    group by snapshot_date, income_group
),

by_age_and_income as (
    select
        snapshot_date,
        age_group,
        income_group,

        count(distinct loan_id) as total_loans,
        avg(avg_monthly_income) as avg_income,

        -- Arrears rate
        case when count(distinct loan_id) > 0
             then count(distinct case when balance_due_status = 'Arrears' then loan_id end)::float / count(distinct loan_id)::float * 100
             else 0 end as arrears_rate_pct,

        avg(days_past_due) as avg_days_past_due

    from credit_with_customer
    where age_group is not null and age_group != 'Unknown'
      and income_group is not null and income_group != 'Unknown'
    group by snapshot_date, age_group, income_group
),

combined as (
    select
        'age' as segment_type,
        snapshot_date,
        age_group as segment_value,
        null as segment_value_2,
        total_loans,
        total_balance,
        total_payments,
        avg_income,
        arrears_count,
        arrears_rate_pct,
        fpd_rate_pct,
        avg_days_past_due
    from by_age_group

    union all

    select
        'income' as segment_type,
        snapshot_date,
        income_group as segment_value,
        null as segment_value_2,
        total_loans,
        total_balance,
        total_payments,
        avg_income,
        arrears_count,
        arrears_rate_pct,
        fpd_rate_pct,
        avg_days_past_due
    from by_income_group

    union all

    select
        'age_x_income' as segment_type,
        snapshot_date,
        age_group as segment_value,
        income_group as segment_value_2,
        total_loans,
        null as total_balance,
        null as total_payments,
        avg_income,
        null as arrears_count,
        arrears_rate_pct,
        null as fpd_rate_pct,
        avg_days_past_due
    from by_age_and_income
)

select * from combined
order by segment_type, snapshot_date, segment_value
