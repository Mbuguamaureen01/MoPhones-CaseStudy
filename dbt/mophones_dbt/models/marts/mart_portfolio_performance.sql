{{
    config(
        materialized='table'
    )
}}

-- Portfolio performance metrics by snapshot date and customer segments
with credit_with_customer as (
    select * from {{ ref('int_credit_with_customer') }}
),

portfolio_metrics as (
    select
        snapshot_date,

        -- Overall portfolio metrics
        count(distinct loan_id) as total_loans,
        sum(balance) as total_balance,
        sum(total_paid) as total_payments,
        sum(arrears) as total_arrears,
        avg(days_past_due) as avg_days_past_due,

        -- Segmentation: Account Status
        count(distinct case when account_status_l2 = 'Active' then loan_id end) as active_loans,
        count(distinct case when account_status_l2 = 'FPD' then loan_id end) as fpd_loans,
        count(distinct case when account_status_l2 = 'Paid Off' then loan_id end) as paid_off_loans,
        count(distinct case when account_status_l2 = 'Inactive' then loan_id end) as inactive_loans,

        -- Balance Due Status
        count(distinct case when balance_due_status = 'up to date' then loan_id end) as up_to_date_count,
        count(distinct case when balance_due_status = 'Arrears' then loan_id end) as arrears_count,
        count(distinct case when balance_due_status = 'Advance' then loan_id end) as advance_count,

        -- Payment rates
        sum(case when balance_due_status = 'up to date' then balance else 0 end) as up_to_date_balance,
        sum(case when balance_due_status = 'Arrears' then balance else 0 end) as arrears_balance,
        sum(case when balance_due_status = 'Arrears' then arrears else 0 end) as total_arrears_amount,

        -- Days Past Due buckets
        count(distinct case when days_past_due = 0 then loan_id end) as dpd_0,
        count(distinct case when days_past_due between 1 and 30 then loan_id end) as dpd_1_30,
        count(distinct case when days_past_due between 31 and 60 then loan_id end) as dpd_31_60,
        count(distinct case when days_past_due between 61 and 90 then loan_id end) as dpd_61_90,
        count(distinct case when days_past_due > 90 then loan_id end) as dpd_90_plus,

        -- Product segmentation
        count(distinct case when product_name like '%Galaxy-S%' then loan_id end) as galaxy_s_loans,
        count(distinct case when product_name like '%Galaxy-A%' then loan_id end) as galaxy_a_loans,
        count(distinct case when product_name like '%Note%' then loan_id end) as note_loans,
        count(distinct case when product_name like '%iPhone%' then loan_id end) as iphone_loans,

        -- Loan term segmentation
        count(distinct case when loan_term = '12M' then loan_id end) as term_12m_loans,
        count(distinct case when loan_term = '18M' then loan_id end) as term_18m_loans,
        count(distinct case when loan_term = '24M' then loan_id end) as term_24m_loans,

        -- Returns
        count(distinct case when returned > 0 then loan_id end) as returned_devices

    from credit_with_customer
    group by snapshot_date
),

calculated_rates as (
    select
        *,

        -- Calculate key ratios
        case when total_loans > 0 then (arrears_count::float / total_loans::float) * 100 else 0 end as arrears_rate_pct,
        case when total_loans > 0 then (fpd_loans::float / total_loans::float) * 100 else 0 end as fpd_rate_pct,
        case when total_loans > 0 then (paid_off_loans::float / total_loans::float) * 100 else 0 end as paid_off_rate_pct,
        case when total_balance > 0 then (total_arrears_amount / total_balance) * 100 else 0 end as arrears_balance_pct,
        case when total_balance > 0 then (total_payments / total_balance) * 100 else 0 end as payment_collection_pct

    from portfolio_metrics
)

select * from calculated_rates
order by snapshot_date
