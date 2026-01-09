{{
    config(
        materialized='table'
    )
}}

-- Analyze relationship between credit outcomes and customer satisfaction (NPS)
with credit_with_nps as (
    select * from {{ ref('int_credit_with_nps') }}
),

-- Get the most recent snapshot for each loan
latest_snapshot as (
    select
        *,
        row_number() over (partition by loan_id order by snapshot_date desc) as rn
    from credit_with_nps
),

credit_nps_analysis as (
    select
        loan_id,
        snapshot_date,
        nps_score,
        nps_category,

        -- Credit metrics
        balance,
        total_paid,
        arrears,
        days_past_due,
        balance_due_status,
        account_status_l2,

        -- Customer experience indicators from NPS
        happy_with_device_quality,
        happy_with_service,
        experienced_payment_delay,
        difficulty_getting_support,
        battery_issues,
        phone_locked_despite_payment,

        -- Product info
        product_name,
        loan_term,

        -- DPD buckets
        case
            when days_past_due = 0 then 'Current'
            when days_past_due between 1 and 30 then '1-30 DPD'
            when days_past_due between 31 and 60 then '31-60 DPD'
            when days_past_due between 61 and 90 then '61-90 DPD'
            else '90+ DPD'
        end as dpd_bucket,

        -- Payment performance categories
        case
            when balance_due_status = 'up to date' then 'Good'
            when balance_due_status = 'Advance' then 'Excellent'
            when arrears > 0 and days_past_due <= 30 then 'Fair'
            when arrears > 0 and days_past_due > 30 then 'Poor'
            else 'Unknown'
        end as payment_performance

    from latest_snapshot
    where rn = 1
        and nps_score is not null  -- Only include loans with NPS responses
),

summary_by_nps as (
    select
        nps_category,
        count(*) as loan_count,
        avg(nps_score) as avg_nps_score,
        avg(days_past_due) as avg_dpd,
        sum(case when balance_due_status = 'Arrears' then 1 else 0 end) as arrears_count,
        sum(case when balance_due_status = 'Arrears' then 1 else 0 end)::float / count(*)::float * 100 as arrears_rate_pct,
        avg(balance) as avg_balance,
        avg(total_paid) as avg_total_paid,

        -- Customer experience metrics
        sum(case when phone_locked_despite_payment = 'Yes' then 1 else 0 end) as phone_locked_count,
        sum(case when experienced_payment_delay = 'Yes' then 1 else 0 end) as payment_delay_count,
        sum(case when difficulty_getting_support = 'Yes' then 1 else 0 end) as support_issues_count

    from credit_nps_analysis
    group by nps_category
),

summary_by_payment_performance as (
    select
        payment_performance,
        count(*) as loan_count,
        avg(nps_score) as avg_nps_score,
        sum(case when nps_category = 'Promoter' then 1 else 0 end) as promoters,
        sum(case when nps_category = 'Passive' then 1 else 0 end) as passives,
        sum(case when nps_category = 'Detractor' then 1 else 0 end) as detractors,

        -- Calculate NPS score: (Promoters - Detractors) / Total * 100
        ((sum(case when nps_category = 'Promoter' then 1 else 0 end)::float -
          sum(case when nps_category = 'Detractor' then 1 else 0 end)::float) /
         count(*)::float) * 100 as nps_score_calculated

    from credit_nps_analysis
    group by payment_performance
)

-- Return detailed loan-level data for further analysis
select * from credit_nps_analysis
