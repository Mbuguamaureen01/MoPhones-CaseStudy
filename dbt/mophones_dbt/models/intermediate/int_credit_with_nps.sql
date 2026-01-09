{{
    config(
        materialized='view'
    )
}}

-- Join credit data with NPS responses
with credit_with_customer as (
    select * from {{ ref('int_credit_with_customer') }}
),

nps as (
    select * from {{ ref('stg_nps') }}
),

-- Get the most recent NPS response for each loan
latest_nps as (
    select
        loan_id,
        nps_score,
        nps_category,
        submitted_at,
        happy_with_device_quality,
        happy_with_service,
        experienced_payment_delay,
        difficulty_getting_support,
        battery_issues,
        phone_locked_despite_payment,
        row_number() over (partition by loan_id order by submitted_at desc) as rn
    from nps
),

joined as (
    select
        cc.*,
        n.nps_score,
        n.nps_category,
        n.submitted_at as nps_submitted_at,
        n.happy_with_device_quality,
        n.happy_with_service,
        n.experienced_payment_delay,
        n.difficulty_getting_support,
        n.battery_issues,
        n.phone_locked_despite_payment

    from credit_with_customer cc
    left join latest_nps n
        on cc.loan_id = n.loan_id
        and n.rn = 1
)

select * from joined
