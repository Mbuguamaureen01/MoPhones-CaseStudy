{{
    config(
        materialized='view'
    )
}}

-- Join credit snapshots with enriched customer data (includes demographics, age/income groups)
with credit_snapshots as (
    select * from {{ ref('stg_credit_snapshots') }}
),

customers_enriched as (
    select * from {{ ref('int_customer_enriched') }}
),

joined as (
    select
        cs.*,
        c.product_name,
        c.product_model,
        c.loan_term,
        c.business_model,
        c.client_model,
        c.seller,
        c.seller_type,
        c.cash_price,
        c.loan_price,
        c.returned,
        c.return_date,

        -- Demographics
        c.date_of_birth,
        c.age_at_snapshot,
        c.age_group,
        c.gender,
        c.citizenship,
        c.avg_monthly_income,
        c.income_group,

        -- Calculate loan pricing metrics
        (c.loan_price - c.cash_price) as financing_markup,
        case
            when c.cash_price > 0 then ((c.loan_price - c.cash_price) / c.cash_price) * 100
            else null
        end as markup_percentage,

        -- Calculate account tenure at snapshot date
        datediff('day', cs.sale_date, cs.snapshot_date) as account_tenure_days,
        datediff('month', cs.sale_date, cs.snapshot_date) as account_tenure_months

    from credit_snapshots cs
    left join customers_enriched c
        on cs.loan_id = c.loan_id
        and cs.snapshot_date = c.snapshot_date
)

select * from joined
