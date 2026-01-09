{{
    config(
        materialized='view'
    )
}}

with customer_raw as (
    select *
    from {{ source('raw', 'customer_sales') }}
),

cleaned as (
    select
        -- Use 'Loan Id' column (with space) as it appears in the data
        "Loan Id" as loan_id,
        sale_id,
        sale_date::date as sale_date,
        returned,
        case
            when return_date is null then null
            when cast(return_date as varchar) = 'NaT' then null
            else return_date::date
        end as return_date,
        sale_type,
        seller,
        seller_type,
        return_policy_compliance,
        cash_price,
        loan_price,
        client_model,
        business_model,
        loan_term,
        product_name,
        model as product_model
    from customer_raw
    -- Filter out invalid/duplicate rows (remove nulls and Excel errors)
    where "Loan Id" is not null
      and "Loan Id" != ''
      and "Loan Id" != '#N/A'
)

select * from cleaned
