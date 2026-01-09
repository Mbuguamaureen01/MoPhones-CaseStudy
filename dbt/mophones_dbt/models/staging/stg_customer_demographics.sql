{{
    config(
        materialized='view'
    )
}}

-- Combine all customer demographic data (DOB, Gender, Income)
with dob_data as (
    select
        -- Note: Column name has trailing space
        "Loan Id " as loan_id,
        date_of_birth::date as date_of_birth
    from {{ source('raw', 'customer_dob') }}
    where "Loan Id " is not null
      and "Loan Id " != ''
      and "Loan Id " != '#N/A'
),

gender_data as (
    select
        "Loan Id" as loan_id,

        -- Standardize citizenship: KENYAN, CITIZEN, Kenyan → "Kenyan"
        case
            when upper(trim(citizenship)) in ('KENYAN', 'CITIZEN') then 'Kenyan'
            else citizenship
        end as citizenship,

        -- Standardize gender: M, Male, MALE → "Male"; F, Female, FEMALE → "Female"
        case
            when upper(trim(gender)) in ('M', 'MALE') then 'Male'
            when upper(trim(gender)) in ('F', 'FEMALE') then 'Female'
            else gender
        end as gender

    from {{ source('raw', 'customer_gender') }}
    where "Loan Id" is not null
      and "Loan Id" != ''
      and "Loan Id" != '#N/A'
),

income_data as (
    select
        "Loan Id" as loan_id,
        duration,
        received,
        "Persons Received From Total" as persons_received_total,
        "Banks Received" as banks_received,
        "Paybills Received Others" as paybills_received_others
    from {{ source('raw', 'customer_income') }}
    where "Loan Id" is not null
      and "Loan Id" != ''
      and "Loan Id" != '#N/A'
),

combined as (
    select
        coalesce(d.loan_id, g.loan_id, i.loan_id) as loan_id,

        -- DOB data
        d.date_of_birth,

        -- Gender data
        g.citizenship,
        g.gender,

        -- Income data
        i.duration,
        i.received,
        i.persons_received_total,
        i.banks_received,
        i.paybills_received_others,

        -- Calculate total income (sum of income-related columns)
        (coalesce(i.received, 0) + coalesce(i.persons_received_total, 0) +
         coalesce(i.banks_received, 0) + coalesce(i.paybills_received_others, 0)) as total_income,

        -- Calculate average monthly income (total / duration)
        case
            when i.duration > 0 then
                (coalesce(i.received, 0) + coalesce(i.persons_received_total, 0) +
                 coalesce(i.banks_received, 0) + coalesce(i.paybills_received_others, 0)) / i.duration
            else null
        end as avg_monthly_income

    from dob_data d
    full outer join gender_data g on d.loan_id = g.loan_id
    full outer join income_data i on coalesce(d.loan_id, g.loan_id) = i.loan_id
)

select * from combined
