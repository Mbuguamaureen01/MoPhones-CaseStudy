{{
    config(
        materialized='view'
    )
}}

with nps_raw as (
    select *
    from {{ source('raw', 'nps_data') }}
),

cleaned as (
    select
        "Loan Id" as loan_id,
        "Submission ID" as submission_id,
        "Respondent ID" as respondent_id,
        "Submitted at"::timestamp as submitted_at,

        -- NPS score (0-10 scale)
        "Using a scale from 0 (not likely) to 10 (very likely), how likely are you to recommend MoPhones to friends or family?" as nps_score,

        -- Feedback fields
        "What is the main reason for your score?" as nps_reason,
        "What is one thing we could do to improve your experience with us?" as improvement_suggestion,

        -- Satisfaction fields
        "Are you happy with the quality and performance of your MoPhones device?" as happy_with_device_quality,
        "Are you happy with the service and support provided by MoPhones?" as happy_with_service,

        -- Experience fields
        "Have you ever experienced a delay in your payment reflecting in your Mophones account?" as experienced_payment_delay,
        "Have you ever had difficulty getting assistance from MoPhones customer support when needed?" as difficulty_getting_support,
        -- Note: Skipping column with special character encoding issues
        "Have you experienced any battery-related issues with your MoPhones device?" as battery_issues,
        "Have you used the MoPhones app (MoApp) to manage your account or make payments?" as moapp_usage,
        "Which communication channel do you prefer when contacting MoPhones for inquiries or support?" as preferred_channel,
        "Have you ever had your phone lock despite making a payment on time?" as phone_locked_despite_payment,
        "Any other Feedback?" as additional_feedback,

        -- Classify NPS into categories
        case
            when "Using a scale from 0 (not likely) to 10 (very likely), how likely are you to recommend MoPhones to friends or family?" >= 9 then 'Promoter'
            when "Using a scale from 0 (not likely) to 10 (very likely), how likely are you to recommend MoPhones to friends or family?" >= 7 then 'Passive'
            else 'Detractor'
        end as nps_category

    from nps_raw
    where "Loan Id" is not null
)

select * from cleaned
