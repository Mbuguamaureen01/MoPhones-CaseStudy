{% macro union_credit_snapshots() %}
    {#
        This macro dynamically unions all credit_data_* tables
        New quarters are automatically detected - no code changes needed!
    #}

    {% set get_credit_tables_query %}
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name LIKE 'credit_data_%'
        ORDER BY table_name
    {% endset %}

    {% set credit_tables = run_query(get_credit_tables_query) %}

    {% if execute %}
        {% for table in credit_tables %}
            select
                loan_id,
                strptime(date, '%m/%d/%Y')::date as snapshot_date,
                customer_age as account_age_days,
                total_paid,
                total_due_today,
                balance,
                days_past_due,
                closing_balance,
                advance,
                balance_due_to_date,
                arrears,
                balance_due_status,
                payment,
                expected_payment,
                first_payment,
                first_expected_payment,
                account_status_l1,
                account_status_l2,
                case when return_date = 'None' then null else strptime(return_date, '%m/%d/%Y')::date end as return_date,
                strptime(sale_date, '%m/%d/%Y')::date as sale_date,
                credit_check_done,
                payment_amount,
                adjustment_amount,
                prepayment_amount,
                deposit,
                weekly_rate,
                case when credit_expiry = 'None' then null else strptime(credit_expiry, '%m/%d/%Y')::date end as credit_expiry,
                strptime(next_invoice_date, '%m/%d/%Y')::date as next_invoice_date,
                discount,
                overpayment_amount,
                case when max_payment_date = 'None' then null else strptime(max_payment_date, '%m/%d/%Y')::date end as max_payment_date,
                initial_pay,
                total_paid_with_adjustments_15d
            from {{ table[0] }}

            {% if not loop.last %}
            union all
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
