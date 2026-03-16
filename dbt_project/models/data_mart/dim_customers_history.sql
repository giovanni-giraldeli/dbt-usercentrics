SELECT
    customer_history_sk,
    customer_sk,
    customer_id,
    customer_country_name,
    customer_plan,
    customer_payment_type,
    is_current,
    is_canceled,
    CAST(customer_created_ts AS DATE) AS customer_created_dt,
    customer_canceled_dt,
    dw_valid_from AS starts_at,
    dw_valid_to AS ends_at,
    CURRENT_TIMESTAMP AS dw_created_at,
    CURRENT_TIMESTAMP AS dw_updated_at			
FROM
    {{ ref('int_customers_history') }}
