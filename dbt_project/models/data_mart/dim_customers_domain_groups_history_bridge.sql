SELECT
    customers_domain_groups_history_sk,
    customer_history_sk,
    domain_group_history_sk,
    is_current,
    is_deleted,
    domain_group_deleted_dt,
    dw_valid_from AS starts_at,
    dw_valid_to AS ends_at,
    CURRENT_TIMESTAMP AS dw_created_at,
    CURRENT_TIMESTAMP AS dw_updated_at			
FROM
    {{ ref('int_customers_domain_groups_history_bridge') }}
