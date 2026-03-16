SELECT
    customers_domain_groups_sk,
    customer_sk,
    domain_group_sk,
    is_deleted,
    CURRENT_TIMESTAMP AS dw_created_at,
    CURRENT_TIMESTAMP AS dw_updated_at
FROM
    {{ ref('int_customers_domain_groups_bridge') }}
