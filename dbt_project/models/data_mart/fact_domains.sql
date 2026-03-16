SELECT
    id.domain_history_sk,
    COALESCE(id.domain_group_history_sk, -1) AS domain_group_history_sk,
    id.domain_sk,
    id.domain_group_sk,
    dps.product_specs_sk,
    id.is_current,
    id.is_deleted,
    id.full_subpage_count,
    id.domain_deleted_dt,
    id.dw_valid_from AS starts_at,
    id.dw_valid_to AS ends_at,
    CURRENT_TIMESTAMP AS dw_created_at,
    CURRENT_TIMESTAMP AS dw_updated_at
FROM
    {{ ref('int_domains') }} AS id
LEFT JOIN
    {{ ref('dim_product_specs') }} AS dps
        ON dps.domain_package_size = id.domain_package_size
        AND dps.is_temp_domains = id.is_temp_domains
