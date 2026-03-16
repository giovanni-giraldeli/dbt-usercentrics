SELECT
    ROW_NUMBER() OVER (ORDER BY MIN(dw_valid_from), is_temp_domains, domain_package_size) AS product_specs_sk,
    domain_package_size,
    is_temp_domains,
    CURRENT_TIMESTAMP AS dw_created_at,
    CURRENT_TIMESTAMP AS dw_updated_at
FROM
    {{ ref('int_domains') }}
GROUP BY
    domain_package_size,
    is_temp_domains
