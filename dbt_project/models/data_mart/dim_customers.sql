WITH customer_metrics AS (
    SELECT
        b.customer_sk,
        -- Metrics that would be nice to have, but aren't required
        --COUNT(DISTINCT id.domain_group_id) AS cnt_dist_domain_groups,
        --COUNT(DISTINCT id.domain_id) AS cnt_dist_domains,
        --SUM(id.full_subpage_count) AS total_subpages_sum
        MAX(fd.full_subpage_count) AS largest_subpage_count
    FROM
        {{ ref('fact_domains') }} AS fd
    INNER JOIN
        {{ ref('dim_customers_domain_groups_bridge') }} AS b
            ON b.domain_group_sk = fd.domain_group_sk
    GROUP BY
        b.customer_sk
)
SELECT
    ic.customer_sk,
    ic.customer_id,
    ic.customer_country_name,
    ic.customer_plan,
    ic.customer_payment_type,
    COALESCE(cm.largest_subpage_count, 0) AS largest_subpage_count,
    CAST(ic.customer_created_ts AS DATE) AS customer_created_dt,
    ic.customer_canceled_dt,
    CURRENT_TIMESTAMP AS dw_created_at,
    CURRENT_TIMESTAMP AS dw_updated_at
FROM
    {{ ref('int_customers') }} AS ic
LEFT JOIN
    customer_metrics AS cm
        ON ic.customer_sk = cm.customer_sk
