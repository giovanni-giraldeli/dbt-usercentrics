WITH last_date_calc AS (
    SELECT
        GREATEST(MAX(starts_at), MAX(ends_at)) AS last_date
    FROM
        {{ ref('fact_domains') }}
    WHERE
        ends_at <> DATE '9999-12-31'
)
, params AS (
    SELECT
        last_date,
        DATE_TRUNC('month', last_date) - INTERVAL 24 MONTH - INTERVAL 1 DAY AS first_snapshot,
        DATE_TRUNC('month', last_date) - INTERVAL 1 DAY AS last_snapshot
    FROM
        last_date_calc
)
, months AS (
    SELECT DISTINCT
        d.month_end_dt
    FROM
    	{{ ref('dim_date') }} AS d
    CROSS JOIN
    	params AS p
    WHERE
    	d.month_end_dt >= p.first_snapshot
    	AND d.month_end_dt <= p.last_snapshot
)
, customer_snapshot AS (
	SELECT
	    dch.customer_history_sk,
	    dch.customer_sk,
	    dps.domain_package_size,
	    dps.is_temp_domains,
	    dch.customer_country_name,
	    dch.customer_plan,
	    dch.customer_payment_type,
		COUNT(DISTINCT fd.domain_sk) AS unique_domains_count,
	    SUM(full_subpage_count) AS full_subpage_total_count,
	    m.month_end_dt AS snapshot_dt
	FROM
		{{ ref('fact_domains') }} AS fd
	INNER JOIN
		months m
			ON fd.starts_at <= m.month_end_dt
	   		AND fd.ends_at  >= m.month_end_dt
	LEFT JOIN
		{{ ref('dim_customers_domain_groups_history_bridge') }} AS b
			ON b.domain_group_history_sk = fd.domain_group_history_sk
	LEFT JOIN
		{{ ref('dim_customers_history') }} AS dch
			ON dch.customer_history_sk = b.customer_history_sk 
	LEFT JOIN
		{{ ref('dim_product_specs') }} AS dps
			ON dps.product_specs_sk = fd.product_specs_sk
	GROUP BY
	    dch.customer_history_sk,
	    dch.customer_sk,
	    dps.domain_package_size,
	    dps.is_temp_domains,
	    dch.customer_country_name,
	    dch.customer_plan,
	    dch.customer_payment_type,
	    m.month_end_dt
)
SELECT
	ROW_NUMBER() OVER (ORDER BY snapshot_dt, customer_history_sk) AS snapshot_sk,
	-- Some domain groups weren't identified and, therefore, customers weren't identified as well
	COALESCE(customer_history_sk, -1) AS customer_history_sk,
	COALESCE(customer_sk, -1) AS customer_sk,
	domain_package_size,
	is_temp_domains,
	COALESCE(customer_country_name, 'Unknown') AS customer_country_name,
	COALESCE(customer_plan, 'Unknown') AS customer_plan,
	COALESCE(customer_payment_type, 'Unknown') AS customer_payment_type,
	unique_domains_count,
	full_subpage_total_count,
	snapshot_dt
FROM
	customer_snapshot