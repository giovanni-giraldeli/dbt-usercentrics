WITH classified_bridge AS (
	SELECT
		-- Creating a surrogate key to identify the bridge between the customers and their current domain groups
		ROW_NUMBER() OVER (ORDER BY dw_valid_from, customer_id, domain_group_id) AS customers_domain_groups_sk,
		customer_sk,
		domain_group_sk,
		customer_id,
		domain_group_id,
		is_deleted,
		domain_group_deleted_dt,
		dw_valid_from
	FROM
        {{ ref('int_customers_domain_groups_history_bridge') }}
	WHERE
		is_current = 1
)
SELECT
	customers_domain_groups_sk,
	customer_sk,
	domain_group_sk,
	customer_id,
	domain_group_id,
	is_deleted,
	domain_group_deleted_dt
FROM
	classified_bridge
