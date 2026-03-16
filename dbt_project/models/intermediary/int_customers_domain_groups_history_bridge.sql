-- Deduplicating the domain_group_sk
WITH domain_groups_identified AS (
	SELECT
		ROW_NUMBER() OVER (ORDER BY MIN(dw_valid_from), domain_group_id) AS domain_group_sk,
		domain_group_id,
		MIN(dw_valid_from) AS dw_valid_from,
		MAX(dw_valid_to) AS dw_valid_to
	FROM
        {{ ref('stg_domains_history') }}
	GROUP BY
		domain_group_id
)
, domain_groups_classified AS (
	SELECT
		-- Ranking the event from last to first, so that we can determine which is the current event
		ROW_NUMBER() OVER (PARTITION BY b.domain_group_id, b.customer_id ORDER BY b.dw_valid_to DESC) AS rn,
		ich.customer_history_sk,
		ich.customer_sk,
		dgi.domain_group_sk,
		b.customer_id,
		b.domain_group_id,
		-- Returning the last event start
		GREATEST(b.dw_valid_from, ich.dw_valid_from, dgi.dw_valid_from) AS dw_valid_from,
		-- Returning the first event end
		LEAST(b.dw_valid_to, ich.dw_valid_to, dgi.dw_valid_to) AS dw_valid_to
	FROM
        {{ ref('stg_customers_domain_groups_bridge') }} AS b
	INNER JOIN
		{{ ref('int_customers_history') }} AS ich
			ON b.customer_id = ich.customer_id
			AND ich.dw_valid_from >= b.dw_valid_from
			AND ich.dw_valid_from < b.dw_valid_to
	INNER JOIN
		domain_groups_identified AS dgi
			ON dgi.domain_group_id = b.domain_group_id
			AND dgi.dw_valid_from >= b.dw_valid_from
			AND dgi.dw_valid_from < b.dw_valid_to
)
, domain_groups_reduced AS (
	SELECT
		ROW_NUMBER() OVER (ORDER BY MIN(dw_valid_from), domain_group_id, customer_id) AS domain_group_history_sk,
		customer_id,
		domain_group_id,
		MIN(dw_valid_from) AS dw_valid_from
	FROM
		domain_groups_classified
	GROUP BY
		customer_id,
		domain_group_id
)
SELECT
	-- Creating a surrogate key to identify historically each record of the bridge between customers and their domain groups
	ROW_NUMBER() OVER (ORDER BY c.dw_valid_from, c.customer_id, c.domain_group_id) AS customers_domain_groups_history_sk,
	c.customer_history_sk,
	r.domain_group_history_sk,
	c.customer_sk,
	c.domain_group_sk,
	c.customer_id,
	c.domain_group_id,
	CASE
		WHEN c.rn = 1
			THEN 1
		ELSE 0
	END AS is_current,
	CASE
		WHEN c.rn = 1
		AND c.dw_valid_to <> '9999-12-31'
			THEN 1
		ELSE 0
	END AS is_deleted,
	CASE
		WHEN c.rn = 1
		AND c.dw_valid_to <> '9999-12-31'
			THEN dw_valid_to
	END AS domain_group_deleted_dt,
	c.dw_valid_from,
	c.dw_valid_to
FROM
	domain_groups_classified AS c
LEFT JOIN
	domain_groups_reduced AS r
		ON c.domain_group_id = r.domain_group_id
		AND c.customer_id = r.customer_id