-- Retrieving the first time that each domain_id appeared to create a surrogate key with them
WITH first_domain AS (
	SELECT
		domain_id,
		MIN(dw_valid_from) AS dw_valid_from
	FROM
		{{ ref('stg_domains_history') }}
	GROUP BY
		domain_id
)
-- Creating the domain surrogate key
, domains_identified AS (
	SELECT
		-- Creating a surrogate key to identify each domain within the data warehouse
		ROW_NUMBER() OVER (ORDER BY dw_valid_from, domain_id) AS domain_sk,
		domain_id		
	FROM
		first_domain
)
-- Retrieving the first time that each domain_id appeared to create a surrogate key with them
, first_domain_group AS (
	SELECT
		domain_group_id,
		MIN(dw_valid_from) AS dw_valid_from
	FROM
		{{ ref('stg_domains_history') }}
	GROUP BY
		domain_group_id
)
-- Creating the domain surrogate key
, domain_groups_identified AS (
	SELECT
		-- Creating a surrogate key to identify each domain within the data warehouse
		ROW_NUMBER() OVER (ORDER BY dw_valid_from, domain_group_id) AS domain_group_sk,
		domain_group_id
	FROM
		first_domain_group
)
, domains_classified AS (
	SELECT
		-- Ranking the transaction from last to first, so that we can determine which is the current transaction
		ROW_NUMBER() OVER (PARTITION BY sdh.domain_id ORDER BY sdh.dw_valid_to DESC) AS rn,
		di.domain_sk,
		dgi.domain_group_sk,
		sdh.domain_id,
		sdh.domain_group_id,
		sdh.is_temp_domains,
		CASE
			WHEN sdh.full_subpage_count <= 500
				THEN 'S'
			WHEN sdh.full_subpage_count <= 5000
				THEN 'M'
			WHEN sdh.full_subpage_count > 5000
				THEN 'L'
		END AS domain_package_size,
		sdh.full_subpage_count,
		sdh.dw_valid_from,
		sdh.dw_valid_to
	FROM
		{{ ref('stg_domains_history') }} AS sdh
	LEFT JOIN
		domains_identified AS di
			ON di.domain_id = sdh.domain_id
	LEFT JOIN
		domain_groups_identified AS dgi
			ON dgi.domain_group_id = sdh.domain_group_id
)
, domains_expanded AS (
	SELECT
		b.domain_group_history_sk,
		dc.domain_sk,
		dc.domain_group_sk,
		dc.domain_id,
		dc.domain_group_id,
		dc.is_temp_domains,
		CASE
			WHEN dc.rn = 1
				THEN 1
			ELSE 0
		END AS is_current,
		CASE
			WHEN dc.rn = 1
			AND dc.dw_valid_to <> '9999-12-31'
				THEN 1
			ELSE 0
		END AS is_deleted,
		dc.domain_package_size,
		dc.full_subpage_count,
		CASE
			WHEN dc.rn = 1
			AND dc.dw_valid_to <> '9999-12-31'
				THEN dc.dw_valid_to
		END AS domain_deleted_dt,
		-- Returning the last event start
		GREATEST(dc.dw_valid_from, b.dw_valid_from) AS dw_valid_from,
		-- Returning the first event end
		LEAST(dc.dw_valid_to, b.dw_valid_to) AS dw_valid_to
	FROM
		domains_classified AS dc
	LEFT JOIN
		{{ ref('int_customers_domain_groups_history_bridge') }} AS b
			ON dc.domain_group_id = b.domain_group_id
			AND dc.dw_valid_from >= b.dw_valid_from
			AND dc.dw_valid_from < b.dw_valid_to
)
SELECT
	-- Creating a surrogate key to identify each domain change historically within the data warehouse
	ROW_NUMBER() OVER (ORDER BY dw_valid_from, domain_id) AS domain_history_sk,
	domain_group_history_sk,
	domain_sk,
	domain_group_sk,
	domain_id,
	domain_group_id,
	is_temp_domains,
	is_current,
	is_deleted,
	domain_package_size,
	full_subpage_count,
	domain_deleted_dt,
	dw_valid_from,
	dw_valid_to
FROM
	domains_expanded