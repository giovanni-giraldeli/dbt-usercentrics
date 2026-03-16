WITH customers AS (
	SELECT
		-- Creating a surrogate key to identify the customer within the data warehouse
		ROW_NUMBER() OVER (ORDER BY dw_valid_from, customer_created_ts, customer_id) AS customer_sk,
		customer_id,
		customer_created_ts
	FROM
		{{ ref('stg_customers_created') }}
)
, customers_classified AS (
	SELECT
		-- Creating a surrogate key to identify the customer historical information changes within the data warehouse
		ROW_NUMBER() OVER (ORDER BY scph.dw_valid_from, c.customer_sk, scph.customer_id) AS customer_history_sk,
		-- Ranking the customer information from last to first, so that we can determine which is the current information
		ROW_NUMBER() OVER (PARTITION BY scph.customer_id ORDER BY scph.dw_valid_to DESC) AS rn,
		c.customer_sk,
		scph.customer_id,
		scph.customer_country_name,
		scph.customer_plan,
		scph.customer_payment_type,
		c.customer_created_ts,
		scph.dw_valid_from,
		scph.dw_valid_to
	FROM
		{{ ref('stg_customer_profiles_history') }} AS scph
	INNER JOIN
		customers AS c
			ON c.customer_id = scph.customer_id
)
SELECT
	customer_history_sk,
	customer_sk,
	customer_id,
	customer_country_name,
	customer_plan,
	customer_payment_type,
	CASE
		WHEN rn = 1
			THEN 1
		ELSE 0
	END AS is_current,
	CASE
		WHEN rn = 1
		AND dw_valid_to <> '9999-12-31'
			THEN 1
		ELSE 0
	END AS is_canceled,	
	CASE
		WHEN rn = 1
		AND dw_valid_to <> '9999-12-31'
			THEN dw_valid_to
	END AS customer_canceled_dt,	
	customer_created_ts,
	dw_valid_from,
	dw_valid_to
FROM
	customers_classified