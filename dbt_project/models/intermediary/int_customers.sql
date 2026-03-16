WITH customer_last_record AS (
SELECT
	-- Ideally I'd execute this operation with the customer_sk, but since customer_sk and customer_id are 1:1, ...
	-- ... I'll execute with the customer_id to save some compute (materialized column vs window function virtualization)
	customer_id,
	MAX(dw_valid_to) AS dw_valid_to
FROM
	{{ ref('int_customers_history') }}
GROUP BY
	customer_id
)
SELECT
	ich.customer_sk,
	ich.customer_id,
	ich.customer_country_name,
	ich.customer_plan,
	ich.customer_payment_type,
	ich.customer_created_ts,
	CASE
		WHEN ich.dw_valid_to <> '9999-12-31'
			THEN ich.dw_valid_to
	END AS customer_canceled_dt
FROM
	{{ ref('int_customers_history') }} AS ich
INNER JOIN
	customer_last_record AS clr
		ON clr.customer_id = ich.customer_id
		AND clr.dw_valid_to = ich.dw_valid_to