SELECT
	user_id AS customer_id,
	MIN(user_create_time) AS customer_created_ts,
	MIN(dw_valid_from) AS dw_valid_from,
	MAX(dw_valid_to) AS dw_valid_to
FROM
    {{ source('aspnet', 'aspnet_membership') }}
GROUP BY
	user_id