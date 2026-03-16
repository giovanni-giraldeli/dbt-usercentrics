SELECT
	domain_group_id,
	customer_id,
	MIN(dw_valid_from) AS dw_valid_from,
	MAX(dw_valid_to) AS dw_valid_to
FROM
    {{ source('domains', 'domain_group') }}
GROUP BY
	domain_group_id,
	customer_id