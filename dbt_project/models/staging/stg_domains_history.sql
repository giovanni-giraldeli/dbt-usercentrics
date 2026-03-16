SELECT
	domain_id,
	domain_group_id,
	is_temp_domains,
	full_subpage_count,
	MIN(dw_valid_from) AS dw_valid_from,
	MAX(dw_valid_to) AS dw_valid_to
FROM
    {{ source('domains', 'domain') }}
GROUP BY
	domain_id,
	domain_group_id,
	is_temp_domains,
	full_subpage_count
