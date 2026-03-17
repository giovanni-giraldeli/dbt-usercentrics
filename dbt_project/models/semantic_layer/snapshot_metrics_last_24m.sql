SELECT
	snapshot_dt,
	-- Calculating the unique customers by their identifier and removing the unknown ones
	COUNT(DISTINCT NULLIF(customer_sk, -1)) AS unique_customers_count,
	-- Calculating the unique domains by their identifier and removing the temporary domains
	-- No need for removing the deleted, since we already did it at the moment determining the iligibility for the snapshot
	SUM(CASE WHEN is_temp_domains = 0 THEN unique_domains_count END) AS unique_domains_count,
	-- Calculating the unique domains, removing the temporary and filtering only domains with package size S
	SUM(
		CASE
			WHEN domain_package_size = 'S'
			AND is_temp_domains = 0
				THEN unique_domains_count
		END
	) AS unique_domains_s_package_count,
	-- Calculating the unique domains, removing the temporary and filtering only domains with package size M
	SUM(
		CASE
			WHEN domain_package_size = 'M'
			AND is_temp_domains = 0
				THEN unique_domains_count
		END
	) AS unique_domains_m_package_count,
	-- Calculating the unique domains, removing the temporary and filtering only domains with package size L
	SUM(
		CASE
			WHEN domain_package_size = 'L'
			AND is_temp_domains = 0
				THEN unique_domains_count
		END
	) AS unique_domains_l_package_count
FROM
	{{ ref('cube_customers_snapshots_last_24m') }}
GROUP BY
	snapshot_dt