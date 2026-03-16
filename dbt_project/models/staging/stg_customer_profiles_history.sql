WITH profile_conformed AS (
	SELECT
		user_id AS customer_id,
		address_country_code,
		CASE
			-- Standardizing the plan names for the free plans
			WHEN customer_plan IN ('Freemium', 'Trial', 'Free Premium')
				THEN 'Free Premium'
			-- Identifying cases that are missing
			WHEN customer_plan IS NULL
				THEN 'Unknown'
			ELSE customer_plan
		END AS customer_plan,
		customer_payment_type,
		dw_valid_from,
		dw_valid_to
	FROM
        {{ source('aspnet', 'aspnet_profile') }}
)
-- Reducing the records to create continuous streams of information
-- This is avoid duplicate lines, but with different DW dates
, profile_clean AS (
	SELECT
		customer_id,
		address_country_code,
		customer_plan,
		customer_payment_type,
		MIN(dw_valid_from) AS dw_valid_from,
		MAX(dw_valid_to) AS dw_valid_to
	FROM
		profile_conformed
	GROUP BY
		customer_id,
		address_country_code,
		customer_plan,
		customer_payment_type
)
SELECT
	pc.customer_id,
	COALESCE(dc.country_name, 'Unknown') AS customer_country_name,
	pc.customer_plan,
	CASE
		-- Every Free Premium plan is not associated with a payment type
		WHEN pc.customer_plan = 'Free Premium'
		AND pc.customer_payment_type IS NULL
			-- Flagging N/A for Free Premium payment types
			THEN 'N/A'
		-- All other plans have payment types associated currently
		WHEN pc.customer_payment_type IS NOT NULL
			THEN pc.customer_payment_type
		-- Identify cases that don't fit in the current rules
		ELSE 'Unknown'
	END AS customer_payment_type,
	pc.dw_valid_from,
	pc.dw_valid_to
FROM
	profile_clean AS pc
LEFT JOIN
	{{ ref('dim_countries') }} AS dc
		ON dc.country_iso_code_2_digits = pc.address_country_code