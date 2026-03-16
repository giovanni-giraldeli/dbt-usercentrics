-- Determining how many days we're adding to the table -> 2020-01-01 + 365 days * 1000 years
WITH RECURSIVE date_range AS (
	-- Base case
    SELECT
		DATE '2010-01-01' AS date_sk,
        1 AS i
    UNION ALL
    -- Recursive step
    SELECT
        DATE_ADD(date_sk, 1) AS date_sk,
        i + 1 AS i
    FROM
        date_range
    WHERE
        i <= ( 365 * 1000 ) 
)
-- Retrieving the most relevant date parts
, date_enhanced AS (
	SELECT
		date_sk,
		DATETRUNC('WEEK', date_sk) AS sunday_week_start_dt,
		DATETRUNC('MONTH', date_sk) AS month_start_dt,
		DATETRUNC('QUARTER', date_sk) AS quarter_start_dt,
		DATETRUNC('YEAR', date_sk) AS year_start_dt
	FROM
		date_range
)
-- Enhancing with the endings of each date part
SELECT
	date_sk,
	sunday_week_start_dt,
	month_start_dt,
	quarter_start_dt,
	year_start_dt,
	-- Adding a month and subtracting a day to get the month end
    sunday_week_start_dt + INTERVAL 1 WEEK - INTERVAL 1 DAY AS sunday_week_end_dt,
	-- Adding a month and subtracting a day to get the month end
    month_start_dt + INTERVAL 1 MONTH - INTERVAL 1 DAY AS month_end_dt,
	-- Adding a quarter and subtracting a day to get the quarter end
    quarter_start_dt + INTERVAL 1 QUARTER - INTERVAL 1 DAY AS quarter_end_dt,
	-- Adding a year and subtracting a day to get the year end
    year_start_dt + INTERVAL 1 YEAR - INTERVAL 1 DAY AS year_end_dt
FROM
	date_enhanced