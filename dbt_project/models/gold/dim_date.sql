{{ config(materialized='table') }}

WITH numbers AS (
    SELECT 0 AS n
    UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
),
tally AS (
    SELECT (n1.n + n2.n * 10 + n3.n * 100 + n4.n * 1000) AS day_offset
    FROM numbers n1
    CROSS JOIN numbers n2
    CROSS JOIN numbers n3
    CROSS JOIN numbers n4
    WHERE (n1.n + n2.n * 10 + n3.n * 100 + n4.n * 1000) <= DATEDIFF(day, '2024-01-01', '2026-12-31')
),
calendar AS (
    SELECT DATEADD(day, day_offset, CAST('2024-01-01' AS DATE)) AS date_val
    FROM tally
)
SELECT
    date_val AS date_id,
    YEAR(date_val) AS year,
    MONTH(date_val) AS month,
    DAY(date_val) AS day,
    DATEPART(quarter, date_val) AS quarter,
    DATEPART(weekday, date_val) AS day_of_week
FROM calendar
