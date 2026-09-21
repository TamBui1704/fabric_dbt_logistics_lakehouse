{{ config(materialized='table') }}

WITH RECURSIVE dates AS (
    SELECT CAST('2024-01-01' AS DATE) AS date_val
    UNION ALL
    SELECT DATEADD(day, 1, date_val)
    FROM dates
    WHERE date_val < '2026-12-31'
)
SELECT
    date_val AS date_id,
    YEAR(date_val) AS year,
    MONTH(date_val) AS month,
    DAY(date_val) AS day,
    DATEPART(quarter, date_val) AS quarter,
    DATEPART(weekday, date_val) AS day_of_week
FROM dates
OPTION (MAXRECURSION 3650)
