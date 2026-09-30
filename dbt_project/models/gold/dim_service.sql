{{ config(materialized='table') }}

SELECT
    s.service_id,
    s.service_name,
    s.service_group,
    CAST(COALESCE(l.service_category, 'Khac') AS VARCHAR(50)) AS service_category,
    COALESCE(l.max_delivery_hours, 48) AS max_delivery_hours,
    COALESCE(l.penalty_rate, 0.0) AS penalty_rate
FROM {{ source('silver_layer', 'silver_services') }} s
LEFT JOIN {{ ref('service_sla_lookup') }} l ON s.service_id = l.service_id


