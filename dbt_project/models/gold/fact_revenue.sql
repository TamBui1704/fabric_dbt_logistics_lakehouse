{{ config(materialized='table') }}

SELECT
    shipment_id,
    customer_id,
    service_id,
    origin_pos_id,
    dest_pos_id,
    revenue_amount,
    weight_kg,
    CAST(created_at AS DATE) AS created_date
FROM {{ source('silver_layer', 'silver_revenue') }}
