{{ config(materialized='table') }}

SELECT
    shipment_id,
    delivery_pos_id,
    service_id,
    delivery_fee,
    CAST(delivered_at AS DATE) AS delivered_date
FROM {{ source('silver_layer', 'silver_delivery') }}
