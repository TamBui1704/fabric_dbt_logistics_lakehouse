{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_name,
    customer_type
FROM {{ source('silver_layer', 'silver_customers') }}
