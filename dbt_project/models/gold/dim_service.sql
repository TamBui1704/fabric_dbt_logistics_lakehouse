{{ config(materialized='table') }}

SELECT
    service_id,
    service_name,
    service_group
FROM {{ source('silver_layer', 'silver_services') }}
