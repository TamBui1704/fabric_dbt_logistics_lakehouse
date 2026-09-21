{{ config(materialized='table') }}

SELECT
    pos_id,
    pos_name,
    province,
    region
FROM {{ source('silver_layer', 'silver_pos_locations') }}
