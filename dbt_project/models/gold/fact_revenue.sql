{{ config(
    materialized='incremental',
    unique_key='shipment_id'
) }}

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

{% if is_incremental() %}
  -- Lọc lấy dữ liệu mới và quét lùi 2 ngày phòng ngừa dữ liệu đến muộn (Late-arriving facts)
  WHERE CAST(created_at AS DATE) >= (
      SELECT COALESCE(DATEADD(day, -2, MAX(created_date)), '1900-01-01') FROM {{ this }}
  )
{% endif %}
