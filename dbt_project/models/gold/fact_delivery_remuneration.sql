{{ config(
    materialized='incremental',
    unique_key='shipment_id'
) }}

SELECT
    shipment_id,
    delivery_pos_id,
    service_id,
    delivery_fee,
    CAST(delivered_at AS DATE) AS delivered_date
FROM {{ source('silver_layer', 'silver_delivery') }}

{% if is_incremental() %}
  -- Lọc lấy dữ liệu mới và quét lùi 2 ngày phòng ngừa dữ liệu đến muộn (Late-arriving facts)
  WHERE CAST(delivered_at AS DATE) >= (
      SELECT COALESCE(DATEADD(day, -2, MAX(delivered_date)), '1900-01-01') FROM {{ this }}
  )
{% endif %}
