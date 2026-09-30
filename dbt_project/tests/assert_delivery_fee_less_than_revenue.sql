{# 
  File: tests/assert_delivery_fee_less_than_revenue.sql
  Ý NGHĨA NGHIỆP VỤ: Kiểm tra logic bất thường về chi phí giao vận.
  Phí thù lao trả cho shipper (delivery_fee) không được vượt quá 
  tổng doanh thu thu từ khách hàng (revenue_amount) của cùng đơn hàng đó.

  QUY TẮC DBT (JINJA COMMENT): 
  - Comment này đóng gói bằng Jinja nên bị xóa sạch hoàn toàn khi dbt biên dịch sang SQL gửi lên DB.
  - Nếu câu SQL này trả về 0 dòng  -> TEST PASS (Không có đơn hàng nào bị bù lỗ bất thường).
  - Nếu câu SQL này trả về > 0 dòng -> TEST FAIL (Phát hiện đơn hàng bị lỗ bất thường).
#}

SELECT
    d.shipment_id,
    d.delivery_fee,
    r.revenue_amount,
    (d.delivery_fee - r.revenue_amount) AS loss_amount
FROM {{ ref('fact_delivery_remuneration') }} d
INNER JOIN {{ ref('fact_revenue') }} r ON d.shipment_id = r.shipment_id
WHERE d.delivery_fee > r.revenue_amount
