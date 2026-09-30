-- File: tests/assert_positive_revenue_and_weight.sql
-- Ý NGHĨA NGHIỆP VỤ: Kiểm tra chất lượng dữ liệu trong bảng fact_revenue.
-- Không được phép tồn tại đơn hàng có doanh thu bị âm (revenue_amount < 0) 
-- hoặc khối lượng hàng bị âm/bằng 0 (weight_kg <= 0).
-- 
-- QUY TẮC DBT: 
-- - Nếu câu SQL này trả về 0 dòng  -> TEST PASS (Dữ liệu hợp lệ).
-- - Nếu câu SQL này trả về > 0 dòng -> TEST FAIL (Có dòng dữ liệu bị lỗi).

SELECT
    shipment_id,
    revenue_amount,
    weight_kg
FROM {{ ref('fact_revenue') }}
WHERE revenue_amount < 0 
   OR weight_kg <= 0
