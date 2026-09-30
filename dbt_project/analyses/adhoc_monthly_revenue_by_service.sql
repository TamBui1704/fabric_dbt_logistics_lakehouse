-- File: analyses/adhoc_monthly_revenue_by_service.sql
-- Ý NGHĨA: Truy vấn phân tích ad-hoc thống kê tổng doanh thu 
-- và số lượng đơn hàng theo từng loại dịch vụ logistics.
-- GHI CHÚ: File này KHÔNG tạo ra Bảng hay View nào trên Database khi chạy dbt run.

SELECT
    s.service_name,
    s.service_category,
    COUNT(r.shipment_id) AS total_shipments,
    SUM(r.revenue_amount) AS total_revenue_vnd,
    AVG(r.weight_kg) AS avg_weight_kg
FROM {{ ref('fact_revenue') }} r
INNER JOIN {{ ref('dim_service') }} s ON r.service_id = s.service_id
GROUP BY 
    s.service_name,
    s.service_category
ORDER BY total_revenue_vnd DESC
