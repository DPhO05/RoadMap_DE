-- Grain: một tháng đặt hàng; lọc năm 2017 sau khi lag đã chạy
WITH theo_thang AS (
  SELECT CAST(date_trunc('month', o.order_purchase_timestamp) AS DATE) AS thang,
         sum(CAST(i.price AS DECIMAL(12,2))) AS tien_hang
  FROM orders o JOIN order_items i ON i.order_id = o.order_id
  GROUP BY 1
), co_lag AS (
  SELECT thang, tien_hang,
         round(100.0 * (tien_hang - lag(tien_hang) OVER (ORDER BY thang))
               / lag(tien_hang) OVER (ORDER BY thang), 1) AS tang_phan_tram
  FROM theo_thang
)
SELECT * FROM co_lag
WHERE thang >= '2017-01-01' AND thang < '2017-07-01'
ORDER BY thang;
