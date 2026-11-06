-- Grain: một ngày đặt hàng
WITH theo_ngay AS (
  SELECT CAST(o.order_purchase_timestamp AS DATE) AS ngay,
         sum(CAST(i.price AS DECIMAL(12,2))) AS tien_hang
  FROM orders o JOIN order_items i ON i.order_id = o.order_id
  GROUP BY 1
), cua_so AS (
  SELECT ngay, tien_hang,
         sum(tien_hang) OVER (ORDER BY ngay
                              ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS tong_7_ngay
  FROM theo_ngay
)
SELECT * FROM cua_so
WHERE ngay BETWEEN '2017-11-20' AND '2017-11-26'
ORDER BY ngay;
