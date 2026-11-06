-- Grain: một tháng và một bang của người mua, tháng 1 tới tháng 4 năm 2018
-- Sau khi join order_items, một dòng là một dòng hàng, nên đếm đơn bằng count(DISTINCT)
SELECT CAST(date_trunc('month', o.order_purchase_timestamp) AS DATE) AS thang,
       c.customer_state AS bang,
       count(DISTINCT o.order_id) AS so_don,
       count(DISTINCT o.order_id) FILTER (WHERE o.order_status = 'canceled') AS bi_huy,
       sum(CAST(i.price AS DECIMAL(12,2))) AS tien_hang
FROM orders o
JOIN order_items i ON i.order_id = o.order_id
JOIN customers c ON c.customer_id = o.customer_id
WHERE o.order_purchase_timestamp >= '2018-01-01'
  AND o.order_purchase_timestamp < '2018-05-01'
GROUP BY 1, 2
HAVING count(DISTINCT o.order_id) > 1000
ORDER BY thang, bang;
