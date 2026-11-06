/* Lời giải tham khảo, Lab 2.1 bước 2: mỗi dòng là một cách hiểu câu hỏi */
WITH thang_2 AS (
  SELECT * FROM orders
  WHERE order_purchase_timestamp >= TIMESTAMP '2017-02-01'
    AND order_purchase_timestamp <  TIMESTAMP '2017-03-01'
), utc AS (  /* đổi mốc UTC sang giờ São Paulo, kiểu của dữ liệu */
  SELECT TIMESTAMPTZ '2017-02-01 00:00:00+00' AT TIME ZONE 'America/Sao_Paulo' AS dau,
         TIMESTAMPTZ '2017-03-01 00:00:00+00' AT TIME ZONE 'America/Sao_Paulo' AS cuoi
)
SELECT 'a. mua trong tháng 2, giờ São Paulo' AS cach_hieu, count(*) AS so_don
FROM thang_2
UNION ALL
SELECT 'b. mua trong tháng 2, giờ UTC', count(*)
FROM orders, utc
WHERE order_purchase_timestamp >= dau AND order_purchase_timestamp < cuoi
UNION ALL
SELECT 'c. trong a, giao từ 00:00 ngày 01/03', count(*) FROM thang_2
WHERE order_delivered_customer_date >= TIMESTAMP '2017-03-01'
UNION ALL
SELECT 'd. trong a, giao từ 00:00 ngày 02/03', count(*) FROM thang_2
WHERE order_delivered_customer_date >= TIMESTAMP '2017-03-02'
UNION ALL
SELECT 'e. trong a, chưa giao lúc 00:00 ngày 01/03', count(*) FROM thang_2
WHERE order_delivered_customer_date >= TIMESTAMP '2017-03-01'
   OR order_delivered_customer_date IS NULL
UNION ALL
SELECT 'f. trong a, giao từ 00:00 ngày 01/03 giờ UTC', count(*) FROM thang_2, utc
WHERE order_delivered_customer_date >= cuoi
ORDER BY 1;
