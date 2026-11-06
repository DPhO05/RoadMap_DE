/* Lời giải tham khảo, Lab 2.2: câu hỏi 3, người mua quay lại */
WITH don AS (
  SELECT c.customer_unique_id, count(*) AS so_don
  FROM orders o JOIN customers c USING (customer_id)
  GROUP BY 1
)
SELECT count(*) AS nguoi_mua,
       count(*) FILTER (WHERE so_don > 1) AS quay_lai,
       round(100 * count(*) FILTER (WHERE so_don > 1) / count(*), 2) AS phan_tram
FROM don;
