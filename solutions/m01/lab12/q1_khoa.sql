-- Grain: một dòng cho mỗi bảng được kiểm
SELECT 'order_items' AS bang, count(*) AS so_dong,
       count(DISTINCT (order_id, order_item_id)) AS so_khoa
FROM order_items
UNION ALL
SELECT 'order_payments', count(*), count(DISTINCT (order_id, payment_sequential))
FROM order_payments
UNION ALL
SELECT 'order_reviews', count(*), count(DISTINCT (review_id, order_id))
FROM order_reviews
ORDER BY bang;
