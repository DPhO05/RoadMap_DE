-- Grain: một bang và một danh mục; chỉ giữ ba hạng đầu của mỗi bang
-- Không dùng QUALIFY, để chạy được cả trên PostgreSQL
SELECT bang, danh_muc, tien_hang, hang
FROM (
  SELECT bang, danh_muc, tien_hang,
         rank() OVER (PARTITION BY bang ORDER BY tien_hang DESC) AS hang
  FROM (
    SELECT c.customer_state AS bang,
           coalesce(t.product_category_name_english, p.product_category_name,
                    'unknown') AS danh_muc,
           sum(CAST(i.price AS DECIMAL(12,2))) AS tien_hang
    FROM order_items i
    JOIN orders o ON o.order_id = i.order_id
    JOIN customers c ON c.customer_id = o.customer_id
    JOIN products p ON p.product_id = i.product_id
    LEFT JOIN category_translation t
      ON t.product_category_name = p.product_category_name
    GROUP BY 1, 2
  ) theo_danh_muc
) xep_hang
WHERE hang <= 3 AND bang IN ('SP', 'RJ', 'MG')
ORDER BY bang, hang;
