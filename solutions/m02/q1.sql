/* Lời giải tham khảo, Lab 2.2: câu hỏi 1, doanh thu theo ngày, danh mục, bang người bán */
CREATE TEMP TABLE doanh_thu_ngay AS
SELECT CAST(o.order_purchase_timestamp AS DATE) AS ngay,
       coalesce(t.product_category_name_english, p.product_category_name, 'unknown')
         AS danh_muc,
       s.seller_state AS bang_nguoi_ban,
       sum(CAST(i.price AS DECIMAL(14,2))) AS doanh_thu
FROM order_items i
JOIN orders o USING (order_id)
JOIN products p USING (product_id)
JOIN sellers s USING (seller_id)
LEFT JOIN category_translation t USING (product_category_name)
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY ALL;
/* Câu kiểm: Black Friday 24/11/2017 */
SELECT count(*) AS so_dong, sum(doanh_thu) AS tong,
       arg_max(danh_muc || ' ' || bang_nguoi_ban, doanh_thu) AS dong_lon_nhat,
       max(doanh_thu) AS doanh_thu_lon_nhat
FROM doanh_thu_ngay WHERE ngay = DATE '2017-11-24';
