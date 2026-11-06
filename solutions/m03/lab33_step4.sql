/* Lời giải Lab 3.3 bước 4: gắn mỗi dòng hàng với version người bán còn hiệu lực vào ngày đặt */
SELECT f.order_id, f.order_item_id, s.seller_key, s.seller_id, s.is_current
FROM fct_order_items f
JOIN dim_seller d ON d.seller_key = f.seller_key
JOIN dim_date dd ON dd.date_key = f.purchase_date_key
LEFT JOIN dim_seller_scd2 s ON s.seller_id = d.seller_id
 AND dd.day >= s.valid_from AND (s.valid_to IS NULL OR dd.day < s.valid_to);
