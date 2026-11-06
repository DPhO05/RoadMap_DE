/* Lời giải Lab 3.3 bước 3: mỗi seller_id có đúng một dòng is_current */
SELECT seller_id FROM dim_seller_scd2
GROUP BY 1 HAVING count(*) FILTER (WHERE is_current) <> 1;
