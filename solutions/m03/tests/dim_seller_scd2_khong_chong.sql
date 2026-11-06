/* Lời giải Lab 3.3 bước 3: hai version của cùng người bán không chồng [valid_from, valid_to) */
SELECT a.seller_id FROM dim_seller_scd2 a JOIN dim_seller_scd2 b
  ON a.seller_id = b.seller_id AND a.seller_key < b.seller_key
 AND a.valid_from < coalesce(b.valid_to, DATE '9999-12-31')
 AND b.valid_from < coalesce(a.valid_to, DATE '9999-12-31');
