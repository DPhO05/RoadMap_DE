/* Grain: một dòng mỗi product_id, cộng một dòng khoá 0 cho sản phẩm chưa biết. */
CREATE OR REPLACE TABLE dim_product AS
SELECT 0 AS product_key, 'unknown' AS product_id, 'unknown' AS category,
       NULL AS product_weight_g, NULL AS product_photos_qty
UNION ALL
SELECT row_number() OVER (ORDER BY p.product_id), p.product_id,
       coalesce(t.product_category_name_english, p.product_category_name, 'unknown'),
       p.product_weight_g, p.product_photos_qty
FROM products p LEFT JOIN category_translation t USING (product_category_name);
