/* Grain: một dòng mỗi seller_id, cộng một dòng khoá 0 cho người bán chưa biết. */
CREATE OR REPLACE TABLE dim_seller AS
SELECT 0 AS seller_key, 'unknown' AS seller_id, 'unknown' AS seller_city,
       'unknown' AS seller_state
UNION ALL
SELECT row_number() OVER (ORDER BY seller_id), seller_id, seller_city, seller_state
FROM sellers;
