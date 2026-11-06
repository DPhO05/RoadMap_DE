/* Grain: một dòng mỗi customer_unique_id, cộng một dòng khoá 0 cho người mua chưa biết. */
CREATE OR REPLACE TABLE dim_customer AS
SELECT 0 AS customer_key, 'unknown' AS customer_unique_id,
       'unknown' AS customer_city, 'unknown' AS customer_state
UNION ALL
SELECT row_number() OVER (ORDER BY customer_unique_id), customer_unique_id,
       customer_city, customer_state
FROM (SELECT c.customer_unique_id, c.customer_city, c.customer_state
      FROM orders o JOIN customers c USING (customer_id)
      QUALIFY row_number() OVER (PARTITION BY c.customer_unique_id
              ORDER BY o.order_purchase_timestamp DESC, c.customer_id DESC) = 1);
