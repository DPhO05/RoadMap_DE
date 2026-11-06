/* Grain: một dòng mỗi order_id và payment_sequential. */
CREATE OR REPLACE TABLE fct_payments AS
SELECT p.order_id, p.payment_sequential,
       coalesce(dc.customer_key, 0) AS customer_key,
       CAST(strftime(o.order_purchase_timestamp, '%Y%m%d') AS INTEGER)
         AS purchase_date_key,
       p.payment_type, p.payment_installments,
       CAST(p.payment_value AS DECIMAL(12,2)) AS payment_value
FROM order_payments p
JOIN orders o USING (order_id)
LEFT JOIN customers c USING (customer_id)
LEFT JOIN dim_customer dc ON dc.customer_unique_id = c.customer_unique_id;
