/* Grain: một dòng mỗi order_id và order_item_id. */
CREATE OR REPLACE TABLE fct_order_items AS
SELECT i.order_id, i.order_item_id,
       coalesce(dc.customer_key, 0) AS customer_key,
       coalesce(dp.product_key, 0) AS product_key,
       coalesce(ds.seller_key, 0) AS seller_key,
       CAST(strftime(o.order_purchase_timestamp, '%Y%m%d') AS INTEGER)
         AS purchase_date_key,
       coalesce(CAST(strftime(o.order_delivered_customer_date, '%Y%m%d') AS INTEGER), 0)
         AS delivered_date_key,
       o.order_status,
       CAST(i.price AS DECIMAL(12,2)) AS price,
       CAST(i.freight_value AS DECIMAL(12,2)) AS freight_value
FROM order_items i
JOIN orders o USING (order_id)
LEFT JOIN customers c USING (customer_id)
LEFT JOIN dim_customer dc ON dc.customer_unique_id = c.customer_unique_id
LEFT JOIN dim_product dp ON dp.product_id = i.product_id
LEFT JOIN dim_seller ds ON ds.seller_id = i.seller_id;
