/* Lời giải tham khảo Lab 3.4 bước 5: trạng thái mọi đơn sau trang 40, dựng thẳng
   từ version, không dùng MERGE. Mỗi đơn giữ version có cursor lớn nhất. */
CREATE OR REPLACE TEMP VIEW order_snapshot_as_of AS
SELECT order_id, customer_id, order_status,
       CAST(order_purchase_timestamp AS TIMESTAMP)      AS purchase_ts,
       CAST(order_approved_at AS TIMESTAMP)             AS approved_ts,
       CAST(order_delivered_carrier_date AS TIMESTAMP)  AS carrier_ts,
       CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_ts,
       CAST(order_estimated_delivery_date AS DATE)      AS estimated_date,
       cursor AS last_cursor
FROM raw_order_versions
WHERE page <= 40
QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY cursor DESC) = 1;
