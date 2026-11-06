/* Lời giải tham khảo, Lab 2.2: câu hỏi 2, giao trễ so với ngày hẹn */
SELECT count(*) FILTER (WHERE CAST(order_delivered_customer_date AS DATE)
                              > CAST(order_estimated_delivery_date AS DATE)) AS tre,
       count(*) FILTER (WHERE CAST(order_delivered_customer_date AS DATE)
                              <= CAST(order_estimated_delivery_date AS DATE)) AS dung_hen,
       count(*) FILTER (WHERE order_delivered_customer_date IS NULL) AS chua_giao
FROM orders;
