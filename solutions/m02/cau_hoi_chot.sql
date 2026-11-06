/* Câu hỏi đã viết lại, Lab 2.1 bước 3 (lời giải tham khảo):
   Đếm đơn có order_purchase_timestamp trong [2017-02-01 00:00, 2017-03-01 00:00)
   và order_delivered_customer_date từ 2017-03-02 00:00 trở đi, mọi mốc theo giờ
   São Paulo như trong CSV. Đơn chưa giao không tính. Số chốt trên toàn bộ CSV,
   tức dữ liệu tới 2018-10-17. */
SELECT count(*) AS so_don FROM orders
WHERE order_purchase_timestamp >= TIMESTAMP '2017-02-01'
  AND order_purchase_timestamp <  TIMESTAMP '2017-03-01'
  AND order_delivered_customer_date >= TIMESTAMP '2017-03-02';
