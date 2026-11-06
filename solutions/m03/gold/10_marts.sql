/* Lời giải tham khảo Lab 3.5: năm mart cho năm câu hỏi ở docs/data-model.md mục 11.
   Chỉ đọc core và semantic; mốc thời gian là giờ Brazil như trong CSV. */
CREATE SCHEMA IF NOT EXISTS marts;

/* Câu 1. Grain: một dòng mỗi ngày đặt, danh mục, bang người bán.
   revenue = tổng price của dòng hàng thuộc đơn không canceled, không unavailable;
   không gồm phí vận chuyển, phí vận chuyển nằm ở cột freight. */
CREATE OR REPLACE TABLE marts.revenue_daily AS
SELECT CAST(o.order_purchase_timestamp AS DATE) AS purchase_date,
       p.product_category_name_english AS category, s.seller_state,
       count(*) AS item_count, sum(i.price) AS revenue, sum(i.freight_value) AS freight
FROM core.order_items i
JOIN core.orders o USING (order_id)
JOIN core.products p USING (product_id)
JOIN core.sellers s USING (seller_id)
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY ALL;

/* Phần bị loại của câu 1. Grain: một dòng mỗi ngày đặt và trạng thái đơn. */
CREATE OR REPLACE TABLE marts.revenue_excluded AS
SELECT CAST(o.order_purchase_timestamp AS DATE) AS purchase_date, o.order_status,
       count(*) AS item_count, sum(i.price) AS revenue
FROM core.order_items i JOIN core.orders o USING (order_id)
WHERE o.order_status IN ('canceled', 'unavailable')
GROUP BY ALL;

/* Câu 2. Grain: một dòng mỗi tháng đặt và bang của người mua.
   Chỉ đơn đã tới tay khách; lưu tử số và mẫu số, chia lúc đọc. */
CREATE OR REPLACE TABLE marts.delivery_state_month AS
SELECT CAST(date_trunc('month', o.order_purchase_timestamp) AS DATE) AS purchase_month,
       c.customer_state,
       count(*) AS delivered_orders,
       count(*) FILTER (WHERE o.is_delivered_late) AS late_orders,
       sum(o.delivery_days) AS delivery_days_sum
FROM core.orders o JOIN core.customers c USING (customer_key)
WHERE o.order_delivered_customer_date IS NOT NULL
GROUP BY ALL;

/* Câu 3. Grain: một dòng mỗi tháng mua lần đầu.
   Người mua quay lại: có từ 2 customer_id trở lên, tức từ 2 đơn, kể cả đơn huỷ. */
CREATE OR REPLACE TABLE marts.repeat_buyers_month AS
SELECT CAST(date_trunc('month', first_order_purchase_timestamp) AS DATE)
         AS first_purchase_month,
       count(*) AS buyers,
       count(*) FILTER (WHERE customer_id_count > 1) AS repeat_buyers
FROM core.customers
GROUP BY ALL;

/* Câu 4. Grain: một dòng mỗi người bán.
   Mỗi cặp đơn và người bán đếm một lần, dù đơn có nhiều dòng hàng của người bán đó. */
CREATE OR REPLACE TABLE marts.review_seller AS
SELECT seller_id, count(*) AS reviewed_orders, sum(review_score) AS review_score_sum
FROM (SELECT DISTINCT i.order_id, i.seller_id, o.review_score
      FROM core.order_items i JOIN core.orders o USING (order_id)
      WHERE o.review_score IS NOT NULL)
GROUP BY ALL;

/* Câu 5. Grain: một dòng mỗi zip; toạ độ NULL khi zip không có trong zip_geo. */
CREATE OR REPLACE TABLE marts.map_zip AS
WITH c AS (SELECT customer_zip_code_prefix AS zip_code_prefix, count(*) AS buyers
           FROM core.customers GROUP BY 1),
     s AS (SELECT seller_zip_code_prefix AS zip_code_prefix, count(*) AS sellers
           FROM core.sellers GROUP BY 1)
SELECT zip_code_prefix, g.lat, g.lng,
       coalesce(c.buyers, 0) AS buyers, coalesce(s.sellers, 0) AS sellers
FROM c FULL JOIN s USING (zip_code_prefix)
LEFT JOIN semantic.zip_geo g USING (zip_code_prefix);
