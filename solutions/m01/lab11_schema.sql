-- Lời giải mẫu Lab 1.1: đủ sáu bảng. Hai bảng đầu là phần cho sẵn trong module.
CREATE TABLE customers (
  customer_id              text PRIMARY KEY,
  customer_unique_id       text NOT NULL,
  customer_zip_code_prefix text NOT NULL,
  customer_city            text NOT NULL,
  customer_state           text NOT NULL
);
CREATE TABLE orders (
  order_id                      text PRIMARY KEY,
  customer_id                   text NOT NULL REFERENCES customers,
  order_status                  text NOT NULL,
  order_purchase_timestamp      timestamp NOT NULL,
  order_approved_at             timestamp,
  order_delivered_carrier_date  timestamp,
  order_delivered_customer_date timestamp,
  order_estimated_delivery_date timestamp NOT NULL
);
-- Phần người học viết: sellers, products, order_items, order_payments
CREATE TABLE sellers (
  seller_id              text PRIMARY KEY,
  seller_zip_code_prefix text NOT NULL,
  seller_city            text NOT NULL,
  seller_state           text NOT NULL
);
CREATE TABLE products (
  product_id                 text PRIMARY KEY,
  product_category_name      text,
  product_name_lenght        int,
  product_description_lenght int,
  product_photos_qty         int,
  product_weight_g           int,
  product_length_cm          int,
  product_height_cm          int,
  product_width_cm           int
);
CREATE TABLE order_items (
  order_id            text NOT NULL REFERENCES orders,
  order_item_id       int  NOT NULL,
  product_id          text NOT NULL REFERENCES products,
  seller_id           text NOT NULL REFERENCES sellers,
  shipping_limit_date timestamp NOT NULL,
  price               numeric(12,2) NOT NULL CHECK (price >= 0),
  freight_value       numeric(12,2) NOT NULL CHECK (freight_value >= 0),
  PRIMARY KEY (order_id, order_item_id)
);
CREATE TABLE order_payments (
  order_id             text NOT NULL REFERENCES orders,
  payment_sequential   int  NOT NULL,
  payment_type         text NOT NULL,
  payment_installments int  NOT NULL,
  payment_value        numeric(12,2) NOT NULL CHECK (payment_value >= 0),
  PRIMARY KEY (order_id, payment_sequential)
);
CREATE INDEX ON order_items (product_id);
CREATE INDEX ON order_items (seller_id);
