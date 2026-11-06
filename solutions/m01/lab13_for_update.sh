# Lời giải mẫu Lab 1.3 bước 2: khối P5 ở Read Committed, khoá dòng bằng FOR UPDATE
pg() { docker exec -i olist-pg18 psql -U postgres -v ON_ERROR_STOP=1 -qtA "$@"; }
K="order_id = '00010242fe8c5a6d1ba2dd792cb16214' AND order_item_id = 1"
mkdir -p lab_m01
pg > lab_m01/phien_a.txt 2>&1 <<SQL &
BEGIN;
SELECT price AS gia FROM order_items WHERE $K FOR UPDATE \gset
SELECT '[1] A khoá dòng, đọc giá ' || :gia || ', định giảm 10%';
SELECT '[2] A ngủ 2 giây' FROM pg_sleep(2);
UPDATE order_items SET price = round(:gia * 0.9, 2) WHERE $K
  RETURNING '[3] A ghi giá mới ' || price || ' rồi commit';
COMMIT;
SQL
sleep 1
pg > lab_m01/phien_b.txt 2>&1 <<SQL
BEGIN;
SELECT price AS gia FROM order_items WHERE $K FOR UPDATE \gset
UPDATE order_items SET price = :gia + 5 WHERE $K
  RETURNING '[4] B chờ A commit, đọc giá ' || :gia || ', cộng 5, ghi ' || price;
COMMIT;
SQL
wait
LC_ALL=C sort lab_m01/phien_a.txt lab_m01/phien_b.txt
pg -c "SELECT '[5] giá cuối cùng: ' || price FROM order_items WHERE $K"
pg -c "UPDATE order_items SET price = 58.90 WHERE $K"
