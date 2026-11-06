# Lời giải mẫu Lab 1.3 bước 1: Read Uncommitted không đọc được dữ liệu chưa commit
pg() { docker exec -i olist-pg18 psql -U postgres -v ON_ERROR_STOP=1 -qtA "$@"; }
K="order_id = '00010242fe8c5a6d1ba2dd792cb16214' AND order_item_id = 1"
mkdir -p lab_m01
pg > lab_m01/phien_b.txt 2>&1 <<SQL &
BEGIN;
UPDATE order_items SET price = price + 1 WHERE $K
  RETURNING '[1] B tăng giá lên ' || price || ', chưa commit';
SELECT '[3] B ngủ 2 giây rồi ROLLBACK' FROM pg_sleep(2);
ROLLBACK;
SQL
sleep 1
pg > lab_m01/phien_a.txt 2>&1 <<SQL
BEGIN ISOLATION LEVEL READ UNCOMMITTED;
SELECT '[2] A ở READ UNCOMMITTED đọc giá: ' || price FROM order_items WHERE $K;
COMMIT;
SQL
wait
LC_ALL=C sort lab_m01/phien_a.txt lab_m01/phien_b.txt
pg -c "SELECT '[4] giá sau khi B rollback: ' || price FROM order_items WHERE $K"
