# Chọn kho cho năm việc của Olist (lời giải tham khảo, Lab 2.3)

| việc | ai đọc, nhịp đọc | kiểu truy cập | kho chọn | bằng chứng | lý do ngược |
|---|---|---|---|---|---|
| API tra đơn theo order_id | app, nhiều lần mỗi giây | tra theo khoá | kho theo dòng có index, như PostgreSQL | Bài 2: SQLite tra khoảng 2 µs mỗi lần, DuckDB khoảng 160 µs, chênh khoảng 70 lần | đặt trên kho theo cột thì mỗi lần tra phải lọc cả cột order_id, cộng chi phí cố định khoảng 33 µs mỗi câu |
| dashboard doanh thu | BA, mỗi sáng | quét phân tích | kho theo cột, như Trino trên Iceberg của repo | Bài 2: gộp 99441 đơn mất 3,7 ms trên DuckDB và 28 ms trên SQLite | đặt trên database của app thì câu quét giành tài nguyên với giao dịch của người dùng |
| tìm review theo chữ | đội chăm sóc khách, khi cần | tìm theo chữ | index đảo ngược, như SQLite FTS5 hay OpenSearch | Lab 2.3 bước 1: FTS5 thấy 2044 bình luận có "não recebi", LIKE thấy 1696 | LIKE trên kho theo dòng hay kho theo cột phải quét mọi bình luận, và bỏ sót bình luận gõ không dấu hay viết hoa có dấu |
| metric độ tươi | người trực pipeline, mỗi 15 phút | chuỗi thời gian | kho chuỗi thời gian của hệ giám sát, như Prometheus | 96 điểm mỗi bảng mỗi ngày, vì Bronze chạy mỗi 15 phút | đặt vào bảng Iceberg thì mỗi điểm là một commit và một snapshot mới |
| tra toạ độ theo zip | dashboard bản đồ | tra theo khoá trên bảng nhỏ | không cần kho mới: core.customers đã có toạ độ | semantic.zip_geo có 19010 zip; core.customers có customer_lat, customer_lng | nếu một API của app cần tra theo zip từng lần, khi đó mới cần kho khoá giá trị như Redis |
