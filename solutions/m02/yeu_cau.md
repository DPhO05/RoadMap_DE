# Thẻ yêu cầu cho năm câu hỏi Gold (lời giải tham khảo, Lab 2.2)

Số trong các câu kiểm chạy bằng lab_m02/hoi.py ngày 06/11/2026, DuckDB 1.5.6.

## 1. Doanh thu theo ngày, danh mục, bang người bán
- Người dùng: team BA, dashboard xem lúc 08:00 mỗi sáng; chọn danh mục chạy khuyến mãi
- Grain: một dòng mỗi (ngày đặt, danh mục, bang người bán)
- Metric: tổng price của dòng hàng, BRL; không cộng freight_value
- Lọc: bỏ đơn canceled và unavailable
- Danh mục: tên tiếng Anh; không có bản dịch thì tên gốc; không có tên thì unknown
- Mốc cắt: ngày của order_purchase_timestamp, giờ São Paulo, [00:00, 00:00 hôm sau)
- Ngày chốt: số của một ngày chốt sau 14 ngày; trước đó là số tạm
- Độ tươi: lúc 08:00 có mọi đơn đặt trước 00:00; đơn chưa duyệt chưa có doanh thu
- Câu kiểm: ngày 24/11/2017, trên toàn bộ dữ liệu, có 146 dòng, tổng 152653.74 BRL; dòng lớn nhất là bed_bath_table SP với 14717.34 BRL

## 2. Giao hàng nhanh hay chậm
- Người dùng: team vận hành, báo cáo tuần; chọn bang cần đổi đối tác vận chuyển
- Grain: một dòng mỗi (tháng đặt hàng, bang của khách)
- Metric: số đơn trễ, số đơn đúng hẹn, tỉ lệ trễ bằng số đơn trễ chia số đơn đã giao
- Định nghĩa trễ: ngày của order_delivered_customer_date lớn hơn ngày của order_estimated_delivery_date; so theo ngày, không theo giờ
- Lọc: chỉ đơn đã giao, tức order_delivered_customer_date khác NULL; đơn chưa giao đếm riêng
- Mốc cắt: tháng của order_purchase_timestamp, giờ São Paulo, [00:00 ngày 1, 00:00 ngày 1 tháng sau); số của một tháng chốt sau 3 tháng vì đơn còn đang giao
- Độ tươi: dữ liệu tới 00:00 thứ Hai có trước 08:00 thứ Hai
- Câu kiểm: trên toàn bộ dữ liệu có 6535 đơn trễ, 89941 đơn đúng hẹn, 2965 đơn chưa giao

## 3. Khách quay lại
- Người dùng: team marketing, báo cáo tháng; đo hiệu quả chương trình giữ chân khách
- Grain: một dòng mỗi tháng có đơn đầu tiên của người mua
- Metric: số người mua, số người mua quay lại, phần trăm quay lại làm tròn 2 chữ số
- Định nghĩa: người mua là customer_unique_id, không phải customer_id; quay lại là có từ 2 đơn trở lên
- Lọc: đơn ở mọi trạng thái
- Mốc cắt: tháng của đơn đầu tiên, giờ São Paulo; số chốt trên toàn bộ dữ liệu tới ngày chạy
- Độ tươi: dữ liệu tới cuối tháng trước có trước ngày 2 của tháng
- Câu kiểm: trên toàn bộ dữ liệu có 96096 người mua, 2997 người quay lại, 3.12 phần trăm

## 4. Điểm review theo người bán hay danh mục
- Người dùng: team quản lý người bán, báo cáo tháng; cảnh báo người bán có điểm thấp
- Grain: một dòng mỗi (tháng đặt hàng, người bán)
- Metric: điểm review trung bình và số đơn có review
- Định nghĩa: mỗi đơn lấy đúng một review, là review có review_creation_date mới nhất; review của đơn có nhiều người bán tính cho mọi người bán có hàng trong đơn
- Lọc: chỉ đơn có review; 768 đơn chưa có review không tính
- Mốc cắt: tháng của order_purchase_timestamp, giờ São Paulo; số chốt trên toàn bộ dữ liệu tới ngày chạy
- Độ tươi: dữ liệu tới cuối tháng trước có trước ngày 2 của tháng
- Câu kiểm: 98673 đơn có review, điểm trung bình 4.0863; 3090 người bán có ít nhất một đơn có review, qua 99247 cặp đơn và người bán

## 5. Bản đồ khách và người bán
- Người dùng: team mở rộng thị trường, xem khi cần; chọn vùng cần thêm người bán
- Grain: một dòng mỗi người mua và một dòng mỗi người bán, kèm toạ độ
- Metric: không có phép cộng; toạ độ là thuộc tính
- Định nghĩa: toạ độ lấy theo zip đã đủ 5 chữ số, theo luật của semantic.zip_geo
- Lọc: người không có toạ độ vẫn giữ, toạ độ để NULL, và bản đồ ghi rõ số người bị bỏ khỏi hình
- Mốc cắt: trạng thái hiện tại; không có lịch sử
- Độ tươi: dựng lại mỗi lần Silver chạy là đủ
- Câu kiểm: 270 người mua và 8 người bán không có toạ độ, theo docs/data-model.md mục 9 hàng 13
