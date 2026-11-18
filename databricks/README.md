# Lakehouse Olist: thiết kế pipeline theo từng layer

Thư mục này thiết kế một lakehouse cho dữ liệu thương mại điện tử Olist trên Databricks, theo kiến trúc medallion Bronze, Silver, Gold. Bản thiết kế dựa trên một lakehouse mã nguồn mở đã dựng và chạy thật trước đó, trên Apache Iceberg.

> **Trạng thái:** bản Iceberg đã chạy end-to-end cho Bronze và Silver. Bản Databricks là thiết kế: lab và số liệu đã chạy trên Spark local, còn code riêng của Databricks viết theo docs và chưa chạy trên Free Edition. Chi tiết ở [mục 6](#6-trạng-thái).

## 1. Bài toán

- **Nguồn:** bộ dữ liệu Olist của Brazil, khoảng 99.441 đơn hàng từ 2016 tới 2018, gồm 9 entity như đơn hàng, dòng hàng, khách hàng, người bán, sản phẩm.
- **Cách nhận dữ liệu:** một REST (Representational State Transfer) API (application programming interface) phát lại dữ liệu theo đồng hồ mô phỏng. Mỗi bản ghi có cursor do server cấp, và một đơn có thể có nhiều version khi trạng thái đổi.
- **Người dùng:** đội phân tích cần dashboard doanh thu, và đội tài chính cần con số khớp với dashboard.
- **Yêu cầu chính:** nạp tăng dần, mỗi bản ghi đúng một lần, chạy lại hay retry không sinh trùng, và có một định nghĩa doanh thu duy nhất.

## 2. Tech stack

Thứ tự các dòng theo dàn ý của [giai_thich_thanh_phan.md](giai_thich_thanh_phan.md). Cột cuối là thứ tương ứng trong bản Iceberg đã dựng.

| thành phần | dùng trên Databricks | vai trò | bản Iceberg tự dựng |
|---|---|---|---|
| Workspace | notebook, SQL (Structured Query Language) editor, Git folders | nơi viết code, chạy query | repo cộng Docker Compose |
| Quản trị | Unity Catalog | tên `catalog.schema.table`, quyền, lineage, audit | Iceberg REST catalog, Postgres 16 |
| Lưu trữ | Delta Lake, volumes | bảng ACID (Atomicity, Consistency, Isolation, Durability), time travel; volume giữ file thô | Iceberg 1.8.1 trên rustfs, một object store tương thích S3 (Amazon Simple Storage Service) |
| Compute | serverless, SQL warehouse, Photon | chạy pipeline và SQL cho dashboard | Spark Connect 3.5.5 |
| Ingestion | Auto Loader, `COPY INTO`, `MERGE`, AUTO CDC | nạp file tăng dần, áp thay đổi theo khoá | client Python gọi API, ghi batch qua Spark |
| Pipeline | Lakeflow pipelines: streaming table, materialized view, expectations | khai báo bảng, tự sắp thứ tự, kiểm chất lượng | job Spark tự viết |
| Điều phối | Lakeflow Jobs | lịch chạy, DAG (directed acyclic graph) của task, retry, backfill | Airflow 3.1, chỉ giao file DAG |
| Chất lượng và CI/CD (continuous integration and continuous delivery) | pytest, Databricks Connect, Declarative Automation Bundles, GitHub Actions | test ở pull request, deploy dev và prod từ một định nghĩa | pytest, ruff, `make e2e` |
| Phục vụ | SQL warehouse, AI/BI dashboards, metric views, Genie Agents, SQL alerts | dashboard, chỉ số dùng chung, cảnh báo | chưa làm |
| Streaming | Structured Streaming | xử lý sự kiện tới liên tục, watermark | chưa làm |
| Cloud và chi phí | AWS (Amazon Web Services), Terraform, `system.billing.usage`, compute policy | dựng workspace bằng code, theo dõi DBU (Databricks Unit) | máy cá nhân |

## 3. Luồng dữ liệu

```text
Olist REST API, mỗi bản ghi có cursor
  1. Landing  client kéo API, ghi file JSON     /Volumes/workspace/bronze/landing/orders
  2. Bronze   Auto Loader vào streaming table   workspace.bronze.raw_orders       giữ mọi version
  3. Silver   MERGE theo khoá và version        workspace.silver.stg_orders       1 dòng mỗi khoá
  4. Gold     materialized view                 workspace.gold.fct_order_items    bảng phân tích
  5. Chỉ số   metric view                       workspace.gold.sales_metrics      1 định nghĩa doanh thu
  6. Phục vụ  SQL warehouse                     AI/BI dashboard, Genie, SQL alerts
```

- Free Edition tạo sẵn catalog `workspace`. Mỗi layer là một schema: `bronze`, `silver`, `gold`.
- Bước 1 chạy trên laptop. Từ bước 2, một Lakeflow Job chạy pipeline khi có file mới, rồi làm mới dashboard khi các task trước đều thành công.

## 4. Thiết kế từng layer

### 4.1 Landing

- **Mục tiêu:** tách lỗi của API khỏi bước biến đổi.
- **Thiết kế:** client chạy trên laptop, kéo API theo cursor rồi ghi file vào volume bằng `databricks fs cp`.
- **Lý do:** workspace Free Edition không gọi được `localhost` của laptop và chỉ ra được một số domain tin cậy. Docs gợi ý 3 mẫu nạp từ REST API, và mẫu "ghi file rồi để Auto Loader đọc" hợp nhất với client có sẵn.

### 4.2 Bronze

- **Mục tiêu:** giữ dữ liệu thô đúng như nguồn gửi, chỉ thêm vào, để có thể tính lại mọi layer sau.
- **Công nghệ:** Auto Loader (`cloudFiles`) trong một streaming table của Lakeflow pipelines. Pipeline chạy kiểu triggered: xử lý hết file mới rồi dừng.
- **Quyết định:** checkpoint của Auto Loader nhớ file đã nạp, nên mỗi file vào đúng một lần, kể cả khi job chết giữa chừng.
- **Quyết định:** cột lạ đi vào `_rescued_data` thay vì làm vỡ job hay bị bỏ âm thầm.

### 4.3 Silver

- **Mục tiêu:** mỗi khoá một dòng, đúng kiểu dữ liệu, đã khử trùng, là trạng thái hiện tại của từng đơn.
- **Công nghệ:** `MERGE INTO` theo khoá và version, hoặc AUTO CDC trong Lakeflow pipelines với `KEYS` và `SEQUENCE BY`. Expectations loại dòng hỏng, ví dụ đơn không có `order_id`.
- **Quyết định:** khử trùng nguồn bằng `QUALIFY ROW_NUMBER()` trước khi `MERGE`, và chỉ update khi version nguồn mới hơn, để version cũ đến muộn không đè version mới.
- **Quyết định:** dev ghi vào `workspace.dev_silver`, prod ghi vào `workspace.silver`, vì Free Edition chỉ có 1 workspace.

### 4.4 Gold

- **Mục tiêu:** bảng phục vụ phân tích, như `fct_order_items` có trạng thái đơn, bang của người bán và giá.
- **Công nghệ:** materialized view trong Lakeflow pipelines; Delta với liquid clustering và predictive optimization để tự gom file nhỏ.
- **Quyết định:** ghi idempotent bằng overwrite hoặc `MERGE`, không bao giờ `append`, vì retry, backfill hay repair đều chạy lại task.
- **Quyết định:** quyền đọc cấp ở mức schema `gold` cho nhóm `analysts` bằng `GRANT`, còn Bronze và Silver chỉ đội data thấy.

### 4.5 Phục vụ và giám sát

- **Metric view** `sales_metrics` định nghĩa `revenue` một lần, bỏ đơn `canceled` và `unavailable`. Dashboard, Genie và BI (business intelligence) ngoài như Power BI dùng chung định nghĩa này.
- **SQL alerts** kiểm độ tươi trên đơn có doanh thu, không trên mọi dòng.
- **System tables** như `system.billing.usage` và `system.access.audit` cho biết chi phí và ai đọc gì, không cần dựng Grafana.

### 4.6 Điều phối, test và deploy

- **Lakeflow Jobs:** trigger theo lịch hoặc khi có file mới; task value `new_rows` quyết định có chạy tiếp hay không; retry và repair chỉ an toàn vì mọi task đã idempotent.
- **Test:** unit test chạy trên dữ liệu mẫu cố định ở pull request; expectations chạy trên chính lô dữ liệu thật.
- **Deploy:** Declarative Automation Bundles mô tả job và pipeline một lần, với target dev và prod. GitHub Actions deploy bằng service principal qua OIDC (OpenID Connect), không lưu token cá nhân.

## 5. Khó khăn khi thiết kế và cách xử lý

Phần lớn khó khăn dưới đây có một lab tái hiện trên dữ liệu Olist trong [lo_trinh_databricks.pdf](lo_trinh_databricks.pdf). Số liệu lấy từ những lần chạy đó trên Spark local, trừ dòng chi phí là ước tính và dòng Free Edition lấy từ docs.

| khó khăn | lab cho thấy gì | thiết kế xử lý |
|---|---|---|
| Chạy lại sinh trùng | job nạp API bị kill sau trang 40, chạy lại thừa 5.000 version | Landing ra file; checkpoint của Auto Loader; Bronze chỉ append |
| Version không phải là dòng | 100.254 dòng mang trạng thái `shipped`, nhưng chỉ 1.107 đơn đang giao | Silver giữ 1 dòng mỗi khoá, lấy version mới nhất |
| Retry nhân đôi doanh thu | ghi Gold bằng `append`, chạy lại làm 13.494.400,74 BRL (Brazilian real) thành 26.988.801,48 BRL | ghi bằng overwrite hoặc `MERGE` |
| Job xanh nhưng dữ liệu sai | đổi sang `inner join` mất 775 đơn; nguồn gửi lại file làm doanh thu tháng 11/2017 tăng 152.653,74 BRL | test ở pull request bắt lỗi code; expectations trên lô bắt lỗi dữ liệu |
| Hai định nghĩa doanh thu | tài chính và dashboard lệch 97.242,96 BRL, không ai viết SQL sai | một metric view cho mọi nơi dùng |
| Kiểm độ tươi sai chỗ | đơn bị huỷ vẫn đẩy thời gian mới nhất lên, che việc dữ liệu có giá trị ngừng chảy 44 ngày | kiểm độ tươi trên đơn có doanh thu |
| Dữ liệu đến trễ bị bỏ im lặng | watermark 10 phút làm mất 7.544 đơn của tháng 11/2017, metric chỉ báo 30 dòng | watermark theo độ trễ thật của nguồn; đối soát tổng sau mỗi lần chạy |
| Xoá chưa phải là mất | sau `DELETE`, `VERSION AS OF 0` vẫn đọc lại được dữ liệu | `DELETE`, rồi `REORG TABLE ... APPLY (PURGE)`, rồi `VACUUM`, bắt đầu từ Bronze |
| Hoá đơn tăng ngoài dự tính | ước tính với giá DBU giả định: warehouse X-Large chạy 46,9 giờ cuối tuần tốn 2.628,27 USD (United States dollar), cỡ 2X-Small tốn 131,41 USD | cỡ nhỏ, Auto Stop, compute policy; budget chỉ gửi email |
| Giới hạn của Free Edition | theo docs: chỉ serverless, 1 workspace, chỉ ra được một số domain tin cậy | client tải file lên volume; tách dev và prod bằng schema |

## 6. Trạng thái

| phần | trạng thái |
|---|---|
| Bản Iceberg, Bronze và Silver | đã chạy trên Docker Compose ngày 19/10/2026, `make e2e` pass |
| Bản Iceberg, Gold | chưa làm |
| Lab Olist của lộ trình | phần không cần workspace đã chạy trên Spark local; số liệu ở mục 5 lấy từ đây |
| Code riêng Databricks: Auto Loader, Lakeflow, metric view, bundles | viết theo docs, chưa chạy trên Free Edition; 192 khối lệnh chờ chạy |

### Bản Iceberg đã dựng

Repo [lakehouse](https://github.com/DPhO05/lakehouse) chạy toàn bộ stack bằng Docker Compose:

- **Stack:** rustfs làm object store, Iceberg REST catalog lưu con trỏ trong Postgres, Spark Connect làm engine, file DAG cho Airflow.
- **Layer:** `raw` cho Bronze, rồi `staging`, `semantic` và `core` cho Silver; 26 bảng trong 5 namespace.
- **Kết quả:** 9 entity, 1.843.032 dòng nạp trong 92 giây.
- **Kiểm chạy lại:** kill job Bronze sau 3 commit rồi chạy lại. Job đọc tiếp từ cursor cuối, và số dòng raw bằng đúng số version trên API.

Khó khăn riêng của bản tự dựng:

- **Tiến độ phải tự lưu.** Bronze đọc tiếp sau `max(_source_cursor)` của bảng raw, Silver đọc cửa sổ từ một bảng watermark. Trên Databricks, checkpoint của Auto Loader và Lakeflow làm việc này.
- **Hạ tầng tự vận hành.** Lần khởi động đầu, Spark hỏng 5 lần khi tải jar từ Maven, nên compose cần `restart: on-failure` và cache jar.
- **Hợp đồng API chưa có.** Lúc dựng, nhóm server chưa chốt API, nên repo dùng một mock tên `source_sim` làm đúng hợp đồng tự viết.

## 7. Đọc thêm

- [lo_trinh_databricks.pdf](lo_trinh_databricks.pdf): lộ trình 14 module, có lab và số liệu của mục 5.
- [giai_thich_thanh_phan.md](giai_thich_thanh_phan.md): giải thích từng thành phần Databricks. Bản tiếng Anh: [component_explainer_en.md](component_explainer_en.md).
- [hoc_pyspark.pdf](hoc_pyspark.pdf): PySpark theo từng notebook.
