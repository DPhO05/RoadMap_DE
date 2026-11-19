# Từ khoá tiếng Anh khi nói về medallion và star schema

Bản ghi này gom các từ tiếng Anh nên dùng khi trình bày hai sơ đồ [olist_medallion_star_schema](images/olist_medallion_star_schema_en.png) và [medallion_general_workflow](images/medallion_general_workflow.png). Mỗi bảng đi theo một phần của sơ đồ. Cột cuối là cụm từ hay đi kèm, để nói thành câu cho tự nhiên.

## 1. Kiến trúc chung

| từ khoá | nghĩa | nói thế nào |
|---|---|---|
| medallion architecture | chia dữ liệu thành các lớp có chất lượng tăng dần | we follow a medallion architecture |
| layer | một lớp: Landing, Bronze, Silver, Gold | each layer has one responsibility |
| lakehouse | bảng ACID trên object storage, dùng cho cả BI và ML | a lakehouse on Delta Lake |
| upstream, downstream | phía trước, phía sau trong luồng dữ liệu | an upstream failure blocks downstream tasks |
| incremental processing | chỉ xử lý phần dữ liệu mới | we process only new data incrementally |
| full refresh | tính lại toàn bộ bảng | we never full refresh Bronze |
| replay, reprocess | chạy lại từ dữ liệu đã lưu | we can replay Bronze from the landing files |
| single source of truth | một nơi duy nhất cho con số đúng | Gold is the single source of truth |

## 2. Nguồn và ingestion

| từ khoá | nghĩa | nói thế nào |
|---|---|---|
| OLTP, online transaction processing | database của ứng dụng, tối ưu cho ghi | the source is a normalized OLTP database |
| OLAP, online analytical processing | truy vấn phân tích trên nhiều dòng | Gold is modeled for OLAP queries |
| ingestion | bước đưa dữ liệu từ nguồn vào lakehouse | the ingestion layer only copies data |
| CDC, change data capture | bắt các thay đổi insert, update, delete | we capture changes with CDC |
| log-based CDC | đọc log của database, ví dụ WAL của Postgres | log-based CDC captures hard deletes |
| query-based CDC | query theo cột `updated_at` | query-based CDC misses intermediate versions |
| initial snapshot | bản chụp đầy đủ lần đầu | take an initial snapshot, then stream the changes |
| cursor, high-water mark | mốc đã đọc tới đâu | we save the cursor only after the write succeeds |
| push, pull | nguồn tự gửi sang, hoặc pipeline đi lấy | the API is pulled, the events are pushed |
| batch, micro-batch, streaming | chạy theo lô, lô nhỏ, hoặc liên tục | we run a micro-batch every 15 minutes |
| at-least-once, exactly-once | có thể nhận trùng, hoặc đúng một lần | at-least-once delivery plus idempotent writes |
| hard delete, soft delete | xoá hẳn dòng, hoặc chỉ đánh dấu đã xoá | a hard delete never shows up in `updated_at` |

## 3. Landing và Bronze

| từ khoá | nghĩa | nói thế nào |
|---|---|---|
| landing zone | thư mục chứa file thô | files land in a landing zone first |
| immutable | không sửa, không ghi đè | landing files are immutable |
| append-only | chỉ thêm dòng, không update | Bronze is append-only |
| raw, as-is | thô, đúng như nguồn gửi | Bronze keeps the data as-is |
| checkpoint | nơi lưu tiến độ đã xử lý | Auto Loader tracks processed files in a checkpoint |
| schema drift | nguồn đổi cấu trúc mà không báo | we expect schema drift from the source |
| schema evolution | bảng tự thêm cột mới | schema evolution adds the new column |
| rescued data | cột giữ phần dữ liệu không khớp schema | unexpected fields go to `_rescued_data` |
| audit columns | cột ghi file nguồn, thời điểm nạp, run id | every row carries audit columns |

## 4. Silver

| từ khoá | nghĩa | nói thế nào |
|---|---|---|
| cleanse, conform | làm sạch, đưa về chung kiểu và tên | Silver cleanses and conforms the data |
| deduplicate, dedupe | khử dòng trùng | we dedupe on the business key |
| business key, natural key | khoá có nghĩa nghiệp vụ, ví dụ `order_id` | one row per business key |
| upsert | update nếu đã có, insert nếu chưa có | we upsert with `MERGE INTO` |
| SCD, slowly changing dimension | cách xử lý khi thuộc tính thay đổi | SCD type 1 overwrites, SCD type 2 keeps history |
| late-arriving data | dữ liệu tới muộn | late-arriving data must not overwrite newer rows |
| out-of-order events | sự kiện tới sai thứ tự | we sequence by log position, not arrival time |
| normalized | đã chuẩn hoá, nhiều bảng nhỏ | Silver keeps the normalized source model |
| data contract | thoả thuận về cấu trúc giữa nguồn và người dùng | a data contract with the source team |

## 5. Gold và star schema

| từ khoá | nghĩa | nói thế nào |
|---|---|---|
| dimensional modeling | cách mô hình hoá của Kimball | we use Kimball dimensional modeling |
| star schema | fact ở giữa, các dim xung quanh | one star schema per business process |
| snowflake schema | dim bị tách thêm thành nhiều bảng | we avoid snowflaking to keep joins simple |
| business process | sự kiện nghiệp vụ đo được | sales, payments and reviews are three processes |
| fact table | bảng sự kiện, chứa số đo | `fct_order_items` is the sales fact |
| dimension table | bảng mô tả ai, cái gì, khi nào, ở đâu | dimensions give context to the facts |
| grain | một dòng của fact là gì | we declare the grain first: one row per order line |
| measure | số đo, ví dụ giá, phí ship | price and freight are additive measures |
| additive, semi-additive, non-additive | cộng được theo mọi dim, theo một số dim, hoặc không cộng được | a ratio is non-additive |
| surrogate key | khoá do kho tự sinh, thay cho khoá nguồn | facts join dimensions on surrogate keys |
| conformed dimension | dim dùng chung cho nhiều fact | `dim_customer` is conformed across all three facts |
| role-playing dimension | một dim dùng với nhiều vai | `dim_date` plays order date and answer date |
| degenerate dimension | khoá nằm trong fact, không có bảng dim | `order_id` is a degenerate dimension |
| bus matrix | bảng cho biết fact nào dùng dim nào | the bus matrix shows the shared dimensions |
| denormalize | gộp bảng lại để bớt join | dimensions are denormalized |
| semantic layer, metric view | nơi định nghĩa chỉ số một lần | revenue is defined once in a metric view |

## 6. Orchestration

| từ khoá | nghĩa | nói thế nào |
|---|---|---|
| orchestration | điều phối thứ tự chạy các task | Lakeflow Jobs handles orchestration |
| DAG, directed acyclic graph | đồ thị task có hướng, không có vòng | the pipeline is a DAG of tasks |
| dependency | task phải chờ task khác xong | Gold depends on Silver |
| trigger | điều kiện bắt đầu một run | scheduled, file arrival or table update trigger |
| retry, timeout, backoff | chạy lại, giới hạn thời gian, chờ lâu dần giữa các lần | retry twice with exponential backoff |
| idempotent, idempotency | chạy lại nhiều lần vẫn cho cùng kết quả | every task is idempotent, so retries are safe |
| repair run | chỉ chạy lại task lỗi và task phía sau | we repair the run instead of rerunning everything |
| backfill | chạy lại cho một khoảng thời gian cũ | we backfill one day at a time |
| time window, logical date | khoảng thời gian cố định mà một run đọc | each run reads a fixed time window |
| SLA, service level agreement | cam kết dữ liệu sẵn sàng lúc nào | data must be ready by 7 a.m. |

## 7. Data quality

| từ khoá | nghĩa | nói thế nào |
|---|---|---|
| expectation, data quality check | luật kiểm tra trên từng dòng | an expectation that `order_id` is not null |
| warn, drop, fail | giữ và đếm, bỏ dòng, hoặc dừng update | warn for monitoring, fail for broken keys |
| quarantine table | bảng giữ dòng sai để xem lại | bad rows go to a quarantine table |
| reconciliation | đối soát số dòng giữa các lớp | Bronze count equals Silver plus quarantine |
| freshness | dữ liệu mới tới mức nào | a freshness check on Gold |
| volume | số dòng so với bình thường | volume dropped by 80 percent |
| uniqueness | khoá không bị trùng | a uniqueness check on the business key |
| referential integrity, orphan rows | khoá ngoại trỏ đúng, dòng không có cha | orphan rows break the join |
| write-audit-publish, WAP | ghi vào chỗ chưa ai đọc, kiểm tra, rồi mới công bố | we audit before we publish |

## 8. Observability

| từ khoá | nghĩa | nói thế nào |
|---|---|---|
| observability | thấy được pipeline và dữ liệu đang ra sao | data observability, not just job monitoring |
| monitoring, alerting | theo dõi, gửi cảnh báo | alerts go to the owner with a runbook link |
| lineage | dữ liệu đi từ đâu tới đâu, tới mức bảng và cột | column-level lineage in Unity Catalog |
| audit table, run log | một dòng cho mỗi lần chạy | the audit table records rows in and out per layer |
| run id | mã của một lần chạy | every row is tagged with the run id |
| runbook | trang hướng dẫn xử lý một cảnh báo | each alert links to a runbook |
| on-call | người trực sự cố | the on-call engineer gets paged |
| anomaly detection | tự phát hiện số liệu bất thường | anomaly detection flags a stale table |
| silent failure | job xanh nhưng dữ liệu sai | the job was green but the data was wrong |
| root cause | nguyên nhân gốc | we traced the root cause with lineage |

## 9. Governance

| từ khoá | nghĩa | nói thế nào |
|---|---|---|
| data governance | quản lý quyền, chất lượng và vòng đời dữ liệu | Unity Catalog handles governance |
| catalog, metastore | nơi lưu định nghĩa và vị trí các bảng | one catalog for every workspace |
| three-level namespace | tên `catalog.schema.table` | each layer is a schema in the catalog |
| access control, least privilege | cấp quyền vừa đủ | analysts only read the Gold schema |
| `GRANT` | lệnh cấp quyền | `GRANT SELECT` on the gold schema |
| PII, personally identifiable information | dữ liệu cá nhân | PII is masked outside the data team |
| column mask, row filter | che cột, lọc dòng theo người đọc | a column mask on the email column |
| retention | thời gian giữ dữ liệu | Delta keeps the log for 30 days by default |
| time travel | đọc lại version cũ của bảng | time travel is not a backup |
| right to erasure | quyền yêu cầu xoá dữ liệu cá nhân | erasure must start from Bronze |

## 10. CI/CD

| từ khoá | nghĩa | nói thế nào |
|---|---|---|
| environment | dev, staging, prod | dev and prod run the same definition |
| infrastructure as code | khai báo hạ tầng bằng code | jobs are defined as code in a bundle |
| deploy, promote | đưa lên môi trường, đẩy từ dev lên prod | we promote to prod after review |
| unit test, data test | test logic trên dữ liệu mẫu, test trên dữ liệu thật | unit tests on fixtures, data tests on real batches |
| pull request, code review | đề xuất thay đổi, xem xét trước khi gộp | every change goes through a pull request |
| service principal | tài khoản máy dùng để deploy | CI deploys with a service principal, not a personal token |

## 11. Tên sản phẩm cũ và mới

Người phỏng vấn có thể dùng tên cũ, nên cần nhận ra cả hai.

| tên cũ | tên trong repo này |
|---|---|
| Delta Live Tables, DLT | Lakeflow pipelines |
| Databricks Workflows | Lakeflow Jobs |
| `APPLY CHANGES INTO` | AUTO CDC |
| Databricks Asset Bundles, DAB | Declarative Automation Bundles |

## 12. Từ hay đọc sai

Chữ in hoa là âm nhấn.

| từ | cách đọc |
|---|---|
| idempotent | eye-dem-POH-tent |
| lineage | LIN-ee-ij, không đọc thành "line age" |
| schema | SKEE-muh |
| quarantine | KWOR-un-teen |
| orchestration | or-kuh-STRAY-shun |
| ingestion | in-JES-chun |
| deduplicate | dee-DOO-plih-kate |
| denormalize | dee-NOR-muh-lize |
| granularity | gran-yuh-LAIR-uh-tee |
| medallion | muh-DAL-yun |
| Debezium | dee-BEE-zee-um |

## 13. Câu mẫu

1. We land raw files first, so we can rebuild Bronze without calling the source again.
2. Bronze is append-only and keeps every version. Silver keeps one row per business key.
3. We use log-based CDC, because it captures deletes and puts less load on the source database.
4. Every write is idempotent: MERGE or overwrite, never a blind append, so retries and backfills are safe.
5. Bad rows go to a quarantine table, and we reconcile row counts between layers after every run.
6. In Gold, we declare the grain first, then pick the dimensions and the facts.
7. `dim_customer` is a conformed dimension, shared by all three fact tables.
8. We monitor the data, not just the job: freshness, volume and schema.
9. When a task fails, we repair the run instead of rerunning the whole pipeline.
10. Revenue is defined once in a metric view, so the dashboard and finance get the same number.
