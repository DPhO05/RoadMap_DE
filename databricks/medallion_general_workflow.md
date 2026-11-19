# Medallion architecture: workflow tổng quát

Sơ đồ đi kèm là `medallion_general_workflow.excalidraw`, mở bằng excalidraw.com hoặc extension Excalidraw của VS Code. Ghi chú này đi theo từng khối của sơ đồ và không gắn với bộ dữ liệu nào.

- Mỗi khối có ba ý: là gì, vì sao có, lỗi cần tránh.
- Mỗi điều nói về một sản phẩm có URL docs ngay cạnh. Chỗ ghi *chưa kiểm* là chỗ chưa có nguồn chính thức xác nhận.

Ý chính: medallion chia dữ liệu thành các lớp có chất lượng tăng dần. Bronze giữ dữ liệu thô, Silver làm sạch và kiểm tra, Gold phục vụ nghiệp vụ, theo [Medallion](https://docs.databricks.com/aws/en/lakehouse/medallion).

## 1. Data flow

### 1.1 Sources
- **Là gì:** Bốn kiểu nguồn hay gặp là OLTP database, SaaS API, event stream và file drop, theo [Ingestion overview](https://docs.databricks.com/aws/en/ingestion/overview).
- **Vì sao phân loại:** Mỗi kiểu có cách lấy phần thay đổi khác nhau. Log-based CDC đọc WAL hoặc binlog nên thấy cả delete, theo [Debezium PostgreSQL](https://debezium.io/documentation/reference/stable/connectors/postgresql.html).
- **Lỗi cần tránh:** Tin rằng query theo cột `updated_at` thấy đủ mọi thay đổi. Cách này mất các version trung gian, theo [Query-based](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/query-based-overview).

### 1.2 Ingestion
- **Là gì:** Bước chép dữ liệu từ nguồn về storage của mình. Push là nguồn tự ghi sang, pull là pipeline gọi nguồn.
- **Vì sao có:** Nó tách việc lấy dữ liệu khỏi việc biến đổi, nên lỗi của nguồn không làm hỏng các lớp sau.
- **Kỹ thuật:** Chụp snapshot đầy đủ một lần rồi đọc tăng dần, theo [Database replication](https://docs.databricks.com/aws/en/ldp/database-replication).
- **Lỗi cần tránh:** Giả định exactly-once. Sau sự cố, consumer của Debezium có thể thấy lại event cũ, theo [Debezium FAQ](https://debezium.io/documentation/faq/).
- *Chưa kiểm:* Với API có cursor, chỉ lưu cursor sau khi ghi file thành công. Đây là thiết kế đề xuất, không có trang docs riêng.

### 1.3 Landing
- **Là gì:** Thư mục chứa file thô bất biến, mỗi bảng một thư mục và chia theo ngày. AWS DMS ghi theo `<schema>/<table>/`, theo [DMS S3 target](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html).
- **Vì sao có:** Bạn chạy lại được Bronze mà không phải gọi lại nguồn. *Chưa kiểm* ở mức "đa số đội làm vậy".
- **Lỗi cần tránh:** Ghi đè file cùng tên. File arrival trigger không chạy khi file bị ghi đè, theo [File arrival](https://docs.databricks.com/aws/en/jobs/file-arrival-triggers).
- Event stream thường vào thẳng Bronze vì broker đã giữ event, mặc định Kafka giữ 7 ngày, theo [Kafka topic configs](https://kafka.apache.org/41/configuration/topic-configs/). Phần "thường vào thẳng" là kinh nghiệm chung, *chưa kiểm*.

### 1.4 Bronze
- **Là gì:** Bảng giữ trạng thái thô của nguồn, được append tăng dần, không làm sạch và không validate, theo [Medallion](https://docs.databricks.com/aws/en/lakehouse/medallion).
- **Vì sao có:** Đây là điểm để dựng lại Silver và Gold khi logic thay đổi.
- **Kỹ thuật:** Auto Loader nhớ file đã xử lý trong checkpoint, theo [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/). Cột lạ được giữ trong `_rescued_data`, theo [Auto Loader schema](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema).
- **Lỗi cần tránh:** Full refresh Bronze khi nguồn đã hết hạn giữ dữ liệu, vì các bản ghi cũ sẽ mất, theo [Pipeline updates](https://docs.databricks.com/aws/en/ldp/updates).

### 1.5 Silver
- **Là gì:** Dữ liệu đã làm sạch, đúng kiểu, một dòng cho mỗi business key. Docs không khuyên ghi thẳng từ ingestion vào Silver, theo [Medallion](https://docs.databricks.com/aws/en/lakehouse/medallion).
- **Vì sao có:** Quy tắc chất lượng được áp ở một chỗ, nên mọi bảng Gold dùng chung dữ liệu đã kiểm.
- **Kỹ thuật:** `MERGE` theo key, hoặc AUTO CDC với `SEQUENCE BY` cho event sai thứ tự, có SCD type 1 và 2, theo [AUTO CDC](https://docs.databricks.com/aws/en/ldp/cdc).
- **Lỗi cần tránh:** Ghi bằng append rồi chạy lại. Lakeflow Jobs không tự làm task idempotent, theo [Repair](https://docs.databricks.com/aws/en/jobs/repair-job-failures).

### 1.6 Gold
- **Là gì:** Dataset có nghĩa nghiệp vụ dành cho business users, ví dụ star schema và bảng aggregate, theo [Medallion](https://docs.databricks.com/aws/en/lakehouse/medallion).
- **Vì sao có:** Mỗi chỉ số được định nghĩa một lần, người dùng không phải tự join.
- **Gate:** Kiểm freshness và volume trước khi công bố. Anomaly detection đánh dấu bảng stale khi commit trễ bất thường, theo [Data quality monitoring](https://docs.databricks.com/aws/en/data-quality-monitoring/).
- **Lỗi cần tránh:** Đặt logic làm sạch ở Gold, vì khi đó mỗi bảng Gold sẽ sạch theo một kiểu khác nhau.

### 1.7 Serving
- **Là gì:** Nơi dữ liệu được dùng: BI, ML, API cho ứng dụng và chia sẻ cho đội khác.
- **Vì sao tách riêng:** Người dùng chỉ đọc Gold, nên bạn đổi được Bronze và Silver mà không làm vỡ dashboard. Đây là quy ước chung, *chưa kiểm*.
- **Sharing:** Databricks chia sẻ được dữ liệu với người ngoài tổ chức, kể cả khi họ không dùng Databricks, theo [Sharing](https://docs.databricks.com/aws/en/delta-sharing/). Trang này hiện dùng tên OpenSharing thay cho Delta Sharing, phần tên gọi *chưa kiểm* kỹ.
- **Lỗi cần tránh:** Cho dashboard đọc thẳng Bronze hoặc Silver.

## 2. Cross-cutting concerns

### 2.1 Orchestration (DAG)
- **Là gì:** Một DAG các task có thứ tự phụ thuộc. Lakeflow Jobs có trigger theo lịch, file arrival và table update, theo [Triggers](https://docs.databricks.com/aws/en/jobs/triggers). Airflow có cron và Asset, theo [Asset scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html).
- **Vì sao có:** Gold không được chạy trên Silver hỏng. `Run if` mặc định là "All succeeded", theo [Run if](https://docs.databricks.com/aws/en/jobs/run-if).
- **Backfill:** Chạy lại job cho nhiều lát thời gian, tối đa 100 run mỗi lần, theo [Backfill](https://docs.databricks.com/aws/en/jobs/backfill-jobs).
- **Lỗi cần tránh:** Task đọc "dữ liệu mới nhất". Hãy đọc theo cửa sổ thời gian cố định để retry cho cùng kết quả, theo [Airflow best practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html).

### 2.2 Data quality
- **Expectations:** warn giữ dòng sai và đếm, drop bỏ dòng sai, fail dừng update và hoàn tác bảng, theo [Expectations](https://docs.databricks.com/aws/en/ldp/expectations).
- **Quarantine:** Dòng sai được đưa vào bảng riêng để xem lại, theo [Expectation patterns](https://docs.databricks.com/aws/en/ldp/expectation-patterns).
- **Reconciliation:** So số dòng giữa các lớp để chống mất dòng âm thầm, theo [Expectation patterns](https://docs.databricks.com/aws/en/ldp/expectation-patterns).
- **Write-audit-publish:** Ghi vào chỗ chưa ai đọc, kiểm tra, rồi mới công bố, theo [lakeFS](https://lakefs.io/blog/data-engineering-patterns-write-audit-publish/). Iceberg làm bằng branch, theo [Iceberg branching](https://iceberg.apache.org/docs/latest/branching/). Tính năng WAP có tên riêng trên Databricks: *chưa kiểm*.
- **Lỗi cần tránh:** Dùng drop mà không có quarantine, vì bạn sẽ không biết vì sao Silver ít dòng hơn Bronze.

### 2.3 Observability
- **Audit columns:** Mỗi dòng ghi file nguồn, thời điểm nạp và run id. Run id lấy từ `{{job.run_id}}`, theo [Dynamic value references](https://docs.databricks.com/aws/en/jobs/dynamic-value-references).
- **Audit table:** Một dòng cho mỗi lần chạy, ghi số dòng vào và ra của từng lớp. Đây là bảng tự thiết kế, *chưa kiểm*. Databricks có sẵn `system.lakeflow.job_run_timeline`, theo [Jobs system tables](https://docs.databricks.com/aws/en/admin/system-tables/jobs).
- **Lineage:** Unity Catalog tự ghi lineage tới mức cột, theo [Lineage](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage). Bên mã nguồn mở dùng OpenLineage với mô hình Job, Run, Dataset, theo [OpenLineage](https://openlineage.io/docs/spec/object-model).
- **Alerts:** Thông báo mức job không gửi khi task lỗi còn đang retry, theo [Notifications](https://docs.databricks.com/aws/en/jobs/notifications).
- **Runbook:** Một trang cho mỗi cảnh báo: triệu chứng, chỗ kiểm tra, cách sửa, lệnh chạy lại. Không có tính năng riêng, *chưa kiểm*.
- **Lỗi cần tránh:** Chỉ theo dõi job xanh hay đỏ mà không theo dõi freshness, volume và schema của dữ liệu.

### 2.4 Governance
- **Là gì:** Một catalog chung, quyền theo lớp và che dữ liệu cá nhân. Column mask là một SQL UDF gắn vào cột, theo [Filters and masks](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks/).
- **Vì sao có:** Bronze chứa dữ liệu thô nên chỉ đội data được đọc, còn người dùng đọc Gold. Đây là quy ước chung, *chưa kiểm*.
- **Lỗi cần tránh:** Coi time travel là bản sao lưu. Delta mặc định giữ log 30 ngày và file đã xoá 7 ngày, theo [Delta history](https://docs.databricks.com/aws/en/delta/history).

### 2.5 CI/CD
- **Là gì:** Job và pipeline được khai báo trong code rồi deploy tới workspace đích. Bundles hỗ trợ source control, code review, testing và CI/CD, theo [Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/).
- **Vì sao có:** Dev và prod chạy cùng một định nghĩa, nên thay đổi được review trước khi tới prod.
- *Chưa kiểm:* Tên target dev và prod, cùng việc chia unit test và data test, là quy ước chung. Trang tổng quan của bundles không nêu.
- **Lỗi cần tránh:** Sửa job bằng tay trên giao diện của prod.

## 3. When a run fails
- **Retry:** Dành cho lỗi tạm thời. Trên Databricks, phần lớn cấu hình mặc định không retry, theo [Configure task](https://docs.databricks.com/aws/en/jobs/configure-task). Tham số `retry_exponential_backoff` của Airflow: *chưa kiểm*.
- **Quarantine hoặc fail:** Dòng xấu thì cách ly và cho run đi tiếp. Lỗi khoá hoặc lỗi còn lại sau retry thì dừng run để chặn các lớp sau.
- **Alert:** Gửi tới người sở hữu kèm link runbook.
- **Repair hoặc backfill:** Repair chỉ chạy lại task lỗi và task phụ thuộc, theo [Repair](https://docs.databricks.com/aws/en/jobs/repair-job-failures). Airflow dùng clear task hoặc `airflow backfill create`, theo [DAG runs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html).
- **Lỗi cần tránh:** Repair một task ghi bằng append, vì dữ liệu sẽ bị nhân đôi.

## 4. Tool map

| Concern | Databricks | Open source |
|---|---|---|
| Ingestion | [Lakeflow Connect](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/), Auto Loader | [Debezium trên Kafka Connect](https://debezium.io/documentation/reference/stable/architecture.html) |
| Table format | Delta Lake | Apache Iceberg |
| Transform | [Lakeflow pipelines](https://docs.databricks.com/aws/en/ldp/), AUTO CDC | Spark, dbt |
| Orchestration | [Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/triggers) | Apache Airflow |
| Data quality | Expectations, [data quality monitoring](https://docs.databricks.com/aws/en/data-quality-monitoring/) | [Great Expectations](https://docs.greatexpectations.io/docs/core/introduction/gx_overview), [Soda](https://docs.soda.io/home.md), [dbt tests](https://docs.getdbt.com/docs/build/data-tests) |
| Lineage | Unity Catalog lineage | [OpenLineage](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/index.html) |
| Catalog, access | Unity Catalog | Iceberg REST catalog, *chưa kiểm* |
| CI/CD | Declarative Automation Bundles | Git, CI runner, pytest, *chưa kiểm* |

*Chưa kiểm* thêm: các loại check freshness, duplicate, schema của Soda, và cấu hình transport của OpenLineage.

## 5. Câu hỏi phỏng vấn hay gặp

**1. What is the medallion architecture and why three layers?**
- Đó là cách chia dữ liệu thành Bronze thô, Silver sạch và Gold cho nghiệp vụ.
- Mỗi lớp có một trách nhiệm, nên lỗi được khoanh lại ở một lớp.
- Lớp sau dựng lại được từ lớp trước khi logic thay đổi.

**2. Why keep raw data in Landing and Bronze if Silver is clean?**
- Để chạy lại khi phát hiện bug trong logic làm sạch.
- Để kiểm toán: lần ngược một dòng về đúng file và đúng lần chạy.
- Nguồn có thể đã xoá hoặc hết hạn giữ dữ liệu, nên bản thô là bản duy nhất còn lại.

**3. How do you make a pipeline idempotent?**
- Ghi bằng `MERGE` theo business key, hoặc overwrite theo partition ngày.
- Mỗi run đọc một cửa sổ thời gian cố định, không đọc "dữ liệu mới nhất".
- Kiểm chứng bằng cách chạy cùng một ngày hai lần và so số dòng.

**4. Is exactly-once delivery real?**
- Trong thực tế đó là at-least-once cộng với ghi idempotent.
- Nguồn như Debezium có thể gửi lại event sau sự cố.
- Vì vậy Silver phải khử trùng theo key và theo sequence.

**5. How do you ingest from an OLTP database?**
- Ưu tiên log-based CDC vì nó thấy delete và ít tải lên database.
- Query theo `updated_at` đơn giản hơn nhưng mất version trung gian và hard delete.
- Luôn chụp snapshot đầy đủ trước rồi mới áp change feed.

**6. How do you handle schema drift?**
- Bronze lưu kiểu rộng như string hoặc VARIANT để không vỡ khi nguồn đổi.
- Cột lạ được giữ lại trong cột rescue thay vì bị bỏ.
- Chọn rõ một chính sách: tự thêm cột, rescue, hoặc dừng pipeline, rồi có cảnh báo.

**7. How do you handle late or out-of-order data?**
- Xếp theo vị trí trong log hoặc cột sequence, không theo giờ dữ liệu tới.
- Bỏ event có sequence nhỏ hơn bản đã áp.
- Với pull theo cursor, đọc lại một khoảng ngắn dưới mốc cũ để bắt dòng tới muộn.

**8. What do you do with bad records?**
- Chia ba mức: warn để theo dõi, drop cho giá trị bất khả thi, fail cho lỗi khoá.
- Dòng bị drop đi vào bảng quarantine kèm lý do.
- Số dòng Silver cộng quarantine phải bằng số dòng Bronze.

**9. The job is green but the dashboard is wrong. How do you catch that?**
- Giám sát dữ liệu chứ không chỉ giám sát job: freshness, volume, schema.
- Reconciliation số dòng giữa các lớp sau mỗi lần chạy.
- Cảnh báo phải kêu trước khi người dùng báo.

**10. How do you backfill after a logic change?**
- Sửa code, rồi chạy lại theo từng lát thời gian có tham số ngày.
- Chỉ dựng lại Silver và Gold từ Bronze, không full refresh Bronze.
- Backfill chỉ an toàn khi ghi idempotent.

**11. Schedule or event trigger?**
- Một batch mỗi đêm thì lịch cố định là đủ.
- File tới không đều thì dùng file arrival trigger.
- Lớp sau khác nhịp hoặc khác đội thì chạy theo cập nhật của bảng phía trên.

**12. How do you trace a wrong number back to its source?**
- Dùng lineage để đi từ bảng Gold về Silver và Bronze.
- Dùng audit columns để tìm file nguồn và run id của dòng đó.
- Dùng audit table hoặc lịch sử chạy để xem lần chạy đó có gì bất thường.
