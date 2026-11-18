# Giải thích các thành phần Databricks cho Data Engineer

Tài liệu này gom các câu hỏi khi bắt đầu học Databricks và câu trả lời cho từng câu. Mỗi thành phần được giải thích theo cùng một khung:

1. **Gồm gì:** thành phần đó có những phần nào.
2. **Tại sao cần:** nó giải quyết vấn đề gì so với khi tự dựng một stack mã nguồn mở.
3. **Khái niệm cần nắm:** phần chi tiết hơn, có lệnh và code.

Tên sản phẩm, giới hạn của Free Edition và số liệu lấy từ docs Databricks bản AWS, kiểm ngày 11/11/2026. Databricks đổi tên sản phẩm nhiều lần trong 2025 và 2026, nên hãy mở docs bản mới nhất khi một tên không khớp với giao diện.

Lab và số liệu trên bộ dữ liệu Olist nằm trong [lo_trinh_databricks.pdf](lo_trinh_databricks.pdf), dưới đây gọi là "lộ trình". Các lab đó đã chạy trên Spark local, chưa chạy trên Free Edition. Code ghi "chưa chạy" là code viết theo docs, chưa chạy ở đâu cả.

Cột "giống bên stack tự dựng" trong các bảng so Databricks với một lakehouse mã nguồn mở chạy bằng Docker Compose: Spark, Iceberg REST catalog, object store và Airflow. Đây là so sánh của người viết, không trích từ docs.

## Mục lục

1. [Tiến độ tài liệu và kiến thức nền](#1-tiến-độ-tài-liệu-và-kiến-thức-nền)
2. [Tổng quan các thành phần](#2-tổng-quan-các-thành-phần)
3. [Workspace](#3-workspace)
4. [Hỏi đáp: PySpark và SQL editor, metastore, tên ba tầng](#4-hỏi-đáp-pyspark-và-sql-editor-metastore-tên-ba-tầng)
5. [Unity Catalog](#5-unity-catalog)
6. [Delta Lake](#6-delta-lake)
7. [Compute và Spark](#7-compute-và-spark)
8. [Ingestion](#8-ingestion)
9. [Lakeflow pipelines và Lakeflow Jobs](#9-lakeflow-pipelines-và-lakeflow-jobs)
10. [Chất lượng, test, bundles và môi trường](#10-chất-lượng-test-bundles-và-môi-trường)
11. [Phục vụ dữ liệu, BI và giám sát](#11-phục-vụ-dữ-liệu-bi-và-giám-sát)
12. [Streaming](#12-streaming)
13. [Databricks trên AWS, Terraform và chi phí](#13-databricks-trên-aws-terraform-và-chi-phí)

---

## 1. Tiến độ tài liệu và kiến thức nền

> **Câu hỏi:** Hiện tại phần Databricks đã làm được gì, và trước khi học Databricks cần nắm những phần cơ bản nào?

### Đã có gì (ngày 16/11/2026)

| tài liệu | trạng thái |
|---|---|
| [lo_trinh_databricks.pdf](lo_trinh_databricks.pdf) | Module 0 tới 13 đã viết, 538 trang |
| [hoc_pyspark.pdf](hoc_pyspark.pdf) | 25 chương: Lab 00 và notebook 09 tới 33 của khoá Learning PySpark with Databricks |
| khoá nhập môn theo đề cương Databricks Academy | 7 chương đã viết, chưa ghép thành PDF, chưa đăng |

Còn mở:

- 192 khối lệnh của lộ trình cần chạy trên workspace. Chúng được đánh dấu "chưa chạy" và sẽ chạy lại sau khi đăng nhập Free Edition.
- Nhánh tuỳ chọn T1 tới T3, dự án capstone, chương chứng chỉ và nghề nghiệp chưa viết.

### Kiến thức nền cần có

Lộ trình ghi: "Bạn cần biết Python cơ bản và dùng được dòng lệnh. Không cần biết trước SQL hay Spark."

| phần | mức | học ở đâu |
|---|---|---|
| Python: hàm, list, dict, đọc file | bắt buộc | tự học trước |
| Dòng lệnh và Git | bắt buộc | tự học trước; Module 10 và 13 dùng nhiều |
| SQL: `SELECT`, `JOIN`, `GROUP BY`, window function | nên có | Module 1 dạy, 40 giờ |
| Khái niệm dữ liệu: CSV, JSON, Parquet, schema, partition | nên có | đã có nếu từng dựng lakehouse Iceberg |
| Spark DataFrame: lazy, transformation, action | học song song | `hoc_pyspark.pdf` và Module 7 |
| Cloud: S3, IAM | chỉ Module 13 | Module 13 |

Thứ tự gợi ý: Module 0, rồi Module 1 về SQL, rồi Module 2 và 3. Đọc `hoc_pyspark.pdf` trước khi vào Module 7. Người đã dựng lakehouse Iceberg có thể đọc lướt phần khái niệm của Module 2, như medallion và cách chọn kho.

---

## 2. Tổng quan các thành phần

> **Câu hỏi:** Các thành phần chính trong Databricks là gì?

Databricks có 5 tầng: nền (workspace), lưu trữ (Delta Lake, volumes), quản trị (Unity Catalog), compute (serverless, SQL warehouse) và pipeline (Lakeflow). Với data engineer, 6 thứ cần nắm trước là Workspace, Unity Catalog, Delta Lake, Compute, Lakeflow pipelines và Lakeflow Jobs.

```text
Account            billing, chứa nhiều workspace
  Workspace        nơi làm việc: notebook, SQL editor, jobs, catalog
    Control plane  web app và backend, do Databricks chạy
    Compute plane  chạy Spark và SQL: serverless (account Databricks) hoặc classic (account AWS của bạn)
Unity Catalog      metastore, tên 3 tầng catalog.schema.table
Storage            S3 hoặc default storage, chứa file Parquet và _delta_log
```

| tầng | thành phần | làm gì | giống bên stack tự dựng | module |
|---|---|---|---|---|
| Lưu trữ | Delta Lake | table format: Parquet cộng `_delta_log`, ACID, time travel | Iceberg | 5 |
| Lưu trữ | Volumes | chứa file thô CSV, JSON; thay cho DBFS | bucket của object store | 4 |
| Quản trị | Unity Catalog | metastore, quyền, lineage | REST catalog | 6 |
| Compute | serverless, classic (all-purpose, jobs compute) | chạy notebook, job, pipeline | Spark cluster | 7 |
| Compute | SQL warehouse và Photon | chạy SQL cho BI và dashboard | Trino, ClickHouse | 1, 11 |
| Nạp dữ liệu | Auto Loader, `COPY INTO`, Lakeflow Connect | nạp file tăng dần, connector cho database và SaaS | Spark job tự viết | 8 |
| Pipeline | Lakeflow pipelines (tên cũ DLT) | khai báo bảng và kiểm chất lượng | Spark job tự viết | 9 |
| Điều phối | Lakeflow Jobs (tên cũ Workflows) | lên lịch, DAG của task | Airflow | 9 |
| Streaming | Structured Streaming | đọc Kafka, xử lý stream | Flink và Redpanda | 12 |
| Dev và CI | notebook, Git folders, CLI, Declarative Automation Bundles | viết code, deploy bằng file cấu hình | repo cộng Docker Compose | 0, 10 |
| Phục vụ | AI/BI dashboards, Genie Agents, OpenSharing | dashboard, hỏi bằng ngôn ngữ tự nhiên, chia sẻ dữ liệu | Grafana | 11 |
| Giám sát | system tables như `system.billing.usage` | usage, chi phí, audit | Grafana | 11, 13 |

Free Edition chỉ có serverless compute, không có classic compute.

---

## 3. Workspace

> **Câu hỏi:** Workspace gồm những gì, tại sao cần nó, nó giải quyết vấn đề gì mà trước kia khó làm, và cần nắm những khái niệm nào ở mức chi tiết?

Workspace là một môi trường Databricks trên cloud. Team để code, chạy compute, xem dữ liệu và lên lịch job ở cùng một chỗ, dùng chung một bộ quyền. Khái niệm quan trọng nhất: **workspace giữ code, Unity Catalog giữ dữ liệu**.

### 3.1 Gồm gì

Docs định nghĩa workspace là "a Databricks deployment in the cloud".

| mục trên sidebar | giữ gì | giống bên stack tự dựng |
|---|---|---|
| Workspace (browser) | notebook, file `.py`, Git folder | thư mục code trên laptop |
| Catalog | catalog, schema, bảng, volume, quyền | REST catalog và giao diện object store |
| SQL Editor, SQL Warehouses | câu SQL và máy chạy SQL | Trino cộng DBeaver |
| Notebook | Python và SQL chạy trên serverless | Jupyter trong container Spark |
| Jobs & Pipelines | Lakeflow Jobs, Lakeflow pipelines | Airflow |
| Compute | serverless, SQL warehouse, cluster classic | container Spark |

### 3.2 Tại sao cần

Trước kia những việc này vẫn làm được, nhưng phải tự ghép và tự vận hành từng công cụ.

| khi tự dựng | workspace giải quyết |
|---|---|
| mỗi công cụ một giao diện, một lần đăng nhập: Jupyter, Spark UI, Airflow, object store | một giao diện, một CLI, một API cho tất cả |
| tự ghim phiên bản Spark và Delta, dễ lệch: pyspark 4.1.4 đi với delta-spark 4.3.1 không resolve được | Databricks Runtime ghim sẵn; serverless do Databricks nâng |
| máy chạy suốt, tự vá, tự scale | serverless chỉ bật khi chạy và tính tiền theo giây; SQL warehouse tự tắt sau 10 phút |
| quyền rải rác: key S3 nằm trong từng job, quyền catalog để riêng | quyền tập trung theo user và group, qua Unity Catalog |
| code nằm trên laptop của từng người | thư mục chung và Git folder, chạy trên cùng compute |
| analyst, data engineer, data scientist mỗi người một công cụ | SQL editor, notebook và dashboard nằm cùng một chỗ |

Điều này làm được vì Databricks tách 2 phần. **Control plane** gồm web app và backend, do Databricks chạy. **Compute plane** là nơi xử lý dữ liệu.

### 3.3 Cây quản lý

```text
Account     billing, support, chứa nhiều workspace
Workspace   1 deployment, 1 region: user, group, thư mục, compute
Metastore   Unity Catalog, nằm ở mức account, gắn vào các workspace cùng region
```

- Tên bảng có 3 tầng: `catalog.schema.table`. Workspace mới có sẵn một catalog đặt theo tên workspace, kèm schema `default`.
- Free Edition có 1 workspace, 1 metastore và không vào được account console.

### 3.4 Control plane và compute plane

| phần | nằm ở | chứa |
|---|---|---|
| control plane | account Databricks | web app, backend |
| serverless compute plane | account Databricks | compute serverless cho notebook, job, pipeline |
| classic compute plane | account AWS của bạn | cluster bạn tự cấu hình; AWS tính thêm tiền VM |

Dữ liệu nằm trên S3 của bạn. Workspace serverless dùng "default storage", vùng lưu trữ do Databricks quản lý.

### 3.5 Workspace object khác data object

Đây là chỗ dễ nhầm nhất.

| loại | ví dụ | đường dẫn | ai quản quyền |
|---|---|---|---|
| workspace object | notebook, file `.py`, Git folder, query, dashboard | `/Workspace/Users/<email>/`, `/Workspace/Shared/` | quyền thư mục của workspace |
| data object | bảng, view, volume, function | `catalog.schema.table`, `/Volumes/<catalog>/<schema>/<volume>/` | Unity Catalog |

- Thư mục `Users/<email>` mặc định là riêng tư. Trong `Shared`, mọi user có full quyền.
- DBFS root và DBFS mount đã deprecated. Dùng volume và external location thay thế.

### 3.6 Notebook

- Định dạng mặc định là `.ipynb`. Source format lưu thành `.py` hoặc `.sql` với dòng đầu `Databricks notebook source`, nên diff trên Git dễ đọc hơn.
- Magic command: `%sql`, `%python`, `%md`, `%pip`, `%run`, `%sh`, `%fs`. Mỗi ngôn ngữ chạy một REPL riêng, nên biến không truyền qua ngôn ngữ khác. Kết quả của cell SQL nằm trong biến `_sqldf`.
- `%pip install` chỉ áp dụng cho notebook hiện tại và mất khi hết session.
- `dbutils` có các module chính `fs`, `secrets`, `widgets`, `notebook`, `jobs`.
- Tách code: `%run` chạy notebook khác trong cùng context và không trả giá trị. `dbutils.notebook.run()` tạo một job tạm và trả về string. Docs khuyên để logic trong file `.py` rồi `import`.
- Widget kết hợp marker `:name` để truyền tham số vào SQL và tránh SQL injection:

```python
display(spark.read.table("samples.nyctaxi.trips"))
dbutils.widgets.text("min_fare", "10")
```

```sql
%sql
SELECT * FROM samples.nyctaxi.trips WHERE fare_amount >= :min_fare LIMIT 100
```

### 3.7 Workspace files và Git folders

- **Workspace file** chứa `.py`, YAML, `.whl`, tối đa 500 MB mỗi file, import tương đối như ở local. Trên serverless nên dùng đường dẫn tuyệt đối vì thư mục hiện hành không được đảm bảo. Executor không ghi được vào workspace file.
- **Git folder**, tên cũ Repos, là bản clone repo nằm trong `/Workspace/Users/<email>/`. Chỉ hỗ trợ HTTPS. Nối GitHub bằng GitHub App hoặc personal access token. Mỗi working branch tối đa 1 GB.

### 3.8 Compute trong workspace

| loại | chọn phiên bản | Free Edition |
|---|---|---|
| serverless | chọn environment version 1 tới 6 cho Python và thư viện; Spark luôn là bản mới nhất | có, và là loại duy nhất |
| classic cluster | chọn Databricks Runtime, ví dụ 18 LTS có Spark 4.1.0 và được hỗ trợ 3 năm | không |
| SQL warehouse | chỉ chạy SQL, tự tắt sau 10 phút theo mặc định | 1 cái, cỡ 2X-Small |

- Serverless không có Spark UI, RDD, `CACHE TABLE`, global temp view. Code đi qua Spark Connect.
- ANSI mode bật sẵn, nên cast sai sẽ báo lỗi. Muốn nhận `NULL` thì dùng `try_cast`.
- Notebook trên serverless timeout sau 2,5 giờ.

### 3.9 Vào workspace từ máy cá nhân

```text
databricks auth login --host https://<workspace-url>
databricks auth profiles -o text
databricks current-user me -p <profile-name>
```

- `~/.databrickscfg` chỉ giữ host và tên profile. Token OAuth nằm trong Keychain trên macOS, sống 1 giờ và tự làm mới.
- Nếu shell còn biến `DATABRICKS_TOKEN` thì biến đó sẽ đè profile.
- Databricks SDK for Python dùng `WorkspaceClient()`, giống boto3. Databricks Connect cho code chạy trên laptop còn Spark chạy trên serverless.

### 3.10 Tiền

- DBU là đơn vị tính tiền, đếm theo giây. Xem chi tiết trong bảng `system.billing.usage`.
- Free Edition không mất tiền. Vượt quota thì compute tắt tới hết ngày, dữ liệu không bị xoá.

> **Bẫy lớn nhất:** coi workspace là máy dev riêng. Free Edition không có SLA, và account không hoạt động có thể bị xoá. Code phải nằm trong Git, logic phải nằm trong `src/`.

Đọc thêm: Module 0 của lộ trình. Docs: [concepts](https://docs.databricks.com/aws/en/getting-started/concepts), [high-level architecture](https://docs.databricks.com/aws/en/getting-started/high-level-architecture), [navigate the workspace](https://docs.databricks.com/aws/en/workspace/navigate-workspace), [notebooks](https://docs.databricks.com/aws/en/notebooks/notebooks-code), [Git folders](https://docs.databricks.com/aws/en/repos/), [CLI](https://docs.databricks.com/aws/en/dev-tools/cli/), [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).

---

## 4. Hỏi đáp: PySpark và SQL editor, metastore, tên ba tầng

> **Câu hỏi 1:** Sao phần workspace không nhắc tới PySpark? Spark là engine có driver và worker để xử lý dữ liệu lớn, không có storage nên không phải database. Vậy SQL editor trong Databricks làm được gì?
>
> **Câu hỏi 2:** Tách metastore ra có phải để phân quyền không?
>
> **Câu hỏi 3:** Tên bảng 3 tầng có liên quan tới việc chia workspace theo 3 tầng của kiến trúc medallion không?

Tóm tắt:

1. Hiểu về Spark như trên là đúng. Nhưng SQL editor **cũng chạy trên Spark**, nên dữ liệu lớn không bắt buộc phải viết PySpark.
2. Metastore chỉ phục vụ phân quyền một phần. Nó giữ metadata và quyền, nhưng lý do tách nó khỏi workspace là để **nhiều workspace dùng chung**. Cô lập quyền thì làm ở tầng catalog.
3. Không liên quan trực tiếp. Workspace không có trong tên bảng. Thường catalog ứng với môi trường, còn schema ứng với tầng medallion.

### 4.1 PySpark nằm ở đâu, SQL editor làm được gì

PySpark và SQL là 2 ngôn ngữ đi vào cùng một engine:

| cửa vào | ngôn ngữ | compute | engine bên dưới |
|---|---|---|---|
| notebook, file `.py`, Lakeflow Jobs, Lakeflow pipelines, Databricks Connect | PySpark và SQL | serverless hoặc classic | Spark qua Spark Connect, Photon |
| SQL editor, dashboard, công cụ BI nối qua JDBC | chỉ SQL | SQL warehouse | Spark SQL và Photon |

- Cả 2 loại compute đều có driver và worker. Trên serverless và SQL warehouse, Databricks quản lý các node nên bạn không thấy chúng. Không có Spark UI, thay vào đó là query profile.
- SQL warehouse có "cluster size"; Free Edition cho "2X-Small cluster size". Warehouse tự thêm cluster khi nhiều query phải chờ.
- Photon luôn bật trên serverless và SQL warehouse. Photon là engine C++ thay phần thực thi của Spark; Catalyst vẫn lập plan.
- `spark.sql("SELECT ...")` và `df.groupBy(...)` được Catalyst dịch ra cùng một kiểu plan. Chọn ngôn ngữ nào cũng không đổi quy mô dữ liệu xử lý được.

SQL editor cộng SQL warehouse làm được:

- Chạy đủ DDL và DML: `CREATE TABLE`, `MERGE`, `QUALIFY`, CTE đệ quy, SQL scripting. Có thể dựng cả Bronze, Silver, Gold chỉ bằng SQL; Module 1 làm đúng việc này.
- Dùng tham số `:name`, lưu query, xem query history và query profile. Query profile cho thấy DAG operator, thời gian, bộ nhớ và phần trăm dữ liệu được bỏ qua nhờ pruning.
- Làm nguồn cho dashboard và alert.
- Phục vụ nhiều người cùng lúc. Serverless SQL warehouse khởi động trong 2 tới 6 giây.

| dùng SQL warehouse | dùng notebook PySpark |
|---|---|
| query phân tích, BI, dashboard | logic phức tạp: hàm, vòng lặp, thư viện Python |
| dựng bảng Gold bằng SQL | ingestion bằng Auto Loader, streaming |
| nhiều người query cùng lúc | code cần test bằng pytest, machine learning |

### 4.2 Metastore có phải để phân quyền không

Docs viết metastore "registers metadata about data, AI, and permissions".

- Metastore nằm ở mức account, mỗi region một cái, và mọi workspace cùng region dùng chung.
- Lý do tách khỏi workspace: một bảng và quyền của nó chỉ cần khai báo một lần, workspace nào gắn vào metastore cũng thấy giống nhau.
- Cô lập không làm bằng cách tạo thêm metastore. Docs khuyên: "Use your single-region metastore for all software development lifecycle scopes and business units." Muốn cô lập thì tách catalog và bind catalog vào workspace.
- Chưa kiểm trong docs: trước khi có Unity Catalog, mỗi workspace có Hive metastore và table ACL riêng, nên cùng một bảng phải khai báo và cấp quyền lại ở từng workspace.

### 4.3 Tên 3 tầng có ứng với medallion hay workspace không

Không ứng trực tiếp. Tên 3 tầng chỉ là 3 cấp container lồng nhau. Medallion là một cách bạn chọn dùng cấp schema.

| tầng tên | thường dùng cho | ví dụ Olist |
|---|---|---|
| catalog | môi trường, team hoặc business unit | `dev_olist`, `prod_olist` |
| schema | tầng medallion hoặc domain | `bronze`, `silver`, `gold` |
| table | một bảng | `fact_orders` |

Tên đầy đủ: `prod_olist.gold.fact_orders`.

- Workspace ứng với môi trường, không ứng với tầng medallion. Docs khuyên tách workspace Dev và Prod, rồi bind catalog, để workspace prod chỉ thấy `prod_olist`.
- Free Edition chỉ có 1 workspace, nên lộ trình dùng `workspace.bronze.*`, `workspace.silver.*`, `workspace.gold.*`. Free Edition có cho `CREATE SCHEMA` hay không thì chưa thử; Lab 0.6 của lộ trình sẽ kiểm.

Docs: [SQL warehouse types](https://docs.databricks.com/aws/en/compute/sql-warehouse/warehouse-types), [Photon](https://docs.databricks.com/aws/en/compute/photon), [Unity Catalog best practices](https://docs.databricks.com/aws/en/data-governance/unity-catalog/best-practices), [workspace strategy](https://docs.databricks.com/aws/en/lakehouse-architecture/deployment-guide/workspace-strategy).

---

## 5. Unity Catalog

> **Câu hỏi:** Giải thích Unity Catalog theo cùng khung.

Unity Catalog (UC) là lớp quản trị dữ liệu của Databricks. Nó gom 3 việc vào một chỗ: biết dữ liệu nằm đâu, ai được đọc hay ghi gì tới từng dòng và cột, và ai đã làm gì. UC cũng giữ chìa khoá S3 thay cho người dùng, nên không ai đọc được file của bảng mà bỏ qua kiểm quyền.

### 5.1 Gồm gì

```text
Metastore            mức account, 1 cái cho 1 region
  Catalog
    Schema
      table, view, materialized view, volume, function, model
  storage credential, external location, connection, share    (nằm ngang catalog)
```

| phần | làm gì |
|---|---|
| danh mục: catalog, schema, object | bảng nào nằm đâu, có schema gì |
| quyền: `GRANT`, owner, kế thừa | ai được làm gì trên object nào |
| quyền mịn: row filter, column mask, ABAC | ai thấy được dòng nào, cột nào |
| truy cập storage: storage credential, external location, volume | UC cầm chìa khoá S3, người dùng không cầm |
| lineage | cột này được tính từ cột nào |
| audit qua `system.access.audit` | ai đọc hay ghi gì, vào lúc nào |
| data classification | tự gắn tag cho cột chứa dữ liệu cá nhân, như `class.email_address` |

### 5.2 Tại sao cần

So với stack Iceberg tự dựng, nơi Spark và REST catalog cùng dùng một access key admin của object store:

| stack tự dựng | Unity Catalog |
|---|---|
| REST catalog chỉ giữ metadata; bản `iceberg-rest-fixture` dùng để test, chưa kiểm có phân quyền hay không | danh mục và quyền nằm chung một chỗ |
| Spark cầm key admin nên đọc và xoá được mọi file, bỏ qua catalog | UC giữ credential; managed table chặn đọc theo đường dẫn |
| muốn che cột dữ liệu cá nhân thì phải tạo bảng copy hoặc view riêng | column mask, row filter, hoặc một luật ABAC theo tag áp cho nhiều bảng |
| muốn biết bảng Gold lấy từ đâu thì phải đọc code | lineage tự động, tới mức cột |
| không biết ai đã đọc bảng khách hàng | `system.access.audit` |

Docs về managed table: "Path-based access to Unity Catalog managed tables isn't supported because it bypasses Unity Catalog access controls."

Vấn đề thật ở Module 6 của lộ trình: chạy `DELETE` một khách hàng xong, `SELECT` trả 0 dòng, nhưng time travel về version 0 vẫn ra 17 dòng, và đọc thẳng file Parquet cũng ra 17 dòng. Ai đọc được thư mục bảng là đọc được khách đó. UC chặn cửa đọc theo đường dẫn. Việc xoá thật vẫn phải tự làm, xem mục 5.8.

### 5.3 Securable và tên

- "Every asset you govern in Unity Catalog is modeled as a securable object." Mọi thứ cấp quyền được đều là securable.
- Tên bảng: `catalog.schema.object`. Đường dẫn volume: `/Volumes/<catalog>/<schema>/<volume>/`.
- Mọi user có `USE CATALOG` trên workspace catalog. Trên schema `default`, user có thêm `USE SCHEMA`, `CREATE TABLE`, `CREATE VOLUME`, `CREATE FUNCTION`. Owner mặc định là workspace admin.

### 5.4 Managed table và external table

| | managed | external |
|---|---|---|
| file nằm ở | storage do UC quản lý | đường dẫn bạn chỉ định, qua external location |
| khi `DROP TABLE` | `UNDROP` được trong 7 ngày, sau đó UC xoá file | phải tự xoá file |
| UC lo phần nào | quản trị và vòng đời file | chỉ quản trị ("handles governance only") |
| docs khuyên | mặc định, "recommended" cho Delta và Iceberg | dùng khi dữ liệu buộc phải nằm ở chỗ có sẵn |

### 5.5 Storage: credential, external location, volume

- **Storage credential:** trên AWS là một IAM role đọc S3.
- **External location:** một đường dẫn S3 cộng một credential. Quyền là `READ FILES` và `WRITE FILES`. `WRITE FILES` cần thêm `READ FILES`, thiếu thì báo `PERMISSION_DENIED`.
- **Volume:** thư mục file nằm trong UC, quyền là `READ VOLUME` và `WRITE VOLUME`. Docs khuyên cho người dùng cuối làm việc qua volume, không cấp quyền trên external location.
- Docs: "The only identity that should have access to data managed by Unity Catalog is the identity used by Unity Catalog."
- Free Edition không cho đặt storage riêng ("Custom workspace storage locations"), nên phần này chỉ học lý thuyết.

### 5.6 Quyền

- **Principal** là user, group hoặc service principal. Service principal là danh tính cho máy, ghi bằng `applicationId`.
- **Đọc 1 bảng cần đủ 3 quyền:** `USE CATALOG`, `USE SCHEMA` và `SELECT`. Quyền `USE` chỉ cho đi qua container, không cho đọc gì bên trong.
- **Kế thừa:** cấp ở catalog hoặc schema thì áp cho object con, cả object hiện có lẫn object tạo sau này.
- `MODIFY` cho insert, update, delete, nhưng vẫn cần `SELECT` và 2 quyền `USE`.
- **Owner** có mọi quyền và cấp lại được cho người khác. **`MANAGE`** cho quản lý quyền, đổi tên, drop, nhưng không tự có quyền đọc dữ liệu.
- **`BROWSE`** cho xem metadata mà không cần `USE CATALOG`.

```sql
GRANT USE CATALOG ON CATALOG workspace TO `analysts`;
GRANT USE SCHEMA ON SCHEMA workspace.gold TO `analysts`;
GRANT SELECT ON TABLE workspace.gold.fact_orders TO `analysts`;
SHOW GRANTS ON TABLE workspace.gold.fact_orders;
```

Best practice của docs: cấp quyền qua group, không cấp thẳng cho user. Job chạy bằng service principal. Chỉ service principal có `MODIFY` trên bảng production. Owner của catalog và schema production là group.

Bẫy:

- `REVOKE` không báo lỗi khi quyền đó chưa từng được cấp.
- `ALL PRIVILEGES` không gồm `MANAGE`.
- `SHOW GRANTS` chỉ hiện `ALL PRIVILEGES`, không liệt kê quyền con.

### 5.7 Quyền mịn

| cách | gắn vào | dùng khi |
|---|---|---|
| dynamic view | một view mới | che cột bằng `CASE WHEN is_account_group_member('auditors') THEN zip ELSE 'REDACTED' END` |
| row filter | bảng | mỗi nhóm chỉ thấy dòng của mình, ví dụ `region = 'US'` |
| column mask | cột | che số an sinh xã hội, email |
| ABAC và governed tag | catalog hoặc schema, theo tag | một luật che mọi cột có tag `pii`; docs khuyên dùng cách này làm chính |

- Dùng `is_account_group_member()`, không dùng `is_member()`, vì hàm sau không xét group ở mức account.
- Từ khi ABAC GA ngày 28/04/2026, row filter và column mask được tính theo user đang chạy query.
- Bẫy: bảng có row filter hoặc column mask thì mất time travel và clone. Không gắn row filter hay column mask lên view được.

### 5.8 Lineage, audit và xoá dữ liệu cá nhân

- **Lineage** được ghi tự động tới mức cột, xem ở `system.access.table_lineage` và `system.access.column_lineage`, giữ 395 ngày. Đổi tên bảng hoặc cột thì lineage cũ không còn.
- **Audit** nằm ở `system.access.audit`, đang Public Preview. Nên lọc theo `event_date`.
- **Xoá dữ liệu cá nhân** cần 3 bước: `DELETE`, rồi `REORG TABLE ... APPLY (PURGE)`, rồi `VACUUM`. Docs khuyên xoá ở Bronze trước, sau đó tới Silver và Gold.

```sql
SELECT source_table_full_name, target_table_full_name, event_time
FROM system.access.table_lineage
WHERE target_table_full_name = 'workspace.gold.fact_orders'
ORDER BY event_time DESC;
```

### 5.9 Free Edition

| có | không có | chưa rõ, Lab 6.0 sẽ kiểm |
|---|---|---|
| UC bật sẵn, catalog `workspace`, tạo được schema, table, volume, function | account console, SCIM, SSO, storage riêng | group, service principal, row filter, column mask, ABAC, lineage, system tables |

> **Bẫy lớn nhất:** `DELETE` báo thành công không có nghĩa là dữ liệu đã mất. Delta vẫn giữ file cũ và version cũ cho tới khi chạy `REORG ... APPLY (PURGE)` và `VACUUM`.

Đọc thêm: Module 6 của lộ trình. Docs: [Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/), [securable objects](https://docs.databricks.com/aws/en/data-governance/unity-catalog/securable-objects), [managed tables](https://docs.databricks.com/aws/en/tables/managed), [privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges), [best practices](https://docs.databricks.com/aws/en/data-governance/unity-catalog/best-practices), [system tables](https://docs.databricks.com/aws/en/admin/system-tables/).

---

## 6. Delta Lake

> **Câu hỏi:** Giải thích Delta Lake theo cùng khung.

Delta Lake là table format, cùng loại với Iceberg: file Parquet cộng một thư mục log `_delta_log` ghi file nào thuộc version nào. Nhờ log đó, một thư mục Parquet có được ACID, `UPDATE`, `DELETE`, `MERGE`, time travel và kiểm schema. Delta là format mặc định trên Databricks.

### 6.1 Gồm gì

```text
orders/                                       thư mục bảng
  part-00000-<uuid>.parquet                   dữ liệu
  part-00001-<uuid>.parquet
  deletion_vector_<uuid>.bin                  đánh dấu dòng đã xoá
  _delta_log/
    00000000000000000000.json                 commit version 0
    00000000000000000001.json                 commit version 1
    00000000000000000010.checkpoint.parquet   gộp trạng thái để đọc nhanh
    _last_checkpoint                          trỏ tới checkpoint mới nhất
```

| phần | làm gì | lệnh |
|---|---|---|
| transaction log | ghi từng commit: thêm hay bỏ file nào, schema, protocol | `DESCRIBE HISTORY` |
| time travel | đọc version cũ, quay về version cũ | `VERSION AS OF`, `RESTORE` |
| DML | sửa và xoá dòng | `UPDATE`, `DELETE`, `MERGE` |
| bảo trì | gom file nhỏ, dọn file cũ | `OPTIMIZE`, `VACUUM` |
| bố cục dữ liệu | xếp dữ liệu để bỏ qua file không cần đọc | `CLUSTER BY` |
| Change Data Feed | lấy phần thay đổi giữa 2 version | `table_changes()` |

### 6.2 Tại sao cần

Bài 1 của Module 5 có tên "Một thư mục Parquet chưa phải là bảng".

| thư mục Parquet trần | Delta |
|---|---|
| job chết giữa chừng thì người đọc thấy cả file ghi dở, vì Spark liệt kê thư mục rồi đọc mọi file `part-*` | chỉ đọc file mà log nhắc tới; commit là atomic |
| 2 job ghi cùng lúc thì đè lên nhau | chỉ 1 job tạo được `N.json`, job kia phải kiểm conflict rồi thử lại |
| sửa 1 dòng phải tự viết lại cả file hoặc cả partition | có `UPDATE`, `DELETE`, `MERGE`; deletion vector chỉ đánh dấu dòng |
| ghi sai schema vẫn vào được | schema enforcement chặn lại |
| không có lịch sử | time travel, `DESCRIBE HISTORY`, `RESTORE` |
| liệt kê thư mục S3 chậm khi có nhiều file | danh sách file và thống kê min, max nằm sẵn trong log, nên đọc ít file hơn |

Iceberg giải đúng những vấn đề này. Trên Databricks, Delta là format gốc: liquid clustering, predictive optimization và deletion vector được xây quanh nó. Iceberg vẫn dùng được; managed Iceberg table đã GA từ tháng 5/2026.

Đối chiếu với Iceberg:

| Iceberg | Delta | khác ở đâu |
|---|---|---|
| `metadata.json` và catalog trỏ tới nó | `_delta_log` | Delta commit được mà không cần catalog |
| snapshot | version | Iceberg ghi ảnh cả bảng qua manifest, Delta ghi diff ở mức file |
| `expire_snapshots` và xoá orphan file | `VACUUM` cộng `logRetentionDuration` | |
| compaction | `OPTIMIZE` | |
| hidden partitioning | liquid clustering | cơ chế khác nhau |
| position delete | deletion vector | |
| schema evolution theo ID cột | column mapping chế độ `id` | Delta phải bật mới có |
| branch, tag, WAP | không có | |
| commit CAS qua REST catalog | put-if-absent trên `N.json`, hoặc catalog commits | |

### 6.3 Transaction log

- Mỗi commit là một file `N.json` trong `_delta_log`; số version có 20 chữ số.
- Trong commit có các action: `add` và `remove` cho file, `metaData` cho schema và property, `protocol`, `commitInfo` ghi ai làm, lúc nào và thao tác gì, cùng `cdc` và `txn`.
- Commit là diff ở mức file, không phải ảnh cả bảng. Muốn đọc bảng, reader lấy checkpoint gần nhất qua `_last_checkpoint`, rồi replay các file JSON sau nó.
- Log chỉ trỏ tới file Parquet, không chép dữ liệu như `.git/objects`. Xoá một file mà log còn trỏ tới là làm hỏng mọi version cần file đó.

Bảng managed trên Databricks không mở được `_delta_log` theo đường dẫn. Muốn xem log thì ghi một bảng Delta theo đường dẫn vào volume (chưa chạy):

```python
path = "/Volumes/workspace/default/lab/orders_delta"
spark.read.table("workspace.default.olist_orders").write.format("delta").mode("overwrite").save(path)
spark.read.json(path + "/_delta_log/00000000000000000000.json").select("add.path", "commitInfo.operation").show(truncate=False)
```

### 6.4 ACID và ghi đồng thời

- **Commit lạc quan:** job ghi file Parquet mới trước, rồi tạo `N.json`, và chỉ tạo được nếu file đó chưa tồn tại. Thua thì đọc commit mới và kiểm xem có đụng dữ liệu mình đã đọc không. Không đụng thì tự thử ở `N+1`; có đụng thì ném exception.
- Docs Databricks: "there are no locks on reading or writing against a table, and deadlock is not a possibility". Ghi dùng write-serializable, đọc dùng snapshot isolation.
- Mức cô lập là `WriteSerializable` (mặc định) và `Serializable`, đặt bằng `delta.isolationLevel`.
- **Row-level concurrency** tự bật khi bảng không partition và có deletion vector: 2 job `UPDATE` chỉ conflict khi sửa cùng dòng.
- Exception hay gặp: `ConcurrentAppendException`, `ConcurrentDeleteReadException`, `MetadataChangedException`.
- Với Delta mã nguồn mở trên S3: S3 không đảm bảo chỉ một bên tạo được file, nên mặc định chỉ một Spark driver được ghi. Nhiều cluster cùng ghi cần `S3DynamoDBLogStore`. Trên Databricks không phải lo việc này.
- **Catalog commits** GA tháng 5/2026: Unity Catalog thay filesystem làm bên phân xử commit. Nhờ đó có transaction nhiều bảng với `BEGIN ATOMIC ... END;`.

### 6.5 Protocol và table features

- Mỗi bảng có `minReaderVersion` và `minWriterVersion`. Feature mới như deletion vector hay liquid clustering yêu cầu reader 3 và writer 7.
- Docs: "Most protocol version upgrades are irreversible."
- Bẫy: bật một feature có thể làm client ngoài như DuckDB, delta-rs hay Spark mã nguồn mở bản cũ không đọc được bảng nữa. Kiểm `tableFeatures` trong `DESCRIBE DETAIL` trước khi bật.

### 6.6 Time travel và thời gian giữ lại

```sql
DESCRIBE HISTORY workspace.default.olist_orders LIMIT 5;
SELECT count(*) FROM workspace.default.olist_orders VERSION AS OF 0;
RESTORE TABLE workspace.default.olist_orders TO VERSION AS OF 0;
DESCRIBE DETAIL workspace.default.olist_orders;
```

| property | mặc định | giữ gì |
|---|---|---|
| `delta.logRetentionDuration` | 30 ngày | các commit JSON |
| `delta.deletedFileRetentionDuration` | 7 ngày | file data đã bị `remove`; `VACUUM` không xoá file chưa quá hạn này |

- `RESTORE` tạo một version mới, không xoá lịch sử. Job đọc phía sau có thể nhận lại dữ liệu trùng.
- Từ Databricks Runtime 18.0, không time travel được về version cũ hơn `deletedFileRetentionDuration`.

### 6.7 Deletion vector, `OPTIMIZE`, `VACUUM`

| lệnh | làm gì | lưu ý |
|---|---|---|
| `DELETE`, `UPDATE` khi có deletion vector | đánh dấu dòng trong file `.bin`, không viết lại Parquet | client không hỗ trợ deletion vector sẽ không đọc được bảng |
| `OPTIMIZE` | gom file nhỏ thành file lớn | idempotent, không chặn người đang đọc |
| `REORG TABLE t APPLY (PURGE)` | viết lại file để bỏ hẳn dòng đã đánh dấu | cần khi xoá dữ liệu cá nhân |
| `VACUUM` | xoá vật lý các file mà không version nào trong hạn giữ lại còn cần | mặc định giữ 7 ngày; dùng `DRY RUN` để xem trước |

### 6.8 Schema

- **Enforcement:** cột ghi vào phải có trong bảng và kiểu phải khớp, nếu không sẽ báo lỗi như `UNRESOLVED_COLUMN.WITH_SUGGESTION`.
- **Evolution:** `ALTER TABLE ... ADD COLUMNS`, option `mergeSchema`, `MERGE WITH SCHEMA EVOLUTION`. `overwriteSchema` thay cả schema khi overwrite.
- **Column mapping** phải bật thì mới `RENAME COLUMN` và `DROP COLUMN` được. `DROP COLUMN` chỉ gỡ cột khỏi metadata, dữ liệu vẫn nằm trong file.
- **Type widening** từ Databricks Runtime 15.4 cho nới kiểu cột, ví dụ từ int sang bigint.

### 6.9 Bố cục và tự bảo trì

- **Liquid clustering** là "recommended for all new tables". Tối đa 4 cột và không dùng chung với partition hay `ZORDER`. `CLUSTER BY AUTO` để Databricks tự chọn cột.
- **Data skipping:** log giữ min và max của 32 cột đầu, nên query lọc theo các cột này sẽ bỏ qua file không khớp.
- **Predictive optimization** tự chạy `OPTIMIZE`, `VACUUM`, `ANALYZE` cho bảng managed. Nó bật mặc định và tính tiền theo SKU serverless jobs.

```sql
CREATE TABLE workspace.default.orders_lc CLUSTER BY (order_purchase_timestamp)
AS SELECT * FROM workspace.default.olist_orders;
```

### 6.10 Change Data Feed

- Bật bằng `delta.enableChangeDataFeed = true`, đọc bằng `table_changes('t', <version>)`.
- Cột `_change_type` có 4 giá trị: `insert`, `update_preimage`, `update_postimage`, `delete`. Kèm theo là `_commit_version` và `_commit_timestamp`.
- Chỉ có thay đổi từ lúc bật trở đi. `VACUUM` cũng xoá file change data.

### 6.11 Free Edition

| có | không | chưa rõ |
|---|---|---|
| time travel, `OPTIMIZE`, `VACUUM`, deletion vector, liquid clustering chạy tay, Change Data Feed bản legacy | mở `_delta_log` của bảng managed, `VACUUM ... RETAIN 0 HOURS` | predictive optimization, `CLUSTER BY AUTO` |

> **Bẫy lớn nhất:** coi lịch sử bảng như Git, tức là còn mãi và dùng làm backup được. Lab Module 5 xoá 97.242,96 BRL; `VERSION AS OF 0` vẫn đọc lại được, nhưng sau `VACUUM ... RETAIN 0 HOURS` thì báo `FAILED_READ_FILE.FILE_NOT_EXIST`. Docs: "Don't use table history as a long-term backup solution for data archival."

Đọc thêm: Module 5 của lộ trình. Docs: [Delta Lake](https://docs.databricks.com/aws/en/delta/), [history](https://docs.databricks.com/aws/en/delta/history), [ACID](https://docs.databricks.com/aws/en/lakehouse/acid), [feature compatibility](https://docs.databricks.com/aws/en/delta/feature-compatibility), [VACUUM](https://docs.databricks.com/aws/en/delta/vacuum), [liquid clustering](https://docs.databricks.com/aws/en/delta/clustering), [Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md).

---

## 7. Compute và Spark

> **Câu hỏi:** Giải thích Compute và Spark trên Databricks theo cùng khung.

Compute là máy chạy Spark trong workspace. Có 3 loại: serverless do Databricks lo máy, classic do bạn tự cấu hình cluster, và SQL warehouse chỉ chạy SQL. Bên trong vẫn là Spark với driver và executor. Databricks thêm 3 thứ: Photon (engine C++), Spark Connect (notebook chỉ gửi plan đi) và runtime được quản lý sẵn.

### 7.1 Gồm gì

| loại | chạy gì | ai lo máy | Free Edition |
|---|---|---|---|
| serverless | notebook, job, pipeline, cả PySpark và SQL | Databricks | có |
| classic all-purpose | notebook dev, dùng chung, chạy lâu | bạn cấu hình | không |
| classic jobs compute | 1 lần chạy job rồi xoá | bạn cấu hình | không |
| SQL warehouse | chỉ SQL: SQL editor, dashboard, BI | Databricks hoặc bạn | 1 cái, cỡ 2X-Small |

```text
Notebook hoặc IDE   client: dựng plan, gửi qua Spark Connect (gRPC)
Driver              Catalyst lập plan, AQE chỉnh lúc chạy, chia job, stage, task
Executors           chạy task song song; Photon thay phần thực thi JVM
Storage             đọc và ghi Delta trên S3, qua Unity Catalog
```

### 7.2 Tại sao cần

| tự chạy Spark | compute của Databricks |
|---|---|
| tự cài Java, Spark, Delta và tự ghim phiên bản | Databricks Runtime ghim sẵn; serverless tự nâng |
| máy chạy suốt, hoặc phải tự bật tắt | serverless cấp máy trong vài giây và tính tiền theo lượng dùng |
| mọi job dùng chung 1 cluster | jobs compute riêng cho mỗi lần chạy |
| nhiều query cùng lúc thì phải xếp hàng | SQL warehouse tự thêm cluster khi query phải chờ |
| engine JVM | Photon (C++) cho scan, join, aggregate và ghi Delta |
| cluster cầm key S3, ai chạy cũng đọc được | access mode Standard cách ly từng user; dữ liệu được đọc qua UC |

Đổi lại, serverless giấu bớt một số thứ: không có Spark UI, không có RDD, chỉ đặt được 6 config, không có Scala và R, và runtime có thể tự nâng giữa 2 lần chạy.

### 7.3 Lazy, job, stage, task

- **Transformation** như `where`, `select`, `join`, `groupBy` chỉ nối thêm một bước vào plan. **Action** như `count`, `show`, `collect` hay ghi bảng mới làm Spark chạy.
- Một action sinh ra 1 hoặc nhiều **job**. Job được cắt thành **stage** ở mỗi chỗ shuffle. Mỗi partition là 1 **task**.
- Mỗi action chạy lại plan từ nguồn. Docs: "In production data pipelines, writing data is typically the only action that should be present."
- Khi `display(df)` chạy chậm 40 giây, phần lớn thời gian đó thường thuộc về mọi transformation đứng trước nó.

### 7.4 Spark Connect: notebook chỉ gửi plan

Serverless và Databricks Connect dùng Spark Connect. Client dịch DataFrame thành plan chưa phân giải, gửi qua gRPC và nhận kết quả về dạng Arrow. Client không có JVM, nên không có `sparkContext` và RDD.

Khác với Spark Classic trên laptop ở 3 chỗ:

- Gõ sai tên cột chỉ báo lỗi lúc action hoặc lúc gọi `df.columns`.
- DataFrame chỉ giữ tên temp view. Tạo lại view cùng tên thì DataFrame cũ đọc dữ liệu mới.
- UDF đọc giá trị biến tại lúc chạy, không phải lúc định nghĩa:

```python
x = 123
@udf("INT")
def foo():
    return x
df = spark.range(1).select(foo())
x = 456
df.show()  # Spark Connect in 456; Spark Classic in 123
```

### 7.5 Giới hạn của serverless

| mảng | giới hạn |
|---|---|
| Spark UI | không có, dùng query profile thay |
| RDD, `sparkContext` | không có |
| cache | `df.cache()` dùng được; `CACHE TABLE` và `df.checkpoint()` báo lỗi |
| thư viện | không Maven, không init script; thêm thư viện qua side panel Environment |
| UDF | tối đa 1 GB, không ra internet |
| ngôn ngữ | không R; notebook không có Scala |
| streaming | chỉ `Trigger.AvailableNow()` |
| timeout | notebook 2,5 giờ; job tối đa 7 ngày |

- 6 config đặt được: `spark.sql.shuffle.partitions`, `spark.sql.ansi.enabled`, `spark.sql.session.timeZone`, `spark.sql.files.maxPartitionBytes`, `spark.sql.legacy.timeParserPolicy`, `spark.databricks.execution.timeout`. Không có `spark.sql.adaptive.enabled`, nên AQE luôn bật.
- Bẫy: không cài PySpark bằng `%pip` trên serverless.

### 7.6 Classic compute

- **Access mode Standard**, tên cũ Shared, cho nhiều user dùng chung và cách ly họ bằng Lakeguard; không có RDD và R. **Access mode Dedicated**, tên cũ Single user, cho 1 người hoặc 1 nhóm; có RDD, R và ML runtime.
- **Databricks Runtime** ghim sẵn Spark, Python và Delta. Ví dụ 18 LTS có Spark 4.1.0 và được hỗ trợ tới 10/06/2029. Docs khuyên dùng bản mới nhất cho all-purpose compute và bản LTS cho job production.
- **Compute policy** giới hạn cấu hình mà người khác được tạo, giống AWS SCP.

### 7.7 Photon

- Docs: "the Databricks-native vectorized query engine", viết bằng C++. Catalyst vẫn lập plan, Photon thay phần thực thi. Phép nào Photon không hỗ trợ thì quay về Spark JVM.
- Luôn bật trên serverless và SQL warehouse.
- Không hỗ trợ UDF, RDD, Dataset API và stateful streaming. Không làm nhanh hơn query vốn chạy dưới 2 giây.
- Xem các node trong query profile để biết phần nào chạy trên Photon.

### 7.8 ANSI mode

ANSI mode bật mặc định trên Spark 4 và serverless. Code Spark 3 dựa vào việc cast sai trả `NULL` sẽ báo lỗi khi chuyển sang đây.

```sql
SELECT CAST('a' AS INT);      -- lỗi: cannot be cast to "INT" because it is malformed
SELECT try_cast('a' AS INT);  -- NULL
SELECT try_divide(1, 0);      -- NULL
```

### 7.9 Shuffle, join, AQE và skew

| config | mặc định |
|---|---|
| `spark.sql.shuffle.partitions` | 200; đặt `auto` để Databricks tự chọn |
| `spark.databricks.adaptive.autoBroadcastJoinThreshold` | 30MB: bên join nhỏ hơn ngưỡng này được broadcast |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64MB: cỡ partition mà AQE nhắm tới |
| ngưỡng skew | partition lớn hơn 5 lần median và lớn hơn 256MB |

- **AQE** chỉnh plan lúc chạy: đổi sort merge join sang broadcast, gộp partition nhỏ và tách partition lệch.
- Bẫy: AQE chỉ xử lý skew trong join, không xử lý skew của `groupBy`. Với `groupBy` bị lệch thì dùng salting:

```python
N = 16
salted = (df.withColumn("salt", (F.rand() * N).cast("int"))
            .groupBy("seller_id", "salt").agg(F.sum("price").alias("p"))
            .groupBy("seller_id").agg(F.sum("p").alias("revenue")))
```

- Broadcast chủ động bằng `df.join(F.broadcast(small_df), "key")` hoặc hint `/*+ BROADCAST(t1) */`.

### 7.10 Đọc plan

| công cụ | ở đâu | Free Edition |
|---|---|---|
| `EXPLAIN FORMATTED` | mọi nơi, không chạy query | có |
| query profile | nút See performance dưới cell, Query History | có |
| Spark UI | chỉ classic compute, laptop ở cổng `4040` | không |

Node `Exchange hashpartitioning(...)` trong plan là một lần shuffle.

### 7.11 UDF

- Docs xếp thứ tự ưu tiên: hàm có sẵn, rồi higher-order function, rồi pandas UDF, cuối cùng mới tới Python UDF thường.
- Python UDF phải đóng gói dữ liệu ra khỏi JVM, và làm phần plan đó rơi khỏi Photon. pandas UDF gửi dữ liệu theo batch qua Arrow.
- Từ Spark 4.2 và Databricks Runtime 19, Python UDF thường cũng dùng Arrow theo mặc định.

### 7.12 Free Edition

| có | không |
|---|---|
| serverless, 1 SQL warehouse cỡ 2X-Small, Photon, AQE, query profile | classic compute, Spark UI, RDD, R, Scala, compute policy |

> **Bẫy lớn nhất:** ghi Gold bằng `mode('append')` giống một lần insert của service. Lab Module 7 cho thấy khi scheduler chạy lại job, doanh thu từ 13.494.400,74 BRL thành 26.988.801,48 BRL mà không có lỗi nào. Job nào cũng sẽ bị chạy lại do retry, backfill hay repair, nên phải ghi idempotent: overwrite hoặc `MERGE`.

Đọc thêm: Module 7 của lộ trình. Docs: [compute](https://docs.databricks.com/aws/en/compute/), [serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations), [Photon](https://docs.databricks.com/aws/en/compute/photon), [AQE](https://docs.databricks.com/aws/en/optimizations/aqe).

---

## 8. Ingestion

> **Câu hỏi:** Giải thích Ingestion (Auto Loader, `COPY INTO`, Lakeflow Connect) theo cùng khung.

Ingestion là bước đưa dữ liệu từ nguồn (file, database, SaaS, API) vào Bronze. Yêu cầu là nạp tăng dần, mỗi bản ghi đúng một lần, và chạy lại không sinh trùng. Databricks chia làm 3 nhóm: nạp file (Auto Loader, `read_files`, `COPY INTO`), connector có sẵn (Lakeflow Connect) và áp thay đổi vào bảng (`MERGE`, AUTO CDC).

### 8.1 Gồm gì

| công cụ | dùng cho | viết bằng |
|---|---|---|
| Auto Loader (`cloudFiles`) | file mới trong S3 hoặc volume, nạp tăng dần | PySpark stream |
| `read_files` và streaming table | như Auto Loader nhưng viết bằng SQL | SQL |
| `COPY INTO` | nạp file bằng SQL, bỏ qua file đã nạp | SQL |
| Lakeflow Connect | connector có sẵn cho database và SaaS | giao diện, API |
| Python Data Source API | tự viết nguồn đọc API, offset nằm trong checkpoint | Python |
| `MERGE INTO` | áp thay đổi vào bảng: upsert, xoá | SQL, Python |
| AUTO CDC | khai báo khoá và thứ tự, engine tự áp thay đổi, SCD 1 hoặc 2 | SQL, Python trong pipeline |
| Change Data Feed | đọc phần thay đổi của bảng Delta để nạp tầng sau | SQL, Python |

### 8.2 Tại sao cần

Vấn đề thật ở Module 8 của lộ trình: job nạp đơn hàng từ một REST API có cursor được viết như một request handler. Nó `INSERT` mọi bản ghi rồi mới lưu cursor. Job bị kill sau khi ghi trang 40, trước khi kịp lưu cursor:

```text
Row(dong=396551, cursor_khac=391551, don=99441, dong_shipped=100254)
đơn đang shipped theo CSV: 1107
```

- **Chạy lại sinh trùng:** trang 40 bị ghi 2 lần, thừa 5.000 version.
- **Version không phải là dòng:** mỗi đơn có tới 4 version, nên 100.254 dòng mang trạng thái `shipped`, trong khi chỉ 1.107 đơn đang được giao.
- Nếu lưu cursor trước rồi mới ghi thì lỗi đổi chiều: mất 5.000 version mà không ai biết.

| tự viết | Databricks |
|---|---|
| tự nhớ file đã đọc; job chết thì trùng hoặc sót | checkpoint của Auto Loader nhớ file đã nạp, mỗi file đúng 1 lần |
| file có cột mới thì job vỡ, hoặc cột bị bỏ âm thầm | schema evolution; cột lạ vào `_rescued_data` |
| tự dựng Debezium và Kafka để lấy CDC từ database | Lakeflow Connect có connector sẵn |
| tự viết upsert và tự xử lý event đến sai thứ tự | `MERGE` có điều kiện version, AUTO CDC với `SEQUENCE BY` |
| liệt kê thư mục S3 lớn rất chậm | file events: được báo khi có file mới |

### 8.3 Auto Loader

- Auto Loader là một Structured Streaming source tên `cloudFiles`. Nó đọc JSON, CSV, XML, Parquet, Avro, ORC, text và binary.
- **Checkpoint** lưu danh sách file đã thấy trong RocksDB, nên mỗi file chỉ được nạp một lần. Thứ tự file không được đảm bảo.
- **Trigger:** trên serverless chỉ có `availableNow` và `Once`. `availableNow` xử lý hết file có trước lúc bắt đầu rồi dừng, tức là chạy như batch theo lịch. Không đặt trigger thì báo lỗi `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`.

```python
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze/_meta/orders_schema")
  .option("cloudFiles.schemaHints", "order_id string, version long")
  .load("/Volumes/workspace/bronze/landing/orders")
  .writeStream
  .option("checkpointLocation", "/Volumes/workspace/bronze/_meta/orders_ckpt")
  .trigger(availableNow=True)
  .toTable("workspace.bronze.raw_orders"))
```

Code trên chưa chạy.

- Checkpoint và schema location không được nằm trong thư mục của bảng. Đổi `checkpointLocation` nghĩa là bắt đầu một stream mới, và Auto Loader quên hết file đã nạp.
- **Phát hiện file:** mặc định là directory listing. Docs khuyên dùng file events (`cloudFiles.useManagedFileEvents = true`) cho phần lớn workload.
- Bẫy: với file bị ghi đè hoặc append, "Auto Loader cannot guarantee which file version will be processed". Chỉ nạp file bất biến.

### 8.4 Schema của file mới

- Đặt `cloudFiles.schemaLocation` để bật suy schema và schema evolution. Auto Loader lấy mẫu 50 GB đầu hoặc 1.000 file đầu.
- JSON, CSV, XML được suy ra thành `string` hết, trừ khi đặt `cloudFiles.inferColumnTypes = true`.
- Bẫy: `read_files` mặc định `inferColumnTypes = true`, còn `cloudFiles` mặc định `false`. Cùng một thư mục có thể ra kiểu cột khác nhau.

| mode | khi có cột mới |
|---|---|
| `addNewColumns`, mặc định khi không cho schema | stream fail với `UnknownFieldException`; chạy lại thì nhận cột mới |
| `rescue` | schema giữ nguyên, không fail, cột mới vào `_rescued_data` |
| `failOnNewColumns` | fail và không tự chạy lại tới khi bạn sửa schema |
| `none`, mặc định khi có schema | bỏ qua cột mới |

`_rescued_data` là một JSON chứa cột lạ, cột sai kiểu và đường dẫn file nguồn. Nó giống một dead-letter queue nằm ngay trong bảng, query được bằng SQL.

### 8.5 Nạp file bằng SQL

```sql
CREATE OR REFRESH STREAMING TABLE workspace.bronze.raw_orders
SCHEDULE EVERY 1 HOUR
AS SELECT * FROM STREAM read_files(
  '/Volumes/workspace/bronze/landing/orders',
  format => 'json',
  schemaHints => 'order_id string, version long');
```

- `read_files` không có `STREAM` thì đọc batch. Có `STREAM` trong streaming table thì Auto Loader chạy bên dưới.
- `COPY INTO` là "retryable and idempotent" và bỏ qua file đã nạp. Option `'force' = 'true'` tắt tính idempotent. Docs khuyên người dùng SQL chuyển sang streaming table.

### 8.6 Lakeflow Connect

| connector | trạng thái |
|---|---|
| SQL Server | GA từ 25/09/2025 |
| PostgreSQL | Public Preview, logical replication qua `pgoutput` |
| MySQL integrated CDC pipeline | GA ngày 15/10/2026, cần `binlog_format=ROW` |
| query-based: Oracle, MySQL, PostgreSQL và một số database khác | GA ngày 29/05/2026, dùng cursor column |

- **Connector CDC** có 2 phần. Ingestion gateway chạy liên tục trên classic compute để đọc log trước khi nguồn cắt log, và ghi tạm vào một volume trong 30 ngày. Ingestion pipeline chạy serverless và ghi vào streaming table.
- **Connector query-based** lấy các dòng có cursor lớn hơn high-water mark lần trước, và đọc lại một khoảng ngắn ngay dưới mốc đó để bắt dòng đến muộn.
- Có 73 connector SaaS. Free tier cho managed connector là 100 DBU mỗi ngày.
- Bẫy: không tự dừng gateway, vì log nguồn có thể bị cắt trước khi gateway đọc tới. Full refresh xoá lịch sử SCD type 2.

### 8.7 Nạp từ REST API

Không có connector REST chung. Docs đưa 3 mẫu:

| mẫu | dùng khi |
|---|---|
| 1. materialized view gọi `requests` | payload nhỏ; đọc lại toàn bộ API mỗi lần update |
| 2. Python Data Source API | API lớn, đọc tăng dần; offset nằm trong checkpoint của Spark |
| 3. job ghi JSON vào volume, Auto Loader đọc | tách lỗi API khỏi bước biến đổi; Auto Loader nhớ file đã nạp |

- Mẫu 3 hợp với repo [lakehouse](https://github.com/DPhO05/lakehouse): client có sẵn ghi dữ liệu thô ra file, Auto Loader lo phần mỗi file đúng một lần.
- Workspace không gọi được `localhost` trên laptop. Free Edition chỉ ra được "a limited set of trusted domains". Vì vậy lab chạy client trên laptop rồi tải file lên volume bằng `databricks fs cp`.

### 8.8 `MERGE INTO` idempotent

```sql
MERGE INTO workspace.silver.stg_orders AS t
USING (
  SELECT * FROM workspace.bronze.raw_orders_batch
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY version DESC) = 1
) AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.op = 'd' THEN DELETE
WHEN MATCHED AND s.version > t.version THEN UPDATE SET *
WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT *;
```

Code trên chưa chạy.

- Khử trùng nguồn trước bằng `QUALIFY`. Nếu nhiều dòng nguồn khớp một dòng đích thì báo lỗi `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`.
- Chỉ update khi version nguồn mới hơn, để version cũ đến muộn không đè lên version mới.
- Các mệnh đề được xét theo thứ tự viết, nên `DELETE` phải đứng trước `UPDATE`.
- Bẫy: `WHEN NOT MATCHED BY SOURCE ... DELETE` không có điều kiện sẽ xoá mọi dòng đích không có trong batch.
- `foreachBatch` chỉ đảm bảo at-least-once. Muốn ghi idempotent thì đặt `txnAppId` và gán `txnVersion` bằng `batchId`.

### 8.9 AUTO CDC

AUTO CDC là tên mới của `APPLY CHANGES`, cùng cú pháp.

```sql
CREATE OR REFRESH STREAMING TABLE users_history;

CREATE FLOW apply_cdc AS AUTO CDC INTO users_history
FROM stream(main.cdc_tutorial.users_cdf)
KEYS (userId)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY sequenceNum
COLUMNS * EXCEPT (operation, sequenceNum)
STORED AS SCD TYPE 2;
```

- Bạn khai báo `KEYS` (khoá) và `SEQUENCE BY` (thứ tự), engine tự viết phần `MERGE`. Event đến muộn có sequence nhỏ hơn sequence đã áp thì bị bỏ.
- Mặc định là SCD type 1. SCD type 2 thêm cột `__START_AT` và `__END_AT`; dòng hiện hành có `__END_AT` là null.
- AUTO CDC chỉ chạy trong Lakeflow pipelines, không có trong bản Spark Declarative Pipelines mã nguồn mở.
- Khi nguồn chỉ có bản chụp toàn bộ, không có CDC, dùng `AUTO CDC ... FROM SNAPSHOT` (Beta). Engine tự so 2 snapshot để suy ra insert, update và delete.

### 8.10 Free Edition

- Có, theo suy luận từ docs: Auto Loader với `availableNow` từ volume, `read_files`, `COPY INTO`, `MERGE`, AUTO CDC, Change Data Feed bản legacy.
- Không có: connector cần gateway, vì gateway phải chạy trên classic compute; gọi `localhost` của laptop.
- Chưa rõ: file events, connector SaaS, gọi API bên ngoài.

> **Bẫy lớn nhất:** nạp như một request handler, tức là `INSERT` mọi bản ghi rồi nhớ cursor. Chạy lại sẽ sinh trùng, còn đổi thứ tự lưu thì sót. Ingestion đúng phải giữ dữ liệu thô dạng append, mỗi file hoặc version đúng một lần, rồi `MERGE` theo khoá và version để ra trạng thái hiện tại.

Đọc thêm: Module 8 của lộ trình. Docs: [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/), [`COPY INTO`](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into), [`read_files`](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files), [Lakeflow Connect](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/), [ingest từ API](https://docs.databricks.com/aws/en/ldp/api-ingestion), [`MERGE INTO`](https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into), [AUTO CDC](https://docs.databricks.com/aws/en/ldp/cdc).

---

## 9. Lakeflow pipelines và Lakeflow Jobs

> **Câu hỏi:** Giải thích Lakeflow pipelines và Lakeflow Jobs theo cùng khung.

Đây là 2 thứ với 2 vai trò khác nhau. Lakeflow pipelines, tên cũ Delta Live Tables (DLT), cho bạn khai báo bảng nào được tính từ đâu; engine tự lo thứ tự, phần dữ liệu mới và kiểm chất lượng. Lakeflow Jobs, tên cũ Workflows, là orchestrator: quyết định khi nào chạy và task nào chạy trước, thay cho Airflow. Một pipeline thường là một task trong job.

### 9.1 Gồm gì

| phần | trả lời câu hỏi | giống bên stack tự dựng |
|---|---|---|
| Lakeflow pipelines | bảng nào được tính từ đâu | Makefile, dbt |
| Lakeflow Jobs | khi nào chạy, task nào trước task nào | Airflow |

Ví dụ minh hoạ, tự dựng theo các loại task trong docs:

```text
Lakeflow Job "olist_daily"          trigger: lịch 02:00 hoặc có file mới
  task 1  ingest_api      notebook: kéo API, ghi JSON vào volume
  task 2  olist_pipeline  pipeline: bronze_orders, silver_orders, gold_revenue
  task 3  check           If/else theo task value new_rows > 0
  task 4  refresh_dash    dashboard, Run if: All succeeded
```

### 9.2 Tại sao cần

| tự dựng | Lakeflow |
|---|---|
| tự sắp thứ tự job Bronze, Silver, Gold bằng tay | pipeline tự sắp thứ tự theo phụ thuộc và chạy song song |
| tự nhớ file hoặc dòng nào đã xử lý | streaming table giữ checkpoint, chỉ xử lý phần mới |
| kiểm chất lượng bằng script riêng | expectations nằm ngay trong định nghĩa bảng, metric vào event log |
| dựng và vận hành Airflow riêng | Jobs có sẵn trong workspace và chạy trên serverless |
| sensor chạy dài để chờ file hoặc bảng | trigger file arrival và table update |
| chạy lại cả DAG khi 1 task lỗi | repair run chỉ chạy task lỗi và các task phía sau |

Vấn đề thật ở Module 9 của lộ trình: job đêm Black Friday có bước 1 `INSERT` dòng hàng và bước 2 gửi báo cáo. Bước 2 timeout, scheduler retry cả job:

```text
nguồn: 1366 dòng hàng, tổng price 152653.74
bảng : 2732 dòng hàng, tổng price 305307.48
```

Job báo thành công và không có cảnh báo nào. Retry, repair và backfill đều là chạy lại, nên chỉ an toàn khi task idempotent.

### 9.3 Pipelines: dataset, flow, pipeline

| dataset | kiểu | dùng khi |
|---|---|---|
| streaming table | đích streaming, có checkpoint | dữ liệu chỉ thêm vào: Bronze, log, file mới |
| materialized view | đích batch, khớp trạng thái nguồn | bảng tổng hợp: Silver đã làm sạch, Gold |
| view (`@dp.temporary_view`) | tính khi được đọc, không lưu | bước trung gian |
| sink | đích bên ngoài: Kafka, Event Hubs | đẩy dữ liệu ra hệ thống khác |

- **Flow** đọc nguồn, áp logic và ghi vào đích. Một streaming table nhận được nhiều flow, ví dụ 2 nguồn cùng ghi vào một bảng.
- **Pipeline** là đơn vị để phát triển và chạy. Engine đọc các tham chiếu giữa bảng để dựng đồ thị.
- API Python mới là `from pyspark import pipelines as dp`; `import dlt` vẫn chạy được. `@dp.table` giờ chỉ tạo streaming table; bảng batch dùng `@dp.materialized_view`.

Viết theo mẫu tutorial, chưa chạy:

```python
from pyspark import pipelines as dp

@dp.table(comment="Bronze: đơn hàng thô từ volume")
def bronze_orders():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load("/Volumes/workspace/bronze/landing/orders"))

@dp.materialized_view(comment="Silver: đơn có id")
@dp.expect_or_drop("has_id", "order_id IS NOT NULL")
def silver_orders():
    return spark.read.table("bronze_orders")
```

### 9.4 Pipelines: cách chạy

- **Triggered**, mặc định, cập nhật xong rồi dừng. **Continuous** luôn chạy và làm mới bảng ngay khi dữ liệu đến.
- Chạy từ editor thì khởi động nhanh, dùng lại cluster và tắt retry để thấy lỗi ngay. Chạy từ Jobs hoặc API thì tự retry và restart.
- **Validate update** (Public Preview) chạy thử mà không ghi bảng nào.
- Serverless là mặc định. Edition gồm `CORE` cho ingest, `PRO` thêm CDC, và `ADVANCED` thêm expectations; `ADVANCED` là mặc định.
- Bẫy: full refresh "clears data from streaming tables, clears checkpoints from flows, and reprocesses all records". Nếu nguồn đã xoá file cũ thì full refresh làm mất dòng.

### 9.5 Pipelines: expectations

| mức | SQL | Python | dòng hỏng |
|---|---|---|---|
| warn, mặc định | `EXPECT (...)` | `@dp.expect` | vẫn được ghi và được đếm |
| drop | `ON VIOLATION DROP ROW` | `@dp.expect_or_drop` | bị bỏ và được đếm |
| fail | `ON VIOLATION FAIL UPDATE` | `@dp.expect_or_fail` | update lỗi và rollback |

- Metric nằm trong event log, đọc bằng `event_log(<pipelineId>)`, ở trường `details:flow_progress.data_quality.expectations` với `passed_records` và `failed_records`.
- Bẫy: mức `fail` không ghi metric. Muốn giữ dòng hỏng để xem lại thì tự dựng bảng quarantine. Trong pipeline continuous, một expectation fail sẽ dừng flow đó và mọi flow phụ thuộc.

### 9.6 Spark Declarative Pipelines bản mã nguồn mở

- Có trong Apache Spark từ 4.1.0. Cài bằng `pip install pyspark[pipelines]`, chạy bằng `spark-pipelines run` và `spark-pipelines dry-run`.
- Không có expectations và AUTO CDC; 2 phần này chỉ có trên Databricks. Lab trên laptop dùng bản này để học đồ thị và flow.

### 9.7 Jobs: task và phụ thuộc

- **Loại task:** Notebook, Python script, Python wheel, SQL, Pipeline, dbt, JAR, Dashboards, Run Job, If/else, For each và một số loại khác.
- **Depends on** và **Run if** với 6 điều kiện: All succeeded (mặc định), At least one succeeded, None failed, All done, At least one failed, All failed.
- **Retry:** phần lớn job mặc định không retry. Đặt cả timeout và retry thì timeout tính cho từng lần thử.
- Giới hạn: 1.000 task mỗi job, 2.000 task chạy cùng lúc mỗi workspace.

### 9.8 Jobs: truyền giá trị giữa task

```python
# Task phía trên
dbutils.jobs.taskValues.set(key="new_rows", value=42)

# Task phía dưới; debugValue dùng khi chạy tay trong notebook
new_rows = dbutils.jobs.taskValues.get(taskKey="bronze_ingest", key="new_rows", debugValue=0)
```

- Điều kiện của If/else: `{{tasks.bronze_ingest.values.new_rows}} > 0`. Docs khuyên dùng tham chiếu `{{...}}` thay vì gọi `get()`.
- **Job parameters** được đẩy xuống mọi task. Trùng khoá thì job parameter thắng.
- **For each** chạy 1 task lồng cho mỗi phần tử qua `{{input}}`. Mặc định chạy song song 1, và không lồng For each trong For each.

### 9.9 Jobs: trigger

| trigger | chạy khi |
|---|---|
| Scheduled | theo lịch cron |
| File arrival | có file mới trong volume hoặc external location |
| Table update | bảng nguồn thay đổi |
| Continuous | luôn giữ 1 run đang chạy; mô tả này chưa kiểm trong docs |
| Job completion (Beta) | một job khác chạy xong |

### 9.10 Jobs: backfill và repair run

- **Backfill:** chọn khoảng ngày và bước; mỗi bước là 1 run, tối đa 100 run. Task đọc ngày qua `{{backfill.iso_datetime}}`.
- **Repair run** chạy lại "only the subset of unsuccessful tasks and any dependent tasks", và nhận được tham số mới.
- Bẫy: "Lakeflow Jobs doesn't make tasks idempotent". Task đã ghi một phần mà chạy lại sẽ ghi trùng. Pipeline task không chạy đồng thời được, nên backfill có pipeline task sẽ chạy tuần tự.

### 9.11 Đối chiếu với Airflow

| Airflow | Lakeflow Jobs | khác ở đâu |
|---|---|---|
| DAG | job | |
| `>>` và trigger rule | Depends on, Run if | 6 điều kiện, gồm cả chạy khi upstream lỗi |
| XCom | task values | tối đa 48 KiB; dữ liệu thật để trong bảng |
| sensor | trigger file arrival, table update | không cần task chạy dài để chờ |
| branching | If/else task | `==` so như chuỗi, nên `12.0 == 12` là false |
| dynamic task mapping | For each | mặc định chạy tuần tự, không lồng For each |
| backfill | job backfill | tối đa 100 run mỗi lần |
| clear task | repair run | không tự làm task idempotent |

- Ánh xạ trên theo blog Databricks ngày 18/03/2026 về chuyển từ Airflow sang Lakeflow Jobs.
- Airflow vẫn gọi được job Databricks bằng `DatabricksRunNowOperator`. Gợi ý của người viết, docs không nói thẳng: giữ Airflow khi DAG điều phối nhiều hệ thống ngoài Databricks, chuyển sang Jobs khi mọi task đều nằm trong Databricks.

### 9.12 Free Edition

- Pipelines chỉ chạy serverless, và "One active pipeline per pipeline type".
- Jobs: "Max of 5 concurrent job tasks per account".
- Chưa rõ: expectations (cần edition `ADVANCED`) và unit test cho pipeline.
- Free Edition chỉ có 1 workspace, nên dev và prod phải tách bằng catalog hoặc schema.

> **Bẫy lớn nhất:** tin rằng scheduler retry là an toàn. Retry, repair và backfill đều chạy lại task, và Lakeflow Jobs không làm task idempotent thay bạn. Hãy ghi bằng overwrite theo ngày, dùng `MERGE`, hoặc để pipeline tự quản checkpoint.

Đọc thêm: Module 9 của lộ trình. Docs: [Lakeflow pipelines](https://docs.databricks.com/aws/en/ldp/), [concepts](https://docs.databricks.com/aws/en/ldp/concepts), [expectations](https://docs.databricks.com/aws/en/ldp/expectations), [event log](https://docs.databricks.com/aws/en/ldp/monitor-event-logs), [Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/), [Run if](https://docs.databricks.com/aws/en/jobs/run-if), [triggers](https://docs.databricks.com/aws/en/jobs/triggers), [backfill](https://docs.databricks.com/aws/en/jobs/backfill-jobs), [repair run](https://docs.databricks.com/aws/en/jobs/repair-job-failures), [Airflow](https://docs.databricks.com/aws/en/jobs/how-to/use-airflow-with-jobs).

---

## 10. Chất lượng, test, bundles và môi trường

> **Câu hỏi:** Giải thích chất lượng, test, bundles và môi trường theo cùng khung.

Phần này gom 4 việc giúp pipeline an toàn khi đổi code và khi nhận dữ liệu: test code bằng pytest, kiểm dữ liệu bằng expectations và cổng chất lượng, deploy bằng file cấu hình qua Declarative Automation Bundles (tên cũ Databricks Asset Bundles), và tách môi trường dev với prod qua CI/CD.

### 10.1 Gồm gì

| phần | bắt lỗi gì | công cụ |
|---|---|---|
| unit test | code sai: đổi kiểu join, sai công thức | pytest trên SparkSession local hoặc Databricks Connect |
| golden dataset | output lệch khỏi một bản đã kiểm tay | pytest so DataFrame |
| expectations | dòng hỏng trong một lô dữ liệu | Lakeflow pipelines |
| cổng chất lượng | lô hỏng ở mức cả lô: trùng, thiếu, lệch tổng | bảng staging, kiểm, rồi publish |
| Declarative Automation Bundles | deploy bằng tay, cấu hình lệch giữa các môi trường | lệnh `databricks bundle` |
| CI/CD | merge code chưa test, deploy từ máy cá nhân | GitHub Actions với OIDC và service principal |

### 10.2 Tại sao cần

Vấn đề thật ở Module 10 của lộ trình: mart cấp đơn hàng gặp 2 lỗi trong 1 tuần, và cả 2 lần job đều xanh.

```text
bản cũ, left join         99441 đơn | huỷ 0.63% | doanh thu 13591643.70 | tháng 11/2017 1010271.37
thứ Hai, inner join       98666 đơn | huỷ 0.47% | doanh thu 13591643.70 | tháng 11/2017 1010271.37
thứ Tư, append lô đó      99441 đơn | huỷ 0.63% | doanh thu 13744297.44 | tháng 11/2017 1162925.11
```

- **Thứ Hai**, đổi `left join` thành `inner join` làm mất 775 đơn không có dòng hàng nào. Tổng doanh thu không đổi nên phép đối soát vẫn qua, nhưng tỉ lệ huỷ trên dashboard tụt từ 0,63% xuống 0,47%.
- **Thứ Tư**, nguồn gửi lại file dòng hàng ngày Black Friday. Pipeline chỉ append nên 1.366 dòng bị cộng lần hai, và doanh thu tháng 11/2017 tăng 152.653,74 BRL.
- Lỗi thứ Hai nằm trong code, nên một test chạy trên dữ liệu mẫu cố định bắt được trước khi merge. Lỗi thứ Tư nằm trong dữ liệu, nên chỉ một phép kiểm chạy trên chính lô đó mới bắt được.

| khi làm tay | Databricks |
|---|---|
| job xanh là coi như xong | test chặn ở pull request; expectations và cổng chất lượng chặn lô hỏng |
| chép notebook qua lại giữa dev và prod | bundle: một định nghĩa, nhiều target |
| deploy từ laptop bằng token cá nhân | CI deploy bằng service principal qua OIDC, không lưu secret dài hạn |
| Iceberg có branch để làm write-audit-publish | Delta không có branch; dùng staging, `FAIL UPDATE` hoặc transaction nhiều câu lệnh |

### 10.3 Test code

- **Unit test:** tách hàm biến đổi ra module `.py`, nhận DataFrame và trả DataFrame, rồi test bằng pytest với fixture nhỏ. Docs khuyên giữ hàm và test ở ngoài notebook.
- **pytest với Databricks Connect** chạy test trên compute của Databricks. Databricks Connect và PySpark không cài chung một venv. Chạy pytest từ terminal chỉ dùng profile `DEFAULT` trong `~/.databrickscfg`.

```python
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession

def get_spark() -> SparkSession:
    return DatabricksSession.builder.getOrCreate()

def test_get_nyctaxi_trips():
    df = get_spark().read.table("samples.nyctaxi.trips")
    assert df.count() > 0
```

- **Golden dataset:** so output với một bản đã kiểm tay. So không theo thứ tự dòng, chú ý kiểu decimal, và giá trị kỳ vọng không được sinh ra từ chính code đang test.
- **Unit test cho pipeline** đang Beta và chỉ chạy trong Lakeflow Pipelines Editor. Nó chỉ cô lập theo tên bảng, nên đọc ghi theo đường dẫn như `/Volumes/...` có thể chạm vào dữ liệu production.

### 10.4 Kiểm dữ liệu: expectations và event log

Ba mức warn, drop, fail của expectations đã có ở mục 9.5. Phần bổ sung:

- Mức drop bỏ dòng hỏng mà không giữ lại. Muốn xem lại dòng hỏng thì tự dựng bảng quarantine, tức một bảng chỉ chứa các dòng vi phạm luật.
- Event log mặc định chỉ run-as user đọc được. Publish event log trong Advanced settings để đọc theo tên bảng.
- Query metric chất lượng trên event log:

```sql
SELECT exp.dataset, exp.name,
       SUM(exp.passed_records) AS passed,
       SUM(exp.failed_records) AS failed
FROM (
  SELECT EXPLODE(FROM_JSON(
    details:flow_progress.data_quality.expectations,
    'array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>'
  )) AS exp
  FROM event_log_raw
  WHERE event_type = 'flow_progress'
)
GROUP BY exp.dataset, exp.name;
```

Ở đây `event_log_raw` là một view tạo trên event log của pipeline.

### 10.5 Cổng chất lượng thay cho branch

Lỗi thứ Tư không phạm luật dòng nào: mỗi dòng gửi lại đều hợp lệ, chỉ là đã có rồi. Expectations kiểm từng dòng nên không bắt được. Bản Iceberg giải bằng write-audit-publish trên branch, nhưng Delta không có branch.

Ý tưởng write-audit-publish cần 3 tính chất: người đọc bảng thật không bao giờ thấy lô chưa kiểm, kiểm được luật trên cả lô, và đưa lô vào bảng thật là một bước nguyên tử. Trên Databricks có 4 cách:

| cách | được | mất |
|---|---|---|
| A. expectation `FAIL UPDATE` trong pipeline | rollback cả update, không cần code thêm | chỉ luật theo dòng; mức fail không ghi metric; không có trên laptop |
| B. ghi vào bảng staging, kiểm, rồi publish bằng `INSERT` hoặc `MERGE` | chạy giống nhau trên laptop và workspace; kiểm được luật trên cả lô; publish là 1 commit | ghi lô 2 lần; phải tự dọn staging và tự giấu staging khỏi người đọc |
| C. shallow clone bảng đích, ghi vào clone, kiểm, rồi ghi lại vào bảng thật | kiểm trên đúng trạng thái bảng sau khi ghi | shallow clone trên Unity Catalog đang Public Preview; nhiều giới hạn khi clone |
| D. transaction nhiều câu `BEGIN ATOMIC ... END` | gần write-audit-publish nhất, không cần staging | cần bảng managed bật catalog commits; chưa kiểm trên Free Edition |

### 10.6 Declarative Automation Bundles

- Bundle đổi tên ngày 16/03/2026. Lệnh `databricks bundle` và file cấu hình cũ giữ nguyên.
- Một file `databricks.yml` mô tả job và pipeline, kèm nhiều target cho từng môi trường:

```yaml
bundle:
  name: olist_lakehouse

variables:
  catalog:
    default: dev_olist

resources:
  jobs:
    daily_job:
      name: daily_job
      tasks:
        - task_key: silver
          notebook_task:
            notebook_path: ./src/silver.py

targets:
  dev:
    mode: development
    default: true
  prod:
    mode: production
    variables:
      catalog: prod_olist
```

```bash
databricks bundle validate
databricks bundle plan -t dev
databricks bundle deploy -t dev
databricks bundle run daily_job -t dev
databricks bundle destroy -t dev
```

- **Biến:** khai trong `variables`, gọi bằng `${var.<name>}`. Thứ tự ưu tiên: `--var`, rồi biến môi trường `BUNDLE_VAR_<name>`, rồi `variable-overrides.json`, rồi `variables` của target, cuối cùng là `default`. Biến được thay lúc deploy, không phải lúc chạy.
- **`mode: development`** thêm tiền tố `[dev <tên user>]` vào tên job, đặt pipeline ở chế độ development và tạm dừng mọi lịch chạy và trigger. Bẫy: mode này đổi tên job nhưng không đổi bảng mà job ghi vào, nên tên catalog hoặc schema phải đi qua biến.
- **`mode: production`** yêu cầu nhánh Git khớp nhánh khai trong target, và docs khuyên `run_as` là service principal.
- **Engine direct:** CLI gọi thẳng API của Databricks và giữ một file trạng thái nhỏ. Engine direct GA ngày 10/06/2026; engine Terraform bị gỡ ở CLI 1.20.0. `databricks bundle plan` cho xem trước deploy sẽ tạo, sửa hay xoá gì, giống `terraform plan`.
- Bẫy: với engine direct, xoá một trường khỏi YAML là đưa trường đó về giá trị mặc định, còn Terraform để nguyên giá trị cũ.

### 10.7 CI/CD với GitHub Actions

Docs: "Declarative Automation Bundles are the recommended approach to CI/CD on Databricks." Workflow mẫu deploy target dev cho pull request và target prod khi push vào `main`. Phần job dev:

```yaml
permissions:
  id-token: write   # Required for workload identity federation
  contents: read

jobs:
  deploy-dev:
    if: github.event_name == 'pull_request'
    runs-on: ubuntu-latest
    environment: dev
    env:
      DATABRICKS_AUTH_TYPE: github-oidc
      DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
      DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
      DATABRICKS_BUNDLE_TARGET: dev
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy
```

- CI xác thực bằng workload identity federation cho một service principal, nên không phải lưu secret dài hạn.
- Bẫy: subject của federation policy "must exactly match the expected token subject". Đổi `environment:` trong workflow thì phải sửa policy. `setup-cli@main` không ghim phiên bản; nên ghim một bản cụ thể.

### 10.8 Môi trường

- Docs: "Prod must be a different workspace than Dev or Staging." Dùng chung một metastore cho mọi môi trường, cô lập dữ liệu bằng catalog và bind catalog vào workspace.
- Free Edition chỉ có 1 workspace và 1 metastore, nên lộ trình tách bằng schema: dev ghi `workspace.dev_silver`, prod ghi `workspace.silver`. Cách này tách dữ liệu nhưng không tách quyền admin hay quota. Module 13 làm lại với workspace riêng trên AWS.

### 10.9 Free Edition

- Có: pytest trên laptop, Lakeflow Pipelines Editor, tách môi trường bằng schema.
- Không có: nhiều workspace cho dev và prod.
- Chưa rõ, docs không nói: bundles và CLI, Databricks Connect, service principal và OIDC cho CI, expectations, unit test cho pipeline.

> **Bẫy lớn nhất:** tin rằng job xanh và test xanh nghĩa là dữ liệu đúng. Job xanh chỉ nói code chạy hết; test xanh chỉ nói code đúng trên dữ liệu mẫu. Cả 2 lỗi trong tuần ở trên đều xanh ở cả 2 chỗ.

Đọc thêm: Module 10 của lộ trình. Docs: [notebook best practices](https://docs.databricks.com/aws/en/notebooks/best-practices), [testing với Databricks Connect](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python/testing), [unit test cho pipeline](https://docs.databricks.com/aws/en/ldp/unit-testing), [bundle commands](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands), [bundle variables](https://docs.databricks.com/aws/en/dev-tools/bundles/variables), [deployment modes](https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes), [CI/CD với bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/ci-cd-bundles), [GitHub Actions](https://docs.databricks.com/aws/en/dev-tools/ci-cd/github), [workspace strategy](https://docs.databricks.com/aws/en/lakehouse-architecture/deployment-guide/workspace-strategy).

---

## 11. Phục vụ dữ liệu, BI và giám sát

> **Câu hỏi:** Giải thích phục vụ dữ liệu, BI và giám sát theo cùng khung.

Sau khi có Gold, dữ liệu phải tới được người và ứng dụng, và bạn phải biết khi nào dữ liệu ngừng chảy. Databricks chia việc này làm 3 nhóm: BI cho người (SQL warehouse, AI/BI dashboards, Genie Agents, metric views, SQL alerts), phục vụ cho ứng dụng (Statement Execution API, SQL Connector, JDBC và ODBC, Databricks Apps, Lakebase, OpenSharing), và giám sát (system tables, job notifications, data quality monitoring).

### 11.1 Gồm gì

| nhóm | thành phần | làm gì |
|---|---|---|
| BI | SQL warehouse | compute chạy SQL cho mọi thứ bên dưới |
| BI | AI/BI dashboards (tên cũ Lakeview) | dashboard trong workspace, nhúng được ra ngoài |
| BI | Genie Agents (tên cũ Genie Spaces) | hỏi dữ liệu bằng ngôn ngữ tự nhiên, Genie sinh SQL |
| BI | metric views | định nghĩa chỉ số một lần, mọi công cụ dùng chung |
| BI | SQL alerts | chạy query theo lịch, báo khi vượt ngưỡng |
| ứng dụng | Statement Execution API, SQL Connector for Python, JDBC, ODBC | app gọi SQL warehouse |
| ứng dụng | Databricks Apps | chạy app Streamlit, Dash hoặc Node.js ngay trên Databricks |
| ứng dụng | Lakebase và synced tables | Postgres có quản lý cho truy vấn điểm độ trễ thấp |
| ứng dụng | OpenSharing (tên cũ Delta Sharing) | chia sẻ bảng cho đối tác mà không chép dữ liệu |
| giám sát | system tables | job, query, chi phí, audit, query được bằng SQL |
| giám sát | job notifications | báo khi job lỗi, chạy quá lâu |
| giám sát | data quality monitoring (tên cũ Lakehouse Monitoring) | tự phát hiện bảng trễ hoặc thiếu dòng |

### 11.2 Tại sao cần

Vấn đề thật ở Module 11 của lộ trình: tối 17/10/2018, dashboard báo doanh thu tháng 9 chỉ có 145 BRL sau tháng 8 có 848.860,10 BRL. Đội tài chính gửi một con số tổng khác. Job nạp dữ liệu chạy xanh cả tuần, và phép kiểm độ tươi bằng `max(order_purchase_timestamp)` báo dữ liệu mới tới 17 giờ 30 chiều nay.

```text
sum(price) mọi đơn: 13591643.70 | doanh thu theo định nghĩa: 13494400.74 | lệch 97242.96
đơn mới nhất: 2018-10-17 17:30:18 | đơn có doanh thu mới nhất: 2018-09-03 09:06:57
```

- **Hai định nghĩa:** đội tài chính cộng `price` của mọi đơn; dashboard bỏ đơn `canceled` và `unavailable`. Không ai viết SQL sai, nhưng 97.242,96 BRL nằm giữa 2 con số.
- **Kiểm độ tươi sai chỗ:** từ 01/09/2018 nguồn chỉ gửi 20 đơn, 19 đơn bị huỷ. Đơn huỷ vẫn đẩy `max` của cột thời gian lên, nên một dòng không có giá trị giữ cho bảng trông như còn sống trong 44 ngày.

| khi tự dựng | Databricks |
|---|---|
| mỗi dashboard tự viết công thức doanh thu | metric view: một định nghĩa cho dashboard, Genie, API và đối tác |
| BI riêng như Metabase, query engine riêng như Trino | SQL warehouse và AI/BI dashboards nằm trong workspace, dùng chung quyền Unity Catalog |
| tự viết API đọc từ warehouse | Statement Execution API, Databricks Apps, Lakebase |
| chép file cho đối tác | OpenSharing: đối tác đọc thẳng bảng, không chép |
| dựng Grafana và Prometheus để giám sát | system tables query bằng SQL, cộng SQL alerts |

### 11.3 SQL warehouse

| loại | khởi động | Photon | Predictive IO | Intelligent Workload Management |
|---|---|---|---|---|
| serverless | 2 tới 6 giây | có | có | có |
| pro | khoảng 4 phút | có | có | không |
| classic | khoảng 4 phút | có | không | không |

- Cỡ từ 2X-Small tới 4X-Large. Bẫy: tạo trên UI thì cỡ mặc định là **X-Large**; đổi về 2X-Small trước khi bấm Create.
- Auto stop mặc định 10 phút cho serverless, 45 phút cho pro và classic. Docs: "Idle SQL warehouses continue to accumulate DBU and cloud instance charges until they are stopped."
- Scaling mặc định min 1, max 1 cluster. Hàng đợi giữ tối đa 1.000 query.
- Quyền trên warehouse: Can view, Can use, Can monitor, Can manage, Is owner.
- Bẫy: một alert chạy mỗi 5 phút sẽ giữ warehouse thức mãi, vì cold start tính cho cả warehouse chứ không cho từng request.

### 11.4 AI/BI dashboards

- Dashboard có bản draft và bản published: "The published version remains unchanged until you publish again."
- **Hai chế độ quyền dữ liệu:**
  - Shared, mặc định: "Viewers run queries using the publisher's data permissions." Docs cảnh báo: "This might expose data to users who have not been granted direct access to it."
  - Individual: "Viewers run queries using their own credentials."
- Ở cả 2 chế độ, compute luôn chạy bằng quyền của người publish, nên người xem không cần quyền trên warehouse.
- Bẫy: row filter theo user chỉ bảo vệ người xem khi dashboard dùng chế độ individual. Ở chế độ shared, người xem thấy cái người publish thấy. Bản draft luôn chạy bằng quyền người xem, nên tác giả thử draft có thể thấy khác người xem bản published.
- Nhúng cho người ngoài không cần tài khoản Databricks đã GA; server của bạn dùng service principal để đổi lấy token hẹp cho từng người xem.

### 11.5 Genie Agents

- Dữ liệu phải đăng ký trong Unity Catalog. Tác giả cấu hình mô tả, synonym, join, instructions, SQL mẫu, trusted assets (query có tham số đã kiểm) và benchmarks để đo độ chính xác.
- Quyền dữ liệu tính theo từng người hỏi; row filter và column mask áp theo người đó. Query sinh ra chỉ đọc.
- Bẫy: hỏi về dữ liệu mình không có quyền thì nhận kết quả rỗng, không báo lỗi. Nên đưa metric view vào Genie để Genie không tự đoán công thức doanh thu.

### 11.6 Metric views

Metric view định nghĩa chỉ số một lần trong Unity Catalog. Dashboard, Genie, alert, notebook và BI ngoài như Power BI, Tableau đều dùng chung định nghĩa đó. Rút gọn từ lab Module 11, chưa chạy trên workspace:

```sql
CREATE OR REPLACE VIEW workspace.gold.sales_metrics
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
source: workspace.gold.fct_order_items
fields:
  - name: seller_state
    expr: seller_state
measures:
  - name: revenue
    expr: SUM(price) FILTER (WHERE order_status NOT IN ('canceled', 'unavailable'))
  - name: gross_revenue
    expr: SUM(price)
  - name: order_count
    expr: COUNT(DISTINCT order_id) FILTER (WHERE order_status NOT IN ('canceled', 'unavailable'))
  - name: avg_order_value
    expr: MEASURE(revenue) / MEASURE(order_count)
$$
```

- `revenue` và `gross_revenue` nằm cạnh nhau, nên đội tài chính và dashboard thấy rõ 2 định nghĩa và phần chênh lệch.
- Metric view có từ Databricks Runtime 16.4. Free Edition có dùng được không thì chưa rõ.

### 11.7 SQL alerts

- SQL alerts bản mới GA tháng 5/2026. Mỗi alert sở hữu query riêng: "An existing saved SQL query cannot be reused when creating an alert."
- Trạng thái chỉ còn `OK`, `TRIGGERED`, `ERROR`. Điều kiện so giá trị dòng đầu, hoặc một phép tổng hợp như `SUM`, `AVG`, với một ngưỡng.
- Chỉ admin workspace tạo được đích thông báo như Slack, Teams, webhook.

### 11.8 Phục vụ cho ứng dụng

| đường | dùng khi | lưu ý |
|---|---|---|
| Statement Execution API | app gọi SQL qua REST | kết quả `INLINE` tối đa 25 MiB; lớn hơn dùng `EXTERNAL_LINKS` |
| SQL Connector for Python, JDBC, ODBC | app hoặc công cụ BI nối trực tiếp | JDBC dùng Databricks JDBC Driver từ bản 3; Simba là legacy |
| Databricks Apps | chạy app Streamlit, Dash, Gradio hoặc Node.js trên serverless | app authorization dùng chung quyền; user authorization áp quyền theo người dùng |
| Lakebase và synced tables | truy vấn điểm độ trễ thấp theo khoá | synced table chép bảng Delta sang Postgres; chế độ Triggered và Continuous cần Change Data Feed |
| OpenSharing | đối tác đọc bảng của bạn | Databricks-to-Databricks không cần token; Databricks-to-Open dùng bearer token hoặc OIDC |

- Synced table giống read model trong CQRS: phía ghi là lakehouse, phía đọc là Postgres có index. Docs: "Databricks strictly recommends running only read queries"; ghi vào bảng synced sẽ bị đè ở lần refresh sau.
- Bảng có row filter hoặc column mask không chia sẻ qua OpenSharing được. Bearer token sống lâu là rủi ro; ưu tiên OIDC hoặc Databricks-to-Databricks.

### 11.9 Giám sát bằng system tables

Docs: "System tables are free to use. You are only charged for the compute used to query them." Mặc định chỉ người vừa là account admin vừa là metastore admin đọc được.

| bảng | lưu giữ | độ trễ |
|---|---|---|
| `system.billing.usage` | vô thời hạn | thường trong 12 giờ |
| `system.lakeflow.job_run_timeline` | 395 ngày | thường trong 1 giờ |
| `system.lakeflow.pipeline_update_timeline` | 395 ngày | docs không ghi |
| `system.query.history` | 365 ngày | thường trong 1 giờ |
| `system.access.audit` (Public Preview) | 395 ngày | docs không ghi |
| `system.data_quality_monitoring.table_results` | 395 ngày | docs không ghi |

Viết từ các cột docs liệt kê, chưa chạy:

```sql
-- Job lỗi trong 24 giờ, dùng làm query cho một SQL alert
SELECT job_id, run_id, result_state, termination_code, period_end_time
FROM system.lakeflow.job_run_timeline
WHERE period_end_time >= current_timestamp() - INTERVAL 24 HOURS
  AND result_state IN ('FAILED', 'ERROR', 'TIMED_OUT');
```

- **Job notifications** báo khi job bắt đầu, thành công, lỗi, chạy quá lâu, hoặc streaming backlog vượt ngưỡng; gửi qua email, Slack, PagerDuty, Teams, webhook. Dùng nó cho cảnh báo tức thì, vì `job_run_timeline` trễ khoảng 1 giờ.
- **Data quality monitoring** có anomaly detection, GA tháng 10/2026. Freshness học từ lịch sử commit để biết khi nào bảng trễ bất thường. Completeness đoán khoảng số dòng ghi trong 24 giờ. Bảng mới phải qua giai đoạn Training trước khi có baseline.
- **SLO độ tươi:** docs không có công thức. Cách của lộ trình là SQL alert trên `max` của cột thời gian ghi, tính trên dữ liệu có giá trị như đơn có doanh thu. Không dùng `information_schema.tables.last_altered`, vì nó đo lần đổi định nghĩa bảng, không đo dữ liệu.
- Bẫy: cộng `usage_quantity` mà không xử lý dòng `RETRACTION` sẽ sai. "Job-level notifications aren't sent when failed tasks are retried."

### 11.10 Free Edition

| có | giới hạn | chưa rõ |
|---|---|---|
| AI/BI dashboards, Genie Agents | 1 SQL warehouse 2X-Small; tối đa 3 Databricks Apps, tự dừng sau 24 giờ; 1 Lakebase project | SQL alerts, metric views, system tables, data quality monitoring, OpenSharing, API từ bên ngoài |

> **Bẫy lớn nhất:** job xanh và kiểm độ tươi xanh trong khi dữ liệu có giá trị đã ngừng chảy 44 ngày. Kiểm độ tươi trên dữ liệu mang giá trị, không trên mọi dòng, và định nghĩa chỉ số một lần trong metric view thay vì trong từng dashboard.

Đọc thêm: Module 11 của lộ trình. Docs: [SQL warehouse types](https://docs.databricks.com/aws/en/compute/sql-warehouse/warehouse-types), [chia sẻ dashboard](https://docs.databricks.com/aws/en/dashboards/share/share), [metric views](https://docs.databricks.com/aws/en/uc-semantics/metric-views/), [Statement Execution API](https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial), [OpenSharing](https://docs.databricks.com/aws/en/opensharing/), [system tables](https://docs.databricks.com/aws/en/admin/system-tables/), [job notifications](https://docs.databricks.com/aws/en/jobs/notifications), [anomaly detection](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-quality-monitoring/anomaly-detection).

---

## 12. Streaming

> **Câu hỏi:** Giải thích streaming trên Databricks theo cùng khung.

Databricks xử lý stream bằng Spark Structured Streaming, với 2 chế độ: micro-batch (mặc định, cho ETL và phân tích) và real-time mode (cho việc vận hành cần dưới 1 giây). Databricks không có Flink. Trên serverless, notebook và job chỉ chạy được trigger `availableNow`; muốn luồng chạy liên tục thì dùng Lakeflow pipeline ở continuous mode.

### 12.1 Gồm gì

| phần | làm gì |
|---|---|
| Structured Streaming, chế độ micro-batch | xử lý dữ liệu mới theo từng lô nhỏ, cùng API DataFrame |
| real-time mode | độ trễ đầu cuối có thể chỉ 5 ms, cho phát hiện gian lận, cá nhân hoá |
| nguồn | Kafka, Auto Loader trên file, bảng Delta |
| đích | bảng Delta, Kafka, `foreachBatch` để tự viết logic ghi |
| checkpoint | lưu offset, commit và state để chạy lại không mất, không trùng |
| watermark và window | gom theo thời điểm sự kiện xảy ra, xử lý sự kiện tới trễ |
| state | RocksDB, `transformWithState` cho logic có trạng thái tự viết |
| chạy liên tục | streaming table trong pipeline continuous mode, continuous job |

### 12.2 Tại sao cần

Vấn đề thật ở Module 12 của lộ trình: bảng "số đơn mỗi ngày" được dựng bằng ví dụ trong tutorial: `withWatermark` 10 phút, `window` 1 ngày, ghi Delta ở append mode. Nguồn gửi mỗi tháng 1 file. File tháng 11/2017 tải lỗi và được gửi lại sau file tháng 1/2018. Query chạy xong, không báo lỗi:

```text
micro-batch: 27 | lỗi: None
số cửa sổ: 603 | tổng đơn: 91896
cửa sổ của tháng 11/2017: 0
ngày cuối trong bảng: 2018-10-16
numRowsDroppedByWatermark cộng dồn: 30
```

- **Mất cả tháng 11/2017**, tức 7.544 đơn của tháng có Black Friday. File tới khi watermark đã qua ngày 31/12/2017, nên mọi cửa sổ của tháng 11 đã đóng và mọi sự kiện trong file bị bỏ.
- **Thiếu ngày cuối:** append mode chỉ ghi một cửa sổ khi watermark vượt qua cuối cửa sổ, mà không còn sự kiện nào sau đó để đẩy watermark.
- **Metric đánh lừa:** `numRowsDroppedByWatermark` báo 30, không phải 7.544, vì nó đếm dòng sau bước gộp sơ bộ trong mỗi partition.

| tự dựng | Databricks |
|---|---|
| Flink cho stream, Spark cho batch, 2 engine 2 API | một API DataFrame cho cả batch và stream |
| Flink checkpoint, sink exactly-once vào Iceberg | checkpoint của Structured Streaming, Delta sink exactly-once qua transaction log |
| keyed state và timer trong ProcessFunction của Flink | `transformWithState` với `ValueState`, `ListState`, `MapState`, timer, TTL |
| Flink SQL job chạy liên tục | Lakeflow pipeline continuous mode hoặc continuous job |
| độ trễ mili giây của Flink | real-time mode |

### 12.3 Trigger

| trigger | hành vi |
|---|---|
| không đặt | tương đương `processingTime` 0 ms, độ trễ khoảng 3 tới 5 giây |
| `processingTime='10 seconds'` | kiểm dữ liệu mới theo chu kỳ |
| `availableNow=True` | xử lý hết dữ liệu có lúc khởi động, chia thành nhiều batch, rồi dừng |
| `realTime='5 minutes'` | real-time mode; khoảng này là độ dài một batch chạy dài và chu kỳ checkpoint, không phải độ trễ |
| `continuous='1 second'` | không hỗ trợ trên Databricks; dùng real-time mode thay |

- Serverless notebook và job chỉ nhận `availableNow` (và `Once` đã deprecated). Trigger khác báo lỗi `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`.
- Bẫy: gọi `display()` trên streaming DataFrame là khởi động một streaming job thật, có thể giữ compute không tự tắt.

### 12.4 Checkpoint

- Checkpoint lưu offset của nguồn, commit của đích, state và metadata của query.
- Docs: "Each query must have a different checkpoint location." Checkpoint nên nằm trên volume, ví dụ `/Volumes/catalog/schema/volume/path`.
- Đổi số nguồn hoặc loại nguồn, topic Kafka, đường dẫn Auto Loader, loại phép có trạng thái, schema của state hoặc loại đích thì cần checkpoint mới. Số shuffle partition bị cố định khi checkpoint được tạo.
- Xoá checkpoint thì lần chạy sau bắt đầu lại từ đầu. Nếu ghi Delta qua `foreachBatch` với `txnAppId` thì phải đổi `txnAppId`, vì batch ID chạy lại từ 0 và các lần ghi mới sẽ bị bỏ qua.

### 12.5 Exactly-once

- **Delta làm đích:** transaction log bảo đảm exactly-once, kể cả khi có stream hay batch khác ghi đồng thời.
- **`foreachBatch`** chỉ at-least-once. Muốn ghi idempotent thì dùng `txnAppId` và `txnVersion` gán bằng `batchId`, hoặc `MERGE` theo khoá.
- **Kafka làm đích** chỉ at-least-once.
- **Delta làm nguồn** chỉ nhận append. `UPDATE`, `DELETE`, `MERGE` hay overwrite trên bảng nguồn làm stream lỗi, trừ khi đặt `skipChangeCommits` hoặc đọc Change Data Feed.
- Bẫy: stream phải chạy ít nhất 1 lần trong cửa sổ giữ lại của bảng nguồn, 7 ngày cho file và 30 ngày cho log, nếu không sẽ lỗi `DELTA_FILE_NOT_FOUND_DETAILED`. Đừng đặt `spark.sql.files.ignoreMissingFiles=true` để lách, vì nó "silently produces incorrect results".

### 12.6 Watermark và window

```python
from pyspark.sql.functions import window
(df
  .withWatermark("event_time", "10 minutes")
  .groupBy(
    window("event_time", "5 minutes"),
    "id")
  .count())
```

- **Watermark** là mốc thời gian sự kiện mà Spark coi là đã đủ dữ liệu. Sự kiện cũ hơn mốc là tới trễ, và "might still process ... but this isn't guaranteed".
- **Append mode** chỉ ghi kết quả của cửa sổ sau khi watermark vượt qua nó. **Update mode** ghi ngay và cập nhật dần.
- Không có watermark thì state "grows indefinitely".
- `dropDuplicatesWithinWatermark` khử trùng trong phạm vi watermark. Docs: "Structured Streaming guarantees exactly-once processing but doesn't deduplicate records from data sources."
- Điểm bắt đầu hợp lý: watermark bằng khoảng 2 lần SLA độ trễ.

### 12.7 State

- Docs: "Databricks recommends using RocksDB with changelog checkpointing." RocksDB là state store mặc định từ Databricks Runtime 17.3 LTS.
- `transformWithState`, từ Databricks Runtime 16.2, thay `mapGroupsWithState`. Bạn viết một `StatefulProcessor` với `init`, `handleInputRows`, `handleExpiredTimer`, dùng state `ValueState`, `ListState`, `MapState`, timer theo processing time hoặc event time, và TTL.

### 12.8 Kafka

```python
df = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "<server:ip>")
  .option("subscribe", "<topic>")
  .option("startingOffsets", "latest")
  .load())
```

- `startingOffsets` chỉ có tác dụng khi query mới bắt đầu. Khởi động lại thì query luôn đọc tiếp từ checkpoint.
- `maxOffsetsPerTrigger` giới hạn số offset mỗi batch. Metric lag gồm `avgOffsetsBehindLatest`, `maxOffsetsBehindLatest`, `estimatedTotalBytesBehindLatest`.
- Xác thực SASL phải dùng tên class có tiền tố `kafkashaded.`. Databricks khuyên dùng Unity Catalog service credential trên serverless.
- SQL có hàm `read_kafka`. Lakeflow Connect có connector Kafka (Beta) để nạp Kafka thành streaming table mà không viết code Spark.

### 12.9 Real-time mode

- Cho việc vận hành dưới 1 giây; độ trễ đầu cuối có thể chỉ 5 ms.
- Chạy trực tiếp cần Databricks Runtime 16.4 LTS và classic compute, chỉ update mode, không chạy trên serverless notebook hay job. Không nhận Delta làm nguồn hay đích.
- Real-time mode trong Lakeflow pipelines GA ngày 29/10/2026.

### 12.10 Khi nào vẫn cần Flink

Suy ra từ giới hạn trong docs Databricks; phía Flink chưa kiểm:

- Cần độ trễ dưới 1 giây mà nguồn hoặc đích là bảng Delta, vì real-time mode không nhận Delta.
- Cần exactly-once đầu cuối khi ghi ra Kafka, vì Kafka sink của Spark chỉ at-least-once.
- Tổ chức đã vận hành Flink, hoặc luồng chỉ đi từ Kafka sang Kafka mà không cần lakehouse.

### 12.11 Giám sát

- Classic compute có tab Streaming trong Spark UI; đặt tên query bằng `.queryName(...)`. Serverless không có Spark UI, dùng query profile.
- `StreamingQueryListener` nhận sự kiện bắt đầu, tiến độ và kết thúc của query, để đẩy metric ra hệ thống giám sát.

### 12.12 Free Edition

| có | không | chưa rõ |
|---|---|---|
| `availableNow`, Auto Loader trên volume, Delta làm nguồn và đích, checkpoint trên volume, `foreachBatch` với `MERGE` | trigger khác `availableNow`, real-time mode, Spark UI, Scala | Kafka bên ngoài, continuous pipeline, `transformWithState`, `StreamingQueryListener` |

Vì vậy lab Module 12 làm phần Kafka và phần chịu lỗi trên laptop, còn trên Free Edition dùng Auto Loader trên volume làm nguồn thay Kafka.

> **Bẫy lớn nhất:** tin rằng query chạy xong không lỗi là đã đếm đủ. Watermark bỏ sự kiện tới trễ một cách im lặng, và metric `numRowsDroppedByWatermark` có thể báo thấp hơn số sự kiện bị mất rất nhiều. Hãy chọn watermark theo độ trễ thật của nguồn, và đối soát tổng với nguồn sau mỗi lần chạy.

Đọc thêm: Module 12 của lộ trình. Docs: [Structured Streaming](https://docs.databricks.com/aws/en/structured-streaming/), [triggers](https://docs.databricks.com/aws/en/structured-streaming/triggers), [serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations), [Kafka](https://docs.databricks.com/aws/en/connect/streaming/kafka), [Delta và streaming](https://docs.databricks.com/aws/en/structured-streaming/delta-lake), [`foreachBatch`](https://docs.databricks.com/aws/en/structured-streaming/foreach).

---

## 13. Databricks trên AWS, Terraform và chi phí

> **Câu hỏi:** Giải thích Databricks trên AWS, Terraform và chi phí theo cùng khung.

Free Edition không có account console, nên phần này học trên free trial của AWS. Nó gồm: tạo workspace trên AWS, mạng ở mức nhập môn, dựng mọi thứ bằng Terraform, và kiểm soát chi phí bằng budget, tag, compute policy và bảng `system.billing.usage`.

### 13.1 Gồm gì

| phần | làm gì |
|---|---|
| account console | quản lý workspace, user, budget ở cấp account, tại `https://accounts.cloud.databricks.com` |
| workspace trên AWS | serverless workspace, hoặc classic workspace cấu hình tự động hay thủ công |
| mạng | VPC, subnet, security group, PrivateLink cho classic compute |
| Terraform provider `databricks/databricks` | dựng workspace và tài nguyên cấp account bằng code |
| budgets | cảnh báo khi chi tiêu vượt ngưỡng, không chặn |
| tag và serverless usage policy | gắn mỗi DBU với một team hay một dự án |
| compute policy và auto termination | giới hạn cỡ máy, tự tắt máy rảnh |
| `system.billing.usage`, `system.billing.list_prices` | nguồn sự thật về lượng dùng và giá niêm yết |

### 13.2 Tại sao cần

Vấn đề thật ở Module 13 của lộ trình: ngày 15 của trial, account đã gắn thẻ nên tự chuyển sang trả theo mức dùng, và trần 50 DBU mỗi giờ của trial không còn. Chiều thứ Sáu, bạn tạo SQL warehouse trên UI mà không đổi cỡ, trỏ một job SQL chạy mỗi 15 phút vào nó, đặt budget 20 USD rồi nghỉ cuối tuần.

```text
2X-Small (4 DBU/giờ): Running 46.9 giờ, 187.7 DBU, 131.41 USD
X-Large (80 DBU/giờ): Running 46.9 giờ, 3754.7 DBU, 2628.27 USD
400 USD credit = 571.4 DBU = 7.1 giờ X-Large nếu không có trần
```

- UI chọn sẵn cỡ X-Large. Mức 80 DBU mỗi giờ cho X-Large và 4 cho 2X-Small lấy từ trang giá của Azure Databricks; với AWS là chưa kiểm. Giá 0,70 USD mỗi DBU là giả định; giá thật nằm trong `system.billing.list_prices`.
- Job mỗi 15 phút cộng Auto Stop 10 phút giữ warehouse ở trạng thái Running 46,9 trên 64 giờ. Cỡ, lịch chạy và Auto Stop cùng quyết định hoá đơn.
- Email của budget có thể tới trễ 24 giờ, và budget không chặn gì.

| làm tay | Databricks và Terraform |
|---|---|
| bấm tạo workspace trên console, khó dựng lại | Terraform `databricks_mws_*`: dựng và xoá bằng 1 lệnh |
| không biết DBU tiêu vào đâu | tag và serverless usage policy, rồi group theo tag trong `system.billing.usage` |
| tin rằng budget sẽ chặn chi tiêu | budget chỉ cảnh báo; chặn bằng compute policy, auto termination và cỡ nhỏ |
| quên tắt cluster | auto termination, compute policy bắt buộc đặt thời gian tắt |

### 13.3 Account console

- Người tạo tài khoản tự có vai trò account owner, quyền như account admin nhưng không gỡ được.
- Cấu hình cấp account gồm credential configuration, storage configuration, network configuration, private access settings, budget và workspace. Mỗi thứ có resource Terraform `databricks_mws_*` tương ứng.
- Free Edition: "No access to the account console or account-level APIs."

### 13.4 Ba cách tạo workspace trên AWS

| cách | tạo gì trong AWS của bạn | khi nào dùng |
|---|---|---|
| serverless workspace | không gì cả; dùng default storage và serverless compute | học, thử, team chỉ dùng serverless; docs khuyên bắt đầu ở đây |
| classic, cấu hình tự động | cross-account IAM role, VPC, S3 bucket, IAM role cho bucket | cần classic compute nhưng không muốn tự dựng |
| classic, cấu hình thủ công | bạn tự tạo role, bucket, VPC rồi đăng ký với Databricks | cần VPC riêng, IAM policy riêng, bucket có sẵn |

- Serverless workspace GA ngày 29/01/2026. Docs: nó "support only serverless compute", hợp với "short-lived environments such as training or testing".
- Cấu hình tự động dùng AWS IAM temporary delegation, thay cho AWS Quickstart bằng CloudFormation từ ngày 21/11/2025.
- Cấu hình thủ công cần 3 đối tượng:
  - Credential configuration: cross-account IAM role tin tài khoản AWS của Databricks `414351767826`, external ID là Databricks account ID.
  - Storage configuration: S3 bucket cùng region, bucket policy, và một IAM role tự assume có quyền đọc ghi prefix `unity-catalog/`.
  - Network configuration: mặc định là VPC do Databricks quản lý; muốn tự quản thì tạo customer-managed VPC trước.
- Bẫy:
  - Đổi `compute_mode` "requires recreation of the workspace".
  - "Databricks trial credits apply only to Databricks usage." VPC và NAT gateway của classic workspace do AWS tính riêng.
  - "Classic compute (any compute that is not serverless) cannot interact with data assets in default storage."

### 13.5 Mạng ở mức nhập môn

- Customer-managed VPC cần ít nhất 2 subnet private ở 2 availability zone, netmask từ `/17` tới `/26`, NAT gateway, và 1 tới 5 security group.
- "You cannot move an existing workspace with a Databricks-managed VPC to use a customer-managed VPC."
- PrivateLink cho classic compute cần gói Enterprise, customer-managed VPC và secure cluster connectivity.
- Sau khi workspace ở trạng thái RUNNING, docs dặn "wait an additional 20 minutes after the status shows RUNNING before creating or using clusters".
- Serverless workspace không cần tự quản mạng: egress kiểm soát bằng network policies, ingress bằng IP access list.

### 13.6 Terraform

- Provider `databricks/databricks` bản mới nhất là v1.142.0, ngày 09/11/2026.
- Docs chia việc: Terraform cho tài nguyên cấp cloud và việc admin như tạo workspace và mạng; Declarative Automation Bundles cho job, pipeline và notebook.
- Provider cấp account dùng `host = "https://accounts.cloud.databricks.com"`; provider cấp workspace dùng URL của workspace.

Serverless workspace bằng Terraform, nguyên văn từ docs:

```hcl
resource "databricks_mws_workspaces" "serverless_workspace" {
  account_id     = "" # Your Databricks account ID
  workspace_name = "serverless-workspace"
  aws_region     = "us-east-1"
  compute_mode   = "SERVERLESS"
}
```

- Serverless workspace "must not include `credentials_id` or `storage_configuration_id`". Classic workspace thì cần thêm `databricks_mws_credentials` và `databricks_mws_storage_configurations`.
- `terraform test` chạy được kiểu unit với `command = plan`; thêm `mock_provider "databricks" {}` thì không cần credential.

### 13.7 Budgets

- Budgets GA ngày 06/07/2026. Budget theo dõi chi tiêu theo giá niêm yết bằng USD, tối đa 4 ngưỡng mỗi budget, lọc theo workspace, loại tài nguyên hoặc tag.
- Budget không phải trần cứng. Chỉ Unity Gateway có tuỳ chọn "Block usage", và docs dặn: "Do not use this feature as a way to ensure an absolute spend cap on final billed amounts."
- Email có thể trễ tới 24 giờ. Budget không tính credit hay chiết khấu.

### 13.8 Tag và serverless usage policy

- Serverless usage policy (Public Preview) là một tập tag gắn vào mọi hoạt động serverless của người được gán. Tag hiện trong cột `custom_tags` của `system.billing.usage`.
- Bẫy: "Policy changes are only applied to usage initiated after the policy update." Policy không gắn tag cho classic compute. Pipeline được job gọi không thừa hưởng policy của job.
- Classic compute có tag mặc định `Vendor`, `ClusterId`, `ClusterName`, `Creator`, được truyền xuống EC2 và EBS. Đừng đặt tag khoá `Name` cho cluster, vì khi đó cluster "can no longer be tracked by Databricks" và có thể không tự tắt.

### 13.9 Compute policy và auto termination

- Compute policy giới hạn compute mà người dùng được tạo: DBU mỗi giờ qua thuộc tính `dbus_per_hour`, số compute mỗi người, giá trị cố định, tag bắt buộc. Kiểu luật gồm `fixed`, `forbidden`, `allowlist`, `blocklist`, `regex`, `range`, `unlimited`.
- Bẫy: giá trị mặc định của policy không tự áp cho compute tạo qua Clusters API, trừ khi đặt `apply_policy_default_values` là `true`.
- Auto termination của classic compute từ 10 tới 10.000 phút; đặt `0` là tắt tính năng này. Docs: "Idle compute continue to accumulate DBU and cloud instance charges during the inactivity period before termination."
- SQL warehouse: Auto Stop mặc định 10 phút cho serverless, 45 phút cho pro và classic. Cỡ mặc định trên UI là X-Large.

### 13.10 Bảng chi phí

```sql
SELECT u.usage_date,
       u.custom_tags['team'] AS team,
       u.billing_origin_product,
       SUM(u.usage_quantity) AS dbu,
       SUM(u.usage_quantity * p.pricing.effective_list.default) AS list_usd
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON p.sku_name = u.sku_name
 AND u.usage_end_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE u.usage_date >= current_date() - INTERVAL 30 DAYS
GROUP BY ALL
HAVING SUM(u.usage_quantity) != 0;
```

Chỉnh từ ví dụ trong docs, chưa chạy.

- Sửa sai được ghi bằng dòng `RETRACTION` mang số âm và dòng `RESTATEMENT`, nên luôn `SUM` rồi bỏ nhóm có tổng bằng 0.
- Nhân với `list_prices` ra giá niêm yết, không phải số tiền thực bị tính.
- System tables miễn phí; chỉ tính compute dùng để query.

### 13.11 Free trial

- Trial dài 14 ngày, tối đa 400 USD credit. Trial giới hạn 1 SQL warehouse tối đa 50 DBU mỗi giờ, và serverless cho notebook, job, pipeline tối đa 50 DBU mỗi giờ.
- Trial qua AWS Marketplace, hoặc account đã gắn phương thức thanh toán, tự chuyển sang trả theo mức dùng khi hết credit.
- Không nâng cấp thì asset của workspace bị xoá 60 ngày sau khi trial kết thúc.
- Trước khi huỷ, docs yêu cầu terminate mọi compute và gỡ thẻ tín dụng.

### 13.12 Free Edition

Free Edition không có account console, nên không học được budget, việc tạo workspace hay Terraform cấp account trên đó. Phần này chỉ làm được trên free trial hoặc account trả phí.

> **Bẫy lớn nhất:** tin rằng budget sẽ chặn chi tiêu. Budget chỉ gửi email, có thể trễ 24 giờ. Thứ thật sự giới hạn hoá đơn là cỡ warehouse, lịch chạy, Auto Stop, auto termination và compute policy, và mọi thứ bạn tạo nên nằm trong Terraform để xoá được bằng 1 lệnh.

Đọc thêm: Module 13 của lộ trình. Docs: [tạo serverless workspace](https://docs.databricks.com/aws/en/admin/workspace/serverless-workspaces), [Terraform provider](https://docs.databricks.com/aws/en/dev-tools/terraform/), [budgets](https://docs.databricks.com/aws/en/admin/account-settings/budgets), [billing system tables](https://docs.databricks.com/aws/en/admin/usage/system-tables), [compute policies](https://docs.databricks.com/aws/en/admin/clusters/policies), [free trial](https://docs.databricks.com/aws/en/getting-started/free-trial), [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).
