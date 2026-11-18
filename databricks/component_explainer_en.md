# Databricks components explained for data engineers

This document collects the questions that come up when you start learning Databricks, with an answer to each one. Each component is explained using the same framework:

1. **What it contains:** the parts that make up the component.
2. **Why it is needed:** which problem it solves compared with building an open source stack yourself.
3. **Concepts to know:** more detail, with commands and code.

Product names, Free Edition limits and numbers come from the Databricks docs for AWS, checked on 2026-11-11. Databricks renamed products several times in 2025 and 2026, so open the latest docs when a name does not match the UI.

The labs and figures on the Olist dataset are in [lo_trinh_databricks.pdf](lo_trinh_databricks.pdf), called "the roadmap" below. Those labs ran on local Spark, not on Free Edition. Code marked "not run yet" was written from the docs and has not been run anywhere. The roadmap PDFs linked in this file are written in Vietnamese. A Vietnamese version of this file is at [giai_thich_thanh_phan.md](giai_thich_thanh_phan.md).

The column "equivalent in a self-built stack" in the tables compares Databricks with an open source lakehouse running on Docker Compose: Spark, Iceberg REST catalog, object store and Airflow. This is the author's comparison, not quoted from the docs.

## Contents

1. [Documentation progress and background knowledge](#1-documentation-progress-and-background-knowledge)
2. [Overview of the components](#2-overview-of-the-components)
3. [Workspace](#3-workspace)
4. [Q&A: PySpark and SQL editor, metastore, three-level names](#4-qa-pyspark-and-sql-editor-metastore-three-level-names)
5. [Unity Catalog](#5-unity-catalog)
6. [Delta Lake](#6-delta-lake)
7. [Compute and Spark](#7-compute-and-spark)
8. [Ingestion](#8-ingestion)
9. [Lakeflow pipelines and Lakeflow Jobs](#9-lakeflow-pipelines-and-lakeflow-jobs)
10. [Quality, tests, bundles and environments](#10-quality-tests-bundles-and-environments)
11. [Data serving, BI and monitoring](#11-data-serving-bi-and-monitoring)
12. [Streaming](#12-streaming)
13. [Databricks on AWS, Terraform and cost](#13-databricks-on-aws-terraform-and-cost)

---

## 1. Documentation progress and background knowledge

> **Question:** What has been done on the Databricks part so far, and which basics do you need before learning Databricks?

### What exists (2026-11-16)

| document | status |
|---|---|
| [lo_trinh_databricks.pdf](lo_trinh_databricks.pdf) | Modules 0 to 13 written, 538 pages |
| [hoc_pyspark.pdf](hoc_pyspark.pdf) | 25 chapters: Lab 00 and notebooks 09 to 33 of the Learning PySpark with Databricks course |
| introductory course following the Databricks Academy outline | 7 chapters written, not yet assembled into a PDF, not published |

Still open:

- 192 code blocks of the roadmap need to run on a workspace. They are marked "not run yet" and will be rerun after signing in to Free Edition.
- The optional branch T1 to T3, the capstone project, and the certification and career chapters are not written yet.

### Background knowledge you need

The roadmap says: "You need basic Python and to be able to use the command line. You do not need to know SQL or Spark beforehand."

| part | level | where to learn |
|---|---|---|
| Python: functions, lists, dicts, reading files | required | learn on your own first |
| Command line and Git | required | learn on your own first; Modules 10 and 13 use them heavily |
| SQL: `SELECT`, `JOIN`, `GROUP BY`, window functions | recommended | Module 1 teaches it, 40 hours |
| Data concepts: CSV, JSON, Parquet, schema, partition | recommended | you already have these if you have built an Iceberg lakehouse |
| Spark DataFrame: lazy evaluation, transformations, actions | learn in parallel | `hoc_pyspark.pdf` and Module 7 |
| Cloud: S3, IAM | Module 13 only | Module 13 |

Suggested order: Module 0, then Module 1 on SQL, then Modules 2 and 3. Read `hoc_pyspark.pdf` before starting Module 7. If you have built an Iceberg lakehouse, you can skim the concepts part of Module 2, such as medallion and how to choose a store.

---

## 2. Overview of the components

> **Question:** What are the main components of Databricks?

Databricks has 5 layers: foundation (workspace), storage (Delta Lake, volumes), governance (Unity Catalog), compute (serverless, SQL warehouse) and pipelines (Lakeflow). For a data engineer, the 6 things to know first are Workspace, Unity Catalog, Delta Lake, Compute, Lakeflow pipelines and Lakeflow Jobs.

```text
Account            billing, contains many workspaces
  Workspace        where you work: notebook, SQL editor, jobs, catalog
    Control plane  web app and backend, run by Databricks
    Compute plane  runs Spark and SQL: serverless (Databricks account) or classic (your AWS account)
Unity Catalog      metastore, three-level namespace catalog.schema.table
Storage            S3 or default storage, holds Parquet files and _delta_log
```

| layer | component | what it does | equivalent in a self-built stack | module |
|---|---|---|---|---|
| Storage | Delta Lake | table format: Parquet plus `_delta_log`, ACID, time travel | Iceberg | 5 |
| Storage | Volumes | hold raw CSV and JSON files; replace DBFS | bucket of the object store | 4 |
| Governance | Unity Catalog | metastore, privileges, lineage | REST catalog | 6 |
| Compute | serverless, classic (all-purpose, jobs compute) | run notebooks, jobs, pipelines | Spark cluster | 7 |
| Compute | SQL warehouse and Photon | run SQL for BI and dashboards | Trino, ClickHouse | 1, 11 |
| Ingestion | Auto Loader, `COPY INTO`, Lakeflow Connect | incremental ingestion of files, connectors for databases and SaaS | hand-written Spark job | 8 |
| Pipelines | Lakeflow pipelines (old name DLT) | declare tables and check quality | hand-written Spark job | 9 |
| Orchestration | Lakeflow Jobs (old name Workflows) | scheduling, DAG of tasks | Airflow | 9 |
| Streaming | Structured Streaming | read Kafka, process streams | Flink and Redpanda | 12 |
| Dev and CI | notebook, Git folders, CLI, Declarative Automation Bundles | write code, deploy with config files | repo plus Docker Compose | 0, 10 |
| Serving | AI/BI dashboards, Genie Agents, OpenSharing | dashboards, ask questions in natural language, share data | Grafana | 11 |
| Monitoring | system tables such as `system.billing.usage` | usage, cost, audit | Grafana | 11, 13 |

Free Edition has only serverless compute, no classic compute.

---

## 3. Workspace

> **Question:** What does a workspace contain, why is it needed, what problem does it solve that used to be hard to do, and which concepts do you need to know in detail?

A workspace is a Databricks environment in the cloud. A team writes code, runs compute, views data and schedules jobs in one place, sharing one set of permissions. The most important idea: **the workspace holds code, Unity Catalog holds data**.

### 3.1 What it contains

The docs define a workspace as "a Databricks deployment in the cloud".

| sidebar item | what it holds | equivalent in a self-built stack |
|---|---|---|
| Workspace (browser) | notebooks, `.py` files, Git folders | code folder on a laptop |
| Catalog | catalogs, schemas, tables, volumes, privileges | REST catalog and the object store UI |
| SQL Editor, SQL Warehouses | SQL statements and the machines that run SQL | Trino plus DBeaver |
| Notebook | Python and SQL running on serverless | Jupyter in a Spark container |
| Jobs & Pipelines | Lakeflow Jobs, Lakeflow pipelines | Airflow |
| Compute | serverless, SQL warehouse, classic clusters | Spark container |

### 3.2 Why it is needed

These things could always be done, but you had to assemble and operate each tool yourself.

| self-built | what the workspace solves |
|---|---|
| one UI and one login per tool: Jupyter, Spark UI, Airflow, object store | one UI, one CLI, one API for everything |
| you pin Spark and Delta versions yourself and they drift: pyspark 4.1.4 with delta-spark 4.3.1 does not resolve | Databricks Runtime pins them; Databricks upgrades serverless |
| machines run all the time, you patch and scale them yourself | serverless starts only when it runs and is billed per second; a SQL warehouse uses Auto Stop and shuts down after 10 minutes |
| permissions scattered: S3 keys inside each job, catalog permissions kept separately | privileges centralized by user and group, through Unity Catalog |
| code sits on each person's laptop | shared folders and Git folders, running on the same compute |
| analysts, data engineers and data scientists each use a different tool | SQL editor, notebooks and dashboards in one place |

This works because Databricks splits the system into 2 parts. The **control plane** contains the web app and backend, run by Databricks. The **compute plane** is where data is processed.

### 3.3 Management tree

```text
Account     billing, support, contains many workspaces
Workspace   1 deployment, 1 region: users, groups, folders, compute
Metastore   Unity Catalog, at account level, attached to workspaces in the same region
```

- A table name has three levels: `catalog.schema.table`. A new workspace comes with a catalog named after the workspace, containing a schema `default`.
- Free Edition has 1 workspace, 1 metastore and no access to the account console.

### 3.4 Control plane and compute plane

| part | lives in | contains |
|---|---|---|
| control plane | Databricks account | web app, backend |
| serverless compute plane | Databricks account | serverless compute for notebooks, jobs, pipelines |
| classic compute plane | your AWS account | clusters you configure yourself; AWS charges extra for the VMs |

Data lives in your S3. A serverless workspace uses "default storage", a storage area managed by Databricks.

### 3.5 Workspace objects versus data objects

This is the easiest place to get confused.

| type | examples | path | who manages access |
|---|---|---|---|
| workspace object | notebook, `.py` file, Git folder, query, dashboard | `/Workspace/Users/<email>/`, `/Workspace/Shared/` | workspace folder permissions |
| data object | table, view, volume, function | `catalog.schema.table`, `/Volumes/<catalog>/<schema>/<volume>/` | Unity Catalog |

- The `Users/<email>` folder is private by default. In `Shared`, every user has full permissions.
- DBFS root and DBFS mounts are deprecated. Use volumes and external locations instead.

### 3.6 Notebook

- The default format is `.ipynb`. Source format saves as `.py` or `.sql` with the first line `Databricks notebook source`, so diffs in Git are easier to read.
- Magic commands: `%sql`, `%python`, `%md`, `%pip`, `%run`, `%sh`, `%fs`. Each language runs in its own REPL, so variables do not pass to another language. The result of a SQL cell is in the variable `_sqldf`.
- `%pip install` applies only to the current notebook and is lost when the session ends.
- `dbutils` has the main modules `fs`, `secrets`, `widgets`, `notebook`, `jobs`.
- Splitting code: `%run` runs another notebook in the same context and returns no value. `dbutils.notebook.run()` creates a temporary job and returns a string. The docs recommend keeping logic in `.py` files and then using `import`.
- A widget combined with the `:name` marker passes parameters into SQL and avoids SQL injection:

```python
display(spark.read.table("samples.nyctaxi.trips"))
dbutils.widgets.text("min_fare", "10")
```

```sql
%sql
SELECT * FROM samples.nyctaxi.trips WHERE fare_amount >= :min_fare LIMIT 100
```

### 3.7 Workspace files and Git folders

- **Workspace files** hold `.py`, YAML and `.whl` files, up to 500 MB per file, with relative imports as on local. On serverless, use absolute paths because the current directory is not guaranteed. Executors cannot write to workspace files.
- A **Git folder**, old name Repos, is a clone of a repo located in `/Workspace/Users/<email>/`. Only HTTPS is supported. Connect GitHub with a GitHub App or a personal access token. Each working branch is limited to 1 GB.

### 3.8 Compute in the workspace

| type | choosing the version | Free Edition |
|---|---|---|
| serverless | choose environment version 1 to 6 for Python and libraries; Spark is always the latest | yes, and the only type |
| classic cluster | choose a Databricks Runtime, for example 18 LTS has Spark 4.1.0 and is supported for 3 years | no |
| SQL warehouse | SQL only, Auto Stop after 10 minutes by default | 1, size 2X-Small |

- Serverless has no Spark UI, RDD, `CACHE TABLE` or global temp view. Code goes through Spark Connect.
- ANSI mode is on by default, so a wrong cast raises an error. Use `try_cast` to get `NULL`.
- A notebook on serverless times out after 2.5 hours.

### 3.9 Accessing the workspace from your own machine

```text
databricks auth login --host https://<workspace-url>
databricks auth profiles -o text
databricks current-user me -p <profile-name>
```

- `~/.databrickscfg` holds only the host and the profile name. The OAuth token lives in the Keychain on macOS, lasts 1 hour and refreshes itself.
- If the shell still has the `DATABRICKS_TOKEN` variable, that variable overrides the profile.
- Databricks SDK for Python uses `WorkspaceClient()`, like boto3. Databricks Connect lets code run on your laptop while Spark runs on serverless.

### 3.10 Cost

- The DBU is the billing unit, counted per second. See the details in the `system.billing.usage` table.
- Free Edition costs nothing. When you go over the quota, compute stops until the end of the day and data is not deleted.

> **Biggest pitfall:** treating the workspace as a personal dev machine. Free Edition has no SLA, and an inactive account may be deleted. Code must live in Git and logic must live in `src/`.

Further reading: Module 0 of the roadmap. Docs: [concepts](https://docs.databricks.com/aws/en/getting-started/concepts), [high-level architecture](https://docs.databricks.com/aws/en/getting-started/high-level-architecture), [navigate the workspace](https://docs.databricks.com/aws/en/workspace/navigate-workspace), [notebooks](https://docs.databricks.com/aws/en/notebooks/notebooks-code), [Git folders](https://docs.databricks.com/aws/en/repos/), [CLI](https://docs.databricks.com/aws/en/dev-tools/cli/), [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).

---

## 4. Q&A: PySpark and SQL editor, metastore, three-level names

> **Question 1:** Why does the workspace section not mention PySpark? Spark is an engine with a driver and workers for processing big data, and it has no storage, so it is not a database. So what can the SQL editor in Databricks do?
>
> **Question 2:** Is the metastore split out in order to assign privileges?
>
> **Question 3:** Is the three-level table name related to splitting workspaces by the three layers of the medallion architecture?

Summary:

1. Your understanding of Spark above is right. But the SQL editor **also runs on Spark**, so big data does not force you to write PySpark.
2. The metastore is only partly about privileges. It holds metadata and privileges, but the reason it is separate from the workspace is so that **many workspaces share it**. Privilege isolation is done at the catalog level.
3. Not directly related. The workspace is not in the table name. Usually a catalog maps to an environment, and a schema maps to a medallion layer.

### 4.1 Where PySpark sits and what the SQL editor can do

PySpark and SQL are 2 languages that go into the same engine:

| entry point | language | compute | engine underneath |
|---|---|---|---|
| notebooks, `.py` files, Lakeflow Jobs, Lakeflow pipelines, Databricks Connect | PySpark and SQL | serverless or classic | Spark through Spark Connect, Photon |
| SQL editor, dashboards, BI tools connected over JDBC | SQL only | SQL warehouse | Spark SQL and Photon |

- Both kinds of compute have a driver and workers. On serverless and SQL warehouses, Databricks manages the nodes, so you do not see them. There is no Spark UI. The query profile takes its place.
- A SQL warehouse has a "cluster size"; Free Edition gives a "2X-Small cluster size". The warehouse adds clusters by itself when many queries have to wait.
- Photon is always on for serverless and SQL warehouses. Photon is a C++ engine that replaces the execution part of Spark; Catalyst still builds the plan.
- `spark.sql("SELECT ...")` and `df.groupBy(...)` are translated by Catalyst into the same kind of plan. Choosing either language does not change how much data you can process.

The SQL editor plus a SQL warehouse can:

- Run full DDL and DML: `CREATE TABLE`, `MERGE`, `QUALIFY`, recursive CTEs, SQL scripting. You can build Bronze, Silver and Gold entirely in SQL; Module 1 does exactly this.
- Use `:name` parameters, save queries, and view query history and the query profile. The query profile shows the operator DAG, time, memory and the percentage of data skipped through pruning.
- Serve as the source for dashboards and alerts.
- Serve many people at the same time. A serverless SQL warehouse starts in 2 to 6 seconds.

| use a SQL warehouse | use a PySpark notebook |
|---|---|
| analytical queries, BI, dashboards | complex logic: functions, loops, Python libraries |
| build Gold tables in SQL | ingestion with Auto Loader, streaming |
| many people querying at once | code that needs testing with pytest, machine learning |

### 4.2 Is the metastore for privileges?

The docs say the metastore "registers metadata about data, AI, and permissions".

- The metastore is at account level, one per region, and every workspace in the same region shares it.
- The reason it is separate from the workspace: a table and its privileges need to be declared only once, and every workspace attached to the metastore sees them the same way.
- Isolation is not done by creating more metastores. The docs advise: "Use your single-region metastore for all software development lifecycle scopes and business units." To isolate, split catalogs and bind each catalog to workspaces.
- Not verified in the docs: before Unity Catalog, each workspace had its own Hive metastore and table ACLs, so the same table had to be declared and granted again in each workspace.

### 4.3 Does the three-level name map to medallion or to the workspace?

Not directly. The three-level name is just 3 levels of nested containers. Medallion is one way you choose to use the schema level.

| name level | commonly used for | Olist example |
|---|---|---|
| catalog | environment, team or business unit | `dev_olist`, `prod_olist` |
| schema | medallion layer or domain | `bronze`, `silver`, `gold` |
| table | one table | `fact_orders` |

Full name: `prod_olist.gold.fact_orders`.

- A workspace maps to an environment, not to a medallion layer. The docs advise splitting Dev and Prod workspaces, then binding catalogs, so that the prod workspace sees only `prod_olist`.
- Free Edition has only 1 workspace, so the roadmap uses `workspace.bronze.*`, `workspace.silver.*`, `workspace.gold.*`. Whether Free Edition allows `CREATE SCHEMA` has not been tried; Lab 0.6 of the roadmap will check.

Docs: [SQL warehouse types](https://docs.databricks.com/aws/en/compute/sql-warehouse/warehouse-types), [Photon](https://docs.databricks.com/aws/en/compute/photon), [Unity Catalog best practices](https://docs.databricks.com/aws/en/data-governance/unity-catalog/best-practices), [workspace strategy](https://docs.databricks.com/aws/en/lakehouse-architecture/deployment-guide/workspace-strategy).

---

## 5. Unity Catalog

> **Question:** Explain Unity Catalog using the same framework.

Unity Catalog (UC) is the data governance layer of Databricks. It puts 3 jobs in one place: knowing where data lives, who may read or write what, down to each row and column, and who did what. UC also holds the S3 keys on behalf of users, so nobody can read a table's files while skipping the privilege check.

### 5.1 What it contains

```text
Metastore            account level, 1 per region
  Catalog
    Schema
      table, view, materialized view, volume, function, model
  storage credential, external location, connection, share    (alongside catalogs)
```

| part | what it does |
|---|---|
| catalog of objects: catalog, schema, object | which table lives where, which schema it has |
| privileges: `GRANT`, owner, inheritance | who may do what on which object |
| fine-grained access control: row filter, column mask, ABAC | who sees which rows and which columns |
| storage access: storage credential, external location, volume | UC holds the S3 keys, users do not |
| lineage | which columns this column is computed from |
| audit through `system.access.audit` | who read or wrote what, and when |
| data classification | automatically tags columns that hold personal data, such as `class.email_address` |

### 5.2 Why it is needed

Compared with a self-built Iceberg stack, where Spark and the REST catalog share one admin access key of the object store:

| self-built stack | Unity Catalog |
|---|---|
| the REST catalog holds only metadata; the `iceberg-rest-fixture` build is for testing, and whether it has access control is not verified | catalog and privileges live in one place |
| Spark holds the admin key, so it can read and delete any file and bypass the catalog | UC holds the credential; managed tables block path-based reads |
| to mask a personal data column you must create a copy table or a separate view | column mask, row filter, or one ABAC rule by tag applied to many tables |
| to know where a Gold table comes from you must read code | automatic lineage, down to the column |
| no way to know who read the customer table | `system.access.audit` |

Docs on managed tables: "Path-based access to Unity Catalog managed tables isn't supported because it bypasses Unity Catalog access controls."

The real problem in Module 6 of the roadmap: after running `DELETE` on one customer, `SELECT` returns 0 rows, but time travel to version 0 still returns 17 rows, and reading the Parquet files directly also returns 17 rows. Anyone who can read the table folder can read that customer. UC blocks the path-based read route. The actual deletion still has to be done by you, see section 5.8.

### 5.3 Securable objects and names

- "Every asset you govern in Unity Catalog is modeled as a securable object." Everything you can grant privileges on is a securable object.
- Table name: `catalog.schema.object`. Volume path: `/Volumes/<catalog>/<schema>/<volume>/`.
- Every user has `USE CATALOG` on the workspace catalog. On the `default` schema, users also have `USE SCHEMA`, `CREATE TABLE`, `CREATE VOLUME`, `CREATE FUNCTION`. The default owner is the workspace admin.

### 5.4 Managed tables and external tables

| | managed | external |
|---|---|---|
| files live in | storage managed by UC | a path you specify, through an external location |
| on `DROP TABLE` | `UNDROP` works for 7 days, after that UC deletes the files | you must delete the files yourself |
| what UC takes care of | governance and the file lifecycle | governance only ("handles governance only") |
| docs recommendation | the default, "recommended" for Delta and Iceberg | use when the data must stay where it already is |

### 5.5 Storage: credential, external location, volume

- **Storage credential:** on AWS, an IAM role that reads S3.
- **External location:** an S3 path plus a credential. The privileges are `READ FILES` and `WRITE FILES`. `WRITE FILES` also needs `READ FILES`, otherwise you get `PERMISSION_DENIED`.
- **Volume:** a folder of files inside UC, with the privileges `READ VOLUME` and `WRITE VOLUME`. The docs recommend that end users work through volumes, without being granted privileges on the external location.
- Docs: "The only identity that should have access to data managed by Unity Catalog is the identity used by Unity Catalog."
- Free Edition does not let you set your own storage ("Custom workspace storage locations"), so this part is theory only.

### 5.6 Privileges

- A **principal** is a user, group or service principal. A service principal is an identity for machines, written as `applicationId`.
- **Reading 1 table needs all 3 privileges:** `USE CATALOG`, `USE SCHEMA` and `SELECT`. A `USE` privilege only lets you pass through the container and does not let you read anything inside.
- **Inheritance:** granting at the catalog or schema level applies to child objects, both existing ones and ones created later.
- `MODIFY` allows insert, update and delete, but still needs `SELECT` and the 2 `USE` privileges.
- The **owner** has every privilege and can grant them to others. **`MANAGE`** allows managing privileges, renaming and dropping, but does not by itself give the privilege to read data.
- **`BROWSE`** allows viewing metadata without `USE CATALOG`.

```sql
GRANT USE CATALOG ON CATALOG workspace TO `analysts`;
GRANT USE SCHEMA ON SCHEMA workspace.gold TO `analysts`;
GRANT SELECT ON TABLE workspace.gold.fact_orders TO `analysts`;
SHOW GRANTS ON TABLE workspace.gold.fact_orders;
```

Best practice from the docs: grant privileges through groups, not directly to users. Jobs run as a service principal. Only the service principal has `MODIFY` on production tables. The owner of the production catalog and schema is a group.

Pitfalls:

- `REVOKE` does not raise an error when the privilege was never granted.
- `ALL PRIVILEGES` does not include `MANAGE`.
- `SHOW GRANTS` shows only `ALL PRIVILEGES`, it does not list the individual privileges inside it.

### 5.7 Fine-grained access control

| method | attached to | use when |
|---|---|---|
| dynamic view | a new view | masking a column with `CASE WHEN is_account_group_member('auditors') THEN zip ELSE 'REDACTED' END` |
| row filter | table | each group sees only its own rows, for example `region = 'US'` |
| column mask | column | masking social security numbers, emails |
| ABAC and governed tag | catalog or schema, by tag | one rule masks every column with the tag `pii`; the docs recommend this as the main approach |

- Use `is_account_group_member()`, not `is_member()`, because the latter does not check groups at account level.
- Since ABAC became GA on 2026-04-28, row filters and column masks are evaluated for the user running the query.
- Pitfall: a table with a row filter or column mask loses time travel and clone. You cannot attach a row filter or column mask to a view.

### 5.8 Lineage, audit and deleting personal data

- **Lineage** is recorded automatically down to the column level, and you can see it in `system.access.table_lineage` and `system.access.column_lineage`, kept for 395 days. If you rename a table or column, the old lineage is gone.
- **Audit** is in `system.access.audit`, currently in Public Preview. Filter by `event_date`.
- **Deleting personal data** takes 3 steps: `DELETE`, then `REORG TABLE ... APPLY (PURGE)`, then `VACUUM`. The docs recommend deleting in Bronze first, then Silver and Gold.

```sql
SELECT source_table_full_name, target_table_full_name, event_time
FROM system.access.table_lineage
WHERE target_table_full_name = 'workspace.gold.fact_orders'
ORDER BY event_time DESC;
```

### 5.9 Free Edition

| yes | no | unknown, Lab 6.0 will check |
|---|---|---|
| UC is on by default, catalog `workspace`, you can create schemas, tables, volumes, functions | account console, SCIM, SSO, own storage | groups, service principals, row filters, column masks, ABAC, lineage, system tables |

> **Biggest pitfall:** `DELETE` reporting success does not mean the data is gone. Delta keeps the old files and old versions until you run `REORG ... APPLY (PURGE)` and `VACUUM`.

Further reading: Module 6 of the roadmap. Docs: [Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/), [securable objects](https://docs.databricks.com/aws/en/data-governance/unity-catalog/securable-objects), [managed tables](https://docs.databricks.com/aws/en/tables/managed), [privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges), [best practices](https://docs.databricks.com/aws/en/data-governance/unity-catalog/best-practices), [system tables](https://docs.databricks.com/aws/en/admin/system-tables/).

---

## 6. Delta Lake

> **Question:** Explain Delta Lake using the same framework.

Delta Lake is a table format, the same kind as Iceberg: Parquet files plus a log folder `_delta_log` that records which files belong to which version. Thanks to that log, a folder of Parquet files gets ACID, `UPDATE`, `DELETE`, `MERGE`, time travel and schema checks. Delta is the default format on Databricks.

### 6.1 What it contains

```text
orders/                                       table folder
  part-00000-<uuid>.parquet                   data
  part-00001-<uuid>.parquet
  deletion_vector_<uuid>.bin                  marks deleted rows
  _delta_log/
    00000000000000000000.json                 commit version 0
    00000000000000000001.json                 commit version 1
    00000000000000000010.checkpoint.parquet   merged state for fast reads
    _last_checkpoint                          points to the latest checkpoint
```

| part | what it does | command |
|---|---|---|
| transaction log | records each commit: which files were added or removed, schema, protocol | `DESCRIBE HISTORY` |
| time travel | read an old version, go back to an old version | `VERSION AS OF`, `RESTORE` |
| DML | change and delete rows | `UPDATE`, `DELETE`, `MERGE` |
| maintenance | compact small files, clean up old files | `OPTIMIZE`, `VACUUM` |
| data layout | arrange data so that files that are not needed can be skipped | `CLUSTER BY` |
| Change Data Feed | get the changes between 2 versions | `table_changes()` |

### 6.2 Why it is needed

Lesson 1 of Module 5 is titled "A folder of Parquet files is not yet a table".

| bare Parquet folder | Delta |
|---|---|
| if a job dies midway, readers see half-written files, because Spark lists the folder and reads every `part-*` file | reads only the files the log mentions; a commit is atomic |
| 2 jobs writing at the same time overwrite each other | only 1 job can create `N.json`, the other must check for conflicts and retry |
| changing 1 row means rewriting a whole file or partition yourself | has `UPDATE`, `DELETE`, `MERGE`; a deletion vector only marks rows |
| a write with the wrong schema still goes through | schema enforcement blocks it |
| no history | time travel, `DESCRIBE HISTORY`, `RESTORE` |
| listing an S3 folder is slow when there are many files | the file list and the min and max statistics are already in the log, so fewer files are read |

Iceberg solves exactly these problems. On Databricks, Delta is the native format: liquid clustering, predictive optimization and deletion vectors are built around it. Iceberg can still be used; managed Iceberg tables have been GA since May 2026.

Compared with Iceberg:

| Iceberg | Delta | what differs |
|---|---|---|
| `metadata.json` and a catalog pointing to it | `_delta_log` | Delta can commit without a catalog |
| snapshot | version | Iceberg records a picture of the whole table through manifests, Delta records a diff at the file level |
| `expire_snapshots` and removing orphan files | `VACUUM` plus `logRetentionDuration` | |
| compaction | `OPTIMIZE` | |
| hidden partitioning | liquid clustering | different mechanisms |
| position delete | deletion vector | |
| schema evolution by column ID | column mapping in `id` mode | Delta has it only after you enable it |
| branch, tag, WAP | not available | |
| commit CAS through the REST catalog | put-if-absent on `N.json`, or catalog commits | |

### 6.3 Transaction log

- Each commit is a file `N.json` in `_delta_log`; the version number has 20 digits.
- A commit contains these actions: `add` and `remove` for files, `metaData` for schema and properties, `protocol`, `commitInfo` which records who did it, when and which operation, plus `cdc` and `txn`.
- A commit is a file-level diff, not a picture of the whole table. To read the table, the reader takes the nearest checkpoint through `_last_checkpoint`, then replays the JSON files after it.
- The log only points to Parquet files and does not copy data like `.git/objects`. Deleting a file that the log still points to breaks every version that needs that file.

You cannot open the `_delta_log` of a managed table on Databricks by path. To see a log, write a path-based Delta table into a volume (not run yet):

```python
path = "/Volumes/workspace/default/lab/orders_delta"
spark.read.table("workspace.default.olist_orders").write.format("delta").mode("overwrite").save(path)
spark.read.json(path + "/_delta_log/00000000000000000000.json").select("add.path", "commitInfo.operation").show(truncate=False)
```

### 6.4 ACID and concurrent writes

- **Optimistic commit:** a job first writes the new Parquet files, then creates `N.json`, and it can create it only if that file does not exist yet. If it loses, it reads the new commits and checks whether they touch data it has read. If they do not, it retries on its own at `N+1`; if they do, it throws an exception.
- Databricks docs: "there are no locks on reading or writing against a table, and deadlock is not a possibility". Writes use write-serializable isolation, reads use snapshot isolation.
- The isolation levels are `WriteSerializable` (the default) and `Serializable`, set with `delta.isolationLevel`.
- **Row-level concurrency** turns on automatically when the table is not partitioned and has deletion vectors: 2 `UPDATE` jobs conflict only when they change the same row.
- Common exceptions: `ConcurrentAppendException`, `ConcurrentDeleteReadException`, `MetadataChangedException`.
- With open source Delta on S3: S3 does not guarantee that only one party can create a file, so by default only one Spark driver may write. Several clusters writing at once need `S3DynamoDBLogStore`. On Databricks you do not need to worry about this.
- **Catalog commits** became GA in May 2026: Unity Catalog replaces the filesystem as the arbiter of commits. This enables multi-table transactions with `BEGIN ATOMIC ... END;`.

### 6.5 Protocol and table features

- Each table has `minReaderVersion` and `minWriterVersion`. New features such as deletion vectors or liquid clustering require reader 3 and writer 7.
- Docs: "Most protocol version upgrades are irreversible."
- Pitfall: enabling a feature can make outside clients such as DuckDB, delta-rs or an older open source Spark unable to read the table. Check `tableFeatures` in `DESCRIBE DETAIL` before you enable it.

### 6.6 Time travel and retention

```sql
DESCRIBE HISTORY workspace.default.olist_orders LIMIT 5;
SELECT count(*) FROM workspace.default.olist_orders VERSION AS OF 0;
RESTORE TABLE workspace.default.olist_orders TO VERSION AS OF 0;
DESCRIBE DETAIL workspace.default.olist_orders;
```

| property | default | what it keeps |
|---|---|---|
| `delta.logRetentionDuration` | 30 days | the commit JSON files |
| `delta.deletedFileRetentionDuration` | 7 days | data files that were `remove`d; `VACUUM` does not delete files that are not yet past this limit |

- `RESTORE` creates a new version and does not delete history. Downstream jobs that read the table may receive duplicate data again.
- From Databricks Runtime 18.0, you cannot time travel to a version older than `deletedFileRetentionDuration`.

### 6.7 Deletion vectors, `OPTIMIZE`, `VACUUM`

| command | what it does | note |
|---|---|---|
| `DELETE`, `UPDATE` with deletion vectors | marks rows in a `.bin` file, does not rewrite Parquet | a client that does not support deletion vectors cannot read the table |
| `OPTIMIZE` | compacts small files into large files | idempotent, does not block readers |
| `REORG TABLE t APPLY (PURGE)` | rewrites files to remove the marked rows for good | needed when deleting personal data |
| `VACUUM` | physically deletes files that no version within the retention period still needs | keeps 7 days by default; use `DRY RUN` to preview |

### 6.8 Schema

- **Enforcement:** the columns you write must exist in the table and the types must match, otherwise an error such as `UNRESOLVED_COLUMN.WITH_SUGGESTION` is raised.
- **Evolution:** `ALTER TABLE ... ADD COLUMNS`, the option `mergeSchema`, `MERGE WITH SCHEMA EVOLUTION`. `overwriteSchema` replaces the whole schema on overwrite.
- **Column mapping** must be enabled before you can use `RENAME COLUMN` and `DROP COLUMN`. `DROP COLUMN` only removes the column from the metadata, and the data stays in the files.
- **Type widening**, from Databricks Runtime 15.4, lets you widen a column type, for example from int to bigint.

### 6.9 Layout and automatic maintenance

- **Liquid clustering** is "recommended for all new tables". Up to 4 columns, and it cannot be combined with partitioning or `ZORDER`. `CLUSTER BY AUTO` lets Databricks choose the columns.
- **Data skipping:** the log keeps the min and max of the first 32 columns, so queries that filter on these columns skip files that do not match.
- **Predictive optimization** automatically runs `OPTIMIZE`, `VACUUM` and `ANALYZE` for managed tables. It is on by default and billed under the serverless jobs SKU.

```sql
CREATE TABLE workspace.default.orders_lc CLUSTER BY (order_purchase_timestamp)
AS SELECT * FROM workspace.default.olist_orders;
```

### 6.10 Change Data Feed

- Turn it on with `delta.enableChangeDataFeed = true`, read it with `table_changes('t', <version>)`.
- The `_change_type` column has 4 values: `insert`, `update_preimage`, `update_postimage`, `delete`. It comes with `_commit_version` and `_commit_timestamp`.
- Only changes made after you turn it on are recorded. `VACUUM` also deletes change data files.

### 6.11 Free Edition

| yes | no | unknown |
|---|---|---|
| time travel, `OPTIMIZE`, `VACUUM`, deletion vectors, liquid clustering run manually, the legacy version of Change Data Feed | opening the `_delta_log` of a managed table, `VACUUM ... RETAIN 0 HOURS` | predictive optimization, `CLUSTER BY AUTO` |

> **Biggest pitfall:** treating table history like Git, as if it stays forever and can be used as a backup. The Module 5 lab deletes 97,242.96 BRL; `VERSION AS OF 0` can still read it back, but after `VACUUM ... RETAIN 0 HOURS` it reports `FAILED_READ_FILE.FILE_NOT_EXIST`. Docs: "Don't use table history as a long-term backup solution for data archival."

Further reading: Module 5 of the roadmap. Docs: [Delta Lake](https://docs.databricks.com/aws/en/delta/), [history](https://docs.databricks.com/aws/en/delta/history), [ACID](https://docs.databricks.com/aws/en/lakehouse/acid), [feature compatibility](https://docs.databricks.com/aws/en/delta/feature-compatibility), [VACUUM](https://docs.databricks.com/aws/en/delta/vacuum), [liquid clustering](https://docs.databricks.com/aws/en/delta/clustering), [Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md).

---

## 7. Compute and Spark

> **Question:** Explain Compute and Spark on Databricks using the same framework.

Compute is the machines that run Spark in the workspace. There are 3 types: serverless, where Databricks takes care of the machines; classic, where you configure the cluster yourself; and SQL warehouse, which runs SQL only. Inside, it is still Spark with a driver and executors. Databricks adds 3 things: Photon (a C++ engine), Spark Connect (the notebook only sends the plan) and a managed runtime.

### 7.1 What it contains

| type | what it runs | who manages the machines | Free Edition |
|---|---|---|---|
| serverless | notebooks, jobs, pipelines, both PySpark and SQL | Databricks | yes |
| classic all-purpose | dev notebooks, shared, long-running | you configure it | no |
| classic jobs compute | 1 job run, then deleted | you configure it | no |
| SQL warehouse | SQL only: SQL editor, dashboards, BI | Databricks or you | 1, size 2X-Small |

```text
Notebook or IDE     client: builds the plan, sends it through Spark Connect (gRPC)
Driver              Catalyst builds the plan, AQE adjusts it at run time, splits it into jobs, stages, tasks
Executors           run tasks in parallel; Photon replaces the JVM execution part
Storage             reads and writes Delta on S3, through Unity Catalog
```

### 7.2 Why it is needed

| running Spark yourself | Databricks compute |
|---|---|
| you install Java, Spark and Delta and pin the versions yourself | Databricks Runtime pins them; serverless upgrades itself |
| machines run all the time, or you switch them on and off yourself | serverless provides machines within seconds and bills by usage |
| every job shares 1 cluster | a separate jobs compute for each run |
| many queries at once means queuing | a SQL warehouse adds clusters by itself when queries have to wait |
| JVM engine | Photon (C++) for scan, join, aggregate and Delta writes |
| the cluster holds the S3 key, so whoever runs on it can read | access mode Standard isolates each user; data is read through UC |

In return, serverless hides some things: no Spark UI, no RDD, only 6 configs can be set, no Scala and R, and the runtime may upgrade itself between 2 runs.

### 7.3 Lazy evaluation, job, stage, task

- A **transformation** such as `where`, `select`, `join` or `groupBy` only appends a step to the plan. An **action** such as `count`, `show`, `collect` or writing a table is what makes Spark run.
- One action produces 1 or more **jobs**. A job is cut into **stages** at each shuffle. Each partition is 1 **task**.
- Each action reruns the plan from the source. Docs: "In production data pipelines, writing data is typically the only action that should be present."
- When `display(df)` runs for 40 seconds, most of that time usually belongs to every transformation before it.

### 7.4 Spark Connect: the notebook only sends the plan

Serverless and Databricks Connect use Spark Connect. The client translates the DataFrame into an unresolved plan, sends it over gRPC and receives the results back as Arrow. The client has no JVM, so there is no `sparkContext` and no RDD.

It differs from Spark Classic on a laptop in 3 ways:

- A mistyped column name raises an error only at an action or when you call `df.columns`.
- A DataFrame holds only the name of a temp view. If you recreate a view with the same name, the old DataFrame reads the new data.
- A UDF reads the value of a variable at run time, not at definition time:

```python
x = 123
@udf("INT")
def foo():
    return x
df = spark.range(1).select(foo())
x = 456
df.show()  # Spark Connect prints 456; Spark Classic prints 123
```

### 7.5 Limits of serverless

| area | limit |
|---|---|
| Spark UI | not available, use the query profile instead |
| RDD, `sparkContext` | not available |
| cache | `df.cache()` works; `CACHE TABLE` and `df.checkpoint()` raise errors |
| libraries | no Maven, no init scripts; add libraries through the Environment side panel |
| UDF | up to 1 GB, no access to the internet |
| languages | no R; notebooks have no Scala |
| streaming | only `Trigger.AvailableNow()` |
| timeout | notebook 2.5 hours; job up to 7 days |

- The 6 configs you can set: `spark.sql.shuffle.partitions`, `spark.sql.ansi.enabled`, `spark.sql.session.timeZone`, `spark.sql.files.maxPartitionBytes`, `spark.sql.legacy.timeParserPolicy`, `spark.databricks.execution.timeout`. There is no `spark.sql.adaptive.enabled`, so AQE is always on.
- Pitfall: do not install PySpark with `%pip` on serverless.

### 7.6 Classic compute

- **Access mode Standard**, old name Shared, lets many users share a cluster and isolates them with Lakeguard; it has no RDD and no R. **Access mode Dedicated**, old name Single user, is for 1 person or 1 group; it has RDD, R and the ML runtime.
- **Databricks Runtime** pins Spark, Python and Delta. For example, 18 LTS has Spark 4.1.0 and is supported until 2029-06-10. The docs recommend the latest version for all-purpose compute and an LTS version for production jobs.
- A **compute policy** limits the configurations that other people may create, like an AWS SCP.

### 7.7 Photon

- Docs: "the Databricks-native vectorized query engine", written in C++. Catalyst still builds the plan, and Photon replaces the execution part. An operation that Photon does not support falls back to Spark on the JVM.
- Always on for serverless and SQL warehouses.
- It does not support UDFs, RDDs, the Dataset API and stateful streaming. It does not speed up queries that already run in under 2 seconds.
- Look at the nodes in the query profile to see which parts ran on Photon.

### 7.8 ANSI mode

ANSI mode is on by default in Spark 4 and on serverless. Spark 3 code that relies on a bad cast returning `NULL` will raise an error when you move it here.

```sql
SELECT CAST('a' AS INT);      -- error: cannot be cast to "INT" because it is malformed
SELECT try_cast('a' AS INT);  -- NULL
SELECT try_divide(1, 0);      -- NULL
```

### 7.9 Shuffle, join, AQE and skew

| config | default |
|---|---|
| `spark.sql.shuffle.partitions` | 200; set `auto` to let Databricks choose |
| `spark.databricks.adaptive.autoBroadcastJoinThreshold` | 30MB: a join side smaller than this threshold is broadcast |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64MB: the partition size that AQE aims for |
| skew threshold | a partition larger than 5 times the median and larger than 256MB |

- **AQE** adjusts the plan at run time: it switches a sort merge join to a broadcast join, merges small partitions and splits skewed partitions.
- Pitfall: AQE handles skew only in joins, not skew in `groupBy`. For a skewed `groupBy`, use salting:

```python
N = 16
salted = (df.withColumn("salt", (F.rand() * N).cast("int"))
            .groupBy("seller_id", "salt").agg(F.sum("price").alias("p"))
            .groupBy("seller_id").agg(F.sum("p").alias("revenue")))
```

- Broadcast explicitly with `df.join(F.broadcast(small_df), "key")` or the hint `/*+ BROADCAST(t1) */`.

### 7.10 Reading plans

| tool | where | Free Edition |
|---|---|---|
| `EXPLAIN FORMATTED` | everywhere, does not run the query | yes |
| query profile | the See performance button under a cell, Query History | yes |
| Spark UI | classic compute only, on a laptop at port `4040` | no |

A node `Exchange hashpartitioning(...)` in a plan is one shuffle.

### 7.11 UDFs

- The docs give a priority order: built-in functions, then higher-order functions, then pandas UDFs, and only last regular Python UDFs.
- A Python UDF must serialize data out of the JVM, so that part of the plan cannot run on Photon. A pandas UDF sends data in batches over Arrow.
- From Spark 4.2 and Databricks Runtime 19, regular Python UDFs also use Arrow by default.

### 7.12 Free Edition

| yes | no |
|---|---|
| serverless, 1 SQL warehouse of size 2X-Small, Photon, AQE, query profile | classic compute, Spark UI, RDD, R, Scala, compute policy |

> **Biggest pitfall:** writing Gold with `mode('append')` as if it were a one-time insert from a service. The Module 7 lab shows that when the scheduler reruns the job, revenue goes from 13,494,400.74 BRL to 26,988,801.48 BRL with no error at all. Every job will be rerun because of retries, backfills or repairs, so writes must be idempotent: overwrite or `MERGE`.

Further reading: Module 7 of the roadmap. Docs: [compute](https://docs.databricks.com/aws/en/compute/), [serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations), [Photon](https://docs.databricks.com/aws/en/compute/photon), [AQE](https://docs.databricks.com/aws/en/optimizations/aqe).

---

## 8. Ingestion

> **Question:** Explain Ingestion (Auto Loader, `COPY INTO`, Lakeflow Connect) using the same framework.

Ingestion is the step that brings data from a source (files, databases, SaaS, APIs) into Bronze. The requirements are incremental ingestion, each record exactly once, and reruns that create no duplicates. Databricks splits this into 3 groups: file ingestion (Auto Loader, `read_files`, `COPY INTO`), ready-made connectors (Lakeflow Connect), and applying changes to a table (`MERGE`, AUTO CDC).

### 8.1 What it contains

| tool | used for | written in |
|---|---|---|
| Auto Loader (`cloudFiles`) | new files in S3 or a volume, incremental ingestion | PySpark stream |
| `read_files` and streaming table | like Auto Loader but written in SQL | SQL |
| `COPY INTO` | ingest files with SQL, skips files already ingested | SQL |
| Lakeflow Connect | ready-made connectors for databases and SaaS | UI, API |
| Python Data Source API | write your own source that reads an API, offset kept in the checkpoint | Python |
| `MERGE INTO` | apply changes to a table: upsert, delete | SQL, Python |
| AUTO CDC | declare the key and the ordering, the engine applies the changes, SCD type 1 or 2 | SQL, Python in a pipeline |
| Change Data Feed | read the changes of a Delta table to ingest into the next layer | SQL, Python |

### 8.2 Why it is needed

The real problem in Module 8 of the roadmap: a job that ingests orders from a REST API with a cursor was written like a request handler. It `INSERT`s every record and only then saves the cursor. The job is killed after writing page 40, before it could save the cursor:

```text
Row(dong=396551, cursor_khac=391551, don=99441, dong_shipped=100254)
đơn đang shipped theo CSV: 1107
```

- **A rerun creates duplicates:** page 40 is written twice, adding 5,000 extra versions.
- **A version is not a row:** each order has up to 4 versions, so 100,254 rows carry the status `shipped`, while only 1,107 orders are actually being delivered.
- If you save the cursor first and write afterward, the error flips direction: 5,000 versions are lost and nobody knows.

| hand-written | Databricks |
|---|---|
| you track which files were read yourself; if the job dies you get duplicates or gaps | the Auto Loader checkpoint remembers ingested files, each file exactly once |
| a file with a new column breaks the job, or the column is silently dropped | schema evolution; unknown columns go into `_rescued_data` |
| build Debezium and Kafka yourself to get CDC from a database | Lakeflow Connect has ready-made connectors |
| write upserts yourself and handle out-of-order events yourself | `MERGE` with a version condition, AUTO CDC with `SEQUENCE BY` |
| listing a large S3 folder is very slow | file events: you are notified when a new file arrives |

### 8.3 Auto Loader

- Auto Loader is a Structured Streaming source named `cloudFiles`. It reads JSON, CSV, XML, Parquet, Avro, ORC, text and binary.
- **Checkpoint** stores the list of files seen in RocksDB, so each file is ingested only once. File order is not guaranteed.
- **Trigger:** on serverless only `availableNow` and `Once` are available. `availableNow` processes all files that exist before it starts and then stops, so it runs like a scheduled batch. If you set no trigger, you get the error `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`.

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

The code above is not run yet.

- The checkpoint and the schema location must not be inside the table's folder. Changing `checkpointLocation` means starting a new stream, and Auto Loader forgets every file it already ingested.
- **File discovery:** the default is directory listing. The docs recommend file events (`cloudFiles.useManagedFileEvents = true`) for most workloads.
- Pitfall: for files that are overwritten or appended to, "Auto Loader cannot guarantee which file version will be processed". Ingest only immutable files.

### 8.4 Schema of new files

- Set `cloudFiles.schemaLocation` to turn on schema inference and schema evolution. Auto Loader samples the first 50 GB or the first 1,000 files.
- JSON, CSV and XML are all inferred as `string`, unless you set `cloudFiles.inferColumnTypes = true`.
- Pitfall: `read_files` defaults to `inferColumnTypes = true`, while `cloudFiles` defaults to `false`. The same folder can produce different column types.

| mode | when a new column appears |
|---|---|
| `addNewColumns`, the default when you give no schema | the stream fails with `UnknownFieldException`; after a rerun it picks up the new column |
| `rescue` | the schema stays unchanged, no failure, the new column goes into `_rescued_data` |
| `failOnNewColumns` | fails and does not rerun by itself until you fix the schema |
| `none`, the default when you give a schema | ignores the new column |

`_rescued_data` is a JSON that holds unknown columns, columns with the wrong type, and the source file path. It works like a dead-letter queue kept right inside the table, and you can query it with SQL.

### 8.5 Ingesting files with SQL

```sql
CREATE OR REFRESH STREAMING TABLE workspace.bronze.raw_orders
SCHEDULE EVERY 1 HOUR
AS SELECT * FROM STREAM read_files(
  '/Volumes/workspace/bronze/landing/orders',
  format => 'json',
  schemaHints => 'order_id string, version long');
```

- Without `STREAM`, `read_files` reads as a batch. With `STREAM` inside a streaming table, Auto Loader runs underneath.
- `COPY INTO` is "retryable and idempotent" and skips files already ingested. The option `'force' = 'true'` turns off idempotency. The docs recommend that SQL users move to streaming tables.

### 8.6 Lakeflow Connect

| connector | status |
|---|---|
| SQL Server | GA since 2025-09-25 |
| PostgreSQL | Public Preview, logical replication through `pgoutput` |
| MySQL integrated CDC pipeline | GA on 2026-10-15, needs `binlog_format=ROW` |
| query-based: Oracle, MySQL, PostgreSQL and some other databases | GA on 2026-05-29, uses a cursor column |

- **CDC connectors** have 2 parts. The ingestion gateway runs continuously on classic compute to read the log before the source truncates it, and writes it temporarily to a volume for 30 days. The ingestion pipeline runs serverless and writes into a streaming table.
- **Query-based connectors** fetch the rows whose cursor is greater than the previous high-water mark, and re-read a short range just below that mark to catch late-arriving rows.
- There are 73 SaaS connectors. The free tier for managed connectors is 100 DBU per day.
- Pitfall: do not stop the gateway yourself, because the source log may be truncated before the gateway reads it. A full refresh erases SCD type 2 history.

### 8.7 Ingesting from a REST API

There is no general REST connector. The docs give 3 patterns:

| pattern | use when |
|---|---|
| 1. a materialized view that calls `requests` | small payload; reads the whole API again on every update |
| 2. Python Data Source API | large API, incremental reads; the offset is kept in the Spark checkpoint |
| 3. a job writes JSON into a volume, Auto Loader reads it | separates API failures from the transform step; Auto Loader remembers ingested files |

- Pattern 3 fits the [lakehouse](https://github.com/DPhO05/lakehouse) repo: the existing client writes raw data to files, and Auto Loader handles the exactly-once-per-file part.
- A workspace cannot call `localhost` on your laptop. Free Edition can only reach "a limited set of trusted domains". So the lab runs the client on the laptop and then uploads the files to a volume with `databricks fs cp`.

### 8.8 Idempotent `MERGE INTO`

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

The code above is not run yet.

- Deduplicate the source first with `QUALIFY`. If several source rows match one target row, you get the error `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`.
- Update only when the source version is newer, so a late-arriving old version does not overwrite a newer one.
- Clauses are evaluated in the order they are written, so `DELETE` must come before `UPDATE`.
- Pitfall: `WHEN NOT MATCHED BY SOURCE ... DELETE` without a condition deletes every target row that is not in the batch.
- `foreachBatch` only guarantees at-least-once. For an idempotent write, set `txnAppId` and set `txnVersion` to `batchId`.

### 8.9 AUTO CDC

AUTO CDC is the new name of `APPLY CHANGES`, with the same syntax.

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

- You declare `KEYS` (the key) and `SEQUENCE BY` (the ordering), and the engine writes the `MERGE` part itself. A late event whose sequence is lower than the sequence already applied is dropped.
- The default is SCD type 1. SCD type 2 adds the columns `__START_AT` and `__END_AT`; the current row has `__END_AT` as null.
- AUTO CDC runs only in Lakeflow pipelines. It is not in the open source Spark Declarative Pipelines.
- When the source has only a full snapshot and no CDC, use `AUTO CDC ... FROM SNAPSHOT` (Beta). The engine compares 2 snapshots to infer inserts, updates and deletes.

### 8.10 Free Edition

- Yes, inferred from the docs: Auto Loader with `availableNow` from a volume, `read_files`, `COPY INTO`, `MERGE`, AUTO CDC, the legacy version of Change Data Feed.
- No: connectors that need a gateway, because the gateway must run on classic compute; calling `localhost` on your laptop.
- Unknown: file events, SaaS connectors, calling external APIs.

> **Biggest pitfall:** ingesting like a request handler, that is, `INSERT`ing every record and then remembering the cursor. A rerun creates duplicates, and reversing the save order loses records. Correct ingestion keeps the raw data append-only, each file or version exactly once, and then `MERGE`s by key and version to produce the current state.

Further reading: Module 8 of the roadmap. Docs: [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/), [`COPY INTO`](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into), [`read_files`](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files), [Lakeflow Connect](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/), [ingest from an API](https://docs.databricks.com/aws/en/ldp/api-ingestion), [`MERGE INTO`](https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into), [AUTO CDC](https://docs.databricks.com/aws/en/ldp/cdc).

---

## 9. Lakeflow pipelines and Lakeflow Jobs

> **Question:** Explain Lakeflow pipelines and Lakeflow Jobs using the same framework.

These are 2 things with 2 different roles. Lakeflow pipelines, old name Delta Live Tables (DLT), let you declare which tables are computed from what; the engine handles the ordering, picks up only new data and runs the quality checks. Lakeflow Jobs, old name Workflows, is the orchestrator: it decides when to run and which task runs first, in place of Airflow. A pipeline is usually one task in a job.

### 9.1 What it contains

| part | answers the question | equivalent in a self-built stack |
|---|---|---|
| Lakeflow pipelines | which tables are computed from what | Makefile, dbt |
| Lakeflow Jobs | when to run, which task goes before which | Airflow |

An illustrative example, put together from the task types in the docs:

```text
Lakeflow Job "olist_daily"          trigger: schedule 02:00 or a new file
  task 1  ingest_api      notebook: pull the API, write JSON to a volume
  task 2  olist_pipeline  pipeline: bronze_orders, silver_orders, gold_revenue
  task 3  check           If/else on task value new_rows > 0
  task 4  refresh_dash    dashboard, Run if: All succeeded
```

### 9.2 Why it is needed

| self-built | Lakeflow |
|---|---|
| order the Bronze, Silver, Gold jobs by hand | a pipeline orders them by dependency and runs them in parallel |
| track yourself which files or rows were processed | a streaming table keeps a checkpoint and processes only the new part |
| check quality with a separate script | expectations live in the table definition, metrics go to the event log |
| set up and operate your own Airflow | Jobs is built into the workspace and runs on serverless |
| a long-running sensor that waits for a file or a table | file arrival and table update triggers |
| rerun the whole DAG when 1 task fails | a repair run reruns only the failed task and the tasks after it |

The real problem in Module 9 of the roadmap: the Black Friday night job has step 1, which `INSERT`s order item rows, and step 2, which sends a report. Step 2 times out, and the scheduler retries the whole job:

```text
nguồn: 1366 dòng hàng, tổng price 152653.74
bảng : 2732 dòng hàng, tổng price 305307.48
```

The job reports success and there is no alert at all. Retry, repair and backfill are all reruns, so they are safe only when the tasks are idempotent.

### 9.3 Pipelines: dataset, flow, pipeline

| dataset | type | use when |
|---|---|---|
| streaming table | streaming target, has a checkpoint | append-only data: Bronze, logs, new files |
| materialized view | batch target, matches the state of the source | aggregate tables: cleaned Silver, Gold |
| view (`@dp.temporary_view`) | computed when read, not stored | intermediate step |
| sink | external target: Kafka, Event Hubs | push data out to another system |

- **Flow** reads a source, applies logic and writes to a target. A streaming table can receive several flows, for example 2 sources writing into one table.
- **Pipeline** is the unit for development and running. The engine reads the references between tables to build a graph.
- The new Python API is `from pyspark import pipelines as dp`; `import dlt` still works. `@dp.table` now creates only a streaming table; for a batch table use `@dp.materialized_view`.

Written following the tutorial pattern, not run yet:

```python
from pyspark import pipelines as dp

@dp.table(comment="Bronze: raw orders from the volume")
def bronze_orders():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load("/Volumes/workspace/bronze/landing/orders"))

@dp.materialized_view(comment="Silver: orders that have an id")
@dp.expect_or_drop("has_id", "order_id IS NOT NULL")
def silver_orders():
    return spark.read.table("bronze_orders")
```

### 9.4 Pipelines: how they run

- **Triggered**, the default, updates and then stops. **Continuous** runs all the time and refreshes the tables as soon as data arrives.
- Running from the editor starts fast, reuses the cluster, and turns off retries so you see errors right away. Running from Jobs or the API retries and restarts automatically.
- **Validate update** (Public Preview) does a trial run without writing any table.
- Serverless is the default. The editions are `CORE` for ingest, `PRO` which adds CDC, and `ADVANCED` which adds expectations; `ADVANCED` is the default.
- Pitfall: a full refresh "clears data from streaming tables, clears checkpoints from flows, and reprocesses all records". If the source has already deleted old files, the full refresh loses rows.

### 9.5 Pipelines: expectations

| level | SQL | Python | bad rows |
|---|---|---|---|
| warn, the default | `EXPECT (...)` | `@dp.expect` | still written and counted |
| drop | `ON VIOLATION DROP ROW` | `@dp.expect_or_drop` | dropped and counted |
| fail | `ON VIOLATION FAIL UPDATE` | `@dp.expect_or_fail` | the update fails and rolls back |

- Metrics are in the event log, read with `event_log(<pipelineId>)`, in the field `details:flow_progress.data_quality.expectations` with `passed_records` and `failed_records`.
- Pitfall: the `fail` level records no metrics. To keep the bad rows for review, build a quarantine table yourself. In a continuous pipeline, one failing expectation stops that flow and every flow that depends on it.

### 9.6 Open source Spark Declarative Pipelines

- Included in Apache Spark from 4.1.0. Install with `pip install pyspark[pipelines]`, run with `spark-pipelines run` and `spark-pipelines dry-run`.
- No expectations and no AUTO CDC; these 2 parts exist only on Databricks. The lab on your laptop uses this version to learn the graph and the flows.

### 9.7 Jobs: tasks and dependencies

- **Task types:** Notebook, Python script, Python wheel, SQL, Pipeline, dbt, JAR, Dashboards, Run Job, If/else, For each and some others.
- **Depends on** and **Run if** with 6 conditions: All succeeded (the default), At least one succeeded, None failed, All done, At least one failed, All failed.
- **Retry:** most jobs do not retry by default. If you set both a timeout and a retry, the timeout applies to each attempt.
- Limits: 1,000 tasks per job, 2,000 tasks running at the same time per workspace.

### 9.8 Jobs: passing values between tasks

```python
# Upstream task
dbutils.jobs.taskValues.set(key="new_rows", value=42)

# Downstream task; debugValue is used when you run by hand in a notebook
new_rows = dbutils.jobs.taskValues.get(taskKey="bronze_ingest", key="new_rows", debugValue=0)
```

- The If/else condition: `{{tasks.bronze_ingest.values.new_rows}} > 0`. The docs recommend using the `{{...}}` reference instead of calling `get()`.
- **Job parameters** are pushed down to every task. If the keys clash, the job parameter wins.
- **For each** runs 1 nested task for each element through `{{input}}`. By default it runs with a parallelism of 1, and you cannot nest a For each inside a For each.

### 9.9 Jobs: triggers

| trigger | runs when |
|---|---|
| Scheduled | on a cron schedule |
| File arrival | a new file appears in a volume or external location |
| Table update | the source table changes |
| Continuous | always keeps 1 run active; this description is not verified in the docs |
| Job completion (Beta) | another job finishes |

### 9.10 Jobs: backfill and repair run

- **Backfill:** choose a date range and a step; each step is 1 run, up to 100 runs. A task reads the date through `{{backfill.iso_datetime}}`.
- **Repair run** reruns "only the subset of unsuccessful tasks and any dependent tasks", and can receive new parameters.
- Pitfall: "Lakeflow Jobs doesn't make tasks idempotent". A task that already wrote part of its output and is rerun will write duplicates. A pipeline task cannot run concurrently, so a backfill with a pipeline task runs sequentially.

### 9.11 Comparison with Airflow

| Airflow | Lakeflow Jobs | where it differs |
|---|---|---|
| DAG | job | |
| `>>` and trigger rule | Depends on, Run if | 6 conditions, including running when upstream fails |
| XCom | task values | at most 48 KiB; keep the real data in tables |
| sensor | file arrival trigger, table update trigger | no long-running task needed to wait |
| branching | If/else task | `==` compares as strings, so `12.0 == 12` is false |
| dynamic task mapping | For each | sequential by default, no nested For each |
| backfill | job backfill | up to 100 runs per backfill |
| clear task | repair run | does not make tasks idempotent for you |

- The mapping above follows a Databricks blog post from 2026-03-18 about moving from Airflow to Lakeflow Jobs.
- Airflow can still call a Databricks job with `DatabricksRunNowOperator`. The author's suggestion, not stated directly in the docs: keep Airflow when the DAG orchestrates many systems beyond Databricks, and switch to Jobs when every task lives inside Databricks.

### 9.12 Free Edition

- Pipelines run only on serverless, and there is "One active pipeline per pipeline type".
- Jobs: "Max of 5 concurrent job tasks per account".
- Unknown: expectations (they need the `ADVANCED` edition) and unit tests for pipelines.
- Free Edition has only 1 workspace, so dev and prod must be separated by catalog or schema.

> **Biggest pitfall:** believing that a scheduler retry is safe. Retry, repair and backfill all rerun tasks, and Lakeflow Jobs does not make tasks idempotent for you. Write with a per-day overwrite, use `MERGE`, or let the pipeline manage its own checkpoints.

Further reading: Module 9 of the roadmap. Docs: [Lakeflow pipelines](https://docs.databricks.com/aws/en/ldp/), [concepts](https://docs.databricks.com/aws/en/ldp/concepts), [expectations](https://docs.databricks.com/aws/en/ldp/expectations), [event log](https://docs.databricks.com/aws/en/ldp/monitor-event-logs), [Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/), [Run if](https://docs.databricks.com/aws/en/jobs/run-if), [triggers](https://docs.databricks.com/aws/en/jobs/triggers), [backfill](https://docs.databricks.com/aws/en/jobs/backfill-jobs), [repair run](https://docs.databricks.com/aws/en/jobs/repair-job-failures), [Airflow](https://docs.databricks.com/aws/en/jobs/how-to/use-airflow-with-jobs).

---

## 10. Quality, tests, bundles and environments

> **Question:** Explain quality, tests, bundles and environments using the same framework.

This section groups 4 jobs that keep a pipeline safe when the code changes and when data arrives: testing code with pytest, checking data with expectations and quality gates, deploying from configuration files through Declarative Automation Bundles (old name Databricks Asset Bundles), and separating dev from prod through CI/CD.

### 10.1 What it contains

| part | what it catches | tool |
|---|---|---|
| unit test | wrong code: a changed join type, a wrong formula | pytest on a local SparkSession or Databricks Connect |
| golden dataset | output that drifts from a copy verified by hand | pytest comparing DataFrames |
| expectations | bad rows within a batch of data | Lakeflow pipelines |
| quality gate | a bad batch at the level of the whole batch: duplicates, missing rows, a mismatched total | staging table, check, then publish |
| Declarative Automation Bundles | manual deploys, configuration drift between environments | `databricks bundle` commands |
| CI/CD | merging untested code, deploying from a personal machine | GitHub Actions with OIDC and a service principal |

### 10.2 Why it is needed

The real problem in Module 10 of the roadmap: the order-level mart had 2 errors in 1 week, and both times the job was green.

```text
bản cũ, left join         99441 đơn | huỷ 0.63% | doanh thu 13591643.70 | tháng 11/2017 1010271.37
thứ Hai, inner join       98666 đơn | huỷ 0.47% | doanh thu 13591643.70 | tháng 11/2017 1010271.37
thứ Tư, append lô đó      99441 đơn | huỷ 0.63% | doanh thu 13744297.44 | tháng 11/2017 1162925.11
```

- **Monday**, changing `left join` to `inner join` lost 775 orders that have no order item row at all. Total revenue did not change, so the reconciliation still passed, but the cancellation rate on the dashboard fell from 0.63% to 0.47%.
- **Wednesday**, the source resent the order item file for Black Friday. The pipeline only appends, so 1,366 rows were added a second time, and revenue for November 2017 rose by 152,653.74 BRL.
- The Monday error is in the code, so a test on fixed sample data catches it before the pull request is merged. The Wednesday error is in the data, so only a check that runs on that very batch can catch it.

| done by hand | Databricks |
|---|---|
| a green job is treated as done | tests block at the pull request; expectations and quality gates block a bad batch |
| copy notebooks back and forth between dev and prod | bundle: one definition, many targets |
| deploy from a laptop with a personal token | CI deploys with a service principal through OIDC, no long-lived secret stored |
| Iceberg has branches for write-audit-publish | Delta has no branches; use staging, `FAIL UPDATE` or a multi-statement transaction |

### 10.3 Testing code

- **Unit test:** pull the transform function out into a `.py` module that takes a DataFrame and returns a DataFrame, then test it with pytest using a small fixture. The docs recommend keeping functions and tests outside notebooks.
- **pytest with Databricks Connect** runs the tests on Databricks compute. Databricks Connect and PySpark are not installed in the same venv. Running pytest from a terminal uses only the `DEFAULT` profile in `~/.databrickscfg`.

```python
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession

def get_spark() -> SparkSession:
    return DatabricksSession.builder.getOrCreate()

def test_get_nyctaxi_trips():
    df = get_spark().read.table("samples.nyctaxi.trips")
    assert df.count() > 0
```

- **Golden dataset:** compare the output with a copy verified by hand. Compare without depending on row order, watch the decimal types, and the expected values must not be generated by the code under test.
- **Unit tests for pipelines** are in Beta and run only in the Lakeflow Pipelines Editor. They isolate only by table name, so reads and writes by path such as `/Volumes/...` can touch production data.

### 10.4 Checking data: expectations and the event log

The three expectation levels warn, drop and fail are already covered in section 9.5. Additions:

- The drop level discards bad rows without keeping them. To review the bad rows, build a quarantine table yourself, that is, a table that holds only the rows that violate the rule.
- By default only the run-as user can read the event log. Publish the event log in Advanced settings to read it by table name.
- Query the quality metrics from the event log:

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

Here `event_log_raw` is a view created on the pipeline's event log.

### 10.5 Quality gates instead of branches

The Wednesday error breaks no row-level rule: every resent row is valid, it is just already there. Expectations check row by row, so they do not catch it. The Iceberg version solves this with write-audit-publish on a branch, but Delta has no branches.

The write-audit-publish idea needs 3 properties: readers of the real table never see an unchecked batch, rules can be checked on the whole batch, and moving the batch into the real table is one atomic step. On Databricks there are 4 ways:

| way | what you gain | what you lose |
|---|---|---|
| A. expectation `FAIL UPDATE` in a pipeline | rolls back the whole update, no extra code | row-level rules only; the fail level records no metrics; not available on a laptop |
| B. write to a staging table, check, then publish with `INSERT` or `MERGE` | works the same on a laptop and on a workspace; can check rules on the whole batch; publish is 1 commit | writes the batch twice; you must clean up staging and hide staging from readers yourself |
| C. shallow clone the target table, write into the clone, check, then write back to the real table | checks the exact state of the table after the write | shallow clone on Unity Catalog is in Public Preview; many limits when cloning |
| D. a multi-statement transaction `BEGIN ATOMIC ... END` | closest to write-audit-publish, no staging needed | needs a managed table with catalog commits turned on; not verified on Free Edition |

### 10.6 Declarative Automation Bundles

- Bundles were renamed on 2026-03-16. The `databricks bundle` command and the old configuration files stay the same.
- One `databricks.yml` file describes jobs and pipelines, with several targets, one for each environment:

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

- **Variables:** declare them in `variables`, reference them with `${var.<name>}`. Order of precedence: `--var`, then the environment variable `BUNDLE_VAR_<name>`, then `variable-overrides.json`, then the target's `variables`, and finally `default`. Variables are substituted at deploy time, not at run time.
- **`mode: development`** adds the prefix `[dev <user name>]` to job names, puts pipelines in development mode, and pauses all schedules and triggers. Pitfall: this mode renames the job but not the tables the job writes to, so the catalog or schema name must go through a variable.
- **`mode: production`** requires the Git branch to match the branch declared in the target, and the docs recommend that `run_as` be a service principal.
- **Engine direct:** the CLI calls the Databricks API directly and keeps a small state file. Engine direct went GA on 2026-06-10; the Terraform engine was removed in CLI 1.20.0. `databricks bundle plan` previews what a deploy will create, change or delete, like `terraform plan`.
- Pitfall: with engine direct, removing a field from the YAML resets that field to its default value, while Terraform leaves the old value in place.

### 10.7 CI/CD with GitHub Actions

Docs: "Declarative Automation Bundles are the recommended approach to CI/CD on Databricks." The sample workflow deploys the dev target for pull requests and the prod target on a push to `main`. The dev job part:

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

- CI authenticates with workload identity federation for a service principal, so no long-lived secret has to be stored.
- Pitfall: the subject of the federation policy "must exactly match the expected token subject". If you change `environment:` in the workflow, you must update the policy. `setup-cli@main` does not pin a version; pin a specific version.

### 10.8 Environments

- Docs: "Prod must be a different workspace than Dev or Staging." Use one shared metastore for all environments, isolate the data by catalog, and bind each catalog to a workspace.
- Free Edition has only 1 workspace and 1 metastore, so the roadmap separates by schema: dev writes to `workspace.dev_silver`, prod writes to `workspace.silver`. This separates the data but not the admin rights or the quotas. Module 13 redoes it with separate workspaces on AWS.

### 10.9 Free Edition

- Yes: pytest on a laptop, Lakeflow Pipelines Editor, separation by schema.
- No: several workspaces for dev and prod.
- Unknown, the docs do not say: bundles and the CLI, Databricks Connect, service principal and OIDC for CI, expectations, unit tests for pipelines.

> **Biggest pitfall:** believing that a green job and green tests mean the data is correct. A green job only says the code ran to the end; green tests only say the code is correct on sample data. Both errors in the week above were green in both places.

Further reading: Module 10 of the roadmap. Docs: [notebook best practices](https://docs.databricks.com/aws/en/notebooks/best-practices), [testing with Databricks Connect](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python/testing), [unit tests for pipelines](https://docs.databricks.com/aws/en/ldp/unit-testing), [bundle commands](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands), [bundle variables](https://docs.databricks.com/aws/en/dev-tools/bundles/variables), [deployment modes](https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes), [CI/CD with bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/ci-cd-bundles), [GitHub Actions](https://docs.databricks.com/aws/en/dev-tools/ci-cd/github), [workspace strategy](https://docs.databricks.com/aws/en/lakehouse-architecture/deployment-guide/workspace-strategy).

---

## 11. Data serving, BI and monitoring

> **Question:** Explain data serving, BI and monitoring using the same framework.

Once Gold exists, the data must reach people and applications, and you must know when data stops flowing. Databricks splits this into 3 groups: BI for people (SQL warehouse, AI/BI dashboards, Genie Agents, metric views, SQL alerts), serving for applications (Statement Execution API, Databricks SQL Connector, JDBC and ODBC, Databricks Apps, Lakebase, OpenSharing), and monitoring (system tables, job notifications, data quality monitoring).

### 11.1 What it contains

| group | component | what it does |
|---|---|---|
| BI | SQL warehouse | compute that runs the SQL for everything below |
| BI | AI/BI dashboards (old name Lakeview) | dashboards inside the workspace, can be embedded outside |
| BI | Genie Agents (old name Genie Spaces) | ask questions about data in natural language, Genie generates the SQL |
| BI | metric views | define a metric once, every tool shares it |
| BI | SQL alerts | run a query on a schedule, notify when a threshold is passed |
| application | Statement Execution API, Databricks SQL Connector for Python, JDBC, ODBC | an app calls a SQL warehouse |
| application | Databricks Apps | run Streamlit, Dash or Node.js apps right on Databricks |
| application | Lakebase and synced tables | managed Postgres for low-latency point queries |
| application | OpenSharing (old name Delta Sharing) | share tables with partners without copying the data |
| monitoring | system tables | jobs, queries, cost, audit, queryable with SQL |
| monitoring | job notifications | notify when a job fails or runs too long |
| monitoring | data quality monitoring (old name Lakehouse Monitoring) | automatically detects tables that are late or missing rows |

### 11.2 Why it is needed

The real problem in Module 11 of the roadmap: on the evening of 2018-10-17, the dashboard reported September revenue of only 145 BRL after August's 848,860.10 BRL. The finance team sent a different total. The ingestion job ran green all week, and the freshness check with `max(order_purchase_timestamp)` showed data as recent as 17:30 that afternoon.

```text
sum(price) mọi đơn: 13591643.70 | doanh thu theo định nghĩa: 13494400.74 | lệch 97242.96
đơn mới nhất: 2018-10-17 17:30:18 | đơn có doanh thu mới nhất: 2018-09-03 09:06:57
```

- **Two definitions:** the finance team adds up `price` for every order; the dashboard excludes `canceled` and `unavailable` orders. Nobody wrote wrong SQL, but 97,242.96 BRL sits between the 2 numbers.
- **The freshness check in the wrong place:** since 2018-09-01 the source sent only 20 orders, 19 of them canceled. Canceled orders still push up the `max` of the time column, so rows with no business value kept the table looking alive for 44 days.

| self-built | Databricks |
|---|---|
| each dashboard writes its own revenue formula | metric view: one definition for dashboards, Genie, the API and partners |
| a separate BI tool such as Metabase, a separate query engine such as Trino | SQL warehouse and AI/BI dashboards live in the workspace, sharing the Unity Catalog privileges |
| write your own API that reads from the warehouse | Statement Execution API, Databricks Apps, Lakebase |
| copy files for partners | OpenSharing: partners read the table directly, no copy |
| set up Grafana and Prometheus for monitoring | system tables queried with SQL, plus SQL alerts |

### 11.3 SQL warehouse

| type | startup | Photon | Predictive IO | Intelligent Workload Management |
|---|---|---|---|---|
| serverless | 2 to 6 seconds | yes | yes | yes |
| pro | about 4 minutes | yes | yes | no |
| classic | about 4 minutes | yes | no | no |

- Sizes range from 2X-Small to 4X-Large. Pitfall: when you create one in the UI, the default size is **X-Large**; change it back to 2X-Small before you click Create.
- Auto Stop defaults to 10 minutes for serverless, 45 minutes for pro and classic. Docs: "Idle SQL warehouses continue to accumulate DBU and cloud instance charges until they are stopped."
- Scaling defaults to min 1, max 1 cluster. The queue holds at most 1,000 queries.
- Permissions on a warehouse: Can view, Can use, Can monitor, Can manage, Is owner.
- Pitfall: an alert that runs every 5 minutes keeps the warehouse awake forever, because the warehouse starts and stops as a whole, not per request.

### 11.4 AI/BI dashboards

- A dashboard has a draft version and a published version: "The published version remains unchanged until you publish again."
- **Two data permission modes:**
  - Shared, the default: "Viewers run queries using the publisher's data permissions." The docs warn: "This might expose data to users who have not been granted direct access to it."
  - Individual: "Viewers run queries using their own credentials."
- In both modes, the compute always runs with the publisher's permissions, so viewers do not need any permission on the warehouse.
- Pitfall: a per-user row filter protects viewers only when the dashboard uses individual mode. In shared mode, viewers see what the publisher sees. The draft always runs with the viewer's permissions, so an author testing the draft may see something different from the viewers of the published version.
- Embedding for outside viewers who do not need a Databricks account is GA; your server uses a service principal to get a narrowly scoped token for each viewer.

### 11.5 Genie Agents

- The data must be registered in Unity Catalog. Authors configure descriptions, synonyms, joins, instructions, example SQL, trusted assets (verified parameterized queries) and benchmarks to measure accuracy.
- Data privileges are evaluated for each person who asks; row filters and column masks apply for that person. The generated queries are read-only.
- Pitfall: asking about data you have no privilege on returns an empty result, not an error. Put a metric view into Genie so that Genie does not guess the revenue formula itself.

### 11.6 Metric views

A metric view defines a metric once in Unity Catalog. Dashboards, Genie, alerts, notebooks and external BI tools such as Power BI and Tableau all share that definition. Shortened from the Module 11 lab, not run yet on a workspace:

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

- `revenue` and `gross_revenue` sit side by side, so the finance team and the dashboard can clearly see the 2 definitions and the gap between them.
- Metric views exist from Databricks Runtime 16.4. Whether Free Edition can use them is unknown.

### 11.7 SQL alerts

- The new SQL alerts went GA in May 2026. Each alert owns its own query: "An existing saved SQL query cannot be reused when creating an alert."
- The status is only `OK`, `TRIGGERED` or `ERROR`. The condition compares the value of the first row, or an aggregate such as `SUM` or `AVG`, with a threshold.
- Only workspace admins can create notification destinations such as Slack, Teams or a webhook.

### 11.8 Serving applications

| path | use when | notes |
|---|---|---|
| Statement Execution API | an app calls SQL over REST | `INLINE` results up to 25 MiB; for larger results use `EXTERNAL_LINKS` |
| Databricks SQL Connector for Python, JDBC, ODBC | an app or BI tool connects directly | JDBC uses the Databricks JDBC Driver from version 3; Simba is legacy |
| Databricks Apps | run Streamlit, Dash, Gradio or Node.js apps on serverless | app authorization: every user shares the app's permissions; user authorization: each user's own permissions apply |
| Lakebase and synced tables | low-latency point queries by key | a synced table copies a Delta table into Postgres; the Triggered and Continuous modes need Change Data Feed |
| OpenSharing | partners read your table | Databricks-to-Databricks needs no token; Databricks-to-Open uses a bearer token or OIDC |

- A synced table is like the read model in CQRS: the write side is the lakehouse, the read side is Postgres with indexes. Docs: "Databricks strictly recommends running only read queries"; writes into a synced table are overwritten at the next refresh.
- A table with a row filter or column mask cannot be shared through OpenSharing. A long-lived bearer token is a risk; prefer OIDC or Databricks-to-Databricks.

### 11.9 Monitoring with system tables

Docs: "System tables are free to use. You are only charged for the compute used to query them." By default only someone who is both an account admin and a metastore admin can read them.

| table | retention | latency |
|---|---|---|
| `system.billing.usage` | indefinite | usually within 12 hours |
| `system.lakeflow.job_run_timeline` | 395 days | usually within 1 hour |
| `system.lakeflow.pipeline_update_timeline` | 395 days | not stated in the docs |
| `system.query.history` | 365 days | usually within 1 hour |
| `system.access.audit` (Public Preview) | 395 days | not stated in the docs |
| `system.data_quality_monitoring.table_results` | 395 days | not stated in the docs |

Written from the columns the docs list, not run yet:

```sql
-- Jobs that failed in the last 24 hours, used as the query for a SQL alert
SELECT job_id, run_id, result_state, termination_code, period_end_time
FROM system.lakeflow.job_run_timeline
WHERE period_end_time >= current_timestamp() - INTERVAL 24 HOURS
  AND result_state IN ('FAILED', 'ERROR', 'TIMED_OUT');
```

- **Job notifications** notify when a job starts, succeeds, fails, runs too long, or when a streaming backlog passes a threshold; they are sent by email, Slack, PagerDuty, Teams or webhook. Use them for immediate alerts, because `job_run_timeline` lags by about 1 hour.
- **Data quality monitoring** has anomaly detection, GA in October 2026. Freshness learns from the commit history to know when a table is unusually late. Completeness estimates the expected range of rows written in 24 hours. A new table must go through a Training period before it has a baseline.
- **Freshness SLO:** the docs have no formula. The roadmap's way is a SQL alert on the `max` of the column that records when each row was written, computed over rows that carry business value, such as orders with revenue. Do not use `information_schema.tables.last_altered`, because it measures the last change to the table definition, not the data.
- Pitfall: summing `usage_quantity` without handling `RETRACTION` rows gives wrong numbers. "Job-level notifications aren't sent when failed tasks are retried."

### 11.10 Free Edition

| yes | limits | unknown |
|---|---|---|
| AI/BI dashboards, Genie Agents | 1 SQL warehouse of size 2X-Small; at most 3 Databricks Apps, which stop automatically after 24 hours; 1 Lakebase project | SQL alerts, metric views, system tables, data quality monitoring, OpenSharing, API access from outside |

> **Biggest pitfall:** a green job and a green freshness check while data that carries business value has stopped flowing for 44 days. Check freshness on data that carries business value, not on every row, and define a metric once in a metric view instead of in each dashboard.

Further reading: Module 11 of the roadmap. Docs: [SQL warehouse types](https://docs.databricks.com/aws/en/compute/sql-warehouse/warehouse-types), [sharing dashboards](https://docs.databricks.com/aws/en/dashboards/share/share), [metric views](https://docs.databricks.com/aws/en/uc-semantics/metric-views/), [Statement Execution API](https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial), [OpenSharing](https://docs.databricks.com/aws/en/opensharing/), [system tables](https://docs.databricks.com/aws/en/admin/system-tables/), [job notifications](https://docs.databricks.com/aws/en/jobs/notifications), [anomaly detection](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-quality-monitoring/anomaly-detection).

---

## 12. Streaming

> **Question:** Explain streaming on Databricks using the same framework.

Databricks processes streams with Spark Structured Streaming, in 2 modes: micro-batch (the default, for ETL and analytics) and real-time mode (for operational work that needs under 1 second). Databricks has no Flink. On serverless, notebooks and jobs can run only the `availableNow` trigger; to keep a stream running continuously, use a Lakeflow pipeline in continuous mode.

### 12.1 What it contains

| part | what it does |
|---|---|
| Structured Streaming, micro-batch mode | processes new data in small batches, same DataFrame API |
| real-time mode | end-to-end latency can be as low as 5 ms, for fraud detection and personalization |
| sources | Kafka, Auto Loader on files, Delta tables |
| sinks | Delta tables, Kafka, `foreachBatch` to write your own write logic |
| checkpoint | stores offsets, commits and state so a rerun loses nothing and duplicates nothing |
| watermark and window | group by the time the event happened, handle late events |
| state | RocksDB, `transformWithState` for your own stateful logic |
| continuous running | a streaming table in a pipeline in continuous mode, a continuous job |

### 12.2 Why it is needed

The real problem in Module 12 of the roadmap: the "orders per day" table was built from the tutorial example: `withWatermark` 10 minutes, `window` 1 day, writing Delta in append mode. The source sends 1 file per month. The November 2017 file failed to upload and was resent after the January 2018 file. The query finished with no error:

```text
micro-batch: 27 | lỗi: None
số cửa sổ: 603 | tổng đơn: 91896
cửa sổ của tháng 11/2017: 0
ngày cuối trong bảng: 2018-10-16
numRowsDroppedByWatermark cộng dồn: 30
```

- **All of November 2017 is lost**, that is 7,544 orders from the month with Black Friday. The file arrived when the watermark had already passed 2017-12-31, so every November window was closed and every event in the file was dropped.
- **The last day is missing:** append mode writes a window only when the watermark passes the end of that window, and no more events arrive afterward to push the watermark forward.
- **A misleading metric:** `numRowsDroppedByWatermark` reports 30, not 7,544, because it counts rows after the partial aggregation within each partition.

| self-built | Databricks |
|---|---|
| Flink for streams, Spark for batch, 2 engines and 2 APIs | one DataFrame API for both batch and stream |
| Flink checkpoint, exactly-once sink into Iceberg | the Structured Streaming checkpoint, Delta sink exactly-once through the transaction log |
| keyed state and timers in Flink's ProcessFunction | `transformWithState` with `ValueState`, `ListState`, `MapState`, timers, TTL |
| a Flink SQL job that runs continuously | a Lakeflow pipeline in continuous mode or a continuous job |
| Flink's millisecond latency | real-time mode |

### 12.3 Trigger

| trigger | behavior |
|---|---|
| not set | same as `processingTime` 0 ms, latency about 3 to 5 seconds |
| `processingTime='10 seconds'` | checks for new data periodically |
| `availableNow=True` | processes all the data present at startup, split into several batches, then stops |
| `realTime='5 minutes'` | real-time mode; this interval is the length of a long-running batch and the checkpoint interval, not the latency |
| `continuous='1 second'` | not supported on Databricks; use real-time mode instead |

- Serverless notebooks and jobs accept only `availableNow` (and the deprecated `Once`). Any other trigger raises the error `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`.
- Pitfall: calling `display()` on a streaming DataFrame starts a real streaming job, which may keep the compute from auto terminating.

### 12.4 Checkpoint

- A checkpoint stores the source offsets, the sink commits, the state and the query metadata.
- Docs: "Each query must have a different checkpoint location." Checkpoints should live on a volume, for example `/Volumes/catalog/schema/volume/path`.
- Changing the number of sources or the type of source, the Kafka topic, the Auto Loader path, the type of stateful operation, the schema of the state or the type of sink requires a new checkpoint. The number of shuffle partitions is fixed when the checkpoint is created.
- If you delete a checkpoint, the next run starts again from the beginning. If you write Delta through `foreachBatch` with `txnAppId`, you must change the `txnAppId`, because the batch ID restarts from 0 and the new writes would be skipped.

### 12.5 Exactly-once

- **Delta as the sink:** the transaction log guarantees exactly-once, even when another stream or batch writes at the same time.
- **`foreachBatch`** is only at-least-once. For an idempotent write, use `txnAppId` and `txnVersion` set to `batchId`, or `MERGE` by key.
- **Kafka as the sink** is only at-least-once.
- **Delta as the source** accepts only appends. `UPDATE`, `DELETE`, `MERGE` or an overwrite on the source table makes the stream fail, unless you set `skipChangeCommits` or read Change Data Feed.
- Pitfall: the stream must run at least once within the retention window of the source table, 7 days for files and 30 days for the log, or it fails with `DELTA_FILE_NOT_FOUND_DETAILED`. Do not set `spark.sql.files.ignoreMissingFiles=true` to get around it, because it "silently produces incorrect results".

### 12.6 Watermark and window

```python
from pyspark.sql.functions import window
(df
  .withWatermark("event_time", "10 minutes")
  .groupBy(
    window("event_time", "5 minutes"),
    "id")
  .count())
```

- **Watermark** is the event-time mark at which Spark considers the data complete enough. An event older than the mark is late, and "might still process ... but this isn't guaranteed".
- **Append mode** writes the result of a window only after the watermark passes it. **Update mode** writes right away and updates the result progressively.
- Without a watermark, the state "grows indefinitely".
- `dropDuplicatesWithinWatermark` deduplicates within the range of the watermark. Docs: "Structured Streaming guarantees exactly-once processing but doesn't deduplicate records from data sources."
- A reasonable starting point: a watermark of about 2 times the latency SLA.

### 12.7 State

- Docs: "Databricks recommends using RocksDB with changelog checkpointing." RocksDB is the default state store from Databricks Runtime 17.3 LTS.
- `transformWithState`, from Databricks Runtime 16.2, replaces `mapGroupsWithState`. You write a `StatefulProcessor` with `init`, `handleInputRows` and `handleExpiredTimer`, using the state types `ValueState`, `ListState` and `MapState`, timers by processing time or event time, and TTL.

### 12.8 Kafka

```python
df = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "<server:ip>")
  .option("subscribe", "<topic>")
  .option("startingOffsets", "latest")
  .load())
```

- `startingOffsets` takes effect only when a query starts fresh. After a restart, the query always continues from the checkpoint.
- `maxOffsetsPerTrigger` limits the number of offsets per batch. The lag metrics include `avgOffsetsBehindLatest`, `maxOffsetsBehindLatest` and `estimatedTotalBytesBehindLatest`.
- SASL authentication must use class names with the `kafkashaded.` prefix. Databricks recommends a Unity Catalog service credential on serverless.
- SQL has the function `read_kafka`. Lakeflow Connect has a Kafka connector (Beta) to ingest Kafka into a streaming table without writing Spark code.

### 12.9 Real-time mode

- For operational work under 1 second; end-to-end latency can be as low as 5 ms.
- Running it directly needs Databricks Runtime 16.4 LTS and classic compute, update mode only, and it does not run on serverless notebooks or jobs. It does not accept Delta as a source or sink.
- Real-time mode in Lakeflow pipelines went GA on 2026-10-29.

### 12.10 When you still need Flink

Inferred from the limits in the Databricks docs; the Flink side is not verified:

- You need latency under 1 second and the source or sink is a Delta table, because real-time mode does not accept Delta.
- You need end-to-end exactly-once when writing to Kafka, because the Spark Kafka sink is only at-least-once.
- The organization already operates Flink, or the stream goes only from Kafka to Kafka with no need for a lakehouse.

### 12.11 Monitoring

- Classic compute has a Streaming tab in the Spark UI; name your queries with `.queryName(...)`. Serverless has no Spark UI, so use query profile.
- `StreamingQueryListener` receives the start, progress and termination events of a query, so you can push metrics to a monitoring system.

### 12.12 Free Edition

| yes | no | unknown |
|---|---|---|
| `availableNow`, Auto Loader on a volume, Delta as source and sink, checkpoint on a volume, `foreachBatch` with `MERGE` | triggers other than `availableNow`, real-time mode, Spark UI, Scala | external Kafka, continuous pipeline, `transformWithState`, `StreamingQueryListener` |

So the Module 12 lab does the Kafka part and the fault-tolerance part on a laptop, and on Free Edition it uses Auto Loader on a volume as the source in place of Kafka.

> **Biggest pitfall:** believing that a query that finishes without error has counted everything. A watermark silently drops late events, and the `numRowsDroppedByWatermark` metric can report far fewer than the events actually lost. Choose the watermark from the real latency of the source, and reconcile the totals with the source after every run.

Further reading: Module 12 of the roadmap. Docs: [Structured Streaming](https://docs.databricks.com/aws/en/structured-streaming/), [triggers](https://docs.databricks.com/aws/en/structured-streaming/triggers), [serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations), [Kafka](https://docs.databricks.com/aws/en/connect/streaming/kafka), [Delta and streaming](https://docs.databricks.com/aws/en/structured-streaming/delta-lake), [`foreachBatch`](https://docs.databricks.com/aws/en/structured-streaming/foreach).

---

## 13. Databricks on AWS, Terraform and cost

> **Question:** Explain Databricks on AWS, Terraform and cost using the same framework.

Free Edition has no account console, so this part is studied on a Databricks free trial on AWS. It covers: creating a workspace on AWS, networking at an introductory level, building everything with Terraform, and controlling cost with budgets, tags, compute policies and the `system.billing.usage` table.

### 13.1 What it contains

| part | what it does |
|---|---|
| account console | manages workspaces, users and account-level budgets, at `https://accounts.cloud.databricks.com` |
| workspace on AWS | a serverless workspace, or a classic workspace with automatic or manual configuration |
| networking | VPC, subnet, security group, PrivateLink for classic compute |
| Terraform provider `databricks/databricks` | builds workspaces and account-level resources with code |
| budgets | alert when spending passes a threshold, do not block |
| tags and serverless usage policy | attach each DBU to a team or a project |
| compute policy and auto termination | limit compute size, automatically terminate idle compute |
| `system.billing.usage`, `system.billing.list_prices` | the source of truth for usage and list price |

### 13.2 Why it is needed

The real problem in Module 13 of the roadmap: on day 15 of the trial, the account has a card attached, so it converts to pay-as-you-go automatically, and the trial's cap of 50 DBU per hour no longer applies. On Friday afternoon you create a SQL warehouse in the UI without changing the size, point a SQL job that runs every 15 minutes at it, set a budget of 20 USD, and leave for the weekend.

```text
2X-Small (4 DBU/giờ): Running 46.9 giờ, 187.7 DBU, 131.41 USD
X-Large (80 DBU/giờ): Running 46.9 giờ, 3754.7 DBU, 2628.27 USD
400 USD credit = 571.4 DBU = 7.1 giờ X-Large nếu không có trần
```

- The UI preselects the X-Large size. The rates of 80 DBU per hour for X-Large and 4 for 2X-Small come from the Azure Databricks pricing page; for AWS this is not verified. The price of 0.70 USD per DBU is an assumption; the real price is in `system.billing.list_prices`.
- A job every 15 minutes plus a 10 minute Auto Stop keeps the warehouse in the Running state for 46.9 out of 64 hours. The size, the run schedule and the Auto Stop together decide the bill.
- The budget email can arrive 24 hours late, and a budget blocks nothing.

| done by hand | Databricks and Terraform |
|---|---|
| click to create a workspace in the console, hard to rebuild | Terraform `databricks_mws_*`: build and destroy with 1 command |
| no idea where the DBUs are spent | tags and serverless usage policy, then group by tag in `system.billing.usage` |
| believe that a budget will block spending | a budget only alerts; block with compute policy, auto termination and small sizes |
| forget to shut down a cluster | auto termination, a compute policy that requires you to set a shutdown time |

### 13.3 Account console

- The person who creates the account automatically gets the account owner role. It has the same permissions as an account admin, but it cannot be removed.
- The account-level configuration includes credential configuration, storage configuration, network configuration, private access settings, budgets and workspaces. Each one has a matching `databricks_mws_*` Terraform resource.
- Free Edition: "No access to the account console or account-level APIs."

### 13.4 Three ways to create a workspace on AWS

| way | what it creates in your AWS | when to use |
|---|---|---|
| serverless workspace | nothing; uses default storage and serverless compute | learning, trying things out, teams that use only serverless; the docs recommend starting here |
| classic, automatic configuration | cross-account IAM role, VPC, S3 bucket, IAM role for the bucket | you need classic compute but do not want to build it yourself |
| classic, manual configuration | you create the role, bucket and VPC yourself and then register them with Databricks | you need your own VPC, your own IAM policy, an existing bucket |

- Serverless workspace went GA on 2026-01-29. Docs: it "support only serverless compute", suited to "short-lived environments such as training or testing".
- Automatic configuration uses AWS IAM temporary delegation, which replaced the CloudFormation-based AWS Quickstart on 2025-11-21.
- Manual configuration needs 3 objects:
  - Credential configuration: a cross-account IAM role that trusts the Databricks AWS account `414351767826`, with the external ID being the Databricks account ID.
  - Storage configuration: an S3 bucket in the same region, a bucket policy, and a self-assuming IAM role with read and write access to the `unity-catalog/` prefix.
  - Network configuration: by default a Databricks-managed VPC; to manage it yourself, create a customer-managed VPC first.
- Pitfall:
  - Changing `compute_mode` "requires recreation of the workspace".
  - "Databricks trial credits apply only to Databricks usage." The VPC and the NAT gateway of a classic workspace are billed separately by AWS.
  - "Classic compute (any compute that is not serverless) cannot interact with data assets in default storage."

### 13.5 Networking at an introductory level

- A customer-managed VPC needs at least 2 private subnets in 2 availability zones, a netmask from `/17` to `/26`, a NAT gateway, and 1 to 5 security groups.
- "You cannot move an existing workspace with a Databricks-managed VPC to use a customer-managed VPC."
- PrivateLink for classic compute needs the Enterprise plan, a customer-managed VPC and secure cluster connectivity.
- After the workspace is in the RUNNING state, the docs advise you to "wait an additional 20 minutes after the status shows RUNNING before creating or using clusters".
- A serverless workspace does not need you to manage the network: egress is controlled by network policies, ingress by IP access lists.

### 13.6 Terraform

- The latest version of the `databricks/databricks` provider is v1.142.0, dated 2026-11-09.
- The docs split the work: Terraform for cloud-level resources and admin tasks such as creating workspaces and networking; Declarative Automation Bundles for jobs, pipelines and notebooks.
- The account-level provider uses `host = "https://accounts.cloud.databricks.com"`; the workspace-level provider uses the workspace URL.

A serverless workspace with Terraform, verbatim from the docs:

```hcl
resource "databricks_mws_workspaces" "serverless_workspace" {
  account_id     = "" # Your Databricks account ID
  workspace_name = "serverless-workspace"
  aws_region     = "us-east-1"
  compute_mode   = "SERVERLESS"
}
```

- A serverless workspace "must not include `credentials_id` or `storage_configuration_id`". A classic workspace additionally needs `databricks_mws_credentials` and `databricks_mws_storage_configurations`.
- `terraform test` can run in unit-test style with `command = plan`; add `mock_provider "databricks" {}` and no credentials are needed.

### 13.7 Budgets

- Budgets went GA on 2026-07-06. A budget tracks spending at list price in USD, with up to 4 thresholds per budget, filtered by workspace, resource type or tag.
- A budget is not a hard cap. Only Unity Gateway has a "Block usage" option, and the docs warn: "Do not use this feature as a way to ensure an absolute spend cap on final billed amounts."
- Email can be delayed by up to 24 hours. A budget does not count credits or discounts.

### 13.8 Tags and serverless usage policy

- A serverless usage policy (Public Preview) is a set of tags attached to all serverless activity of the people it is assigned to. The tags appear in the `custom_tags` column of `system.billing.usage`.
- Pitfall: "Policy changes are only applied to usage initiated after the policy update." A policy does not tag classic compute. A pipeline called by a job does not inherit the job's policy.
- Classic compute has the default tags `Vendor`, `ClusterId`, `ClusterName` and `Creator`, which are passed down to EC2 and EBS. Do not set a tag with the key `Name` on a cluster, because then the cluster "can no longer be tracked by Databricks" and may not auto terminate.

### 13.9 Compute policy and auto termination

- A compute policy limits the compute that users may create: DBU per hour through the `dbus_per_hour` attribute, the number of compute resources per user, fixed values, required tags. The rule types are `fixed`, `forbidden`, `allowlist`, `blocklist`, `regex`, `range` and `unlimited`.
- Pitfall: the default values of a policy are not applied automatically to compute created through the Clusters API, unless you set `apply_policy_default_values` to `true`.
- Auto termination of classic compute is from 10 to 10,000 minutes; setting `0` turns this feature off. Docs: "Idle compute continue to accumulate DBU and cloud instance charges during the inactivity period before termination."
- SQL warehouse: Auto Stop defaults to 10 minutes for serverless and 45 minutes for pro and classic. The default size in the UI is X-Large.

### 13.10 Cost tables

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

Adapted from an example in the docs, not run yet.

- Corrections are recorded as `RETRACTION` rows that carry negative numbers and as `RESTATEMENT` rows, so always `SUM` and then drop the groups whose total is 0.
- Multiplying by `list_prices` gives the cost at list price, not the amount actually billed.
- System tables are free; you are only charged for the compute used to query them.

### 13.11 Free trial

- The trial lasts 14 days, with up to 400 USD of credit. The trial allows 1 SQL warehouse of at most 50 DBU per hour, and caps serverless for notebooks, jobs and pipelines at 50 DBU per hour.
- A trial through AWS Marketplace, or an account with a payment method attached, converts automatically to pay-as-you-go when the credit runs out.
- If you do not upgrade, the workspace assets are deleted 60 days after the trial ends.
- Before canceling, the docs require you to terminate all compute and remove the credit card.

### 13.12 Free Edition

Free Edition has no account console, so you cannot learn budgets, workspace creation or account-level Terraform on it. This part can be done only on a free trial or a paid account.

> **Biggest pitfall:** believing that a budget will block spending. A budget only sends email, which can be 24 hours late. What really limits the bill is the warehouse size, the run schedule, Auto Stop, auto termination and compute policy, and everything you create should be in Terraform so you can delete it with 1 command.

Further reading: Module 13 of the roadmap. Docs: [create a serverless workspace](https://docs.databricks.com/aws/en/admin/workspace/serverless-workspaces), [Terraform provider](https://docs.databricks.com/aws/en/dev-tools/terraform/), [budgets](https://docs.databricks.com/aws/en/admin/account-settings/budgets), [billing system tables](https://docs.databricks.com/aws/en/admin/usage/system-tables), [compute policies](https://docs.databricks.com/aws/en/admin/clusters/policies), [free trial](https://docs.databricks.com/aws/en/getting-started/free-trial), [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).
