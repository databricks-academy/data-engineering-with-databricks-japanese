# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="bfb2d018-5d5a-4475-bf1e-293e2a5b0100"/>
# MAGIC 
# MAGIC # Databricksによるデータエンジニアリング
# MAGIC 
# MAGIC このコースは、Databricks Certified Associate Data Engineer認定試験のトピックに備えるものです。
# MAGIC 
# MAGIC Databricks Lakehouse PlatformのETLパイプラインの本番稼動をサポートするコンポーネントを包括的に紹介することで、あらゆる分野のデータプロフェッショナルの仕事に役に立ちます。SQLとPythonを活用して、レイクハウスの分析アプリケーションやダッシュボードを構築するため、様々なデータソースから新しいデータを段階的に処理するパイプラインを定義・スケジュールします。このコースでは、Databricks Data Science & Engineering Workspace、Databricks SQL、Delta Live Tables、Databricks Repos、Databricks Task Orchestration、およびUnity Catalogをハンズオンで学ぶことができます。
# MAGIC 
# MAGIC **期間:** 全日制2日または半日制4日
# MAGIC 
# MAGIC #### 目的
# MAGIC - Databricks Lakehouse Platformを活用し、データパイプライン開発の中核を担います。
# MAGIC - SQLとPythonを使用して、Lakehouseのテーブルやビューにデータを抽出・変換・ロードするための本番データパイプラインを作成します。
# MAGIC - Databricksのネイティブな機能と構文（Delta Live Tablesを含む）を使用して、データの取り込みと増分変更の伝達を簡素化します。
# MAGIC - プロダクションパイプラインをオーケストレーションし、アドホック分析およびダッシュボードに最新な結果を提供します。
# MAGIC 
# MAGIC #### 前提条件
# MAGIC -  `SELECT`、`WHERE`、`GROUP BY`、`ORDER BY`、`LIMIT`、`JOIN`を使ってクエリの作成などSQLクエリ構文の基本知識を得ます。
# MAGIC - データベースやテーブルを作成、変更、削除するためのSQL DDL文の基本的な知識を得ます。
# MAGIC - DELETE`、`INSERT`、`UPDATE`、`MERGE`を含むSQL DML文の基本的な知識を得ます。
# MAGIC - 仮想マシン、オブジェクトストレージ、ID管理、メタストアなどのクラウド機能を含む、クラウドプラットフォーム上でのデータエンジニアリングの実践経験または知識を得ます。

# COMMAND ----------

# MAGIC %md <i18n value="2fbffe4f-04e7-46db-8ed0-af4991565700"/>
# MAGIC 
# MAGIC ## コースアジェンダ (Course Agenda)
# MAGIC 
# MAGIC 1日目
# MAGIC 
# MAGIC | フォルダ|モジュール名|
# MAGIC | --- | --- |
# MAGIC | `01 - Databricks Workspace and Services` | Databricksワークスペースとサービスの紹介 |
# MAGIC | `02 - Delta Lake` | デルタ・レイクの紹介|
# MAGIC | `03 - Relational Entities on Databricks` | Databricksの関連エンティティ
# MAGIC | `04 - ETL with Spark SQL` |  Spark SQLによるETL
# MAGIC | `05 - OPTIONAL Python for Spark SQL` | Spark SQLに必要なPython |
# MAGIC | `06 - Incremental Data Processing` | 構造化ストリーミングとオートローダーによるインクリメンタルデータ処理 |
# MAGIC 
# MAGIC 2日目
# MAGIC 
# MAGIC |フォルダ| モジュール名|
# MAGIC | --- | --- |
# MAGIC | `07 - Multi-Hop Architecture` | データレイクハウスのメダリオンアーキテクチャ |
# MAGIC | `08 - Delta Live Tables` |  Delta Live Tablesの使用方法
# MAGIC | `09 - Task Orchestration with Jobs` |  Databricksジョブによるタスクのオーケストレーション
# MAGIC | `10 - Running a DBSQL Query` |  Databricks SQL クエリ実行
# MAGIC | `11 - Managing Permissions` | レイクハウスでの権限管理 |
# MAGIC | `12 - Productionalizing Dashboards and Queries in DBSQL` | Databricks SQLにおけるダッシュボードとクエリの製品化
# MAGIC 
# MAGIC 各モジュールに含まれるノートブックは以下の通りです。

# COMMAND ----------

# MAGIC %md <i18n value="8bcc5220-9489-49ec-ba62-8260e9871f38"/>
# MAGIC 
# MAGIC ## 01 - Databricks Workspace and Services
# MAGIC * [DE 1.1 - Create and Manage Interactive Clusters]($./01 - Databricks Workspace and Services/DE 1.1 - Create and Manage Interactive Clusters)
# MAGIC * [DE 1.2 - Notebook Basics]($./01 - Databricks Workspace and Services/DE 1.2 - Notebook Basics)
# MAGIC * [DE 1.3L - Getting Started with the Databricks Platform Lab]($./01 - Databricks Workspace and Services/DE 1.3L - Getting Started with the Databricks Platform Lab)

# COMMAND ----------

# MAGIC %md <i18n value="c57df770-302b-4b87-ad3b-c34bdef01029"/>
# MAGIC 
# MAGIC ## 02 - Delta Lake
# MAGIC * [DE 2.1 - Managing Delta Tables]($./02 - Delta Lake/DE 2.1 - Managing Delta Tables)
# MAGIC * [DE 2.2L - Manipulating Tables with Delta Lake Lab]($./02 - Delta Lake/DE 2.2L - Manipulating Tables with Delta Lake Lab)
# MAGIC * [DE 2.3 - Advanced Delta Lake Features]($./02 - Delta Lake/DE 2.3 - Advanced Delta Lake Features)
# MAGIC * [DE 2.4L - Delta Lake Versioning, Optimization, and Vacuuming Lab]($./02 - Delta Lake/DE 2.4L - Delta Lake Versioning, Optimization, and Vacuuming Lab)

# COMMAND ----------

# MAGIC %md <i18n value="6ed0e37b-3299-47fe-b78d-7f7e5fca9396"/>
# MAGIC 
# MAGIC ## 03 - Relational Entities on Databricks
# MAGIC * [DE 3.1 - Databases and Tables on Databricks]($./03 - Relational Entities on Databricks/DE 3.1 - Databases and Tables on Databricks)
# MAGIC * [DE 3.2A - Views and CTEs on Databricks]($./03 - Relational Entities on Databricks/DE 3.2A - Views and CTEs on Databricks)
# MAGIC * [DE 3.3L - Databases, Tables & Views Lab]($./03 - Relational Entities on Databricks/DE 3.3L - Databases, Tables & Views Lab)

# COMMAND ----------

# MAGIC %md <i18n value="f958dddc-d0e2-4c21-82ad-db6aaebabda4"/>
# MAGIC 
# MAGIC ## 04 - ETL with Spark SQL
# MAGIC * [DE 4.1 - Querying Files Directly]($./04 - ETL with Spark SQL/DE 4.1 - Querying Files Directly)
# MAGIC * [DE 4.2 - Providing Options for External Sources]($./04 - ETL with Spark SQL/DE 4.2 - Providing Options for External Sources)
# MAGIC * [DE 4.3 - Creating Delta Tables]($./04 - ETL with Spark SQL/DE 4.3 - Creating Delta Tables)
# MAGIC * [DE 4.4 - Writing to Tables]($./04 - ETL with Spark SQL/DE 4.4 - Writing to Tables)
# MAGIC * [DE 4.5L - Extract and Load Data Lab]($./04 - ETL with Spark SQL/DE 4.5L - Extract and Load Data Lab)
# MAGIC * [DE 4.6 - Cleaning Data]($./04 - ETL with Spark SQL/DE 4.6 - Cleaning Data)
# MAGIC * [DE 4.7 - Advanced SQL Transformations]($./04 - ETL with Spark SQL/DE 4.7 - Advanced SQL Transformations)
# MAGIC * [DE 4.8 - SQL UDFs and Control Flow]($./04 - ETL with Spark SQL/DE 4.8 - SQL UDFs and Control Flow)
# MAGIC * [DE 4.9L - Reshaping Data Lab]($./04 - ETL with Spark SQL/DE 4.9L - Reshaping Data Lab)

# COMMAND ----------

# MAGIC %md <i18n value="ce1da17d-44fa-47f4-bd66-af6e0c0f179d"/>
# MAGIC 
# MAGIC ## 05 - OPTIONAL Python for Spark SQL
# MAGIC * [DE 5.1 - Python Basics]($./05 - OPTIONAL Python for Spark SQL/DE 5.1 - Python Basics)
# MAGIC * [DE 5.2 - Python Control Flow]($./05 - OPTIONAL Python for Spark SQL/DE 5.2 - Python Control Flow)
# MAGIC * [DE 5.3L - Python for SQL Lab]($./05 - OPTIONAL Python for Spark SQL/DE 5.3L - Python for SQL Lab)

# COMMAND ----------

# MAGIC %md <i18n value="8fbeed27-4056-4024-afc7-859873916b70"/>
# MAGIC 
# MAGIC ## 06 - Incremental Data Processing
# MAGIC * [DE 6.1 - Incremental Data Ingestion with Auto Loader.py]($./06 - Incremental Data Processing/DE 6.1 - Incremental Data Ingestion with Auto Loader)
# MAGIC * [DE 6.2 - Reasoning about Incremental Data.py]($./06 - Incremental Data Processing/DE 6.2 - Reasoning about Incremental Data)
# MAGIC * [DE 6.3L - Using Auto Loader and Structured Streaming with Spark SQL Lab.py]($./06 - Incremental Data Processing/DE 6.3L - Using Auto Loader and Structured Streaming with Spark SQL Lab)

# COMMAND ----------

# MAGIC %md <i18n value="2d9f1a05-fd79-4766-a492-74c21c1d6bad"/>
# MAGIC 
# MAGIC ## 07 - Multi-Hop Architecture
# MAGIC * [DE 7.1 - Incremental Multi-Hop in the Lakehouse]($./07 - Multi-Hop Architecture/DE 7.1 - Incremental Multi-Hop in the Lakehouse)
# MAGIC * [DE 7.2L - Propagating Incremental Updates with Structured Streaming and Delta Lake Lab]($./07 - Multi-Hop Architecture/DE 7.2L - Propagating Incremental Updates with Structured Streaming and Delta Lake Lab)

# COMMAND ----------

# MAGIC %md <i18n value="c633fc58-9310-48e0-bcfa-974b30b75849"/>
# MAGIC 
# MAGIC ## 08 - Delta Live Tables
# MAGIC * DE 8.1 - DLT
# MAGIC   * [DE 8.1.1 - DLT UI Walkthrough]($./08 - Delta Live Tables/DE 8.1 - DLT/DE 8.1.1 - DLT UI Walkthrough)
# MAGIC   * [DE 8.1.2 - SQL for Delta Live Tables]($./08 - Delta Live Tables/DE 8.1 - DLT/DE 8.1.2 - SQL for Delta Live Tables)
# MAGIC   * [DE 8.1.3 - Pipeline Results]($./08 - Delta Live Tables/DE 8.1 - DLT/DE 8.1.3 - Pipeline Results)
# MAGIC 
# MAGIC * DE 8.2 - DLT Lab
# MAGIC   * [DE 8.2.1L - Lab Instructions]($./08 - Delta Live Tables/DE 8.2 - DLT Lab/DE 8.2.1L - Lab Instructions)
# MAGIC   * [DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab]($./08 - Delta Live Tables/DE 8.2 - DLT Lab/DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab)
# MAGIC   * [DE 8.2.3L - Lab Conclusion]($./08 - Delta Live Tables/DE 8.2 - DLT Lab/DE 8.2.3L - Lab Conclusion)

# COMMAND ----------

# MAGIC %md <i18n value="74e6ecb4-6268-4c2c-a6a1-726b02cf392e"/>
# MAGIC 
# MAGIC ## 09 - Task Orchestration with Jobs
# MAGIC * DE 9.1 - Scheduling Tasks with the Jobs UI
# MAGIC   * [DE 9.1.1 - Task Orchestration with Databricks Jobs]($./09 - Task Orchestration with Jobs/DE 9.1 - Scheduling Tasks with the Jobs UI/DE 9.1.1 - Task Orchestration with Databricks Jobs)
# MAGIC   * [DE 9.1.2 - Reset]($./09 - Task Orchestration with Jobs/DE 9.1 - Scheduling Tasks with the Jobs UI/DE 9.1.2 - Reset)
# MAGIC   * [DE 9.1.3 - DLT Job]($./09 - Task Orchestration with Jobs/DE 9.1 - Scheduling Tasks with the Jobs UI/DE 9.1.3 - DLT Job)
# MAGIC 
# MAGIC * DE 9.2L - Jobs Lab
# MAGIC   * [DE 9.2.1L - Lab Instructions]($./09 - Task Orchestration with Jobs/DE 9.2L - Jobs Lab/DE 9.2.1L - Lab Instructions)
# MAGIC   * [DE 9.2.2L - Batch Job]($./09 - Task Orchestration with Jobs/DE 9.2L - Jobs Lab/DE 9.2.2L - Batch Job)
# MAGIC   * [DE 9.2.3L - DLT Job]($./09 - Task Orchestration with Jobs/DE 9.2L - Jobs Lab/DE 9.2.3L - DLT Job)
# MAGIC   * [DE 9.2.4L - Query Results Job]($./09 - Task Orchestration with Jobs/DE 9.2L - Jobs Lab/DE 9.2.4L - Query Results Job)

# COMMAND ----------

# MAGIC %md <i18n value="498a81f3-4eda-49ac-aadb-ac33360af496"/>
# MAGIC 
# MAGIC ## 10 - Running a DBSQL Query
# MAGIC * [DE 10.1 - Navigating Databricks SQL and Attaching to Endpoints]($./10 - Running a DBSQL Query/DE 10.1 - Navigating Databricks SQL and Attaching to Endpoints)

# COMMAND ----------

# MAGIC %md <i18n value="ea5067ca-80b5-44d0-9446-8c591649d515"/>
# MAGIC 
# MAGIC ## 11 - Managing Permissions
# MAGIC * [DE 11.1 - Managing Permissions for Databases, Tables, and Views]($./11 - Managing Permissions/DE 11.1 - Managing Permissions for Databases, Tables, and Views)
# MAGIC * [DE 11.2L - Configuring Privileges for Production Data and Derived Tables Lab]($./11 - Managing Permissions/DE 11.2L - Configuring Privileges for Production Data and Derived Tables Lab)

# COMMAND ----------

# MAGIC %md <i18n value="76d731fd-01f1-4602-9d7b-4554eed88b8f"/>
# MAGIC 
# MAGIC ## 12 - Productionalizing Dashboards and Queries in DBSQL
# MAGIC * [DE 12.1 - Last Mile ETL with DBSQL]($./12 - Productionalizing Dashboards and Queries in DBSQL/DE 12.1 - Last Mile ETL with DBSQL)
# MAGIC * DE 12.2L - OPTIONAL Capstone
# MAGIC   * [DE 12.2.1L - Instructions and Configuration]($./12 - Productionalizing Dashboards and Queries in DBSQL/DE 12.2L - OPTIONAL Capstone/DE 12.2.1L - Instructions and Configuration)
# MAGIC   * [DE 12.2.2L - DLT Task]($./12 - Productionalizing Dashboards and Queries in DBSQL/DE 12.2L - OPTIONAL Capstone/DE 12.2.2L - DLT Task)
# MAGIC   * [DE 12.2.3L - Land New Data]($./12 - Productionalizing Dashboards and Queries in DBSQL/DE 12.2L - OPTIONAL Capstone/DE 12.2.3L - Land New Data)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
