-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="047624c1-1764-4d00-8f75-2640a0b9ad8e"/>
-- MAGIC 
-- MAGIC # データベース、テーブル、ビューのラボ（Databases, Tables, and Views Lab）
-- MAGIC 
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このラボでは、以下のことが学べます。
-- MAGIC - 次を含めてさまざまなリレーショナルエンティティの間の相互作用を作成して学びます。
-- MAGIC   - データベース
-- MAGIC   - テーブル（マネージドおよび外部）
-- MAGIC   - ビュー（ビュー、テンポラリビューおよびグローバルのテンポラリビュー）
-- MAGIC 
-- MAGIC **リソース**
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">データベースとテーブル - Databricksドキュメント</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">マネージドテーブルおよびアンマネージドテーブル</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">UIを使用したテーブル作成</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">ローカルテーブルの作成</a>
-- MAGIC * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">永続的テーブルへの保存</a>

-- COMMAND ----------

-- MAGIC %md <i18n value="702fc20d-b0bf-4138-9045-49571c496cc0"/>
-- MAGIC 
-- MAGIC ### はじめる（Getting Started）
-- MAGIC 
-- MAGIC 次のセルを実行してこのレッスン用の変数とデータセットを設定します。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-03.3L

-- COMMAND ----------

-- MAGIC %md <i18n value="306e4a60-45cf-40af-850f-4339700000b8"/>
-- MAGIC 
-- MAGIC ## データの概要（Overview of the Data）
-- MAGIC 
-- MAGIC このデータには、華氏もしくは摂氏で記録された平均気温を含めるさまざまな測候所からの複数項目が含まれています。 テーブルのスキーマ：
-- MAGIC 
-- MAGIC | 列名        | データ型   | 説明                  |
-- MAGIC | --------- | ------ | ------------------- |
-- MAGIC | NAME      | string | Station name        |
-- MAGIC | STATION   | string | Unique ID           |
-- MAGIC | LATITUDE  | float  | Latitude            |
-- MAGIC | LONGITUDE | float  | Longitude           |
-- MAGIC | ELEVATION | float  | Elevation           |
-- MAGIC | DATE      | date   | YYYY-MM-DD          |
-- MAGIC | UNIT      | string | Temperature units   |
-- MAGIC | TAVG      | float  | Average temperature |
-- MAGIC 
-- MAGIC このデータは、Parquet形式で保存されています。以下のクエリを使用してデータをプレビューします。

-- COMMAND ----------

SELECT * 
FROM parquet.`${DA.paths.datasets}/weather/StationData-parquet`

-- COMMAND ----------

-- MAGIC %md <i18n value="9b640cc4-561c-4f2e-8db4-806496e0300f"/>
-- MAGIC 
-- MAGIC ## データベースの作成（Create a Database）
-- MAGIC 
-- MAGIC セットアップスクリプトで定義されている **`da.db_name`** 変数を使用してデフォルトの場所にデータベースを作成します。

-- COMMAND ----------

-- TODO

<FILL-IN> ${da.schema_name}

-- COMMAND ----------

-- MAGIC %md <i18n value="eb27d1be-83d9-44d6-a3c5-a330d58f4d1b"/>
-- MAGIC 
-- MAGIC 以下のセルを実行して、結果を確認します。

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC assert spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{DA.schema_name}'").count() == 1, "Database not present"

-- COMMAND ----------

-- MAGIC %md <i18n value="4007b86a-f1c2-4431-9605-278256a18502"/>
-- MAGIC 
-- MAGIC ## 新しいデータベースに切り替える（Change to Your New Database）
-- MAGIC 
-- MAGIC 新しく作成したデータベースを **`USE`** します。

-- COMMAND ----------

-- TODO

<FILL-IN> ${da.schema_name}

-- COMMAND ----------

-- MAGIC %md <i18n value="a1799160-4921-48a9-84fd-d6b30eda2294"/>
-- MAGIC 
-- MAGIC 以下のセルを実行して、結果を確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql(f"SHOW CURRENT DATABASE").first()["namespace"] == DA.schema_name, "Not using the correct database"

-- COMMAND ----------

-- MAGIC %md <i18n value="29616225-cd27-4d1f-abf3-70257760ba80"/>
-- MAGIC 
-- MAGIC ## マネージドテーブルの作成（Create a Managed Table）
-- MAGIC CTAS文を使用して **`weather_managed`** というのマネージドテーブルを作成します。

-- COMMAND ----------

-- TODO

<FILL-IN>
SELECT * 
FROM parquet.`${DA.paths.datasets}/weather/StationData-parquet`

-- COMMAND ----------

-- MAGIC %md <i18n value="77e8b20b-0627-49a9-9d59-cf7e02225a64"/>
-- MAGIC 
-- MAGIC 以下のセルを実行して、結果を確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("weather_managed"), "Table named `weather_managed` does not exist"
-- MAGIC assert spark.table("weather_managed").count() == 2559, "Incorrect row count"

-- COMMAND ----------

-- MAGIC %md <i18n value="155e14f1-65cf-40be-9d01-68b3775c2381"/>
-- MAGIC 
-- MAGIC ## 外部テーブルの作成（Create an External Table）
-- MAGIC 
-- MAGIC 外部テーブルとマネージドテーブルの違いは場所の指定の有無です。 以下に **`weather_external`** というの外部テーブルを作成します。

-- COMMAND ----------

-- TODO

<FILL-IN>
LOCATION "${da.paths.working_dir}/lab/external"
AS SELECT * 
FROM parquet.`${DA.paths.datasets}/weather/StationData-parquet`

-- COMMAND ----------

-- MAGIC %md <i18n value="b7853935-465f-406f-8742-46a2a00ad3b5"/>
-- MAGIC 
-- MAGIC 以下のセルを実行して、結果を確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("weather_external"), "Table named `weather_external` does not exist"
-- MAGIC assert spark.table("weather_external").count() == 2559, "Incorrect row count"

-- COMMAND ----------

-- MAGIC %md <i18n value="3992398d-0f77-4fc2-8b9e-2f4064f10480"/>
-- MAGIC 
-- MAGIC ## テーブルの詳細を調べる（Examine Table Details）
-- MAGIC  **`DESCRIBE EXTENDED table_name`** というSQLコマンドを使用して2つの天気テーブルを調べます。

-- COMMAND ----------

DESCRIBE EXTENDED weather_managed

-- COMMAND ----------

DESCRIBE EXTENDED weather_external

-- COMMAND ----------

-- MAGIC %md <i18n value="6996903c-737b-4b88-8d51-3e0a01b347be"/>
-- MAGIC 
-- MAGIC 次のヘルパコードを実行して、テーブルの場所を抽出して比較します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def getTableLocation(tableName):
-- MAGIC     return spark.sql(f"DESCRIBE DETAIL {tableName}").select("location").first()[0]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC managedTablePath = getTableLocation("weather_managed")
-- MAGIC externalTablePath = getTableLocation("weather_external")
-- MAGIC 
-- MAGIC print(f"""The weather_managed table is saved at: 
-- MAGIC 
-- MAGIC     {managedTablePath}
-- MAGIC 
-- MAGIC The weather_external table is saved at:
-- MAGIC 
-- MAGIC     {externalTablePath}""")

-- COMMAND ----------

-- MAGIC %md <i18n value="63b73ebc-ccd0-460b-87ae-e09addada714"/>
-- MAGIC 
-- MAGIC これらのディレクトリの中身を一覧表示させてデータが両方の場所に存在することを確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(managedTablePath)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(externalTablePath)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md <i18n value="09f43b5b-e050-4e58-9769-e2e01829ddbc"/>
-- MAGIC 
-- MAGIC ### データベースとすべてのテーブルを削除したらディレクトリの中身を確認する（Check Directory Contents after Dropping Database and All Tables）
-- MAGIC これは **`CASCADE`** キーワードを使用して実行できます。

-- COMMAND ----------

-- TODO

<FILL_IN> ${da.schema_name}

-- COMMAND ----------

-- MAGIC %md <i18n value="e7babacd-bed8-47d7-ad52-7cc644e4f06a"/>
-- MAGIC 
-- MAGIC 以下のセルを実行して、結果を確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{DA.schema_name}'").count() == 0, "Database present"

-- COMMAND ----------

-- MAGIC %md <i18n value="7d7053b8-e1b9-421a-9687-b205feadbf68"/>
-- MAGIC 
-- MAGIC データベースを削除すると、ファイルも削除されます。
-- MAGIC 
-- MAGIC 次のセルからコメントアウトを外して実行すると、ファイルが存在しない証拠として **`FileNotFoundException`** が投げられます。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # files = dbutils.fs.ls(managedTablePath)
-- MAGIC # display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(externalTablePath)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(DA.paths.working_dir)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md <i18n value="5b66fbc5-c641-40e1-90a9-d69271bc0e8b"/>
-- MAGIC 
-- MAGIC **これはマネージドテーブルと外部テーブルの主な違いを示します。**デフォルトでは、マネージドテーブルに関連付けられているファイルは、ワークスペースにリンクされているDBFSストレージのルート上のこの場所に保存され、テーブルが削除されたときに削除されます。
-- MAGIC 
-- MAGIC 外部テーブルのファイルは、テーブル作成時に指定された場所に保持され、基礎ファイルを誤って削除してしまうことを防ぎます。 **外部テーブルは簡単に他のデータベースに移行させたり名前変更をしたりできますが、マネージドテーブルでこれらの操作を実行した場合は≪すべて≫の基礎ファイルの上書きが必要となります。**

-- COMMAND ----------

-- MAGIC %md <i18n value="1928eede-5218-47df-affe-b6a7654524ab"/>
-- MAGIC 
-- MAGIC ## パスが指定されているデータベースを作成する（Create a Database with a Specified Path）
-- MAGIC 
-- MAGIC 前のステップでデータベースを削除した場合は、同じ **`データベース`** の名前を使用できます。

-- COMMAND ----------

CREATE DATABASE ${da.schema_name} LOCATION '${da.paths.working_dir}/${da.schema_name}';
USE ${da.schema_name};

-- COMMAND ----------

-- MAGIC %md <i18n value="5b5dbf00-f9ee-4bc5-964e-22376e09be79"/>
-- MAGIC 
-- MAGIC この新しいデータベースに **`weather_managed`** テーブルを再作成して、このテーブルの場所を表示します。

-- COMMAND ----------

-- TODO

<FILL_IN>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC getTableLocation("weather_managed")

-- COMMAND ----------

-- MAGIC %md <i18n value="91684fed-3851-4979-be1d-ba8af8cfe314"/>
-- MAGIC 
-- MAGIC 以下のセルを実行して、結果を確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("weather_managed"), "Table named `weather_managed` does not exist"
-- MAGIC assert spark.table("weather_managed").count() == 2559, "Incorrect row count"

-- COMMAND ----------

-- MAGIC %md <i18n value="7ddc86a3-fbc4-4373-aacb-2f510c1eb708"/>
-- MAGIC 
-- MAGIC ここでは、DBFSルート上に作成された **`userhome`** ディレクトリを使用していますが、データベースディレクトリとして_どんなオブジェクトストアも_使用できます。 **ユーザーのグループ用にデータベースディレクトリを定義すると、誤ったデータ漏洩の確率を大幅に下げられます**。

-- COMMAND ----------

-- MAGIC %md <i18n value="07f87efe-13e4-48c3-84a7-576828359464"/>
-- MAGIC 
-- MAGIC ## ビューとその範囲（Views and their Scoping）
-- MAGIC 
-- MAGIC 用意されている **`AS`** 句を使用して次のものを登録します：
-- MAGIC -  **`celsius`** というのビュー
-- MAGIC -  **`celsius_temp`** というのテンポラリビュー
-- MAGIC -  **`celsius_global`** というのグローバルテンポラリビュー
-- MAGIC 
-- MAGIC 以下のコード セルで最初のビューを作成することから始めます。

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md <i18n value="14937501-7f2a-469d-b47f-db0c656d8da3"/>
-- MAGIC 
-- MAGIC 以下のセルを実行して、結果を確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("celsius"), "Table named `celsius` does not exist"
-- MAGIC assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'celsius'").first()["isTemporary"] == False, "Table is temporary"

-- COMMAND ----------

-- MAGIC %md <i18n value="1465024c-c6c9-4043-9bfa-54a5e9ad8b04"/>
-- MAGIC 
-- MAGIC 次に新しいテンポラリビューを作成しましょう。

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md <i18n value="5593edb6-0a98-4b8f-af24-4c9caa65dac2"/>
-- MAGIC 
-- MAGIC 以下のセルを実行して、結果を確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("celsius_temp"), "Table named `celsius_temp` does not exist"
-- MAGIC assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'celsius_temp'").first()["isTemporary"] == True, "Table is not temporary"

-- COMMAND ----------

-- MAGIC %md <i18n value="a89de892-1615-4f60-a8d2-0da7aebfda14"/>
-- MAGIC 
-- MAGIC 次にグローバルのテンポラリビューを登録しましょう。

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md <i18n value="4cb8cfe6-f058-48df-8807-1338907261f7"/>
-- MAGIC 
-- MAGIC 以下のセルを実行して、結果を確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("global_temp.celsius_global"), "Global temporary view named `celsius_global` does not exist"

-- COMMAND ----------

-- MAGIC %md <i18n value="dde3cbe9-3ed4-497a-b778-ca6268b57973"/>
-- MAGIC 
-- MAGIC カタログから表示するとき、ビューはテーブルと一緒に表示されます。

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md <i18n value="6cf76f7b-8577-4cfe-b3a1-dc08a9ac24de"/>
-- MAGIC 
-- MAGIC 次のことに注意：
-- MAGIC - ビューは現在のデータベースと関連付けられています。 このビューは、このデータベースにアクセスできるすべてのユーザーが利用でき、セッションの間保持されます。
-- MAGIC - テンポラリビューはどんなデータベースとも関連付けられていません。 テンポラリビューは一時的で、現在のSparkSessionでしかアクセスできません。
-- MAGIC - グローバルのテンポラリビューはカタログには表示されません。 **グローバルのテンポラリビューは常に **`global_temp`** データベース**に登録されます。  **`global_temp`** データベースは一時的ですが、クラスタのライフタイムに依存しています。しかし、このデータベースは、データベースが作成されたクラスタにアタッチされているノートブックのみがアクセスできます。

-- COMMAND ----------

SELECT * FROM global_temp.celsius_global

-- COMMAND ----------

-- MAGIC %md <i18n value="eda8a0b1-94f8-4c61-bf07-c86a016746ba"/>
-- MAGIC 
-- MAGIC これらのビューを定義したときジョブはトリガーされませんでしたが、ビューに対してクエリが実行される _度_ にジョブがトリガーされます。

-- COMMAND ----------

-- MAGIC %md <i18n value="1723a769-272c-47ca-93e8-7b3a9b0674dd"/>
-- MAGIC 
-- MAGIC ## クリーンアップ（Clean up）
-- MAGIC データベースとすべてのテーブルを削除してワークスペースを片付けます。

-- COMMAND ----------

DROP DATABASE ${da.schema_name} CASCADE

-- COMMAND ----------

-- MAGIC %md <i18n value="03511454-16d6-40e6-a62a-cd8bdb7de57d"/>
-- MAGIC 
-- MAGIC ## 概要（Synopsis）
-- MAGIC 
-- MAGIC このテーブルでは：
-- MAGIC - データベースを作成して削除しました
-- MAGIC - マネージドテーブルと外部テーブルの動作を調べました
-- MAGIC - ビューの範囲について学びました

-- COMMAND ----------

-- MAGIC %md <i18n value="de458d67-efe9-4d5e-87d7-240093072332"/>
-- MAGIC 
-- MAGIC 次のセルを実行して、このレッスンに関連するテーブルとファイルを削除してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
