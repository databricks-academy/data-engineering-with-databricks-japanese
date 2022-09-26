-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
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

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ### はじめる（Getting Started）
-- MAGIC 
-- MAGIC 次のセルを実行してこのレッスン用の変数とデータセットを設定します。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-3.3L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
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
FROM parquet.`${da.paths.working_dir}/weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## データベースの作成（Create a Database）
-- MAGIC 
-- MAGIC セットアップスクリプトで定義されている **`da.db_name`** 変数を使用してデフォルトの場所にデータベースを作成します。

-- COMMAND ----------

-- TODO

<FILL-IN> ${da.db_name}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 新しいデータベースに切り替える（Change to Your New Database）
-- MAGIC 
-- MAGIC 新しく作成したデータベースを **`USE`** します。

-- COMMAND ----------

-- TODO

<FILL-IN> ${da.db_name}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## マネージドテーブルの作成（Create a Managed Table）
-- MAGIC CTAS文を使用して **`weather_managed`** というのマネージドテーブルを作成します。

-- COMMAND ----------

-- TODO

<FILL-IN>
SELECT * 
FROM parquet.`${da.paths.working_dir}/weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 外部テーブルの作成（Create an External Table）
-- MAGIC 
-- MAGIC 外部テーブルとマネージドテーブルの違いは場所の指定の有無です。 以下に **`weather_external`** というの外部テーブルを作成します。

-- COMMAND ----------

-- TODO

<FILL-IN>
LOCATION "${da.paths.working_dir}/lab/external"
AS SELECT * 
FROM parquet.`${da.paths.working_dir}/weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## テーブルの詳細を調べる（Examine Table Details）
-- MAGIC  **`DESCRIBE EXTENDED table_name`** というSQLコマンドを使用して2つの天気テーブルを調べます。

-- COMMAND ----------

DESCRIBE EXTENDED weather_managed

-- COMMAND ----------

DESCRIBE EXTENDED weather_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
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

-- MAGIC %md
-- MAGIC 
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

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ### データベースとすべてのテーブルを削除したらディレクトリの中身を確認する（Check Directory Contents after Dropping Database and All Tables）
-- MAGIC これは **`CASCADE`** キーワードを使用して実行できます。

-- COMMAND ----------

-- TODO

<FILL_IN> ${da.db_name}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
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

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC **これはマネージドテーブルと外部テーブルの主な違いを示します。**デフォルトでは、マネージドテーブルに関連付けられているファイルは、ワークスペースにリンクされているDBFSストレージのルート上のこの場所に保存され、テーブルが削除されたときに削除されます。
-- MAGIC 
-- MAGIC 外部テーブルのファイルは、テーブル作成時に指定された場所に保持され、基礎ファイルを誤って削除してしまうことを防ぎます。 **外部テーブルは簡単に他のデータベースに移行させたり名前変更をしたりできますが、マネージドテーブルでこれらの操作を実行した場合は≪すべて≫の基礎ファイルの上書きが必要となります。**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## パスが指定されているデータベースを作成する（Create a Database with a Specified Path）
-- MAGIC 
-- MAGIC 前のステップでデータベースを削除した場合は、同じ **`データベース`** の名前を使用できます。

-- COMMAND ----------

CREATE DATABASE ${da.db_name} LOCATION '${da.paths.working_dir}/${da.db_name}';
USE ${da.db_name};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC この新しいデータベースに **`weather_managed`** テーブルを再作成して、このテーブルの場所を表示します。

-- COMMAND ----------

-- TODO

<FILL_IN>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC getTableLocation("weather_managed")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ここでは、DBFSルート上に作成された **`userhome`** ディレクトリを使用していますが、データベースディレクトリとして_どんなオブジェクトストアも_使用できます。 **ユーザーのグループ用にデータベースディレクトリを定義すると、誤ったデータ漏洩の確率を大幅に下げられます**。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## ビューとその範囲（Views and their Scoping）
-- MAGIC 
-- MAGIC 用意されている **`AS`** 句を使用して次のものを登録します：
-- MAGIC -  **`celsius`** というのビュー
-- MAGIC -  **`celsius_temp`** というのテンポラリビュー
-- MAGIC -  **`celsius_global`** というのグローバルテンポラリビュー

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 次に新しいテンポラリビューを作成しましょう。

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 次にグローバルのテンポラリビューを登録しましょう。

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC カタログから表示するとき、ビューはテーブルと一緒に表示されます。

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 次のことに注意：
-- MAGIC - ビューは現在のデータベースと関連付けられています。 このビューは、このデータベースにアクセスできるすべてのユーザーが利用でき、セッションの間保持されます。
-- MAGIC - テンポラリビューはどんなデータベースとも関連付けられていません。 テンポラリビューは一時的で、現在のSparkSessionでしかアクセスできません。
-- MAGIC - グローバルのテンポラリビューはカタログには表示されません。 **グローバルのテンポラリビューは常に **`global_temp`** データベース**に登録されます。  **`global_temp`** データベースは一時的ですが、クラスタのライフタイムに依存しています。しかし、このデータベースは、データベースが作成されたクラスタにアタッチされているノートブックのみがアクセスできます。

-- COMMAND ----------

SELECT * FROM global_temp.celsius_global

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC これらのビューを定義したときジョブはトリガーされませんでしたが、ビューに対してクエリが実行される _度_ にジョブがトリガーされます。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## クリーンアップ（Clean up）
-- MAGIC データベースとすべてのテーブルを削除してワークスペースを片付けます。

-- COMMAND ----------

DROP DATABASE ${da.db_name} CASCADE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 概要（Synopsis）
-- MAGIC 
-- MAGIC このテーブルでは：
-- MAGIC - データベースを作成して削除しました
-- MAGIC - マネージドテーブルと外部テーブルの動作を調べました
-- MAGIC - ビューの範囲について学びました

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
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
