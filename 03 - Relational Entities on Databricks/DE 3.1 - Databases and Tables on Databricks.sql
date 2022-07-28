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
-- MAGIC # Databricks上のデータベースとテーブル（Databases and Tables on Databricks）
-- MAGIC このデモンストレーションでは、データベースとテーブルを作成して調べます。
-- MAGIC 
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このレッスンでは、以下のことが学べます。
-- MAGIC * Spark SQL DDLを使用してデータベースとテーブルを定義する
-- MAGIC *  **`LOCATION`** キーワードがデフォルトのストレージディレクトリにどのような影響を与えるかを説明する
-- MAGIC 
-- MAGIC 
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
-- MAGIC ## レッスンのセットアップ（Lesson Setup）
-- MAGIC 次のスクリプトは、このデモの以前の実行をクリアして、SQLクエリで使用するHive変数を設定します。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-3.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Hive変数を使用する（Using Hive Variables）
-- MAGIC 
-- MAGIC このパターンは基本、Spark SQLではお勧めしませんが、このノートブックでは現在のユーザーのアカウントのメールアドレスから得られた文字列を置き換えるためにHive変数を使用します。
-- MAGIC 
-- MAGIC 次のセルはこのパターンを示しています。

-- COMMAND ----------

SELECT "${da.db_name}" AS db_name,
       "${da.paths.working_dir}" AS working_dir

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 共有ワークスペースで作業している可能性があるため、データベースが他のユーザーと競合しないようにするためにこのコースでは、あなたのユーザー名から得られた変数を使用します。 繰り返しますが、このHive変数の使用は、開発のための良い習慣というよりはレッスン環境のための裏技だと考えてください。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## データベース（Databases）
-- MAGIC はじめに2つのデータベースを作成しましょう：
-- MAGIC -  **`LOCATION`** 指定するデータベース
-- MAGIC -  **`LOCATION`** を指定しないデータベース

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ${da.db_name}_default_location;
CREATE DATABASE IF NOT EXISTS ${da.db_name}_custom_location LOCATION '${da.paths.working_dir}/_custom_location.db';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 最初のデータベースの場所は、 **`dbfs:/user/hive/warehouse/`** にあるデフォルトの場所で、データベースディレクトリは **`.db`** の拡張子が付いているデータベースの名前であることにご注意ください。

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED ${da.db_name}_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 2個目のデータベースの場所は、 **`LOCATION`** キーワードの後ろに指定されているディレクトリであることにご注意ください。

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED ${da.db_name}_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC デフォルトの場所を使用してデータベースにテーブルを作成して、データを挿入します。
-- MAGIC 
-- MAGIC スキーマを推測するためのデータがないため、スキーマを指定する必要があることにご注意ください。

-- COMMAND ----------

USE ${da.db_name}_default_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_default_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_default_location 
VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルの詳細な説明で（結果で下にスクロール）場所を見つけられます。

-- COMMAND ----------

DESCRIBE EXTENDED managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC デフォルトでは、場所を指定しないデータベースのマネージドテーブルは **`dbfs:/user/hive/warehouse/<database_name>.db/`** ディレクトリに作成されます。
-- MAGIC 
-- MAGIC Deltaテーブルのデータとメタデータは予想通り、その場所に保存されていることが分かります。

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC hive_root =  f"dbfs:/user/hive/warehouse"
-- MAGIC db_name =    f"{DA.db_name}_default_location.db"
-- MAGIC table_name = f"managed_table_in_db_with_default_location"
-- MAGIC 
-- MAGIC tbl_location = f"{hive_root}/{db_name}/{table_name}"
-- MAGIC print(tbl_location)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルを削除します。

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルのディレクトリとそのログ、およびデータのファイルが削除されていることにご注意ください。 データベースディレクトリのみが残りました。

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC db_location = f"{hive_root}/{db_name}"
-- MAGIC print(db_location)
-- MAGIC dbutils.fs.ls(db_location)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC カスタムの場所を使用してデータベースにテーブルを作成し、データを挿入します。
-- MAGIC 
-- MAGIC スキーマを推測するためのデータがないため、スキーマを指定する必要があることにご注意ください。

-- COMMAND ----------

USE ${da.db_name}_custom_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_custom_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_custom_location VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 再び説明でテーブルの場所を見つけます。

-- COMMAND ----------

DESCRIBE EXTENDED managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 予想どおり、このマネージドテーブルは、データベース作成時に **`LOCATION`** キーワードで指定されたパスに作成されました。 したがって、テーブルのデータとメタデータはこちらのディレクトリに保持されます。

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC table_name = f"managed_table_in_db_with_custom_location"
-- MAGIC tbl_location =   f"{DA.paths.working_dir}/_custom_location.db/{table_name}"
-- MAGIC print(tbl_location)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルを削除しましょう。

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルのフォルダとログファイル、データファイルが削除されていることにご注意ください。
-- MAGIC 
-- MAGIC データベースの場所のみが残りました。

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC db_location =   f"{DA.paths.working_dir}/_custom_location.db"
-- MAGIC print(db_location)
-- MAGIC 
-- MAGIC dbutils.fs.ls(db_location)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## テーブル（Tables）
-- MAGIC サンプルデータを使用して外部（アンマネージド）テーブルを作成します。
-- MAGIC 
-- MAGIC CSV形式のデータを使用します。 好きなディレクトリに指定した **`LOCATION`** のDeltaテーブルを作成します。

-- COMMAND ----------

USE ${da.db_name}_default_location;

CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${da.paths.working_dir}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);
CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
  SELECT * FROM temp_delays;

SELECT * FROM external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC このレッスンの作業ディレクトリのテーブルデータの場所にご注意ください。

-- COMMAND ----------

DESCRIBE TABLE EXTENDED external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルを削除しましょう。

-- COMMAND ----------

DROP TABLE external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルの定義はもうメタストアには存在しませんが、その元になっているデータは残っています。

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_path = f"{DA.paths.working_dir}/external_table"
-- MAGIC files = dbutils.fs.ls(tbl_path)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## クリーンアップ（Clean up）
-- MAGIC 両方のデータベースを削除します。

-- COMMAND ----------

DROP DATABASE ${da.db_name}_default_location CASCADE;
DROP DATABASE ${da.db_name}_custom_location CASCADE;

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
