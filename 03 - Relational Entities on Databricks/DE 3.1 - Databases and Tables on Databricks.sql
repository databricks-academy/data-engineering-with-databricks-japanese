-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="731b610a-2018-40a2-8eae-f6f01ae7a788"/>
-- MAGIC 
-- MAGIC # Databricks上のデータベースとテーブル（Databases and Tables on Databricks）
-- MAGIC このデモンストレーションでは、データベースとテーブルを作成して調べます。
-- MAGIC 
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このレッスンでは、以下のことが学べます。
-- MAGIC * Spark SQL DDLを使用してスキーマとテーブルを定義する
-- MAGIC *  **`LOCATION`** キーワードがデフォルトのストレージディレクトリにどのような影響を与えるかを説明する
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC **リソース**
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">スキーマとテーブル - Databricksドキュメント</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">マネージドテーブルおよびアンマネージドテーブル</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">UIを使用したテーブル作成</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">ローカルテーブルの作成</a>
-- MAGIC * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">永続的テーブルへの保存</a>

-- COMMAND ----------

-- MAGIC %md <i18n value="10b2fb72-8534-4903-98a1-26716350dd20"/>
-- MAGIC 
-- MAGIC ## レッスンのセットアップ（Lesson Setup）
-- MAGIC 次のスクリプトは、このデモの以前の実行をクリアして、SQLクエリで使用するHive変数を設定します。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-03.1

-- COMMAND ----------

-- MAGIC %md <i18n value="1cbf441b-a62f-4202-af2a-677d37a598b2"/>
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

-- MAGIC %md <i18n value="014c9f3d-ffd0-48b8-989e-b80b2568d642"/>
-- MAGIC 
-- MAGIC 共有ワークスペースで作業している可能性があるため、スキーマが他のユーザーと競合しないようにするためにこのコースでは、あなたのユーザー名から得られた変数を使用します。 繰り返しますが、このHive変数の使用は、開発のための良い習慣というよりはレッスン環境のための裏技だと考えてください。

-- COMMAND ----------

-- MAGIC %md <i18n value="ff022f79-7f38-47ea-809e-537cf00526d0"/>
-- MAGIC 
-- MAGIC ## スキーマ（Schemas）
-- MAGIC はじめに2つのスキーマを作成しましょう：
-- MAGIC -  **`LOCATION`** 指定するスキーマ
-- MAGIC -  **`LOCATION`** を指定しないスキーマ

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${da.db_name}_default_location;
CREATE SCHEMA IF NOT EXISTS ${da.db_name}_custom_location LOCATION '${da.paths.working_dir}/_custom_location.db';

-- COMMAND ----------

-- MAGIC %md <i18n value="4eff4961-9de3-4d5d-836e-cc48862ef4e6"/>
-- MAGIC 
-- MAGIC 最初のスキーマの場所は、 **`dbfs:/user/hive/warehouse/`** にあるデフォルトの場所で、スキーマのディレクトリは **`.db`** の拡張子が付いているスキーマの名前であることにご注意ください。

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED ${da.db_name}_default_location;

-- COMMAND ----------

-- MAGIC %md <i18n value="58292139-abd2-453b-b327-9ec2ab76dd0a"/>
-- MAGIC 
-- MAGIC 2個目のスキーマの場所は、 **`LOCATION`** キーワードの後ろに指定されているディレクトリであることにご注意ください。

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED ${da.db_name}_custom_location;

-- COMMAND ----------

-- MAGIC %md <i18n value="d794ab19-e4e8-4f5c-b784-385ac7c27bc2"/>
-- MAGIC 
-- MAGIC デフォルトの場所を使用してスキーマにテーブルを作成して、データを挿入します。
-- MAGIC 
-- MAGIC スキーマを推測するためのデータがないため、スキーマを指定する必要があることにご注意ください。

-- COMMAND ----------

USE ${da.db_name}_default_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_default_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_default_location 
VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md <i18n value="17403d69-25b1-44d5-b37f-bab7c091a01b"/>
-- MAGIC 
-- MAGIC テーブルの詳細な説明で（結果で下にスクロール）場所を見つけられます。

-- COMMAND ----------

DESCRIBE DETAIL managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md <i18n value="71f3a626-a3d4-48a6-8489-6c9cffd021fc"/>
-- MAGIC 
-- MAGIC デフォルトでは、場所を指定しないスキーマのマネージドテーブルは **`dbfs:/user/hive/warehouse/<database_name>.db/`** ディレクトリに作成されます。
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

-- MAGIC %md <i18n value="ff92a2d3-9bf0-45d0-b78a-c25638ab9479"/>
-- MAGIC 
-- MAGIC テーブルを削除します。

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md <i18n value="e9c2d161-c157-4d67-8b8d-dbd3d89b6460"/>
-- MAGIC 
-- MAGIC テーブルのディレクトリとそのログ、およびデータのファイルが削除されていることにご注意ください。 スキーマのディレクトリのみが残りました。

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC db_location = f"{hive_root}/{db_name}"
-- MAGIC print(db_location)
-- MAGIC dbutils.fs.ls(db_location)

-- COMMAND ----------

-- MAGIC %md <i18n value="bd185ea7-cd88-4453-a77a-1babe4633451"/>
-- MAGIC 
-- MAGIC カスタムの場所を使用してスキーマにテーブルを作成し、データを挿入します。
-- MAGIC 
-- MAGIC スキーマを推測するためのデータがないため、スキーマを指定する必要があることにご注意ください。

-- COMMAND ----------

USE ${da.db_name}_custom_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_custom_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_custom_location VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md <i18n value="68e86e08-9400-428d-9c56-d47439af7dff"/>
-- MAGIC 
-- MAGIC 再び説明でテーブルの場所を見つけます。

-- COMMAND ----------

DESCRIBE DETAIL managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md <i18n value="878787b3-1178-44d1-a775-0bcd6c483184"/>
-- MAGIC 
-- MAGIC 予想どおり、このマネージドテーブルは、スキーマ作成時に **`LOCATION`** キーワードで指定されたパスに作成されました。 したがって、テーブルのデータとメタデータはこちらのディレクトリに保持されます。

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

-- MAGIC %md <i18n value="699d9cda-0276-4d93-bf8c-5e1d370ce113"/>
-- MAGIC 
-- MAGIC テーブルを削除しましょう。

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md <i18n value="c87c1801-0101-4378-9f52-9a8d052a38e1"/>
-- MAGIC 
-- MAGIC テーブルのフォルダとログファイル、データファイルが削除されていることにご注意ください。
-- MAGIC 
-- MAGIC スキーマの場所のみが残りました。

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC db_location =   f"{DA.paths.working_dir}/_custom_location.db"
-- MAGIC print(db_location)
-- MAGIC 
-- MAGIC dbutils.fs.ls(db_location)

-- COMMAND ----------

-- MAGIC %md <i18n value="67fd15cf-0ca9-4e76-8806-f24c60d324b1"/>
-- MAGIC 
-- MAGIC ## テーブル（Tables）
-- MAGIC サンプルデータを使用して外部（アンマネージド）テーブルを作成します。
-- MAGIC 
-- MAGIC CSV形式のデータを使用します。 好きなディレクトリに指定した **`LOCATION`** のDeltaテーブルを作成します。

-- COMMAND ----------

USE ${da.db_name}_default_location;

CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${DA.paths.datasets}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);
CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
  SELECT * FROM temp_delays;

SELECT * FROM external_table;

-- COMMAND ----------

-- MAGIC %md <i18n value="367720a7-b738-4782-8f42-571b522c95c2"/>
-- MAGIC 
-- MAGIC このレッスンの作業ディレクトリのテーブルデータの場所にご注意ください。

-- COMMAND ----------

DESCRIBE TABLE EXTENDED external_table;

-- COMMAND ----------

-- MAGIC %md <i18n value="3267ab86-f8fe-40dc-aa52-44aecf8d8fc1"/>
-- MAGIC 
-- MAGIC テーブルを削除しましょう。

-- COMMAND ----------

DROP TABLE external_table;

-- COMMAND ----------

-- MAGIC %md <i18n value="b9b3c493-3a09-4fdb-9615-1e8c56824b12"/>
-- MAGIC 
-- MAGIC テーブルの定義はもうメタストアには存在しませんが、その元になっているデータは残っています。

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_path = f"{DA.paths.working_dir}/external_table"
-- MAGIC files = dbutils.fs.ls(tbl_path)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md <i18n value="c456ac65-ab0b-435a-ae00-acbde5048a96"/>
-- MAGIC 
-- MAGIC ## クリーンアップ（Clean up）
-- MAGIC 両方のスキーマを削除します。

-- COMMAND ----------

DROP SCHEMA ${da.db_name}_default_location CASCADE;
DROP SCHEMA ${da.db_name}_custom_location CASCADE;

-- COMMAND ----------

-- MAGIC %md <i18n value="6fa204d5-12ff-4ede-9fe1-871a346052c4"/>
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
