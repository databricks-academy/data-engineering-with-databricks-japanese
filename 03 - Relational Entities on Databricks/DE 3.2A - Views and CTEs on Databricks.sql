-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="73b4cbc8-b2b3-4d51-8443-0280a10127e9"/>
# Databricks上のビューとCTE（Views and CTEs on Databricks）
このデモンストレーションでは、ビューと共通テーブル式（CTE）を作成して調べます。

## 学習目標（Learning Objectives）
このレッスンでは、以下のことが学べます。
* Spark SQL DDLを使用してビューを定義する
* 共通テーブル式を使用したクエリを実行する



**リソース**
* <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-view.html" target="_blank">ビューの作成 - Databricksドキュメント</a>
* <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-qry-select-cte.html" target="_blank">共通テーブル式 - Databricksドキュメント</a>

-- COMMAND ----------

-- MAGIC %md <i18n value="c297b643-5e56-4ed9-928a-b4261b206461"/>
## クラスルームのセットアップ
次のスクリプトは、このデモの以前の実行をクリアして、SQLクエリで使用するHive変数を設定します。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-03.2A

-- COMMAND ----------

-- MAGIC %md <i18n value="f94b665d-e3c5-4dc7-8f40-6e892bdbe71a"/>
はじめに、デモンストレーションに使用できるデータのテーブルを作成します。

-- COMMAND ----------

-- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
CREATE TABLE external_table
USING CSV OPTIONS (
  path = '${da.paths.datasets}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST"
);

SELECT * FROM external_table;

-- COMMAND ----------

-- MAGIC %md <i18n value="8bc49e5c-12e9-4458-90aa-88b67091f6f7"/>
テーブル（とビュー）の一覧を表示させるには **`SHOW TABLES`** コマンドを使用します。

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md <i18n value="b80b82c4-c65f-47fe-8968-4c3051f59ba1"/>
## ビュー、テンポラリビュー、およびグローバルテンポラリビュー（Views, Temp Views & Global Temp Views）

このデモンストレーションをセットアップするには、各ビューを一種類ずつ作成します。

そして、次のノートブックでは、それぞれのビューの動作の違いを見ていきます。

-- COMMAND ----------

-- MAGIC %md <i18n value="ead94707-a156-4282-9f11-b4976c39470d"/>
### ビュー（Views）
はじめに元が「ABQ」で先が「LAX」のデータのみを含むビューを作成しましょう。

-- COMMAND ----------

CREATE VIEW view_delays_abq_lax AS
  SELECT * 
  FROM external_table 
  WHERE origin = 'ABQ' AND destination = 'LAX';

SELECT * FROM view_delays_abq_lax;

-- COMMAND ----------

-- MAGIC %md <i18n value="f7cc0d7b-eb93-406a-8925-60ea057466ea"/>
**`view_delays_abq_lax`** が以下の一覧に追加されたことにご注意ください。

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md <i18n value="6badc00c-9bf4-47cb-aac8-a474d678e4f6"/>
### テンポラリビュー（Temporary Views）

次にテンポラリビューを作成しましょう。

構文はとても似ていますが、コマンドに **`TEMPORARY`** を追加します。

-- COMMAND ----------

CREATE TEMPORARY VIEW temp_view_delays_gt_120
AS SELECT * FROM external_table WHERE delay > 120 ORDER BY delay ASC;

SELECT * FROM temp_view_delays_gt_120;

-- COMMAND ----------

-- MAGIC %md <i18n value="b19e8641-b379-4bab-83e7-3aff6dacd8ec"/>
これで、テーブルをまた表示すると、1つのテーブルと両方のビューが表示されます。

 **`isTemporary`** 列の値をメモしましょう。

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md <i18n value="7ac13dd9-3f9f-4a41-8945-3405d7a1e86a"/>
### グローバルテンポラリビュー（Global Temp Views）

最後に、グローバルテンポラリビューを作成します。

ここでは単に、コマンドに **`GLOBAL`** を追加します。

次の **`SELECT`** 文にある **`global_temp`** のデータベース修飾子にもご注意ください。

-- COMMAND ----------

CREATE GLOBAL TEMPORARY VIEW global_temp_view_dist_gt_1000 
AS SELECT * FROM external_table WHERE distance > 1000;

SELECT * FROM global_temp.global_temp_view_dist_gt_1000;

-- COMMAND ----------

-- MAGIC %md <i18n value="83ab4417-60d5-4077-8947-ad53d6eb1dce"/>
先に進む前に、データベースのテーブルとビュー…

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md <i18n value="acf19ac9-f423-4ce6-85c1-e313672645e2"/>
…および **`global_temp`** データベースのテーブルとビューを確認しましょう。

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

-- MAGIC %md <i18n value="4b98c78a-c415-4a5c-a4cc-980d28e216b7"/>
次に、テーブルとビューは複数のセッションの間で永続化されるのに対し、テンポラリビューは永続化されないことを示します。

これを行うには、次のノートブック [DE 3.2B - Views and CTEs on Databricks, Cont]($./DE 3.2B - Views and CTEs on Databricks, Cont)を開いてレッスンを続けます。

<img src="https://files.training.databricks.com/images/icon_note_24.png" /> 注：新しいセッションが作成されるシナリオはいくつかあります：
* クラスタを再起動したとき
* クラスタのデタッチと再アタッチのとき
* PythonパッケージをインストールしてPythonのインタプリタが再起動されたとき
* 新しいノートブックを開いたとき

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
