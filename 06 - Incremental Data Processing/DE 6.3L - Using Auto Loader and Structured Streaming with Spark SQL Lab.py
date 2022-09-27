# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="e6dedae8-1335-494e-acdf-4a1906f8c826"/>
# MAGIC 
# MAGIC # Spark SQLを用いたAuto Loaderと構造化ストリーミングを使用する（Using Auto Loader and Structured Streaming with Spark SQL）
# MAGIC 
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このラボでは、以下のことが学べます。
# MAGIC * Auto Loaderを利用してデータを取り込む
# MAGIC * ストリーミングデータを集約する
# MAGIC * Deltaテーブルにデータをストリームする

# COMMAND ----------

# MAGIC %md <i18n value="ab5018b7-17b9-4f66-a32d-9c86860f6f30"/>
# MAGIC 
# MAGIC ## セットアップ（Setup）
# MAGIC 次のスクリプトを実行して必要な変数をセットアップし、このノートブックにおける過去の実行を消去します。 このセルを再実行するとラボを再起動できる点に注意してください。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-06.3L

# COMMAND ----------

# MAGIC %md <i18n value="03347519-151b-4304-8cda-1cbd91af0737"/>
# MAGIC 
# MAGIC ## ストリーミング読み取りを構成する（Configure Streaming Read）
# MAGIC 
# MAGIC このラボでは、 **retail-org/customers** の顧客関連csvデータのコレクションを使います。
# MAGIC 
# MAGIC スキーマ推論を使って<a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a>でこのデータを読み取ります（ **`customers_checkpoint_path`** を使ってスキーマ情報を格納する）。  **`customers_raw_temp`** というストリーミングテンポラリビューを作成します。

# COMMAND ----------

# TODO
dataset_source = f"{DA.paths.datasets}/retail-org/customers/"
customers_checkpoint_path = f"{DA.paths.checkpoints}/customers"

(spark
  .readStream
  <FILL-IN>
  .load(dataset_source)
  .createOrReplaceTempView("customers_raw_temp"))

# COMMAND ----------

from pyspark.sql import Row
assert Row(tableName="customers_raw_temp", isTemporary=True) in spark.sql("show tables").select("tableName", "isTemporary").collect(), "Table not present or not temporary"
assert spark.table("customers_raw_temp").dtypes ==  [('customer_id', 'string'),
 ('tax_id', 'string'),
 ('tax_code', 'string'),
 ('customer_name', 'string'),
 ('state', 'string'),
 ('city', 'string'),
 ('postcode', 'string'),
 ('street', 'string'),
 ('number', 'string'),
 ('unit', 'string'),
 ('region', 'string'),
 ('district', 'string'),
 ('lon', 'string'),
 ('lat', 'string'),
 ('ship_to_address', 'string'),
 ('valid_from', 'string'),
 ('valid_to', 'string'),
 ('units_purchased', 'string'),
 ('loyalty_segment', 'string'),
 ('_rescued_data', 'string')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md <i18n value="4582665f-8192-4751-83f8-8ae1a4d55f22"/>
# MAGIC 
# MAGIC ## ストリーミング集約を定義する（Define a streaming aggregation）
# MAGIC 
# MAGIC CTAS構文を使って、 **`customer_count`** のフィールドにおける **`state`** ごとの顧客数をカウントする **`customer_count_by_state_temp`** という新しいストリーミングビューを定義します。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_count_by_state_temp AS
# MAGIC SELECT
# MAGIC   <FILL-IN>

# COMMAND ----------

assert Row(tableName="customer_count_by_state_temp", isTemporary=True) in spark.sql("show tables").select("tableName", "isTemporary").collect(), "Table not present or not temporary"
assert spark.table("customer_count_by_state_temp").dtypes == [('state', 'string'), ('customer_count', 'bigint')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md <i18n value="bef919d7-d681-4233-8da5-39ca94c49a8b"/>
# MAGIC 
# MAGIC ## 集約されたデータをDeltaテーブルに書き込む（Write aggregated data to a Delta table）
# MAGIC 
# MAGIC データを **`customer_count_by_state_temp`** ビューから **`customer_count_by_state`** というDeltaテーブルにストリームします。

# COMMAND ----------

# TODO
customers_count_checkpoint_path = f"{DA.paths.checkpoints}/customers_count"

query = (spark
  <FILL-IN>

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

assert Row(tableName="customer_count_by_state", isTemporary=False) in spark.sql("show tables").select("tableName", "isTemporary").collect(), "Table not present or not temporary"
assert spark.table("customer_count_by_state").dtypes == [('state', 'string'), ('customer_count', 'bigint')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md <i18n value="f74f262f-10c4-4f2f-84d6-f69e56c54ac6"/>
# MAGIC 
# MAGIC ## 結果を照会する（Query the results）
# MAGIC 
# MAGIC  **`customer_count_by_state`** テーブルを照会します（これはストリーミングクエリではありません）。 結果を棒グラフとしてプロットし、マッププロットを使用してプロットします。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO

# COMMAND ----------

# MAGIC %md <i18n value="e2cf644d-96f9-47f7-ad81-780125d3ad4b"/>
# MAGIC 
# MAGIC ## まとめ（Wrapping Up）
# MAGIC 
# MAGIC 次のセルを実行して、このラボに関連するデータベースと全てのデータを削除します。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md <i18n value="8f3c4c52-b5d9-4f8a-974c-ce5db6430c43"/>
# MAGIC 
# MAGIC このラボでは次のことを学びました。
# MAGIC * PySparkを使用して、増分データの取り込み用Auto Loaderを構成する
# MAGIC * Spark SQLを使用して、ストリーミングデータを集約する
# MAGIC * Deltaテーブルにデータをストリーミングする

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
