# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # 構造化ストリーミングとDelta Lakeを使った増分更新を伝播する（Propagating Incremental Updates with Structured Streaming and Delta Lake）
# MAGIC 
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このラボでは、以下のことが学べます。
# MAGIC * 構造化ストリーミングとAuto Loaderの知識を応用して、シンプルなマルチホップアーキテクチャを実装する

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## セットアップ（Setup）
# MAGIC 次のスクリプトを実行して必要な変数をセットアップし、このノートブックにおける過去の実行を消去します。 このセルを再実行するとラボを再起動できる点に注意してください。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-7.2L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## データを取り込む（Ingest data）
# MAGIC 
# MAGIC このラボでは、*/databricks-datasets/retail-org/customers/*にあるDBFSの顧客関連CSVデータのコレクションを使います。
# MAGIC 
# MAGIC スキーマ推論を使ってAuto Loaderでこのデータを読み取ります ( **`customers_checkpoint_path`** を使ってスキーマ情報を格納する)。  **`bronze`** というDeltaテーブルに未加工のデータをストリームします。

# COMMAND ----------

# TODO
customers_checkpoint_path = f"{DA.paths.checkpoints}/customers"

query = (spark
  .readStream
  <FILL-IN>
  .load("/databricks-datasets/retail-org/customers/")
  .writeStream
  <FILL-IN>
  .table("bronze")
)

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC SQLを使って変換が実行できるように、ブロンズテーブルにストリーミングテンポラリビューを作成しましょう。

# COMMAND ----------

(spark
  .readStream
  .table("bronze")
  .createOrReplaceTempView("bronze_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## データのクリーンアップと強化（Clean and enhance data）
# MAGIC 
# MAGIC CTAS構文を使って、以下を行う **`bronze_enhanced_temp`** という新しいストリーミングビューを定義します。
# MAGIC * NULL値 **`postcode`** （ゼロに設定）でレコードをスキップする
# MAGIC * 現在のタイムスタンプを含む **`receipt_time`** という列を挿入する
# MAGIC * 入力ファイル名を含む **`source_file`** という列を挿入する

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC CREATE OR REPLACE TEMPORARY VIEW bronze_enhanced_temp AS
# MAGIC SELECT
# MAGIC   <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## シルバーテーブル（Silver table）
# MAGIC 
# MAGIC データを **`bronze_enhanced_temp`** から **`silver`** というテーブルにストリームします。

# COMMAND ----------

# TODO
silver_checkpoint_path = f"{DA.paths.checkpoints}/silver"

query = (spark.table("bronze_enhanced_temp")
  <FILL-IN>
  .table("silver"))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC SQLを使ってビジネスレベルの集計を実行できるように、シルバーテーブルにストリーミングテンポラリビューを作成しましょう。

# COMMAND ----------

(spark
  .readStream
  .table("silver")
  .createOrReplaceTempView("silver_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## ゴールドテーブル（Gold tables）
# MAGIC 
# MAGIC CTAS構文を使って、州別顧客数をカウントする **`customer_count_temp`** というストリーミングビューを定義します。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_count_temp AS
# MAGIC SELECT 
# MAGIC <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 最後に、データを **`customer_count_by_state_temp`** ビューから **`gold_customer_count_by_state`** というDeltaテーブルにストリームします。

# COMMAND ----------

# TODO
customers_count_checkpoint_path = f"{DA.paths.checkpoints}/customers_counts"

query = (spark
  .table("customer_count_temp")
  .writeStream
  <FILL-IN>
  .table("gold_customer_count_by_state"))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## 結果を照会する（Query the results）
# MAGIC 
# MAGIC  **`gold_customer_count_by_state`** テーブルを照会します（これはストリーミングクエリではありません）。 結果を棒グラフとしてプロットし、マッププロットを使用してプロットします。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_customer_count_by_state

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## まとめ（Wrapping Up）
# MAGIC 
# MAGIC 次のセルを実行して、このラボに関連するデータベースと全てのデータを削除します。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC 
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
