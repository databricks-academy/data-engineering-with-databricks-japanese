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
-- MAGIC # ラボ：SQLパイプラインをDelta Live Tablesに移行する（Lab: Migrating a SQL Pipeline to Delta Live Tables）
-- MAGIC 
-- MAGIC このノートブックは、SQLを使ってDLTを実装し、あなたが完了させるものです。
-- MAGIC 
-- MAGIC これはインタラクティブに実行することを**意図しておらず**、むしろ一度変更を完了したらパイプラインとしてデプロイすることを目的としています。
-- MAGIC 
-- MAGIC このノートブックを完成させるためには、<a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-language-ref.html#sql" target="_blank">DLT構文の文書化</a>を参照してください。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## ブロンズテーブルを宣言する（Declare Bronze Table）
-- MAGIC 
-- MAGIC シミュレートされたクラウドソースから（Auto Loaderを用いて）JSONデータを段階的に取り込むブロンズテーブルを宣言します。 ソースの場所はすでに引数として提供されています。この値の使い方は以下のセルに示しています。
-- MAGIC 
-- MAGIC 以前と同様に、2つの追加列を含みます。
-- MAGIC *  **`current_timestamp()`** によって返されるタイムスタンプを記録する **`receipt_time`** 
-- MAGIC *  **`input_file_name()`** によって取得される **`source_file`** 

-- COMMAND ----------

-- ANSWER
CREATE OR REFRESH STREAMING LIVE TABLE recordings_bronze
AS SELECT current_timestamp() receipt_time, input_file_name() source_file, *
  FROM cloud_files("${source}", "json", map("cloudFiles.schemaHints", "time DOUBLE"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ### PIIファイル（PII File）
-- MAGIC 
-- MAGIC 同じようなCTAS構文を使用して、 */mnt/training/healthcare/patient* にあるCSVデータにライブ**テーブル**を作成します。
-- MAGIC 
-- MAGIC このソースのAuto Loaderを適切に構成するために、次の追加パラメーターを指定する必要があります。
-- MAGIC 
-- MAGIC | オプション                             | 値          |
-- MAGIC | --------------------------------- | ---------- |
-- MAGIC |  **`header`**                       |  **`true`**  |
-- MAGIC |  **`cloudFiles.inferColumnTypes`**  |  **`true`**  |
-- MAGIC 
-- MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png" /> CSV用のAuto Loader構成は<a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-csv.html" target="_blank">こちら</a>を参照してください。

-- COMMAND ----------

-- ANSWER
CREATE OR REFRESH STREAMING LIVE TABLE pii
AS SELECT *
  FROM cloud_files("/mnt/training/healthcare/patient", "csv", map("header", "true", "cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## シルバーテーブルを宣言する（Declare Silver Tables）
-- MAGIC 
-- MAGIC  **`recordings_parsed`** のシルバーテーブルは、以下のフィールドで構成されます。
-- MAGIC 
-- MAGIC | フィールド           | 型                      |
-- MAGIC | --------------- | ---------------------- |
-- MAGIC |  **`device_id`**  |  **`INTEGER`**           |
-- MAGIC |  **`mrn`**        |  **`LONG`**              |
-- MAGIC |  **`heartrate`**  |  **`DOUBLE`**            |
-- MAGIC |  **`time`**       |  **`TIMESTAMP`** （下に例あり） |
-- MAGIC |  **`name`**       |  **`STRING`**            |
-- MAGIC 
-- MAGIC また、このクエリでは、共通の **`mrn`** フィールドで **`pii`** テーブルとinner joinを行って名前を取得し、データをエンリッチ化します。
-- MAGIC 
-- MAGIC 無効な **`heartrate`** （つまり、ゼロ以下の数値）を持つレコードを削除する制約を適用することで、品質管理を実装します。

-- COMMAND ----------

-- ANSWER

CREATE OR REFRESH STREAMING LIVE TABLE recordings_enriched
  (CONSTRAINT positive_heartrate EXPECT (heartrate > 0) ON VIOLATION DROP ROW)
AS SELECT 
  CAST(a.device_id AS INTEGER) device_id, 
  CAST(a.mrn AS LONG) mrn, 
  CAST(a.heartrate AS DOUBLE) heartrate, 
  CAST(from_unixtime(a.time, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) time,
  b.name
  FROM STREAM(live.recordings_bronze) a
  INNER JOIN STREAM(live.pii) b
  ON a.mrn = b.mrn

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## ゴールドテーブル（Gold table）
-- MAGIC 
-- MAGIC ゴールドテーブル **`daily_patient_avg`** を作成します。このテーブルは、  **`mrn`** 、 **`name`** 、 **`date`** で **`recordings_enriched`** を集約し、以下のような列を作成します。
-- MAGIC 
-- MAGIC | 列名                  | 値                           |
-- MAGIC | ------------------- | --------------------------- |
-- MAGIC |  **`mrn`**            | ソースからの **`mrn`**              |
-- MAGIC |  **`name`**           | ソースからの **`name`**             |
-- MAGIC |  **`avg_heartrate`**  | グループ化による平均 **`heartrate`**  f |
-- MAGIC |  **`date`**           |  **`time`** から抽出された日付         |

-- COMMAND ----------

-- ANSWER

CREATE OR REFRESH STREAMING LIVE TABLE daily_patient_avg
  COMMENT "Daily mean heartrates by patient"
  AS SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE(time) `date`
    FROM STREAM(live.recordings_enriched)
    GROUP BY mrn, name, DATE(time)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
