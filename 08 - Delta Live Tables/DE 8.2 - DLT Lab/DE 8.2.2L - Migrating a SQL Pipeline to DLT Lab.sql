-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="cffedc4f-a6a4-4412-8193-46a7a8a6a213"/>
# ラボ：SQLパイプラインをDelta Live Tablesに移行する（Lab: Migrating a SQL Pipeline to Delta Live Tables）

このノートブックは、SQLを使ってDLTを実装し、あなたが完了させるものです。

これはインタラクティブに実行することを**意図しておらず**、むしろ一度変更を完了したらパイプラインとしてデプロイすることを目的としています。

このノートブックを完成させるためには、<a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-language-ref.html#sql" target="_blank">DLT構文の文書化</a>を参照してください。

-- COMMAND ----------

-- MAGIC %md <i18n value="01e06565-56e7-4581-8833-14bc0db8c281"/>
## ブロンズテーブルを宣言する（Declare Bronze Table）

シミュレートされたクラウドソースから（Auto Loaderを用いて）JSONデータを段階的に取り込むブロンズテーブル **`recordings_bronze`** を宣言します。 ソースの場所はすでに引数として提供されています。この値の使い方は以下のセルに示しています。

以前と同様に、2つの追加列を含みます。
*  **`current_timestamp()`** によって返されるタイムスタンプを記録する **`receipt_time`** 
*  **`input_file_name()`** によって取得される **`source_file`**

-- COMMAND ----------

-- TODO
CREATE <FILL-IN>
AS SELECT <FILL-IN>
  FROM cloud_files("${source}", "json", map("cloudFiles.schemaHints", "time DOUBLE, mrn INTEGER"))

-- COMMAND ----------

-- MAGIC %md <i18n value="57422f74-b830-4abb-b4a9-969d0ab90be6"/>
### PIIファイル（PII File）

同じようなCTAS構文を使用して、 *healthcare/patient* にあるCSVデータにライブ**テーブル**を作成します。

このソースのAuto Loaderを適切に構成するために、次の追加パラメーターを指定する必要があります。

| オプション                             | 値          |
| --------------------------------- | ---------- |
|  **`header`**                       |  **`true`**  |
|  **`cloudFiles.inferColumnTypes`**  |  **`true`**  |

<img src="https://files.training.databricks.com/images/icon_note_24.png" /> CSV用のAuto Loader構成は<a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-csv.html" target="_blank">こちら</a>を参照してください。

-- COMMAND ----------

-- TODO
CREATE <FILL-IN> pii
AS SELECT *
  FROM cloud_files("${datasets_path}/healthcare/patient", "csv", map(<FILL-IN>))

-- COMMAND ----------

-- MAGIC %md <i18n value="3573b6a4-233a-4f23-a002-aab072eb5096"/>
## シルバーテーブルを宣言する（Declare Silver Tables）

 **`recordings_parsed`** のシルバーテーブルは、以下のフィールドで構成されます。

| フィールド           | 型                      |
| --------------- | ---------------------- |
|  **`device_id`**  |  **`INTEGER`**           |
|  **`mrn`**        |  **`LONG`**              |
|  **`heartrate`**  |  **`DOUBLE`**            |
|  **`time`**       |  **`TIMESTAMP`** （下に例あり） |
|  **`name`**       |  **`STRING`**            |

また、このクエリでは、共通の **`mrn`** フィールドで **`pii`** テーブルとinner joinを行って名前を取得し、データをエンリッチ化します。

無効な **`heartrate`** （つまり、ゼロ以下の数値）を持つレコードを削除する制約を適用することで、品質管理を実装します。

-- COMMAND ----------

-- TODO
CREATE OR REFRESH STREAMING LIVE TABLE recordings_enriched
  (<FILL-IN add a constraint to drop records when heartrate ! > 0>)
AS SELECT 
  CAST(<FILL-IN>) device_id, 
  <FILL-IN mrn>, 
  <FILL-IN heartrate>, 
  CAST(FROM_UNIXTIME(DOUBLE(time), 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) time 
  FROM STREAM(live.recordings_bronze)
  <FILL-IN specify an inner join with the pii table on the mrn field>

-- COMMAND ----------

-- MAGIC %md <i18n value="3b9309a8-9e1d-46a2-a0eb-e95fe698d23b"/>
## ゴールドテーブル（Gold table）

ゴールドテーブル **`daily_patient_avg`** を作成します。このテーブルは、  **`mrn`** 、 **`name`** 、 **`date`** で **`recordings_enriched`** を集約し、以下のような列を作成します。

| 列名                  | 値                           |
| ------------------- | --------------------------- |
|  **`mrn`**            | ソースからの **`mrn`**              |
|  **`name`**           | ソースからの **`name`**             |
|  **`avg_heartrate`**  | グループ化による平均 **`heartrate`**  f |
|  **`date`**           |  **`time`** から抽出された日付         |

-- COMMAND ----------

-- TODO
CREATE <FILL-IN> daily_patient_avg
  COMMENT <FILL-IN insert comment here>
AS SELECT <FILL-IN>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
