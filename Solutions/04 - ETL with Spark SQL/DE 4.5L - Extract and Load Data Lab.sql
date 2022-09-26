-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="d2c3cdc2-5fcf-4edc-8101-2964a9355000"/>
# データを抽出して読み込むラボ（Extract and Load Data Lab）

このラボでは、JSONファイルから未加工のデータを抽出してDeltaテーブルに読み込みます。

## 学習目標（Learning Objectives）
このラボでは、以下のことが学べます。
- 外部テーブルを作成してJSONファイルからデータを抽出する
- スキーマを指定した空のDeltaテーブルを作成する
- 既存のテーブルからDeltaテーブルにレコードを挿入する
- CTAS文を使用してファイルからDeltaテーブルを作成する

-- COMMAND ----------

-- MAGIC %md <i18n value="e261fd97-ffd7-44b2-b1ca-61b843ee8961"/>
## セットアップを実行する（Run Setup）

次のセルを実行してこのレッスン用の変数とデータセットを設定します。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-04.5L

-- COMMAND ----------

-- MAGIC %md <i18n value="d7759322-f9b9-4abe-9b30-25f5a7e30d9c"/>
## データの概要（Overview of the Data）

JSONファイルとして書き込まれる未加工のKafkaデータのサンプルを扱っていきます。

各ファイルには、5秒の間隔で消費されるすべてのレコードが含まれています。レコードは、複数のレコードのJSONファイルとして完全なKafkaスキーマで保存されています。

テーブルのスキーマ：

| フィールド     | 型       | 説明                                                                      |
| --------- | ------- | ----------------------------------------------------------------------- |
| key       | BINARY  |  **`user_id`** フィールドはキーとして使用されます。これは、セッション/クッキーの情報に対応する固有の英数字フィールドです      |
| offset    | LONG    | これは各パーティションに対して単調に増加していく固有値です                                           |
| partition | INTEGER | こちらのKafkaの実装では2つのパーティションのみ（0および1）が使用されています                              |
| timestamp | LONG    | このタイムスタンプは、エポックからの経過ミリ秒数として記録され、作成者がパーティションにレコードを加えた時間を表します             |
| topic     | STRING  | Kafkaサービスには複数のトピックがホスティングされていますが、ここには **`clickstream`** トピックのレコードのみが含まれます |
| value     | BINARY  | これはJSONとして送信される完全なデータペイロード（後ほど説明します）です                                  |

-- COMMAND ----------

-- MAGIC %md <i18n value="f2cd70fe-65a1-4dce-b264-c0c7d225640a"/>
## JSONファイルから未加工のイベントを抽出する（Extract Raw Events From JSON Files）
データを正しくDeltaに読み込むには、まずは正しいスキーマを使用してJSONデータを抽出する必要があります。

以下で指定されているファイルパスにあるJSONファイルに対して外部テーブルを作成しましょう。 このテーブルに **`events_json`** というを付けて、上記のスキーマを宣言します。

-- COMMAND ----------

-- ANSWER
CREATE TABLE IF NOT EXISTS events_json
  (key BINARY, offset BIGINT, partition INT, timestamp BIGINT, topic STRING, value BINARY)
USING JSON 
OPTIONS (path = "${da.paths.datasets}/ecommerce/raw/events-kafka")

-- COMMAND ----------

-- MAGIC %md <i18n value="07ce3850-fdc7-4dea-9335-2a093c2e200c"/>
**注**：このラボでは、Pythonを使って時々チェックを実行します。 手順に従っていない場合、次のセルは変更すべきことについてのメッセージを記載したエラーを返します。 セルを実行しても出力がない場合、このステップは完了です。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_json"), "Table named `events_json` does not exist"
-- MAGIC assert spark.table("events_json").columns == ['key', 'offset', 'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_json").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC 
-- MAGIC total = spark.table("events_json").count()
-- MAGIC assert total == 2252, f"Expected 2252 records, found {total}"

-- COMMAND ----------

-- MAGIC %md <i18n value="ae3b8554-d0e7-4fd7-b25a-27bfbc5f7c13"/>
## Deltaテーブルに未加工のイベントを挿入する（Insert Raw Events Into Delta Table）
同じスキーマを使用して **`events_raw`** というの空のマネージドDeltaテーブルを作成します。

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TABLE events_raw
  (key BINARY, offset BIGINT, partition INT, timestamp BIGINT, topic STRING, value BINARY);

-- COMMAND ----------

-- MAGIC %md <i18n value="3d56975b-47ba-4678-ae7b-7c5e4ac20a97"/>
以下のセルを実行して、テーブルが正しく作成されたことを確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_raw"), "Table named `events_raw` does not exist"
-- MAGIC assert spark.table("events_raw").columns == ['key', 'offset', 'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_raw").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC assert spark.table("events_raw").count() == 0, "The table should have 0 records"

-- COMMAND ----------

-- MAGIC %md <i18n value="61815a62-6d4f-47fb-98a9-73c39842ac56"/>
抽出されたデータとDeltaテーブルの準備ができたら、 **`events_json`** のテーブルから新しい **`events_raw`** のDeltaテーブルにJSONレコードを挿入します。

-- COMMAND ----------

-- ANSWER
INSERT INTO events_raw
SELECT * FROM events_json

-- COMMAND ----------

-- MAGIC %md <i18n value="4f545052-31c6-442b-a5e8-4c5892ec912f"/>
手動でテーブルの内容を確認し、データが期待通りに書き込まれたことを確認します。

-- COMMAND ----------

-- ANSWER
SELECT * FROM events_raw

-- COMMAND ----------

-- MAGIC %md <i18n value="0d66f26b-3df6-4819-9d84-22da9f55aeaa"/>
次のセルを実行してデータが正しく読み込まれたことを確認しましょう。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_raw").count() == 2252, "The table should have 2252 records"
-- MAGIC assert set(row['timestamp'] for row in spark.table("events_raw").select("timestamp").limit(5).collect()) == {1593880885085, 1593880892303, 1593880889174, 1593880886106, 1593880889725}, "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md <i18n value="e9565088-1762-4f89-a06f-49576a53526a"/>
## クエリからDeltaテーブルを作成する（Create Delta Table from a Query）
新しいイベントデータに加えて、コースの後半で使用する製品の詳細を獲得できる小さなルックアップテーブルも読み込みましょう。 CTAS文を使用して、以下のparquetディレクトリからデータを抽出する **`item_lookup`** というのマネージドDeltaテーブルを作成します。

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TABLE item_lookup 
AS SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/item-lookup`

-- COMMAND ----------

-- MAGIC %md <i18n value="9f1ad20f-1238-4a12-ad2a-f10169ed6475"/>
次のセルを実行してルックアップテーブルが正しく読み込まれていることを確認しましょう。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("item_lookup").count() == 12, "The table should have 12 records"
-- MAGIC assert set(row['item_id'] for row in spark.table("item_lookup").select("item_id").orderBy('item_id').limit(5).collect()) == {'M_PREM_F', 'M_PREM_K', 'M_PREM_Q', 'M_PREM_T', 'M_STAN_F'}, "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md <i18n value="c24885ea-010e-4b76-9e9d-cc749f10993a"/>
次のセルを実行して、このレッスンに関連するテーブルとファイルを削除してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
