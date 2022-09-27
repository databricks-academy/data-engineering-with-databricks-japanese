-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="ba5cb184-9677-4b79-b000-f42c5fff9044"/>
-- MAGIC 
-- MAGIC # データをファイルから直接抽出する方法（Extracting Data Directly from Files）
-- MAGIC 
-- MAGIC このノートブックでは、DatabricksでSpark SQLを使用してデータをファイルから直接抽出する方法を学びます。
-- MAGIC 
-- MAGIC このオプションは複数のファイル形式でサポートされていますが、（parquetやJSONなど）自己記述的なデータ形式に最も役立ちます。
-- MAGIC 
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このレッスンでは、以下のことが学べます。
-- MAGIC - Spark SQLを使用してデータファイルを直接照会する
-- MAGIC -  **`text`** および **`binaryFile`** メソッドを活用して生のファイルコンテンツを確認する

-- COMMAND ----------

-- MAGIC %md <i18n value="e9800a3a-c96c-4ce2-a835-b5f058e26ead"/>
-- MAGIC 
-- MAGIC ## セットアップを実行する（Run Setup）
-- MAGIC 
-- MAGIC セットアップスクリプトでは、このノートブックの実行に必要なデータを作成し値を宣言します。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-04.1

-- COMMAND ----------

-- MAGIC %md <i18n value="fedca70d-2bf7-415b-8ab9-1691c2366b24"/>
-- MAGIC 
-- MAGIC ## データの概要（Data Overview）
-- MAGIC 
-- MAGIC この例では、JSONファイルとして書き込まれる未加工のKafkaデータのサンプルを扱っていきます。
-- MAGIC 
-- MAGIC 各ファイルには、5秒の間隔で消費されるすべてのレコードが含まれています。レコードは、複数のレコードのJSONファイルとして完全なKafkaスキーマで保存されています。
-- MAGIC 
-- MAGIC | フィールド     | 型       | 説明                                                                      |
-- MAGIC | --------- | ------- | ----------------------------------------------------------------------- |
-- MAGIC | key       | BINARY  |  **`user_id`** フィールドはキーとして使用されます。これは、セッション/クッキーの情報に対応する固有の英数字フィールドです      |
-- MAGIC | value     | BINARY  | これはJSONとして送信される完全なデータペイロード（後ほど説明します）です                                  |
-- MAGIC | topic     | STRING  | Kafkaサービスには複数のトピックがホスティングされていますが、ここには **`clickstream`** トピックのレコードのみが含まれます |
-- MAGIC | partition | INTEGER | こちらのKafkaの実装では2つのパーティションのみ（0および1）が使用されています                              |
-- MAGIC | offset    | LONG    | これは各パーティションに対して単調に増加していく固有値です                                           |
-- MAGIC | timestamp | LONG    | このタイムスタンプは、エポックからの経過ミリ秒数として記録され、作成者がパーティションにレコードを加えた時間を表します             |

-- COMMAND ----------

-- MAGIC %md <i18n value="00f263ec-293b-4adf-bf4b-b81f04de6e31"/>
-- MAGIC 
-- MAGIC ソースディレクトリにたくさんのJSONファイルが含まれていることにご注意ください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(DA.paths.kafka_events)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(DA.paths.kafka_events)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md <i18n value="f4cbde61-2f10-4758-82ca-786e16606d60"/>
-- MAGIC 
-- MAGIC ここでは、DBFSルートに書き込まれたデータへの相対ファイルパスを使用します。
-- MAGIC 
-- MAGIC ほとんどのワークフローでは、ユーザーが外部のクラウドストレージの場所からデータにアクセスする必要があります。
-- MAGIC 
-- MAGIC ほとんどの会社では、それらの格納先へのアクセスの設定はワークスペース管理者が行います。
-- MAGIC 
-- MAGIC これらの格納先を設定してアクセスするための手順は、自分のペースで進められるクラウドベンダー特有の「クラウドアーキテクチャとシステムの統合」というコースをご覧ください。

-- COMMAND ----------

-- MAGIC %md <i18n value="8e04a0d2-4d79-4547-b2b2-765eefaf6285"/>
-- MAGIC 
-- MAGIC ## 単一のファイルを照会する（Query a Single File）
-- MAGIC 
-- MAGIC 単一のファイルのデータを照会するには、クエリを次のパターンで実行しましょう：
-- MAGIC 
-- MAGIC <strong><code>SELECT * FROM file_format.&#x60;/path/to/file&#x60;</code></strong>
-- MAGIC 
-- MAGIC パスを一重引用符ではなくバックティックで囲っていることにご注意ください。

-- COMMAND ----------

SELECT * FROM json.`${DA.paths.kafka_events}/001.json`

-- COMMAND ----------

-- MAGIC %md <i18n value="02c296c6-80be-4bd8-99cc-29f2e44e1d2d"/>
-- MAGIC 
-- MAGIC こちらのプレビューには、ソースファイルの321行すべてが表示されています。

-- COMMAND ----------

-- MAGIC %md <i18n value="01cd5a22-a236-4ef6-bf65-7c40379d7ef9"/>
-- MAGIC 
-- MAGIC ## ファイルのディレクトリを照会する（Query a Directory of Files）
-- MAGIC 
-- MAGIC ディレクトリにあるファイルがすべて同じ形式とスキーマを持っている場合は、個別のファイルではなくディレクトリパスを指定することですべてのファイルを同時にクエリできます。

-- COMMAND ----------

SELECT * FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- MAGIC %md <i18n value="f36c3a77-4b84-41a0-95ae-a8f999a0f60e"/>
-- MAGIC 
-- MAGIC デフォルトでは、このクエリは、最初の1000行のみを表示します。

-- COMMAND ----------

-- MAGIC %md <i18n value="46590bb8-cf4b-4c3d-a9c6-e431bad4a5e9"/>
-- MAGIC 
-- MAGIC ## ファイルへの参照の作成（Create References to Files）
-- MAGIC ファイルとディレクトリを直接クエリできるのは、ファイルに対するクエリに追加のSparkロジックを連結できるということです。
-- MAGIC 
-- MAGIC パスに対してクエリからビューを作成すると、後のクエリでこのビューを参照できます。 ここではテンポラリビューを作成しますが、通常のビューで永続的な参照も作成できます。

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_temp_view
AS SELECT * FROM json.`${DA.paths.kafka_events}`;

SELECT * FROM events_temp_view

-- COMMAND ----------

-- MAGIC %md <i18n value="0a627f4b-ec2c-4002-bf9b-07a788956f03"/>
-- MAGIC 
-- MAGIC ## テキストファイルを未加工の文字列として抽出する（Extract Text Files as Raw Strings）
-- MAGIC 
-- MAGIC （JSON、CSV、TSV、およびTXT形式を含む）テキストベースのファイルを扱っているときは、 **`text`** 形式を使用してファイルの各行を **`value`** というの文字列の1列がある行として読み込みこませることができます。 これは、データソースが破損しがちでテキスト解析の関数がテキストフィールドから値を抽出するために使用される場合に役立ちます。

-- COMMAND ----------

SELECT * FROM text.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- MAGIC %md <i18n value="ffae0f7a-b956-431d-b1cb-6d2be33b4f6c"/>
-- MAGIC 
-- MAGIC ## ファイルの未加工のバイトとメタデータを抽出する（Extract the Raw Bytes and Metadata of a File）
-- MAGIC 
-- MAGIC 一部のワークフローでは、画像もしくは非構造化データを扱うときなど、ファイルを丸ごと扱う必要があります ディレクトリを照会するのに **`binaryFile`** を使用すると、ファイルコンテンツの2進法表示とともにメタデータも表示されます。
-- MAGIC 
-- MAGIC 具体的には、作成されたフィールドは、 **`path`** 、 **`modificationTime`** 、 **`length`** および **`content`** を示します。

-- COMMAND ----------

SELECT * FROM binaryFile.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- MAGIC %md <i18n value="fa8fcc72-31c0-4825-ae6f-bf194d715f14"/>
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
