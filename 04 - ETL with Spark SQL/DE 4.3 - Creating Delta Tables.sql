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
-- MAGIC # Deltaテーブルの作成（Creating Delta Tables）
-- MAGIC 
-- MAGIC 外部のデータソースからデータを抽出した後、Databricksの利点をすべて最大限に活用できるよう、データをレイクハウスに読み込みます。
-- MAGIC 
-- MAGIC 組織によってDatabricksにデータを読み込むポリシーは異なりますが、弊社は、初期のテーブルではほとんど処理されていないバージョンのデータを表し、検証やエンリッチ化は後の段階で行うことをお勧めします。 このパターンを使用することにより、データは、データ型や例の名前において期待に一致しなかった場合でも削除されません。つまり、プログラムもしくは手動の介入によって、部分的に破損している状態のデータまたは無効な状態のデータをまだ復旧できるということです。
-- MAGIC 
-- MAGIC このレッスンでは、ほとんどのテーブルの作成に使用されるパターンである **`CREATE TABLE _ AS SELECT`** （CTAS）文に焦点を当てます。
-- MAGIC 
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このレッスンでは、以下のことが学べます。
-- MAGIC - CTAS文を使用してDelta Lakeテーブルを作成する
-- MAGIC - 既存のビューやテーブルから新しいテーブルを作成する
-- MAGIC - 読み込んだデータにメタデータを追加してエンリッチ化する
-- MAGIC - テーブルスキーマを、生成された列と説明的なコメントで宣言する
-- MAGIC - データの場所、クォリティの強制、およびパーティショニングを制御するための詳細オプションを設定する

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## セットアップを実行する（Run Setup）
-- MAGIC 
-- MAGIC セットアップスクリプトでは、このノートブックの実行に必要なデータを作成し値を宣言します。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Selectした結果で新しいテーブルを作成する（CTAS)（Create Table as Select (CTAS)）
-- MAGIC 
-- MAGIC  **`CREATE TABLE AS SELECT`** 文は、入力クエリから取得したデータを使用してDeltaテーブルを作成してデータを投入します。

-- COMMAND ----------

CREATE OR REPLACE TABLE sales AS
SELECT * FROM parquet.`${da.paths.datasets}/raw/sales-historical/`;

DESCRIBE EXTENDED sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC CTAS文は、自動的にクエリの結果からスキーマ情報を推測して、手動のスキーマ宣言をサポート**しません**。
-- MAGIC 
-- MAGIC つまり、CTAS文は、明確に定義されているスキーマのソース（例えば、Parquetのファイルとテーブル）からの外部データの取り込みに役立ちます。
-- MAGIC 
-- MAGIC CTAS文では、追加ファイルオプションの指定もサポートされていません。
-- MAGIC 
-- MAGIC これは、CSVファイルからデータを取り込む際にいかに大きな制限となってしまうかが分かります。

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_unparsed AS
SELECT * FROM csv.`${da.paths.datasets}/raw/sales-csv/`;

SELECT * FROM sales_unparsed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Delta Lakeテーブルにこのようなデータを正しく取り込ませるには、オプションを指定できるファイルへの参照を使用する必要があります。
-- MAGIC 
-- MAGIC 以前のレッスンで、外部テーブルを登録することでこれを示しました。 ここでは、テンポラリビューへのオプションを指定することで構文を進化させ、Deltaテーブルを正常に登録するためにこのテンポラリビューをCTAS文のソースとして使用します。

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW sales_tmp_vw
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  path "${da.paths.datasets}/raw/sales-csv",
  header "true",
  delimiter "|"
);

CREATE TABLE sales_delta AS
  SELECT * FROM sales_tmp_vw;
  
SELECT * FROM sales_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 既存のテーブルの列のフィルタリングと名前変更（Filtering and Renaming Columns from Existing Tables）
-- MAGIC 
-- MAGIC 列の名前の変更やターゲットテーブルからの列の除外などの簡単な変換は、テーブルの作成時に簡単に行えます。
-- MAGIC 
-- MAGIC 以下の文では、 **`sales`** テーブルの一部の列を含む新しいテーブルを作成します。
-- MAGIC 
-- MAGIC ここでは、ユーザーを特定する情報もしくは項目別の購入詳細の情報を意図的に除外していることを前提とします。 また、ダウンストリームシステムがこのソースデータと異なる命名規則を使用している前提でフィールドの名前を変更します。

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 以下の通り、ビューでも同じ目標を達成できたことにご注意ください。

-- COMMAND ----------

CREATE OR REPLACE VIEW purchases_vw AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases_vw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 生成した列を含むスキーマを宣言する（Declare Schema with Generated Columns）
-- MAGIC 
-- MAGIC 前述のとおり、CTAS分はスキーマ宣言をサポートしていません。 前述のとおり、タイムスタンプの列はUnixタイムスタンプの一種で、アナリストが情報を得るにはさほど役に立ちません。 これは、生成された列が役立つ場面です。
-- MAGIC 
-- MAGIC 生成された列は、ユーザーが指定した関数に基づいてDeltaテーブルの他の列に対して値が自動的に生成される特別な列の一種です（DBR 8.3で導入）。
-- MAGIC 
-- MAGIC 以下のコードでは、次の処理を行ってテーブルを作成することを示しています。
-- MAGIC 1. 列の名前と型の指定
-- MAGIC 1. 日付を計算するための<a href="https://docs.databricks.com/delta/delta-batch.html#deltausegeneratedcolumns" target="_blank">生成された列</a>の追加
-- MAGIC 1. 生成された列への説明的な列コメントの追加

-- COMMAND ----------

CREATE OR REPLACE TABLE purchase_dates (
  id STRING, 
  transaction_timestamp STRING, 
  price STRING,
  date DATE GENERATED ALWAYS AS (
    cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
    COMMENT "generated based on `transactions_timestamp` column")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC  **`date`** は生成された列なので、 **`date`** の列の値を指定せずに **`purchase_dates`** に書き込んだ場合、値は自動的にDelta Lakeに計算されます。
-- MAGIC 
-- MAGIC **注**：以下のセルでは、Delta Lakeの **`MERGE`** 文を使用する際に列の生成を可能にする設定を行います。 この構文の詳細は、コースの後半で紹介します。

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled=true; 

MERGE INTO purchase_dates a
USING purchases b
ON a.id = b.id
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 以下を見ると、このフィールドの値はソースデータにも挿入クエリにも指定されていなかったのにも関わらず、データの挿入時にすべての日付が正しく計算されたことが分かります。
-- MAGIC 
-- MAGIC どんなDelta Lakeソースでもそうですが、クエリは、すべてのクエリに対してテーブルの最新スナップショットを自動的に読み取り、 **`REFRESH TABLE`** を実行する必要はありません。

-- COMMAND ----------

SELECT * FROM purchase_dates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 本来生成されるはずだったフィールドがテーブルへの挿入処理に含まれている場合、指定された値が、生成された列を定義するために使用されるロジックによって算出されるはずの値と完全一致しないと、この挿入は失敗するのでご注意ください。
-- MAGIC 
-- MAGIC このエラーは、以下のセルからコメントアウトを外して実行すると表示できます：

-- COMMAND ----------

-- INSERT INTO purchase_dates VALUES
-- (1, 600000000, 42.0, "2020-06-18")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## テーブルの制約を追加する（Add a Table Constraint）
-- MAGIC 
-- MAGIC 上記のエラーメッセージは **`CHECK制約`** を指しています。 生成された列は、CHECK制約の特別な実装です。
-- MAGIC 
-- MAGIC Delta Lakeが書き込み時にスキーマを強制するため、Databricksは、標準SQL制約管理の句をサポートできテーブルに追加するデータの品質と整合性を確保できます。
-- MAGIC 
-- MAGIC 現在Databricksでは、2つの制約がサポートされています。
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#not-null-constraint" target="_blank"> **`NOT NULL`**  制約</a>
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#check-constraint" target="_blank"> **`CHECK`**  制約</a>
-- MAGIC 
-- MAGIC どちらの制約の場合でも、制約を定義する前に、制約に違反するデータが既にテーブルに入っていないことを確認する必要があります。 テーブルに制約を追加すると、制約に違反しているデータの書き込みは失敗します。
-- MAGIC 
-- MAGIC 以下では、テーブルの **`date`** 列に **`CHECK`** 制約を追加します。  **`CHECK`** 制約は、データセットのフィルタリングに使用する標準の **`WHERE`** 句に似ていることにご注意ください。

-- COMMAND ----------

ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルの制約は **`TBLPROPERTIES`** フィールドに表示されます。

-- COMMAND ----------

DESCRIBE EXTENDED purchase_dates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## オプションとメタデータを追加してテーブルをエンリッチ化する（Enrich Tables with Additional Options and Metadata）
-- MAGIC 
-- MAGIC Delta Lakeテーブルをエンリッチ化するオプションはまだわずかしか紹介していません。
-- MAGIC 
-- MAGIC 以下では、追加設定とメタデータをいくつか含めることでCTAS文を進化させる例を示します。
-- MAGIC 
-- MAGIC こちらの **`SELECT`** 句ではファイル取り込みに役立つ2つの組み込みSpark SQLコマンドを活用します
-- MAGIC *  **`current_timestamp()`** は、ロジックがいつ実行されたかのタイムスタンプを記録します
-- MAGIC *  **`input_file_name()`** は、テーブルの各レコードに対してソースデータファイルを記録します
-- MAGIC 
-- MAGIC また、ソースのタイムスタンプデータから算出される新しい日付の列を作成するためのロジックも含めます
-- MAGIC 
-- MAGIC  **`CREATE TABLE`** 句には複数のオプションが含まれます：
-- MAGIC * テーブルの中身をより簡単に見つけられるようにするために **`COMMENT`** を追加します
-- MAGIC *  **`LOCATION`** を指定します。これにより、（マネージドテーブルではなく）外部テーブルになります。
-- MAGIC * テーブルは日付の列で **`PARTITIONED BY`** されています。つまり、各データのデータは、格納先のそれぞれのディレクトリに保存されます。
-- MAGIC 
-- MAGIC **注意**：ここでは、主に構文と効果を示すためにパーティショニングを紹介しています。 ほとんどのDelta Lakeテーブル（特に中小規模のデータ）は、パーティショニングから利益が得られません。 パーティショニングによりデータファイルが物理的に分けられるため、この手法を使用すると、ファイルが小さくなってしまうおそれがあり、ファイルの圧縮および効率的なデータのスキップを妨げてしまいます。 HiveもしくはHDFSで見られる利点はDelta Lakeでは得られません。テーブルをパーティショニングする前に経験のあるDelta Lakeアーキテクトと相談してください。
-- MAGIC 
-- MAGIC **ベストプラクティスとしては、Delta Lakeで作業する際、ほとんどの場合は、パーティショニングされていないテーブルを使用することをお勧めします。**

-- COMMAND ----------

CREATE OR REPLACE TABLE users_pii
COMMENT "Contains PII"
LOCATION "${da.paths.working_dir}/tmp/users_pii"
PARTITIONED BY (first_touch_date)
AS
  SELECT *, 
    cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
    current_timestamp() updated,
    input_file_name() source_file
  FROM parquet.`${da.paths.datasets}/raw/users-historical/`;
  
SELECT * FROM users_pii;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルに追加されたメタデータフィールドには、レコードがどこから、いつ挿入されたかの便利な情報が載っています。 これは、ソースデータで問題をトラブルシューティングすることが必要になった場合に特に便利です。
-- MAGIC 
-- MAGIC  **`DESCRIBE TABLE EXTENDED`** を使用すると、特定のテーブルのコメントとプロパティをすべて確認できます。
-- MAGIC 
-- MAGIC **注**：Delta Lakeは、テーブル作成時に複数のテーブルプロパティを追加します。

-- COMMAND ----------

DESCRIBE EXTENDED users_pii

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルに使用されている場所を表示させると、 **`first_touch_date`** にある固有の値がデータディレクトリの作成に使用されていることが分かります。

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC files = dbutils.fs.ls(f"{DA.paths.working_dir}/tmp/users_pii")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Delta Lakeテーブルの複製（Cloning Delta Lake Tables）
-- MAGIC Delta Lakeテーブルを効率的に複製するために、Delta Lakeには2つの方法があります。
-- MAGIC 
-- MAGIC  **`DEEP CLONE`** は、ソーステーブルからターゲットテーブルにデータとメタデータを完全にコピーします。 このコピーは段階的に行われるので、このコマンドを再度実行するとソースからターゲットの場所に変更を同期できます。

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_clone
DEEP CLONE purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC すべてのデータファイルをコピーする必要があるため、大きなデータセットの場合、この処理は、長時間がかかる場合があります。
-- MAGIC 
-- MAGIC 現在のテーブルを変更してしまうリスクを冒さずに変更の適用を試し、簡単にテーブルのコピーを作成したいときは、 **`SHALLOW CLONE`** が便利です。 シャロークローンを使用すると、Deltaトランザクションログのみがコピーされるため、データは移動しません。

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_shallow_clone
SHALLOW CLONE purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC いずれにしても、テーブルの複製バージョンに適用されたデータ変更は、ソースとはまた別に追跡および保管されます。 複製は、開発中にSQLコードを試すためにテーブルを設定する良い方法です。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 概要（Summary）
-- MAGIC 
-- MAGIC このノートブックでは。DDLとDelta Lakeテーブルを作成するための構文に焦点を当てました。 次のノートブックでは、テーブルに更新を書き込むための方法を説明します。

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
