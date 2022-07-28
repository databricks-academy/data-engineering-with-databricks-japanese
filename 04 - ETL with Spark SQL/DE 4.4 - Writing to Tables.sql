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
-- MAGIC # Deltaテーブルへの書き込み（Writing to Delta Tables）
-- MAGIC Delta Lakeテーブルでは、クラウドのオブジェクトストレージにあるデータファイルによってバックアップされているテーブルへのACIDに準拠した更新が可能です。
-- MAGIC 
-- MAGIC このノートブックでは、Delta Lakeを使用して更新を処理するためのSQL構文を説明します。 多くの操作は標準SQLとなりますが、SparkとDelta Lakeの実行に合わせてちょっとした違いがあります。
-- MAGIC 
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このレッスンでは、以下のことが学べます。
-- MAGIC -  **`INSERT OVERWRITE`** を使用してテーブルを上書きする
-- MAGIC -  **`INSERT INTO`** を使用してテーブルに追加する
-- MAGIC -  **`MERGE INTO`** を使用してテーブルに対して追加、更新、削除を行う
-- MAGIC -  **`COPY INTO`** を使用してデータを段階的にテーブルに取り込む

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## セットアップを実行する（Run Setup）
-- MAGIC 
-- MAGIC セットアップスクリプトでは、このノートブックの実行に必要なデータを作成し値を宣言します。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 完全な上書き（Complete Overwrites）
-- MAGIC 
-- MAGIC 上書きを使用して、テーブル内のすべてのデータを置き換えられます。 テーブルを削除して再作成するのに比べて、テーブルを上書きすることに複数の利点があります：
-- MAGIC - ディレクトリを再帰的に表示したりファイルを削除したりする必要がないため、テーブルを上書きしたほうがはるかに早いです。
-- MAGIC - テーブルの前のバージョンはまだ存在しており、タイムトラベルを使用して前のデータを簡単に取得できます。
-- MAGIC - これはアトミック操作です。 並行のクエリは、テーブルを削除している途中でもテーブルを読み取れます。
-- MAGIC - ACIDトランザクションの保証に従って、テーブルの上書きに失敗した場合、テーブルはその前の状態のままとなります。
-- MAGIC 
-- MAGIC Spark SQLには、完全な上書きを行うための2つの簡単なメソッドが備わっています。
-- MAGIC 
-- MAGIC 一部の学習者は、CTAS文についての以前のレッスンで、実は（セルが複数回実行されたときに発生する可能性のあるエラーを避けるために）CRAS文が使用されていたことに気づいたかもしれません。
-- MAGIC 
-- MAGIC  **`CREATE OR REPLACE TABLE`** （CRAS）文は、実行されるたびにテーブルの中身を完全に置き換えます。

-- COMMAND ----------

CREATE OR REPLACE TABLE events AS
SELECT * FROM parquet.`${da.paths.datasets}/raw/events-historical`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブル履歴を確認すると、テーブルの以前のバージョンが置き換えられたことが分かります。

-- COMMAND ----------

DESCRIBE HISTORY events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC  **`INSERT OVERWRITE`** では、ほとんど同じ結果を得られます。ターゲットテーブルのデータがクエリのデータに置き換えられます。
-- MAGIC 
-- MAGIC  **`INSERT OVERWRITE`** は：
-- MAGIC 
-- MAGIC - CRAS文と違って新しいテーブルを作成することはできず、既存のテーブルを上書きすることしかできません。
-- MAGIC - 現在のテーブルスキーマに一致する新しいレコードでのみ上書きできるため、ダウンストリーム消費者に悪影響を与えずに既存のテーブルを上書きできるより安全なテクニックとなります。
-- MAGIC - 個別のパーセンテージを上書きできます

-- COMMAND ----------

INSERT OVERWRITE sales
SELECT * FROM parquet.`${da.paths.datasets}/raw/sales-historical/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC CRAS文とは異なるメトリックが表示されることにご注意ください。 テーブル履歴に操作が記録される方法も異なります。

-- COMMAND ----------

DESCRIBE HISTORY sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ここでの主な違いは、Delta Lakeが書き込み時にスキーマを強制する方法に関係しています。
-- MAGIC 
-- MAGIC CRAS文を使用するとターゲットテーブルの中身を完全に再定義できるのに対し、 **`INSERT OVERWRITE`** を使用すると、スキーマを変更しようとした場合（任意の設定を指定しない限り）失敗します。
-- MAGIC 
-- MAGIC 以下のセルからコメントアウトを外して実行すると、予期せぬエラーメッセージを生成できます。

-- COMMAND ----------

-- INSERT OVERWRITE sales
-- SELECT *, current_timestamp() FROM parquet.`${da.paths.datasets}/raw/sales-historical`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## 行の追加（Append Rows）
-- MAGIC 
-- MAGIC  **`INSERT INTO`** を使用して、既存のDeltaテーブルにアトミックに新しい行を追加できます。 これにより、既存のテーブルを段階的に更新でき、毎回上書きするよりも効率的です。
-- MAGIC 
-- MAGIC  **`INSERT INTO`** を使用して、 **`sales`** テーブルに新しい売上レコードを追加します。

-- COMMAND ----------

INSERT INTO sales
SELECT * FROM parquet.`${da.paths.datasets}/raw/sales-30m`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 同じレコードを何度も追加してしまうのを防ぐ組み込み保証は **`INSERT INTO`** にはありませんので、ご注意ください。 上記のセルを再実行するとターゲットテーブルに同一のレコードが書き込まれ、重複レコードにつながります。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## 更新のマージ（Merge Updates）
-- MAGIC 
-- MAGIC  **`MERGE`** のSQL操作を使用して、ソーステーブル、ビュー、もしくはデータフレームからターゲットDeltaテーブルにデータをアップサートできます。 Delta Lakeは、 **`MERGE`** で挿入、更新、削除をサポートしており、SQL標準構文の他にも、高度な使い方を助けるために構文拡張もサポートしています。
-- MAGIC 
-- MAGIC <strong><code>
-- MAGIC MERGE INTO target a<br/>
-- MAGIC USING source b<br/>
-- MAGIC ON {merge_condition}<br/>
-- MAGIC WHEN MATCHED THEN {matched_action}<br/>
-- MAGIC WHEN NOT MATCHED THEN {not_matched_action}<br/>
-- MAGIC </code></strong>
-- MAGIC 
-- MAGIC  **`MERGE`** 操作を使用して、更新されたメールアドレスと新しいユーザーで過去のユーザーデータを更新します。

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users_update AS 
SELECT *, current_timestamp() AS updated 
FROM parquet.`${da.paths.datasets}/raw/users-30m`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC  **`MERGE`** の主な利点は：
-- MAGIC * 新、挿入、削除が単一のトランザクションとして行われる
-- MAGIC * 一致するフィールドの他にも、複数の条件文を追加できる
-- MAGIC * カスタムロジックを実装するための方法がたくさんある
-- MAGIC 
-- MAGIC 以下では、現在の列のメールアドレスが **`NULL`** なっており、新しい列のメールアドレスアドレスが<0><1>NULL</1></0>となっていない場合にのみレコードを更新します。
-- MAGIC 
-- MAGIC 新しいバッチの一致しないレコードはすべて挿入されます。

-- COMMAND ----------

MERGE INTO users a
USING users_update b
ON a.user_id = b.user_id
WHEN MATCHED AND a.email IS NULL AND b.email IS NOT NULL THEN
  UPDATE SET email = b.email, updated = b.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC  **`MATCHED`** および **`NOT MATCHED`** の両方の条件においても、この関数の動作を明示的に指定していることにご注意ください。ここに示されている例は、すべての **`MERGE`** の動作を表すものではなく、適用できるロジックの一例にすぎません。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 重複排除のためのInsert-Onlyマージ（Insert-Only Merge for Deduplication）
-- MAGIC 
-- MAGIC 連続の追加操作でログもしくは常に追加されるその他のデータセットをDeltaテーブルに集めるのが一般的なETLの使い方です。
-- MAGIC 
-- MAGIC 多くのソースシステムは重複レコードを生成します。 マージでは、insert-onlyマージを実行すれば重複レコードの挿入を予防できます。
-- MAGIC 
-- MAGIC この最適化されたコマンドは同様の **`MERGE`** 構文を使用しますが、 **`WHEN NOT MATCHED`** 句のみを指定します。
-- MAGIC 
-- MAGIC 以下では、これを使用して、同じ **`user_id`** および **`event_timestamp`** を持つレコードが既に **`events`** テーブルにないことを確認します。

-- COMMAND ----------

MERGE INTO events a
USING events_update b
ON a.user_id = b.user_id AND a.event_timestamp = b.event_timestamp
WHEN NOT MATCHED AND b.traffic_source = 'email' THEN 
  INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 段階的な読み込み（Load Incrementally）
-- MAGIC 
-- MAGIC  **`COPY INTO`** は、SQLエンジニアに、外部システムからデータを段階的に取り込める、べき等の方法を提供します。
-- MAGIC 
-- MAGIC この操作にはいくつかの条件があることにご注意ください：
-- MAGIC - データスキーマは一貫している必要がある
-- MAGIC - 重複レコードは、除外するか、ダウンストリームで処理する必要がある
-- MAGIC 
-- MAGIC この操作は、予想どおりに増大するデータの全テーブルスキャンよりもはるかに軽い可能性があります。
-- MAGIC 
-- MAGIC ここでは、静的ディレクトリでの単純な実行を示しますが、実際の価値は、ソース内の新しいファイルを自動的に取得する時間の経過に伴う複数の実行にあります。

-- COMMAND ----------

COPY INTO sales
FROM "${da.paths.datasets}/raw/sales-30m"
FILEFORMAT = PARQUET

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
