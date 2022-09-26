-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="84dff2fc-1e27-46ea-8120-0a501795da47"/>
# データのクリーンアップ（Cleaning Data）

SQLに詳しい開発者であれば、Spark SQLで行うほとんどの変換には馴染みがあるかと思います。

データを調べてクリーンアップするとき、データセットに適用する変換を表現するために、さまざまな列式とクエリを構築する必要が出てきます。

列式は、既存の列、演算子、および組み込みのSpark SQL関数から構築されます。 列式は、 **`SELECT`** 文で使用して、データセットから新しい列を作成する変換を表現できます。

Spark SQLでは、 **`WHERE`** 、 **`DISTINCT`** 、 **`ORDER BY`** 、 **`GROUP BY`** など、 **`SELECT`** の他にも、変換を表現するための多くの追加クエリコマンドがあります。

このノートブックでは、これまで使用してきた他のシステムとは異なるいくつかの概念を見たり、一般的な操作に役立ついくつかの関数を呼び出したりします。

 **`NULL`** 値のにおける動作、および文字列と日時フィールドの書式設定に特に注意を払います。

## 学習目標（Learning Objectives）
このレッスンでは、以下のことが学べます。
- データセットを要約し、nullの動作を説明する
- 重複を取得して削除する
- 予想されるカウント、欠落値、重複レコードについてデータセットを検証する
- データをきれいにして変換するための一般的な変換を適用する

-- COMMAND ----------

-- MAGIC %md <i18n value="763d80d3-b7ed-4baf-98f1-ec6e0fb03d67"/>
## セットアップを実行する（Run Setup）

セットアップスクリプトでは、このノートブックの実行に必要なデータを作成し値を宣言します。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-04.6

-- COMMAND ----------

-- MAGIC %md <i18n value="fe2033f4-7752-408a-9175-1dc28d63b445"/>
このレッスンでは、 **`users_dirty`** テーブルにある新しいユーザーレコードを扱います。

-- COMMAND ----------

SELECT * FROM users_dirty

-- COMMAND ----------

-- MAGIC %md <i18n value="9b9352ce-28ed-420c-814d-dbf773067e32"/>
## データを調べる（Inspect Data）

まずは、データの各フィールドの値をカウントしましょう。

-- COMMAND ----------

SELECT count(user_id), count(user_first_touch_timestamp), count(email), count(updated), count(*)
FROM users_dirty

-- COMMAND ----------

-- MAGIC %md <i18n value="1770892b-4fc9-40a8-a03a-c72353507b01"/>
**`count(col)`** は特定の列もしくは式をカウントするときに **`NULL`** 値をスキップするのでご注意ください。

ただし、 **`count(*)`** は、行の総数（ **`NULL`** 値のみの行を含む）をカウントする特例です。

null値をカウントする場合は、 **`count_if`** 関数もしくは **`WHERE`** 句を使用して、値が **`IS NULL`** のレコードをフィルタリングする条件を設けましょう。

-- COMMAND ----------

SELECT
  count_if(user_id IS NULL) AS missing_user_ids, 
  count_if(user_first_touch_timestamp IS NULL) AS missing_timestamps, 
  count_if(email IS NULL) AS missing_emails,
  count_if(updated IS NULL) AS missing_updates
FROM users_dirty

-- COMMAND ----------

-- MAGIC %md <i18n value="7e48564f-85a8-4c9c-8b0a-2f1c1606109b"/>
これらのすべてのフィールドにはnull値がある程度あります。 これの原因を突き止めてみましょう。

-- COMMAND ----------

-- MAGIC %md <i18n value="f6662ed5-cb5e-44a9-ad9e-d036cbc9df26"/>
## 個別のレコード（Distinct Records）

まずは、ユニークな行を探しましょう。

-- COMMAND ----------

SELECT count(DISTINCT(*))
FROM users_dirty

-- COMMAND ----------

SELECT count(DISTINCT(user_id))
FROM users_dirty

-- COMMAND ----------

-- MAGIC %md <i18n value="df59b9d2-9931-472c-9149-1d1bd33a2995"/>
**`user_id`** が **`user_first_touch_timestamp`** と同時に生成されるため、これらのフィールドのカウントは常に同じなはずです。

-- COMMAND ----------

SELECT count(DISTINCT(user_first_touch_timestamp))
FROM users_dirty

-- COMMAND ----------

-- MAGIC %md <i18n value="4814a464-6333-445f-b178-1bf65c62a7d5"/>
ここでは、合計行数に比べて重複するレコードがいくつかありますが、ユニークな値の数のほうがはるかに多いことに注意しましょう。

では、ユニークな値のカウントと列のカウントを組み合わせて、これらの値を並べて確認しましょう。

-- COMMAND ----------

SELECT 
  count(user_id) AS total_ids,
  count(DISTINCT user_id) AS unique_ids,
  count(email) AS total_emails,
  count(DISTINCT email) AS unique_emails,
  count(updated) AS total_updates,
  count(DISTINCT(updated)) AS unique_updates,
  count(*) AS total_rows, 
  count(DISTINCT(*)) AS unique_non_null_rows
FROM users_dirty

-- COMMAND ----------

-- MAGIC %md <i18n value="e83d57db-b194-444a-bcd7-7ff748da4983"/>
上記の概要から次のことが分かります：
* メールアドレスは全部固有
* メールアドレスには最も多くのnull値の数が含まれています
*  **`updated`** 列にはユニークな値が1つのみ含まれていますが、値のほとんどはnullでない値です

-- COMMAND ----------

-- MAGIC %md <i18n value="240c6103-ced1-4b14-a772-4172e01f39bd"/>
## 行の重複排除（Deduplicate Rows）
上記の動作からすれば、 **`DISTINCT *`** を使用して重複レコードを排除しようとした場合はどうなると思いますか？

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users_deduped AS
  SELECT DISTINCT(*) FROM users_dirty;

SELECT * FROM users_deduped

-- COMMAND ----------

-- MAGIC %md <i18n value="363ae50a-0345-4b86-9dee-8a0c277872f9"/>
上記のプレビューにはnull値がありますが、 **`COUNT(DISTINCT(*))`** はそのnull値を除外しました。

何行がこの **`DISTINCT`** コマンドを通過できたと思いますか？

-- COMMAND ----------

SELECT COUNT(*) FROM users_deduped

-- COMMAND ----------

-- MAGIC %md <i18n value="61822099-55ff-4c28-8f34-51bf22e46e73"/>
全く別の数字になったことにご注意ください。

Sparkは、列の値をカウントするとき、またはフィールドのユニークな値をカウントするときにnull値をスキップしますが、 **`DISTINCT`** クエリからnullのある行は省略しません。

実際、以前のカウントより1多い新しい数値が表示される理由は、すべてnullである3つの行があるためです（ここでは単一のユニークな行として含まれています）。

-- COMMAND ----------

SELECT * FROM users_dirty
WHERE
  user_id IS NULL AND
  user_first_touch_timestamp IS NULL AND
  email IS NULL AND
  updated IS NULL

-- COMMAND ----------

-- MAGIC %md <i18n value="ef1c4bde-aa99-4cde-bd49-f976a770e071"/>
## 特定の列に基づいて重複を排除する（Deduplicate Based on Specific Columns）

 **`user_id`** および **`user_first_touch_timestamp`** は、あるユーザーに初めて遭遇したときに生成されるため、固有タプルを構成します。

これらの各フィールドにいくつかのnull値があることが分かります。これらのフィールドのペアのユニークな数をカウントするときnullを除外すると、テーブル内のユニークな値の正しいカウントが得られます。

-- COMMAND ----------

SELECT COUNT(DISTINCT(user_id, user_first_touch_timestamp))
FROM users_dirty
WHERE user_id IS NOT NULL

-- COMMAND ----------

-- MAGIC %md <i18n value="08631519-40fe-45f5-bdf7-3267ec3229c3"/>
ここでは、ユニークなペアを使用してデータから不要な行を削除します。

以下のコードでは、 **`GROUP BY`** を使用して、 **`user_id`** と **`user_first_touch_timestamp`** に基づいて重複レコードを削除します。

複数のレコードがあるとき、裏技としてnullでない値のメールアドレスアドレスを獲得するために、  **`email`** 列に対して **`max()`** の集計関数を使用します。このバッチでは **`updated`** 値は全部同じでしたが、この値をGROUP BYの結果に残すには集計関数を使用する必要があります。

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW deduped_users AS
SELECT user_id, user_first_touch_timestamp, max(email) AS email, max(updated) AS updated
FROM users_dirty
WHERE user_id IS NOT NULL
GROUP BY user_id, user_first_touch_timestamp;

SELECT count(*) FROM deduped_users

-- COMMAND ----------

-- MAGIC %md <i18n value="09a6b88a-e02c-4835-ab62-0e82a1162c22"/>
## データセットの検証（Validate Datasets）
手動で確認して、カウントが予想通りであることを目で確認しました。

以下では、単純なフィルタと **`WHERE`** 句を使用してプログラムで検証を行います。

 **`user_id`** が各行に対して固有であることを検証しましょう。

-- COMMAND ----------

SELECT max(row_count) <= 1 no_duplicate_ids FROM (
  SELECT user_id, count(*) AS row_count
  FROM deduped_users
  GROUP BY user_id)

-- COMMAND ----------

-- MAGIC %md <i18n value="39a69a24-b1e1-42cc-83f1-115b69a2b891"/>
各メールアドレスが少なくとも1つの **`user_id`** と関連付けられていることを確認します。

-- COMMAND ----------

SELECT max(user_id_count) <= 1 at_most_one_id FROM (
  SELECT email, count(user_id) AS user_id_count
  FROM deduped_users
  WHERE email IS NOT NULL
  GROUP BY email)

-- COMMAND ----------

-- MAGIC %md <i18n value="17288207-e22e-43cc-8785-4d993cf71a1c"/>
## 日付の形式と正規表現 （Date Format and Regex）
nullフィールドをなくして重複を排除しましたので、データからさらに価値を引き出しましょう。

以下のコードは：
-  **`user_first_touch_timestamp`** を正しくスケーリングして有効なタイムスタンプに変換する
- このタイムスタンプのカレンダーデータと時刻を人間が読める形式で抽出する
-  **`regexp_extract`** を使用して、正規表現を介してメールアドレスの列からドメインを抽出します

-- COMMAND ----------

SELECT *,
  date_format(first_touch, "MMM d, yyyy") AS first_touch_date,
  date_format(first_touch, "HH:mm:ss") AS first_touch_time,
  regexp_extract(email, "(?<=@).+", 0) AS email_domain
FROM (
  SELECT *,
    CAST(user_first_touch_timestamp / 1e6 AS timestamp) AS first_touch 
  FROM deduped_users
)

-- COMMAND ----------

-- MAGIC %md <i18n value="8678a7d8-2d31-4022-84b6-785a6ae260e9"/>
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
