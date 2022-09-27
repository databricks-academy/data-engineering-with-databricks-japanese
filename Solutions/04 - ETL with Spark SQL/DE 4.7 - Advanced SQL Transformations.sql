-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="bc50b1e9-781a-405d-bed4-c80dbd97e0d1"/>
-- MAGIC 
-- MAGIC # 高度なSQL変換（Advanced SQL Transformations）
-- MAGIC 
-- MAGIC MAGIC Spark SQLを使用してデータレイクハウスに保存されているテーブル形式データを照会するのは、簡単、効率的、かつ高速です。
-- MAGIC 
-- MAGIC これは、データ構造の規則性が低くなる場合、1つのクエリで多くのテーブルを使用する必要がある場合、またはデータの形状を大幅に変更する必要がある場合により複雑になります。 このノートブックでは、エンジニアがとても複雑な変換を実行するのに役立つ、Spark SQLに存在する多くの関数を紹介しています。
-- MAGIC 
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このレッスンでは、以下のことが学べます。
-- MAGIC -  **`.`** と **`:`** の構文を使用してネスト化したデータを照会する
-- MAGIC - JSONを扱う
-- MAGIC - 配列と構造体のフラット化および解凍
-- MAGIC - joinとsetの演算子を使用してデータセットを結合する
-- MAGIC - ピボットテーブルを使用してデータを再形成する
-- MAGIC - 配列を操作するために高階関数を使用する

-- COMMAND ----------

-- MAGIC %md <i18n value="4c84edde-f73e-4873-aa45-aca0cf4c7159"/>
-- MAGIC 
-- MAGIC ## セットアップを実行する（Run Setup）
-- MAGIC 
-- MAGIC セットアップスクリプトでは、このノートブックの実行に必要なデータを作成し値を宣言します。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-04.7

-- COMMAND ----------

-- MAGIC %md <i18n value="836f7278-abe6-42e5-8e54-73410c439a55"/>
-- MAGIC 
-- MAGIC ## JSONデータの取り扱い（Interacting with JSON Data）
-- MAGIC 
-- MAGIC  **`events_raw`** テーブルはKafkaペイロードを表すデータに対して登録されました。
-- MAGIC 
-- MAGIC ほとんどの場合、Kafkaデータは2進コード化したJSON値となります。 以下で **`key`** と **`value`** を文字列に変換して人間が読める形式で確認します。

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_strings AS
  SELECT string(key), string(value) 
  FROM events_raw;
  
SELECT * FROM events_strings

-- COMMAND ----------

-- MAGIC %md <i18n value="c758863e-eb79-4b7b-b397-255b78699287"/>
-- MAGIC 
-- MAGIC Spark SQLには、文字列として保存されているJSONデータを扱うための組み込み機能があります。  **`:`** 構文を使用して、ネスト化したデータ構造を移動できます。

-- COMMAND ----------

SELECT value:device, value:geo:city 
FROM events_strings

-- COMMAND ----------

-- MAGIC %md <i18n value="773a02a1-6208-4f7c-ad6a-e50850be0055"/>
-- MAGIC 
-- MAGIC Spark SQLには、JSONオブジェクトを構造体型（ネスト化した属性を持つネイティブのSpark型）に解析する機能もあります。
-- MAGIC 
-- MAGIC しかし、 **`from_json`** 関数にはスキーマが必要です。 現在のデータのスキーマを導き出すには、まずは、必ずnullフィールドなしのJSON値を返すクエリを実行します。

-- COMMAND ----------

SELECT value 
FROM events_strings 
WHERE value:event_name = "finalize" 
ORDER BY key
LIMIT 1

-- COMMAND ----------

-- MAGIC %md <i18n value="99cdb6bd-f03b-487d-a581-98ecf5fb86a3"/>
-- MAGIC 
-- MAGIC また、Spark SQLには、例からJSONスキーマを導き出す **`schema_of_json`** 関数もあります。 ここでは、サンプルのJSONをコピーして関数に貼り付け、それを **`from_json`** 関数にチェーンして、 **`value`** フィールドを構造体タイプに変換します。

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_events AS
  SELECT from_json(value, schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}')) AS json 
  FROM events_strings;
  
SELECT * FROM parsed_events

-- COMMAND ----------

-- MAGIC %md <i18n value="72757378-3490-4160-af61-cc8e86986633"/>
-- MAGIC 
-- MAGIC JSON文字列が構造体型にアンパックされると、Sparkがサポートしているフィールドを列にフラット化するための **`*`** （スター）アンパックを使用できます。

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW new_events_final AS
  SELECT json.* 
  FROM parsed_events;
  
SELECT * FROM new_events_final

-- COMMAND ----------

-- MAGIC %md <i18n value="71294bed-5a75-4577-8e34-d6cf319f7925"/>
-- MAGIC 
-- MAGIC ## データ構造体を調べる（Explore Data Structures）
-- MAGIC 
-- MAGIC Spark SQLには、複雑でネスト化しているデータ型を扱えるための堅牢な構文があります。
-- MAGIC 
-- MAGIC まずは、 **`events`** テーブルのフィールドを見ましょう。

-- COMMAND ----------

DESCRIBE events

-- COMMAND ----------

-- MAGIC %md <i18n value="50bc40ad-f9fc-42f9-bb00-e1afecceff26"/>
-- MAGIC 
-- MAGIC **`ecommerce`** フィールドは、doubleと2つのlongを含む構造体です。
-- MAGIC 
-- MAGIC このフィールドのサブフィールドは、JSONでネスト化したデータを移動する方法と同様に、標準の **`.`** 構文を使用して操作できます。

-- COMMAND ----------

SELECT ecommerce.purchase_revenue_in_usd 
FROM events
WHERE ecommerce.purchase_revenue_in_usd IS NOT NULL

-- COMMAND ----------

-- MAGIC %md <i18n value="0c5d4757-46c8-4ddd-8ad0-b81bcfd6d178"/>
-- MAGIC 
-- MAGIC ## 配列の取り扱い (Working with Arrays)
-- MAGIC 
-- MAGIC **`events`** テーブルの **`items`** フィールドは構造体の配列です。
-- MAGIC 
-- MAGIC Spark SQLには、特に配列を処理するための関数が多数あります。
-- MAGIC 
-- MAGIC たとえば、 **`size`** 関数は、各行の配列内のアイテム数をカウントします。
-- MAGIC 
-- MAGIC これを使用して、3つ以上のアイテムを含む配列を持つイベント レコードをフィルター処理してみましょう。

-- COMMAND ----------

SELECT user_id, event_timestamp, event_name, items
FROM events
WHERE size(items) > 2

-- COMMAND ----------

-- MAGIC %md <i18n value="0167fd9c-9374-4b86-90cf-53ae9feae297"/>
-- MAGIC 
-- MAGIC ## 配列の分割（Explode Arrays）
-- MAGIC 
-- MAGIC  **`explode`** 関数を使用すると、配列にある各要素をそれぞれの行に配置できます。
-- MAGIC  これを使用して、3つ以上のアイテムを持つイベント レコードを、配列内の各アイテムに1つずつ、別々の行に配置してみましょう。

-- COMMAND ----------

SELECT user_id, event_timestamp, event_name, explode(items) AS item
FROM events
WHERE size(items) > 2

-- COMMAND ----------

-- MAGIC %md <i18n value="df218c13-c1e9-4644-8859-d1d66106f224"/>
-- MAGIC 
-- MAGIC ## 配列を集める（Collect Arrays）
-- MAGIC 
-- MAGIC  **`collect_set`** 関数を使用すると、配列内のフィールドを含めて、フィールドに対して固有の値を集められます。
-- MAGIC 
-- MAGIC  **`flatten`** 関数を使用すると、複数の配列を1つの配列に結合できます。
-- MAGIC 
-- MAGIC  **`array_distinct`** 関数を使用すると、配列から重複要素を排除できます。
-- MAGIC 
-- MAGIC ここでは、これらのクエリを組み合わせて、ユーザーのアクションとカート内の項目の一意のコレクションを示す単純なテーブルを作成します。

-- COMMAND ----------

SELECT user_id,
  collect_set(event_name) AS event_history,
  array_distinct(flatten(collect_set(items.item_id))) AS cart_history
FROM events
GROUP BY user_id

-- COMMAND ----------

-- MAGIC %md <i18n value="211f8b57-6202-4fdb-a60c-50dab87f48ca"/>
-- MAGIC 
-- MAGIC ## テーブルの結合（Join Tables）
-- MAGIC 
-- MAGIC Spark SQLは、標準のjoin操作（inner、outer、left、right、anti、cross、semi）をサポートしています。
-- MAGIC 
-- MAGIC ここでは、ルックアップテーブルを使用したjoinを **`explode`** 操作にチェーンして、表示される標準の項目名を取得します。

-- COMMAND ----------

CREATE OR REPLACE VIEW sales_enriched AS
SELECT *
FROM (
  SELECT *, explode(items) AS item 
  FROM sales) a
INNER JOIN item_lookup b
ON a.item.item_id = b.item_id;

SELECT * FROM sales_enriched

-- COMMAND ----------

-- MAGIC %md <i18n value="ee523edb-b563-41af-82e1-f9b28a076989"/>
-- MAGIC 
-- MAGIC ## Set演算子（Set Operators）
-- MAGIC Spark SQLは、 **`UNION`** 、 **`MINUS`** 、および **`INTERSECT`** のセット演算子をサポートしています。
-- MAGIC 
-- MAGIC  **`UNION`** は2つのクエリのコレクションを返します。
-- MAGIC 
-- MAGIC 以下のクエリは、 **`events`** テーブルに **`new_events_final`** を挿入した場合と同じ結果を返します。

-- COMMAND ----------

SELECT * FROM events 
UNION 
SELECT * FROM new_events_final

-- COMMAND ----------

-- MAGIC %md <i18n value="99487e80-251a-4468-98e2-6f7d25b147ef"/>
-- MAGIC 
-- MAGIC **`INTERSECT`** は、両方のリレーションで見つかったすべての行を返します。

-- COMMAND ----------

SELECT * FROM events 
INTERSECT 
SELECT * FROM new_events_final

-- COMMAND ----------

-- MAGIC %md <i18n value="c00adc33-1831-407b-b787-8d3bfaddadf9"/>
-- MAGIC 
-- MAGIC この2つのデータセットに共通の値がないため、上記のクエリは結果を返しません。
-- MAGIC 
-- MAGIC  **`MINUS`** は、片方のデータセットにはあるけどもう片方のデータセットにはない行を返します。以前のクエリで共通の値がないことが分かったのでこの操作はスキップします。

-- COMMAND ----------

-- MAGIC %md <i18n value="e43bb5f8-d2d6-440f-a8f5-15387bd5bff1"/>
-- MAGIC 
-- MAGIC ## ピボットテーブル（Pivot Tables）
-- MAGIC  **`PIVOT`** 句は、データの全体像を見るために使用されます。 特定の列の値に基づいて集計された値を取得できます。この値は、 **`SELECT`** 句で使用される複数の列に変換されます。  **`PIVOT`** 句はテーブル名もしくはサブクエリの後に指定できます。
-- MAGIC 
-- MAGIC  **`SELECT * FROM ()`** ：括弧内の **`SELECT`** 文はこのテーブルの入力となります。
-- MAGIC 
-- MAGIC  **`PIVOT`** ：この句の中の最初の引数は集計関数と集計する列です。 次に、 **`FOR`** サブ句でピボット列を指定します。  **`IN`** 演算子には、ピボット列の値が含まれています。
-- MAGIC 
-- MAGIC ここでは、 **`PIVOT`** を使用して、 **`sales`** テーブルにある情報をフラット化する新しい **`transactions`** テーブルを作成します。
-- MAGIC 
-- MAGIC このフラット化したデータ形式は、ダッシュボードだけではなく、推測もしくは予測のための機械学習アルゴリズムを適用するのにも役立ちます。

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    email,
    order_id,
    transaction_timestamp,
    total_item_quantity,
    purchase_revenue_in_usd,
    unique_items,
    item.item_id AS item_id,
    item.quantity AS quantity
  FROM sales_enriched
) PIVOT (
  sum(quantity) FOR item_id in (
    'P_FOAM_K',
    'M_STAN_Q',
    'P_FOAM_S',
    'M_PREM_Q',
    'M_STAN_F',
    'M_STAN_T',
    'M_PREM_K',
    'M_PREM_F',
    'M_STAN_K',
    'M_PREM_T',
    'P_DOWN_S',
    'P_DOWN_K'
  )
);

SELECT * FROM transactions

-- COMMAND ----------

-- MAGIC %md <i18n value="d1ed83fa-4d2f-4138-b343-4d070c0d0e40"/>
-- MAGIC 
-- MAGIC ## 高階関数（Higher Order Functions）
-- MAGIC Spark SQLで高階関数を使用すると、複雑なデータ型を直接操作できます。 階層データを操作する場合、レコードは配列またはよくマップ型のオブジェクトとして保存されます。 高階関数を使用すると、元の構造を維持しながらデータを変換できます。
-- MAGIC 
-- MAGIC 高階関数には次のものが含まれています：
-- MAGIC -  **`FILTER`** は指定されたラムダ関数を使用して配列をフィルタリングします。
-- MAGIC -  **`EXIST`** は、文が配列内の1つ以上の要素に対して真であるかどうかをテストします。
-- MAGIC -  **`TRANSFORM`** は指定されたラムダ関数を使用して配列内の要素をすべて変換します。
-- MAGIC -  **`REDUCE`** は2つのラムダ関数を使用して、要素をバッファーにマージして仕上げに最終バッファーに関数を適用することで配列の要素を1つの値に減らします。

-- COMMAND ----------

-- MAGIC %md <i18n value="5fcad266-d5a0-4255-9c6b-be3634ea9e79"/>
-- MAGIC 
-- MAGIC ## フィルタ（Filter）
-- MAGIC  **`items`** 列にあるすべてのキング（King）サイズじゃない項目を削除します。  **`FILTER`** の関数を使用すると、その値が各配列から排除されている新しい列を作成できます。
-- MAGIC 
-- MAGIC  **`FILTER (items, i -> i.item_id LIKE "%K") AS king_items`** 
-- MAGIC 
-- MAGIC 上記の分では：
-- MAGIC -  **`FILTER`** ：高階関数の名前 <br>
-- MAGIC -  **`items`** ：入力配列の名前 <br>
-- MAGIC -  **`i`** ：イテレーター変数の名前。 この名前を選択してラムダ関数で使用します。 配列を移動し、値を1個ずつ関数に送っていきます<br>
-- MAGIC -  **`->`** ：関数の開始点を示します <br>
-- MAGIC -  **`i.item_id LIKE "%K"`** ：これは関数本体です。 各アイテムが大文字のKで終わるかを確認します。Kで終わる場合は、新しい列 **`king_items`** にフィルタリングされます。

-- COMMAND ----------

-- filter for sales of only king sized items
SELECT
  order_id,
  items,
  FILTER (items, i -> i.item_id LIKE "%K") AS king_items
FROM sales

-- COMMAND ----------

-- MAGIC %md <i18n value="7ef7b728-dfad-4cdf-8f41-8c72a76d4310"/>
-- MAGIC 
-- MAGIC 作ったフィルタが、作成された列に多くの空の配列を作成してしまう場合があります。 そういった場合は、 **`WHERE`** 句を使用して、返された列に空でない配列の値のみを表示させるのが便利です。
-- MAGIC 
-- MAGIC この例では、サブクエリ（クエリの中のクエリ）を使用してそれを行います。 サブクエリは、複数のステップで操作を行うのに便利です。 この場合は、 **`WHERE`** 句で使用する名前付き列を作成するために使用します。

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW king_size_sales AS

SELECT order_id, king_items
FROM (
  SELECT
    order_id,
    FILTER (items, i -> i.item_id LIKE "%K") AS king_items
  FROM sales)
WHERE size(king_items) > 0;
  
SELECT * FROM king_size_sales

-- COMMAND ----------

-- MAGIC %md <i18n value="e30bf997-6eb6-4ee7-94b8-6827ecdcce7d"/>
-- MAGIC 
-- MAGIC ## 変換（Transform）
-- MAGIC 組み込み関数は、セルにある単一で単純なデータ型を操作するためのもので、配列の値を処理することができません。  **`TRANSFORM`** は、配列の各要素に既存の関数を適用する場合に特に便利です。
-- MAGIC 
-- MAGIC キングサイズ項目の注文ごとの総収益を計算します。
-- MAGIC 
-- MAGIC  **`TRANSFORM(king_items, k -> CAST(k.item_revenue_in_usd * 100 AS INT)) AS item_revenues`** 
-- MAGIC 
-- MAGIC 上記の文では、入力配列にある各値に対して、項目の収入の値を抽出して100で掛け算し、結果を整数に変換します。 以前のコマンドのと同じ型を参照として使用していますが、イテレータを新しい変数 **`k`** として名づけていることにご注意ください。

-- COMMAND ----------

-- get total revenue from king items per order
CREATE OR REPLACE TEMP VIEW king_item_revenues AS

SELECT
  order_id,
  king_items,
  TRANSFORM (
    king_items,
    k -> CAST(k.item_revenue_in_usd * 100 AS INT)
  ) AS item_revenues
FROM king_size_sales;

SELECT * FROM king_item_revenues

-- COMMAND ----------

-- MAGIC %md <i18n value="6c15bff7-7667-4118-9b72-27068d6fa6be"/>
-- MAGIC 
-- MAGIC ## 概要（Summary）
-- MAGIC Spark SQLは、高度にネスト化したデータを操作するための包括的なネイティブ機能を備えています。
-- MAGIC 
-- MAGIC この機能の構文は、一部のSQLユーザーにとってなじみがないかもしれませんが、高階関数のような組み込み関数を活用すると、SQLエンジニアは、非常に複雑なデータを扱うときにカスタムロジックに頼る必要がなくなります。

-- COMMAND ----------

-- MAGIC %md <i18n value="2f9ae39d-2908-4ee1-9609-594c3d043a38"/>
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
