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
-- MAGIC # Delta Live Tables用のSQL（SQL for Delta Live Tables）
-- MAGIC 
-- MAGIC 前回のレッスンでは、Delta Live Tables（DLT）パイプラインとしてこのノートブックをスケジューリングするプロセスを見てきました。 ここからはDelta Live Tablesで使用される構文をより理解するために、このノートブックの内容を詳しく見ていきます。
-- MAGIC 
-- MAGIC このノートブックではSQLを使って、簡単なマルチホップアーキテクチャを一緒に実装するDelta Live Tablesを宣言します。この実装は、デフォルトでDatabricksワークスペースに読み込まれる、Databricksが提供するサンプルデータセットに基づいています。
-- MAGIC 
-- MAGIC 簡単に言うと、DLT SQLは従来のCTAS文にわずかな修正を加えたものだと考えても良いです。 DLTテーブルとビューの前には常に **`LIVE`** キーワードが付きます。
-- MAGIC 
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このレッスンでは、以下のことが学べます。
-- MAGIC * Delta Live Tablesを使ってテーブルとビューを定義する
-- MAGIC * SQLを使ってAuto Loaderで未加工のデータを段階的に取り込む
-- MAGIC * SQLを使ってDeltaテーブルで増分読み取りを実行する
-- MAGIC * コードを更新してパイプラインを再デプロイする

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## ブロンズレイヤーテーブルを宣言する（Declare Bronze Layer Tables）
-- MAGIC 
-- MAGIC 以下では、ブロンズレイヤーを実装する2つのテーブルを宣言します。 これは一切加工していないデータを表していますが、無期限に保持でき、Delta Lakeが提供するパフォーマンスと利点を用いてクエリできる形式にキャプチャされています。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ### sales_orders_raw
-- MAGIC 
-- MAGIC  **`sales_orders_raw`** は、 */databricks-datasets/retail-org/sales_orders/* にあるサンプルデータセットからJSONデータを段階的に取り込みます。
-- MAGIC 
-- MAGIC （構造化ストリーミングと同じ処理モデルを使用した）<a herf="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a>を介した増分処理は、以下のように宣言に  **`STREAMING`**  キーワードを追加する必要があります。  **`cloud_files()`** メソッドを使うと、Auto LoaderをSQLでネイティブに使用できます。 このメソッドは、次の位置パラメーターを取ります。
-- MAGIC * 上記の通り、ソースの場所
-- MAGIC * ソースデータフォーマット。今回の場合はJSONを指す
-- MAGIC * 任意読み取りオプションの配列。 この場合、 **`cloudFiles.inferColumnTypes`** を **`true`** に設定します。
-- MAGIC 
-- MAGIC また下記には、データカタログを探索するすべての人に表示される追加のテーブルメタデータ （この場合はコメントとプロパティ）の宣言も示しています。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sales_orders_raw
COMMENT "The raw sales orders, ingested from /databricks-datasets."
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/sales_orders/", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ### customers
-- MAGIC 
-- MAGIC  **`customers`** は、 */databricks-datasets/retail-org/customers/* にあるCSV顧客データを表します。 このテーブルは購入記録に基づいた顧客データを調べるために、この後すぐに結合操作で使用します。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customers
COMMENT "The customers buying finished products, ingested from /databricks-datasets."
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/customers/", "csv");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## シルバーレイヤーテーブルを宣言する（Declare Silver Layer Tables）
-- MAGIC 
-- MAGIC 次に、シルバーレイヤーを実装する2つのテーブルを宣言します。 このレイヤーは、ダウンストリームアプリケーションの最適化を目的に、ブロンズレイヤーから得られる精製されたデータのコピーを表しています。 このレベルでは、データクレンジングやエンリッチ化などの操作を適用します。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ### sales_orders_cleaned
-- MAGIC 
-- MAGIC ここで最初のシルバーテーブルを宣言します。これは、注文番号がNULL値のレコードを拒否して品質管理を行うだけでなく、顧客情報を持つ販売トランザクションデータをエンリッチ化します。
-- MAGIC 
-- MAGIC この宣言は多くの新しい概念を導入します。
-- MAGIC 
-- MAGIC #### 品質管理（Quality Control）
-- MAGIC 
-- MAGIC  **`CONSTRAINT`** キーワードで品質管理を導入します。 従来の **`WHERE`** 句の機能と同じように、 **`CONSTRAINT`** はDLTと統合することで制約違反のメトリクスを集めることができます。 制約はオプションの **`ON VIOLATION`** 句を提供し、制約違反のレコードに対して実行するアクションを指定します。 現在DLTでサポートされている3つのモードは以下の通りです
-- MAGIC 
-- MAGIC |  **`ON VIOLATION`**  | 動作                             |
-- MAGIC | ------------------ | ------------------------------ |
-- MAGIC |  **`FAIL UPDATE`**   | 制約違反が発生した際のパイプライン障害            |
-- MAGIC |  **`DROP ROW`**      | 制約違反のレコードを破棄する                 |
-- MAGIC | 省略                 | 制約違反のレコードが含まれる（但し、違反はメトリクスで報告） |
-- MAGIC 
-- MAGIC #### DLTテーブルとビューの参照（References to DLT Tables and Views）
-- MAGIC 他のDLTテーブルとビューへの参照は、常に **`live.`** プレフィックスを含みます。 ターゲットのデータベース名はランタイム時に自動で置き換えられるため、DEV/QA/PROD環境間でのパイプラインの移行が簡単に行えます。
-- MAGIC 
-- MAGIC #### ストリーミングテーブルの参照（References to Streaming Tables）
-- MAGIC 
-- MAGIC ストリーミングDLTテーブルへの参照は **`STREAM()`** を使用して、テーブル名を引数として渡します。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sales_orders_cleaned(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned sales orders with valid order_number(s) and partitioned by order_datetime."
AS
  SELECT f.customer_id, f.customer_name, f.number_of_line_items, 
         timestamp(from_unixtime((cast(f.order_datetime as long)))) as order_datetime, 
         date(from_unixtime((cast(f.order_datetime as long)))) as order_date, 
         f.order_number, f.ordered_products, c.state, c.city, c.lon, c.lat, c.units_purchased, c.loyalty_segment
  FROM STREAM(LIVE.sales_orders_raw) f
  LEFT JOIN LIVE.customers c
    ON c.customer_id = f.customer_id
    AND c.customer_name = f.customer_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## ゴールドテーブルを宣言する（Declare Gold Table）
-- MAGIC 
-- MAGIC 最も洗練されたレベルのアーキテクチャでは、ビジネス価値のある集約を実現するテーブルを宣言します。この場合だと、特定の地域における販売注文データのコレクションです。 集約では、レポートは日付と顧客ごとのオーダー数と合計を生成します。

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE sales_order_in_la
COMMENT "Sales orders in LA."
AS
  SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, 
         sum(ordered_products_explode.price) as sales, 
         sum(ordered_products_explode.qty) as quantity, 
         count(ordered_products_explode.id) as product_count
  FROM (SELECT city, order_date, customer_id, customer_name, explode(ordered_products) as ordered_products_explode
        FROM LIVE.sales_orders_cleaned 
        WHERE city = 'Los Angeles')
  GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 結果を調べる（Explore Results）
-- MAGIC 
-- MAGIC パイプラインに関わるエンティティとそれらの関係を表すDAG（有向非巡回グラフ）を調べます。 それぞれをクリックして、以下を含むサマリーを表示します。
-- MAGIC * 実行ステータス
-- MAGIC * メタデータサマリー
-- MAGIC * スキーマ
-- MAGIC * データ品質メトリック
-- MAGIC 
-- MAGIC テーブルとログを調べるには、この<a href="$./DE 8.3 - Pipeline Results" target="_blank">付録のノートブック</a>を参照してください。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## パイプラインを更新する（Update Pipeline）
-- MAGIC 
-- MAGIC 次のセルからコメントアウトを外して、他のゴールドテーブルを宣言します。 前のゴールドテーブル宣言と同様に、これはシカゴの **`city`** をフィルタしています。
-- MAGIC 
-- MAGIC パイプラインを再実行して、更新された結果を調べます。
-- MAGIC 
-- MAGIC 期待通りに実行されていますか？
-- MAGIC 
-- MAGIC 何か問題が起きていませんか？

-- COMMAND ----------

-- TODO
-- CREATE OR REFRESH LIVE TABLE sales_order_in_chicago
-- COMMENT "Sales orders in Chicago."
-- AS
--   SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, 
--          sum(ordered_products_explode.price) as sales, 
--          sum(ordered_products_explode.qty) as quantity, 
--          count(ordered_products_explode.id) as product_count
--   FROM (SELECT city, order_date, customer_id, customer_name, explode(ordered_products) as ordered_products_explode
--         FROM sales_orders_cleaned 
--         WHERE city = 'Chicago')
--   GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
