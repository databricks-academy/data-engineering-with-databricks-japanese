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
-- MAGIC # データ再形成のラボ（Reshaping Data Lab）
-- MAGIC 
-- MAGIC このラボでは、 **`clickpaths`** テーブルを作成します。このテーブルは各ユーザーが **`events`** で特定のアクションを実行した回数を集計したものです。次に、この情報を、以前のノートブックで作成した **`transactions`** のフラット化されたビューと結合します。
-- MAGIC 
-- MAGIC 項目の名前から抽出した情報に基づいて、 **`sales`** に記録されている項目にフラグを立てるための新しい高階関数について学びます。
-- MAGIC 
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このラボでは、以下のことが学べます。
-- MAGIC - テーブルをパイボットして結合し、各ユーザーのためのクリックパスを作成する
-- MAGIC - 購入された商品の種類にフラグを立てるために高階関数を適用する

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## セットアップを実行する（Run Setup）
-- MAGIC 
-- MAGIC セットアップスクリプトでは、このノートブックの実行に必要なデータを作成し値を宣言します。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.9L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## データセットを再形成してクリックパスを作成する（Reshape Datasets to Create Click Paths）
-- MAGIC この操作は、 **`events`** と **`transactions`** のテーブルのデータを結合して、ユーザーがサイト上で行ったアクションとその最終注文のレコードを作成します。
-- MAGIC 
-- MAGIC  **`clickpaths`** テーブルは、 **`transactions`** テーブルのすべてのフィールドおよびすべての **`event_name`** のカウントを別々の列に含める必要があります。 最終テーブルには、購入を行ったことがある各ユーザーに対して1列が含まれている必要があります。 まずは、 **`events`** テーブルをパイボットして各 **`event_name`** に対してカウントを取得しましょう。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ### 1.  **`events`** をパイボットして、各ユーザーに対してアクションをカウントします
-- MAGIC 各ユーザーが、 **`event_name`** 列に記載されている特定のイベントを行った数を集計したいと思います。 これを行うには、 **`user`** でグループ化して **`event_name`** でパイボットして各種のイベントのカウントを独自の列に記録し、その結果、以下のスキーマになります。
-- MAGIC 
-- MAGIC | フィールド         | 型      |
-- MAGIC | ------------- | ------ |
-- MAGIC | user          | STRING |
-- MAGIC | cart          | BIGINT |
-- MAGIC | pillows       | BIGINT |
-- MAGIC | login         | BIGINT |
-- MAGIC | main          | BIGINT |
-- MAGIC | careers       | BIGINT |
-- MAGIC | guest         | BIGINT |
-- MAGIC | faq           | BIGINT |
-- MAGIC | down          | BIGINT |
-- MAGIC | warranty      | BIGINT |
-- MAGIC | finalize      | BIGINT |
-- MAGIC | register      | BIGINT |
-- MAGIC | shipping_info | BIGINT |
-- MAGIC | checkout      | BIGINT |
-- MAGIC | mattresses    | BIGINT |
-- MAGIC | add_item      | BIGINT |
-- MAGIC | press         | BIGINT |
-- MAGIC | email_coupon  | BIGINT |
-- MAGIC | cc_info       | BIGINT |
-- MAGIC | foam          | BIGINT |
-- MAGIC | reviews       | BIGINT |
-- MAGIC | original      | BIGINT |
-- MAGIC | delivery      | BIGINT |
-- MAGIC | premium       | BIGINT |
-- MAGIC 
-- MAGIC イベントの名前の一覧は以下をご参照ください。

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE VIEW events_pivot AS
SELECT * FROM (
  SELECT user_id user, event_name 
  FROM events
) PIVOT ( count(*) FOR event_name IN (
    "cart", "pillows", "login", "main", "careers", "guest", "faq", "down", "warranty", "finalize", 
    "register", "shipping_info", "checkout", "mattresses", "add_item", "press", "email_coupon", 
    "cc_info", "foam", "reviews", "original", "delivery", "premium" ))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC **注**：このラボでは、Pythonを使って時々チェックを実行します。 手順に従っていない場合、以下のヘルパー関数は変更すべきことについてのメッセージを記載したエラーを返します。 出力がない場合、このステップは完了です。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, column_names, num_rows):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert spark.table(table_name).columns == column_names, "Please name the columns in the order provided above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 以下のセルを実行して、ビューが正しく作成されたことを確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC event_columns = ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium']
-- MAGIC check_table_results("events_pivot", event_columns, 204586)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ### 2. すべてのユーザーに対してイベントカウントとトランザクションを結合します
-- MAGIC 
-- MAGIC 次に **`events_pivot`** を **`transactions`** と結合して **`clickpaths`** テーブルを作成します。 このテーブルには、以下の通り、上記で作成した **`events_pivot`** テーブルと同じイベントの名前の列に続いて、 **`transactions`** テーブルの列が含まれている必要があります。
-- MAGIC 
-- MAGIC | フィールド                     | 型      |
-- MAGIC | ------------------------- | ------ |
-- MAGIC | user                      | STRING |
-- MAGIC | cart                      | BIGINT |
-- MAGIC | ...                       | ...    |
-- MAGIC | user_id                   | STRING |
-- MAGIC | order_id                  | BIGINT |
-- MAGIC | transaction_timestamp     | BIGINT |
-- MAGIC | total_item_quantity     | BIGINT |
-- MAGIC | purchase_revenue_in_usd | DOUBLE |
-- MAGIC | unique_items              | BIGINT |
-- MAGIC | P_FOAM_K                | BIGINT |
-- MAGIC | M_STAN_Q                | BIGINT |
-- MAGIC | P_FOAM_S                | BIGINT |
-- MAGIC | M_PREM_Q                | BIGINT |
-- MAGIC | M_STAN_F                | BIGINT |
-- MAGIC | M_STAN_T                | BIGINT |
-- MAGIC | M_PREM_K                | BIGINT |
-- MAGIC | M_PREM_F                | BIGINT |
-- MAGIC | M_STAN_K                | BIGINT |
-- MAGIC | M_PREM_T                | BIGINT |
-- MAGIC | P_DOWN_S                | BIGINT |
-- MAGIC | P_DOWN_K                | BIGINT |

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE VIEW clickpaths AS
SELECT * 
FROM events_pivot a
JOIN transactions b 
  ON a.user = b.user_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 以下のセルを実行して、テーブルが正しく作成されたことを確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clickpath_columns = event_columns + ['user_id', 'order_id', 'transaction_timestamp', 'total_item_quantity', 'purchase_revenue_in_usd', 'unique_items', 'P_FOAM_K', 'M_STAN_Q', 'P_FOAM_S', 'M_PREM_Q', 'M_STAN_F', 'M_STAN_T', 'M_PREM_K', 'M_PREM_F', 'M_STAN_K', 'M_PREM_T', 'P_DOWN_S', 'P_DOWN_K']
-- MAGIC check_table_results("clickpaths", clickpath_columns, 9085)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 購入した商品の種類にフラグを立てる（Flag Types of Products Purchased）
-- MAGIC ここでは高階関数である **`EXISTS`** を使用して、購入された商品がマットレスか枕かを示す **`mattress`** と **`pillow`** というブールの列を作成します。
-- MAGIC 
-- MAGIC 例えば、  **`items`** 列の **`item_name`** が **`"Mattress"`** という文字列で終わる場合、 **`mattress`** 列の値は **`true`** で **`pillow`** 列の値は **`false`** になります。 以下は、項目とその結果の値の例です。
-- MAGIC 
-- MAGIC | 項目                                                                               | mattress | pillow |
-- MAGIC | -------------------------------------------------------------------------------- | -------- | ------ |
-- MAGIC |  **`[{..., "item_id": "M_PREM_K", "item_name": "Premium King Mattress", ...}]`**   | true     | false  |
-- MAGIC |  **`[{..., "item_id": "P_FOAM_S", "item_name": "Standard Foam Pillow", ...}]`**    | false    | true   |
-- MAGIC |  **`[{..., "item_id": "M_STAN_F", "item_name": "Standard Full Mattress", ...}]`**  | true     | false  |
-- MAGIC 
-- MAGIC <a href="https://docs.databricks.com/sql/language-manual/functions/exists.html" target="_blank">exists</a>関数のドキュメントをご参照ください。  
-- MAGIC  **`item_name LIKE "%Mattress"`** の条件表現を使用すると、 **`item_name`** の文字列が「Mattress」で終わるかどうかを確認できます。

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TABLE sales_product_flags AS
SELECT
  items,
  EXISTS (items, i -> i.item_name LIKE "%Mattress") AS mattress,
  EXISTS (items, i -> i.item_name LIKE "%Pillow") AS pillow
FROM sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 以下のセルを実行して、テーブルが正しく作成されたことを確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("sales_product_flags", ['items', 'mattress', 'pillow'], 10539)
-- MAGIC product_counts = spark.sql("SELECT sum(CAST(mattress AS INT)) num_mattress, sum(CAST(pillow AS INT)) num_pillow FROM sales_product_flags").first().asDict()
-- MAGIC assert product_counts == {'num_mattress': 10015, 'num_pillow': 1386}, "There should be 10015 rows where mattress is true, and 1386 where pillow is true"

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
