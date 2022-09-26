-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="7d67b345-b680-4f39-a384-31655b390a78"/>
# データ再形成のラボ（Reshaping Data Lab）

このラボでは、 **`clickpaths`** テーブルを作成します。このテーブルは各ユーザーが **`events`** で特定のアクションを実行した回数を集計したものです。次に、この情報を、以前のノートブックで作成した **`transactions`** のフラット化されたビューと結合します。

項目の名前から抽出した情報に基づいて、 **`sales`** に記録されている項目にフラグを立てるための新しい高階関数について学びます。

## 学習目標（Learning Objectives）
このラボでは、以下のことが学べます。
- テーブルをパイボットして結合し、各ユーザーのためのクリックパスを作成する
- 購入された商品の種類にフラグを立てるために高階関数を適用する

-- COMMAND ----------

-- MAGIC %md <i18n value="db657989-4a07-41c5-acc8-18d6ceabbc85"/>
## セットアップを実行する（Run Setup）

セットアップスクリプトでは、このノートブックの実行に必要なデータを作成し値を宣言します。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-04.9L

-- COMMAND ----------

-- MAGIC %md <i18n value="3b76127f-ef49-4024-a970-67ac52a1fa63"/>
## データセットを再形成してクリックパスを作成する（Reshape Datasets to Create Click Paths）
この操作は、 **`events`** と **`transactions`** のテーブルのデータを結合して、ユーザーがサイト上で行ったアクションとその最終注文のレコードを作成します。

 **`clickpaths`** テーブルは、 **`transactions`** テーブルのすべてのフィールドおよびすべての **`event_name`** のカウントを別々の列に含める必要があります。 最終テーブルには、購入を行ったことがある各ユーザーに対して1列が含まれている必要があります。 まずは、 **`events`** テーブルをパイボットして各 **`event_name`** に対してカウントを取得しましょう。

-- COMMAND ----------

-- MAGIC %md <i18n value="a4124fa1-e5cc-467f-8a7c-fb5978fefad1"/>
### 1.  **`events`** をパイボットして、各ユーザーに対してアクションをカウントします
各ユーザーが、 **`event_name`** 列に記載されている特定のイベントを行った数を集計したいと思います。 これを行うには、 **`user_id`** でグループ化して **`event_name`** でパイボットして各種のイベントのカウントを独自の列に記録し、その結果、以下のスキーマになります。

| フィールド         | 型      |
| ------------- | ------ |
| user_id       | STRING |
| cart          | BIGINT |
| pillows       | BIGINT |
| login         | BIGINT |
| main          | BIGINT |
| careers       | BIGINT |
| guest         | BIGINT |
| faq           | BIGINT |
| down          | BIGINT |
| warranty      | BIGINT |
| finalize      | BIGINT |
| register      | BIGINT |
| shipping_info | BIGINT |
| checkout      | BIGINT |
| mattresses    | BIGINT |
| add_item      | BIGINT |
| press         | BIGINT |
| email_coupon  | BIGINT |
| cc_info       | BIGINT |
| foam          | BIGINT |
| reviews       | BIGINT |
| original      | BIGINT |
| delivery      | BIGINT |
| premium       | BIGINT |

イベントの名前の一覧は以下をご参照ください。

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

-- MAGIC %md <i18n value="8646882a-e334-4293-ba4d-550e86a2ed79"/>
**注**：このラボでは、Pythonを使って時々チェックを実行します。 手順に従っていない場合、以下のヘルパー関数は変更すべきことについてのメッセージを記載したエラーを返します。 出力がない場合、このステップは完了です。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, column_names, num_rows):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert spark.table(table_name).columns == column_names, "Please name the columns in the order provided above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- MAGIC %md <i18n value="75fd31d6-3674-48c4-bc6b-c321a47ade9b"/>
以下のセルを実行して、ビューが正しく作成されたことを確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC event_columns = ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium']
-- MAGIC check_table_results("events_pivot", event_columns, 204586)

-- COMMAND ----------

-- MAGIC %md <i18n value="bb41e6ea-aeae-4f4f-97c5-cad043d151cd"/>
### 2. すべてのユーザーに対してイベントカウントとトランザクションを結合します

次に **`events_pivot`** を **`transactions`** と結合して **`clickpaths`** テーブルを作成します。 このテーブルには、以下の通り、上記で作成した **`events_pivot`** テーブルと同じイベントの名前の列に続いて、 **`transactions`** テーブルの列が含まれている必要があります。

| フィールド                     | 型      |
| ------------------------- | ------ |
| user                      | STRING |
| cart                      | BIGINT |
| ...                       | ...    |
| user_id                   | STRING |
| order_id                  | BIGINT |
| transaction_timestamp     | BIGINT |
| total_item_quantity     | BIGINT |
| purchase_revenue_in_usd | DOUBLE |
| unique_items              | BIGINT |
| P_FOAM_K                | BIGINT |
| M_STAN_Q                | BIGINT |
| P_FOAM_S                | BIGINT |
| M_PREM_Q                | BIGINT |
| M_STAN_F                | BIGINT |
| M_STAN_T                | BIGINT |
| M_PREM_K                | BIGINT |
| M_PREM_F                | BIGINT |
| M_STAN_K                | BIGINT |
| M_PREM_T                | BIGINT |
| P_DOWN_S                | BIGINT |
| P_DOWN_K                | BIGINT |

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE VIEW clickpaths AS
SELECT * 
FROM events_pivot a
JOIN transactions b 
  ON a.user = b.user_id

-- COMMAND ----------

-- MAGIC %md <i18n value="db501e67-0444-4988-a850-e7f374b38f3e"/>
以下のセルを実行して、テーブルが正しく作成されたことを確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clickpath_columns = event_columns + ['user_id', 'order_id', 'transaction_timestamp', 'total_item_quantity', 'purchase_revenue_in_usd', 'unique_items', 'P_FOAM_K', 'M_STAN_Q', 'P_FOAM_S', 'M_PREM_Q', 'M_STAN_F', 'M_STAN_T', 'M_PREM_K', 'M_PREM_F', 'M_STAN_K', 'M_PREM_T', 'P_DOWN_S', 'P_DOWN_K']
-- MAGIC check_table_results("clickpaths", clickpath_columns, 9085)

-- COMMAND ----------

-- MAGIC %md <i18n value="ed22d836-9233-469c-8aa1-4bea42f84517"/>
## 購入した商品の種類にフラグを立てる（Flag Types of Products Purchased）
ここでは高階関数である **`EXISTS`** を使用して、 **`sales`** テーブルのデータから購入された商品がマットレスか枕かを示す **`mattress`** と **`pillow`** というブールの列を作成します。

例えば、  **`items`** 列の **`item_name`** が **`"Mattress"`** という文字列で終わる場合、 **`mattress`** 列の値は **`true`** で **`pillow`** 列の値は **`false`** になります。 以下は、項目とその結果の値の例です。

| 項目                                                                               | mattress | pillow |
| -------------------------------------------------------------------------------- | -------- | ------ |
|  **`[{..., "item_id": "M_PREM_K", "item_name": "Premium King Mattress", ...}]`**   | true     | false  |
|  **`[{..., "item_id": "P_FOAM_S", "item_name": "Standard Foam Pillow", ...}]`**    | false    | true   |
|  **`[{..., "item_id": "M_STAN_F", "item_name": "Standard Full Mattress", ...}]`**  | true     | false  |

<a href="https://docs.databricks.com/sql/language-manual/functions/exists.html" target="_blank">exists</a>関数のドキュメントをご参照ください。  
 **`item_name LIKE "%Mattress"`** の条件表現を使用すると、 **`item_name`** の文字列が「Mattress」で終わるかどうかを確認できます。

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TABLE sales_product_flags AS
SELECT
  items,
  EXISTS (items, i -> i.item_name LIKE "%Mattress") AS mattress,
  EXISTS (items, i -> i.item_name LIKE "%Pillow") AS pillow
FROM sales

-- COMMAND ----------

-- MAGIC %md <i18n value="3d56b9b4-957d-4ea2-8c49-f4e92a4918c9"/>
以下のセルを実行して、テーブルが正しく作成されたことを確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("sales_product_flags", ['items', 'mattress', 'pillow'], 10539)
-- MAGIC product_counts = spark.sql("SELECT sum(CAST(mattress AS INT)) num_mattress, sum(CAST(pillow AS INT)) num_pillow FROM sales_product_flags").first().asDict()
-- MAGIC assert product_counts == {'num_mattress': 10015, 'num_pillow': 1386}, "There should be 10015 rows where mattress is true, and 1386 where pillow is true"

-- COMMAND ----------

-- MAGIC %md <i18n value="e78c6d7f-bd25-4af8-8d01-a6943f24b8d6"/>
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
