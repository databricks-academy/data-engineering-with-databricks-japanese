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
-- MAGIC # SQLのUDFと制御流れ（SQL UDFs and Control Flow）
-- MAGIC 
-- MAGIC DatabricksはDBR 9.1以降に、SQLでネイティブに登録できる、ユーザーによって定義された関数（UDF）のサポートを追加しました。
-- MAGIC 
-- MAGIC この機能を使用すると、SQLロジックのカスタムな組み合わせを関数としてデータベースに登録できます。これにより、これらのメソッドは、DatabricksでSQLを実行できる場所であれば再利用できます。 これらの関数は、Spark SQLを直接活用し、カスタムロジックを大きなデータセットに適用する際にSparkの最適化をすべて維持します。
-- MAGIC 
-- MAGIC このノートブックでは、まずはこれらのメソッドを簡単に紹介します。次に、再利用可能なカスタムの制御流れのロジックを構築するために、このロジックを **`CASE`**  /  **`WHEN`** 句と組み合わせる方法を学びます。
-- MAGIC 
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このレッスンでは、以下のことが学べます。
-- MAGIC * SQL UDFの定義と登録
-- MAGIC * SQL UDFの共有で使用するセキュリティモデルを説明する
-- MAGIC * SQLコードで **`CASE`**  /  **`WHEN`** 文を使用する
-- MAGIC * カスタムの制御流れのためにSQL UDFで **`CASE`**  /  **`WHEN`** 文を活用する

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## セットアップ（Setup）
-- MAGIC 次のセルを実行して、環境をセットアップします。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.8

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 簡単なデータセットを作成する（Create a Simple Dataset）
-- MAGIC 
-- MAGIC このノートブックでは、ここにテンポラリビューとして登録されている次のデータセットを扱います。

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW foods(food) AS VALUES
("beef"),
("beans"),
("potatoes"),
("bread");

SELECT * FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## SQL UDF
-- MAGIC SQL UDFには少なくとも、関数名、任意のパラメーター、戻り値の型、いくつかのカスタムロジックが必要です。
-- MAGIC 
-- MAGIC 以下の簡単な関数である **`yelling`** は **`text`** というの1つのパラメーターを取ります。 この関数は、最後に3つのはてなマークがついている全ての文字が大文字の文字列を返します。

-- COMMAND ----------

CREATE OR REPLACE FUNCTION yelling(text STRING)
RETURNS STRING
RETURN concat(upper(text), "!!!")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC この関数が列にあるすべての値にSpark処理エンジン内で平行に適用されることにご注意ください。 SQL UDFを使用すると、Databricksでの実行に最適化されているカスタムロジックを効率的に定義できます。

-- COMMAND ----------

SELECT yelling(food) FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## SQL UDFの範囲と権限（Scoping and Permissions of SQL UDFs）
-- MAGIC 
-- MAGIC SQL UDFは（ノートブック、DBSQLクエリ、およびジョブなど）実行環境の間で保持されるのでご注意ください。
-- MAGIC 
-- MAGIC 関数の定義を表示すると、登録された場所および予想入力と戻り値についての基本情報を確認できます。

-- COMMAND ----------

DESCRIBE FUNCTION yelling

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC DESCRIBE EXTENDEDを実行するとさらに多くの情報を表示できます。
-- MAGIC 
-- MAGIC 関数の説明の下にある **`Body`** フィールドは、関数自体の中で使用されているSQLロジックを表します。

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED yelling

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC SQL UDFsはメタストア内のオブジェクトとして存在し、データベース、テーブル、ビューと同じテーブルACLによって管理されます。
-- MAGIC 
-- MAGIC SQL UDFを使用するのにユーザーは、関数に対して **`USAGE`** と **`SELECT`** の権限を持っている必要があります。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## CASE/WHEN
-- MAGIC 
-- MAGIC SQLの標準統語構造である **`CASE`**  /  **`WHEN`** を使用すると、テーブルの内容によって結果が異なる複数の条件文を評価できます。
-- MAGIC 
-- MAGIC 繰り返しますが、すべての評価がSparkでネイティブに実行されるため、並行処理に最適化されています。

-- COMMAND ----------

SELECT *,
  CASE 
    WHEN food = "beans" THEN "I love beans"
    WHEN food = "potatoes" THEN "My favorite vegetable is potatoes"
    WHEN food <> "beef" THEN concat("Do you have any good recipes for ", food ,"?")
    ELSE concat("I don't eat ", food)
  END
FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 簡単な流れ制御の関数（Simple Control Flow Functions）
-- MAGIC 
-- MAGIC SQL UDFを **`CASE `**  /  **`WHEN `** 句の形式で制御流れと組み合わせると、SQL内のワークロードの制御流れの実行が最適化されます。
-- MAGIC 
-- MAGIC ここでは、前のロジックを、SQLを実行できる場所ならどこでも再利用できる関数で包む方法を示します。

-- COMMAND ----------

CREATE FUNCTION foods_i_like(food STRING)
RETURNS STRING
RETURN CASE 
  WHEN food = "beans" THEN "I love beans"
  WHEN food = "potatoes" THEN "My favorite vegetable is potatoes"
  WHEN food <> "beef" THEN concat("Do you have any good recipes for ", food ,"?")
  ELSE concat("I don't eat ", food)
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC このメソッドをこのデータに使用すると、予想した結果が得られます。

-- COMMAND ----------

SELECT foods_i_like(food) FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ここに用意されている例は簡単な文字列のメソッドですが、この基本原理を使用してカスタムの計算およびSpark SQLでのネイティブの実行のためのロジックを追加できます。
-- MAGIC 
-- MAGIC 特に、多くの定義済み手順もしくはカスタムで定義された式を持つシステムからユーザーを移行させている企業の場合、SQL UDFを使用すると、一般的な報告および分析のクエリに必要な複雑なロジックを少数のユーザーが定義できるようになります。

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
