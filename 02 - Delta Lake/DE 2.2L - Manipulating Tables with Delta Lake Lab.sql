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
-- MAGIC 
-- MAGIC # Delta Lakeを使用してテーブルを操作する（Manipulating Tables with Delta Lake）
-- MAGIC 
-- MAGIC このノートブックでは、Delta Lakeの基本機能の一部を実践的に説明します。
-- MAGIC 
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このラボでは、以下のことが学べます。
-- MAGIC - 次の操作を含めて、Delta Lakeテーブルを作成および操作をするための標準的な操作を実行する：
-- MAGIC   -  **`CREATE TABLE`** 
-- MAGIC   -  **`INSERT INTO`** 
-- MAGIC   -  **`SELECT FROM`** 
-- MAGIC   -  **`UPDATE`** 
-- MAGIC   -  **`DELETE`** 
-- MAGIC   -  **`MERGE`** 
-- MAGIC   -  **`DROP TABLE`** 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## セットアップ（Setup）
-- MAGIC 次のスクリプトを実行して必要な変数をセットアップし、このノートブックにおける過去の実行を消去します。 このセルを再実行するとラボを再起動できる点に注意してください。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2.2L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## テーブルを作成する（Create a Table）
-- MAGIC 
-- MAGIC このノートブックでは、豆のコレクションを管理するためのテーブルを作成します。
-- MAGIC 
-- MAGIC 以下のセルを使って、 **`beans`** というのマネージドDelta Lakeテーブルを作成します。
-- MAGIC 
-- MAGIC 次のスキーマを指定します：
-- MAGIC 
-- MAGIC | フィールド名    | フィールド型  |
-- MAGIC | --------- | ------- |
-- MAGIC | name      | STRING  |
-- MAGIC | color     | STRING  |
-- MAGIC | grams     | FLOAT   |
-- MAGIC | delicious | BOOLEAN |

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC **注**：このラボでは、Pythonを使って時々チェックを実行します。 手順に従っていない場合、次のセルは変更すべきことについてのメッセージを記載したエラーを返します。 セルを実行しても出力がない場合、このステップは完了です。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans"), "Table named `beans` does not exist"
-- MAGIC assert spark.table("beans").columns == ["name", "color", "grams", "delicious"], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("beans").dtypes == [("name", "string"), ("color", "string"), ("grams", "float"), ("delicious", "boolean")], "Please make sure the column types are identical to those provided above"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## データを挿入する（Insert Data）
-- MAGIC 
-- MAGIC 以下のセルを実行し、テーブルに3行を挿入します。

-- COMMAND ----------

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 手動でテーブルの内容を確認し、データが期待通りに書き込まれたことを確認します。

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 以下に用意された追加のレコードを挿入します。 これは必ず単一のトランザクションとして実行してください。

-- COMMAND ----------

-- TODO
<FILL-IN>
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 以下のセルを実行して、データが適切な状態にあることを確認しましょう。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").count() == 6, "The table should have 6 records"
-- MAGIC assert spark.conf.get("spark.databricks.delta.lastCommitVersionInSession") == "2", "Only 3 commits should have been made to the table"
-- MAGIC assert set(row["name"] for row in spark.table("beans").select("name").collect()) == {'beanbag chair', 'black', 'green', 'jelly', 'lentils', 'pinto'}, "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## レコードの更新（Update Records）
-- MAGIC 
-- MAGIC 友人が豆の一覧表を吟味しています。 大いに議論した後、ゼリービーンはとても美味しいということで意見が一致します。
-- MAGIC 
-- MAGIC 次のセルを実行して、このレコードを更新してください。

-- COMMAND ----------

UPDATE beans
SET delicious = true
WHERE name = "jelly"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC うっかり、うずら豆の重量を間違って入力したことに気づきます。
-- MAGIC 
-- MAGIC このレコードの **`grams`** 列を正しい重量1500に更新してください。

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 以下のセルを実行して、これが適切に完了したことを確認しましょう。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").filter("name='pinto'").count() == 1, "There should only be 1 entry for pinto beans"
-- MAGIC row = spark.table("beans").filter("name='pinto'").first()
-- MAGIC assert row["color"] == "brown", "The pinto bean should be labeled as the color brown"
-- MAGIC assert row["grams"] == 1500, "Make sure you correctly specified the `grams` as 1500"
-- MAGIC assert row["delicious"] == True, "The pinto bean is a delicious bean"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## レコードの削除（Delete Records）
-- MAGIC 
-- MAGIC 美味しい豆だけを記録すると決めます。
-- MAGIC 
-- MAGIC クエリを実行して、あまり美味しくない豆をすべて削除します。

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 次のセルを実行して、この操作が成功したことを確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").filter("delicious=true").count() == 5, "There should be 5 delicious beans in your table"
-- MAGIC assert spark.table("beans").filter("name='beanbag chair'").count() == 0, "Make sure your logic deletes non-delicious beans"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## マージを使ってレコードをアップサートする（Using Merge to Upsert Records）
-- MAGIC 
-- MAGIC 友人がいくつか新しい豆をくれます。 以下のセルは、これらをテンポラリビューとして登録します。

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

SELECT * FROM new_beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 以下のセルでは、上のビューを使ってMERGE文を書き出し、1つのトランザクションとして **`beans`** テーブルを更新して新しいレコードを挿入します。
-- MAGIC 
-- MAGIC ロジックを確認します：
-- MAGIC - 名前**および**色で豆を一致させる
-- MAGIC - 新しい重量を既存の重量に追加することで、既存の豆を更新する
-- MAGIC - 特に美味しい場合にだけ、新しい豆を挿入する

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 以下のセルを実行して、結果を確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC version = spark.sql("DESCRIBE HISTORY beans").selectExpr("max(version)").first()[0]
-- MAGIC last_tx = spark.sql("DESCRIBE HISTORY beans").filter(f"version={version}")
-- MAGIC assert last_tx.select("operation").first()[0] == "MERGE", "Transaction should be completed as a merge"
-- MAGIC metrics = last_tx.select("operationMetrics").first()[0]
-- MAGIC assert metrics["numOutputRows"] == "3", "Make sure you only insert delicious beans"
-- MAGIC assert metrics["numTargetRowsUpdated"] == "1", "Make sure you match on name and color"
-- MAGIC assert metrics["numTargetRowsInserted"] == "2", "Make sure you insert newly collected beans"
-- MAGIC assert metrics["numTargetRowsDeleted"] == "0", "No rows should be deleted by this operation"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## テーブルの削除（Dropping Tables）
-- MAGIC 
-- MAGIC マネージドDelta Lakeテーブルで作業する場合、テーブルをドロップすると、そのテーブルと元になるすべてのデータファイルへのアクセスを永久削除することになります。
-- MAGIC 
-- MAGIC **注**：このコースの後半で、ファイルのコレクションとしてDelta Lakeテーブルを取り扱い、さまざまな永続性を保証する外部テーブルについて学習します。
-- MAGIC 
-- MAGIC 以下のセルに、クエリを書き込み、 **`beans`** テーブルをドロップします。

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 以下のセルを実行し、テーブルがもう存在しないことをアサートします。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql("SHOW TABLES LIKE 'beans'").collect() == [], "Confirm that you have dropped the `beans` table from your current database"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## まとめ（Wrapping Up）
-- MAGIC 
-- MAGIC このラボでは次のことを学びました。
-- MAGIC * 標準的なDelta Lakeテーブルの作成およびデータ操作コマンドの完了

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
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
