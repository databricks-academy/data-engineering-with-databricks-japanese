-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="a209ac48-08a6-4b89-b728-084a515fd335"/>
# Delta Lakeを使用してテーブルを操作する（Manipulating Tables with Delta Lake）

このノートブックでは、Delta Lakeの基本機能の一部を実践的に説明します。

## 学習目標（Learning Objectives）
このラボでは、以下のことが学べます。
- 次の操作を含めて、Delta Lakeテーブルを作成および操作をするための標準的な操作を実行する：
  -  **`CREATE TABLE`** 
  -  **`INSERT INTO`** 
  -  **`SELECT FROM`** 
  -  **`UPDATE`** 
  -  **`DELETE`** 
  -  **`MERGE`** 
  -  **`DROP TABLE`**

-- COMMAND ----------

-- MAGIC %md <i18n value="6582dbcd-72c7-496b-adbd-23aef98e20e9"/>
## セットアップ（Setup）
次のスクリプトを実行して必要な変数をセットアップし、このノートブックにおける過去の実行を消去します。 このセルを再実行するとラボを再起動できる点に注意してください。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-02.2L

-- COMMAND ----------

-- MAGIC %md <i18n value="0607f2ed-cfe6-4a38-baa4-e6754ec1c664"/>
## テーブルを作成する（Create a Table）

このノートブックでは、豆のコレクションを管理するためのテーブルを作成します。

以下のセルを使って、 **`beans`** というのマネージドDelta Lakeテーブルを作成します。

次のスキーマを指定します：

| フィールド名    | フィールド型  |
| --------- | ------- |
| name      | STRING  |
| color     | STRING  |
| grams     | FLOAT   |
| delicious | BOOLEAN |

-- COMMAND ----------

-- ANSWER
CREATE TABLE beans 
(name STRING, color STRING, grams FLOAT, delicious BOOLEAN);

-- COMMAND ----------

-- MAGIC %md <i18n value="2167d7d7-93d1-4704-a7cb-a0335eaf8da7"/>
**注**：このラボでは、Pythonを使って時々チェックを実行します。 手順に従っていない場合、次のセルは変更すべきことについてのメッセージを記載したエラーを返します。 セルを実行しても出力がない場合、このステップは完了です。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans"), "Table named `beans` does not exist"
-- MAGIC assert spark.table("beans").columns == ["name", "color", "grams", "delicious"], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("beans").dtypes == [("name", "string"), ("color", "string"), ("grams", "float"), ("delicious", "boolean")], "Please make sure the column types are identical to those provided above"

-- COMMAND ----------

-- MAGIC %md <i18n value="89004ef0-db16-474b-8cce-eff85c225a65"/>
## データを挿入する（Insert Data）

以下のセルを実行し、テーブルに3行を挿入します。

-- COMMAND ----------

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false)

-- COMMAND ----------

-- MAGIC %md <i18n value="48d649a1-cde1-491f-a90d-95d2e336e140"/>
手動でテーブルの内容を確認し、データが期待通りに書き込まれたことを確認します。

-- COMMAND ----------

-- ANSWER
SELECT * FROM beans

-- COMMAND ----------

-- MAGIC %md <i18n value="f0406eef-6973-47c9-8f89-a667c53cfea7"/>
以下に用意された追加のレコードを挿入します。 これは必ず単一のトランザクションとして実行してください。

-- COMMAND ----------

-- ANSWER
INSERT INTO beans VALUES
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false)

-- COMMAND ----------

-- MAGIC %md <i18n value="e1764f9f-8052-47bb-a862-b52ca438378a"/>
以下のセルを実行して、データが適切な状態にあることを確認しましょう。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").count() == 6, "The table should have 6 records"
-- MAGIC assert spark.conf.get("spark.databricks.delta.lastCommitVersionInSession") == "2", "Only 3 commits should have been made to the table"
-- MAGIC assert set(row["name"] for row in spark.table("beans").select("name").collect()) == {'beanbag chair', 'black', 'green', 'jelly', 'lentils', 'pinto'}, "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md <i18n value="e38adafa-bd10-4191-9c69-e6a4363532ec"/>
## レコードの更新（Update Records）

友人が豆の一覧表を吟味しています。 大いに議論した後、ゼリービーンはとても美味しいということで意見が一致します。

次のセルを実行して、このレコードを更新してください。

-- COMMAND ----------

UPDATE beans
SET delicious = true
WHERE name = "jelly"

-- COMMAND ----------

-- MAGIC %md <i18n value="d8637fab-6d23-4458-bc08-ff777021e30c"/>
うっかり、うずら豆の重量を間違って入力したことに気づきます。

このレコードの **`grams`** 列を正しい重量1500に更新してください。

-- COMMAND ----------

-- ANSWER
UPDATE beans
SET grams = 1500
WHERE name = 'pinto'

-- COMMAND ----------

-- MAGIC %md <i18n value="954bf892-4db6-4b25-9a9a-83b0f6ecc123"/>
以下のセルを実行して、これが適切に完了したことを確認しましょう。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").filter("name='pinto'").count() == 1, "There should only be 1 entry for pinto beans"
-- MAGIC row = spark.table("beans").filter("name='pinto'").first()
-- MAGIC assert row["color"] == "brown", "The pinto bean should be labeled as the color brown"
-- MAGIC assert row["grams"] == 1500, "Make sure you correctly specified the `grams` as 1500"
-- MAGIC assert row["delicious"] == True, "The pinto bean is a delicious bean"

-- COMMAND ----------

-- MAGIC %md <i18n value="f36d551a-f588-43e4-84a2-6aa49a420c04"/>
## レコードの削除（Delete Records）

美味しい豆だけを記録すると決めます。

クエリを実行して、あまり美味しくない豆をすべて削除します。

-- COMMAND ----------

-- ANSWER
DELETE FROM beans
WHERE delicious = false

-- COMMAND ----------

-- MAGIC %md <i18n value="1c8d924c-3e97-49a0-b5e4-0378c5acd3c8"/>
次のセルを実行して、この操作が成功したことを確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").filter("delicious=true").count() == 5, "There should be 5 delicious beans in your table"
-- MAGIC assert spark.table("beans").filter("name='beanbag chair'").count() == 0, "Make sure your logic deletes non-delicious beans"

-- COMMAND ----------

-- MAGIC %md <i18n value="903473f1-ddca-41ea-ae2f-dc2fac64936e"/>
## マージを使ってレコードをアップサートする（Using Merge to Upsert Records）

友人がいくつか新しい豆をくれます。 以下のセルは、これらをテンポラリビューとして登録します。

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

SELECT * FROM new_beans

-- COMMAND ----------

-- MAGIC %md <i18n value="58d50e50-65f1-403b-b74e-1143cde49356"/>
以下のセルでは、上のビューを使ってMERGE文を書き出し、1つのトランザクションとして **`beans`** テーブルを更新して新しいレコードを挿入します。

ロジックを確認します：
- 名前**および**色で豆を一致させる
- 新しい重量を既存の重量に追加することで、既存の豆を更新する
- 特に美味しい場合にだけ、新しい豆を挿入する

-- COMMAND ----------

-- ANSWER
MERGE INTO beans a
USING new_beans b
ON a.name=b.name AND a.color = b.color
WHEN MATCHED THEN
  UPDATE SET grams = a.grams + b.grams
WHEN NOT MATCHED AND b.delicious = true THEN
  INSERT *

-- COMMAND ----------

-- MAGIC %md <i18n value="9fbb65eb-9119-482f-ab77-35e11af5fb24"/>
以下のセルを実行して、結果を確認します。

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

-- MAGIC %md <i18n value="4a668d7c-e16b-4061-a5b7-1ec732236308"/>
## テーブルの削除（Dropping Tables）

マネージドDelta Lakeテーブルで作業する場合、テーブルをドロップすると、そのテーブルと元になるすべてのデータファイルへのアクセスを永久削除することになります。

**注**：このコースの後半で、ファイルのコレクションとしてDelta Lakeテーブルを取り扱い、さまざまな永続性を保証する外部テーブルについて学習します。

以下のセルに、クエリを書き込み、 **`beans`** テーブルをドロップします。

-- COMMAND ----------

-- ANSWER
DROP TABLE beans

-- COMMAND ----------

-- MAGIC %md <i18n value="4cc5c126-5e56-423e-a814-f6c422312802"/>
以下のセルを実行し、テーブルがもう存在しないことをアサートします。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql("SHOW TABLES LIKE 'beans'").collect() == [], "Confirm that you have dropped the `beans` table from your current database"

-- COMMAND ----------

-- MAGIC %md <i18n value="f4d330e3-dc40-4b6e-9911-34902bab22ae"/>
## まとめ（Wrapping Up）

このラボでは次のことを学びました。
* 標準的なDelta Lakeテーブルの作成およびデータ操作コマンドの完了

-- COMMAND ----------

-- MAGIC %md <i18n value="d59f9828-9b13-4e0e-ae98-7e852cd32198"/>
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
