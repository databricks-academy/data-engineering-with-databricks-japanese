-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="7aa87ebc-24dd-4b39-bb02-7c59fa083a14"/>
-- MAGIC 
-- MAGIC # Deltaテーブルの管理（Managing Delta Tables）
-- MAGIC 
-- MAGIC いずれかの種類のSQLを知っている場合は、データレイクハウスで効率的に作業するのに必要な知識はすでにたくさん持っているはずです。
-- MAGIC 
-- MAGIC このノートブックでは、DatabricksでのSQLを使ったデータとテーブルの基本的な操作について説明します。
-- MAGIC 
-- MAGIC Delta LakeはDatabricksで作成されたすべてのテーブルのデフォルト形式であることに注意してください。DatabricksでSQL文を実行している場合は、すでにDelta Lakeを使用している可能性があります。
-- MAGIC 
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このレッスンでは、以下のことが学べます。
-- MAGIC * Delta Lakeテーブルを作成する
-- MAGIC * Delta Lakeテーブルからデータを照会する
-- MAGIC * Delta Lakeテーブルでレコードを挿入、更新、削除をする
-- MAGIC * Delta Lakeでアップサーと文を作成する
-- MAGIC * Delta Lakeテーブルを削除する

-- COMMAND ----------

-- MAGIC %md <i18n value="add37b8c-6a95-423f-a09a-876e489ef17d"/>
-- MAGIC 
-- MAGIC ## セットアップを実行する（Run Setup）
-- MAGIC まずはセットアップスクリプトを実行します。 セットアップスクリプトは、ユーザー名、ユーザーホーム、各ユーザーを対象とするデータベースを定義します。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-02.1

-- COMMAND ----------

-- MAGIC %md <i18n value="3b9c0755-bf72-480e-a836-18a4eceb97d2"/>
-- MAGIC 
-- MAGIC ## Deltaテーブルの作成（Creating a Delta Table）
-- MAGIC 
-- MAGIC Delta Lakeでは、あまりコードを書かずにテーブルを作成できます。 Delta Lakeテーブルの作成方法はいくつかあり、コース全体を通して見ていきます。 最も簡単な方法の1つから始めます：空のDelta Lakeテーブルの登録。
-- MAGIC 
-- MAGIC 必要なもの：
-- MAGIC -  **`CREATE TABLE`** 文
-- MAGIC - テーブルの名前（以下では **`students`** を使用します）
-- MAGIC - スキーマ
-- MAGIC 
-- MAGIC **注意：**Databricks Runtime 8.0以降では、Delta Lakeがデフォルトの形式であるため、 **`USING DELTA`** は不要です。

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);

-- COMMAND ----------

-- MAGIC %md <i18n value="a00174f3-bbcd-4ee3-af0e-b8d4ccb58481"/>
-- MAGIC 
-- MAGIC 戻ってこのセルを再び実行しようとすると…エラーになります！ これは予想されたことです。そのテーブルはすでに存在するため、エラーが発生します。
-- MAGIC 
-- MAGIC テーブルが存在するかどうかをチェックする **`IF NOT EXISTS`** という引数を追加できます。 これでエラーを解消できます。

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS students 
  (id INT, name STRING, value DOUBLE)

-- COMMAND ----------

-- MAGIC %md <i18n value="408b1c71-b26b-43c0-b144-d5e92064a5ac"/>
-- MAGIC 
-- MAGIC ## データを挿入する（Insert Data）
-- MAGIC ほとんどの場合、データは別のソースからのクエリの結果として、テーブルに挿入されます。
-- MAGIC 
-- MAGIC しかしながら標準的なSQLと同様、ここに示すように値を直接挿入することもできます。

-- COMMAND ----------

INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

-- COMMAND ----------

-- MAGIC %md <i18n value="853dd803-9f64-42d7-b5e8-5477ea61029e"/>
-- MAGIC 
-- MAGIC 上のセルでは、3つの別々の **`INSERT`** 文を完成させました。 これらは独自のACID保証付きの別個のトランザクションとして処理されます。 ほとんどの場合、1回のトランザクションで多くのレコードを挿入します。

-- COMMAND ----------

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3)

-- COMMAND ----------

-- MAGIC %md <i18n value="7972982a-05be-46ce-954e-e9d29e3b7329"/>
-- MAGIC 
-- MAGIC Databricksには **`COMMIT`** キーワードがないことに注意してください。トランザクションは実行されるとすぐに開始され、成功するとコミットされます。

-- COMMAND ----------

-- MAGIC %md <i18n value="121bd36c-10c4-41fc-b730-2a6fb626c6af"/>
-- MAGIC 
-- MAGIC ## Deltaテーブルの照会（Querying a Delta Table）
-- MAGIC 
-- MAGIC Delta Lakeテーブルを照会することが、標準的な **`SELECT`** 文を使うのと同じくらい簡単であることにはすでに気付いているかと思います。

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md <i18n value="4ecaf351-d4a4-4803-8990-5864995287a4"/>
-- MAGIC 
-- MAGIC しかし、Delta Lakeでは、テーブルに対する任意の読み取りが**常に**最新バージョンのテーブルを返すこと、そして進行中の操作によるデッドロック状態に遭遇することは決してないことを保証することにはまだ気づいていないかもしれません。
-- MAGIC 
-- MAGIC 繰り返しますが、テーブルの読み込みは他の操作と決して競合しませんし、レイクハウスにクエリ可能な全クライアントが最新版のデータをすぐに利用できます。 すべてのトランザクション情報はデータファイルと一緒にクラウドオブジェクトストレージに保存されるので、Delta Lakeテーブルでの同時読み取りはクラウドベンダーのハードの制約にのみ制限されます。 （**注**：無限ではありませんが、少なくとも1秒間に何千回もの読み取りです。）

-- COMMAND ----------

-- MAGIC %md <i18n value="8a379d8d-7c48-43b0-8e25-3e653d8d6e86"/>
-- MAGIC 
-- MAGIC ## レコードの更新（Updating Records）
-- MAGIC 
-- MAGIC レコードの更新により、アトミック性も保証されます：現バージョンのテーブルのスナップショット読み取りを行い、  **`WHERE`** 句と一致するすべてのフィールドを見つけ、記述された変更を適用します。
-- MAGIC 
-- MAGIC 以下では、名前が**T**の文字で始まる受講者をすべて見つけ、その **`value`** の列の数字に1を足します。

-- COMMAND ----------

UPDATE students 
SET value = value + 1
WHERE name LIKE "T%"

-- COMMAND ----------

-- MAGIC %md <i18n value="b307b3e7-5ed2-4df8-bdd5-6c25acfd072f"/>
-- MAGIC 
-- MAGIC 再びテーブルのクエリを行って、これらの変更が適用されているか確かめます。

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md <i18n value="d581b9a2-f450-43dc-bff3-2ea9cc46ad4c"/>
-- MAGIC 
-- MAGIC ## レコードの削除（Deleting Records）
-- MAGIC 
-- MAGIC 削除もアトミックなので、データをデータレイクハウスから削除した場合、部分的にしか成功しないというリスクはありません。
-- MAGIC 
-- MAGIC  **`DELETE`** 文は1つ以上のレコードを削除できますが、常に単一のトランザクションになります。

-- COMMAND ----------

DELETE FROM students 
WHERE value > 6

-- COMMAND ----------

-- MAGIC %md <i18n value="b5b346b8-a3df-45f2-88a7-8cf8dea6d815"/>
-- MAGIC 
-- MAGIC ## マージを使う（Using Merge）
-- MAGIC 
-- MAGIC 一部のSQLシステムにはアップサートの概念があり、単一のコマンドで更新、挿入、その他のデータの操作を実行できます。
-- MAGIC 
-- MAGIC Databricksは **`MERGE`** キーワードを使ってこの操作を行います。
-- MAGIC 
-- MAGIC 次のテンポラリビューを考えてみましょう。このテンポラリビューには、変更データキャプチャ（CDC）フィードにより出力される可能性のある4つのレコードが含まれています。

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
SELECT * FROM updates;

-- COMMAND ----------

-- MAGIC %md <i18n value="6fe009d5-513f-4b93-994f-1ae9a0f30a80"/>
-- MAGIC 
-- MAGIC これまで見てきた構文を使えば、このビューを種類ごとにフィルタリングして、レコードの挿入、更新、削除をそれぞれ1つずつ、計3つの文を書くことができます。 ですがこの結果、3つの別個のトランザクションが発生します。これらのトランザクションのいずれかが失敗すれば、データが無効な状態になる可能性があります。
-- MAGIC 
-- MAGIC その代わり、これらのアクションを組み合わせて単一のアトミックなトランザクションにし、3種類の変更をまとめて適用します。
-- MAGIC 
-- MAGIC  **`MERGE`** 文には少なくとも1つの一致したフィールドが必要で、各 **`WHEN MATCHED`** または **`WHEN NOT MATCHED`** 句は任意の数の追加の条件文を含むことができます。
-- MAGIC 
-- MAGIC ここでは **`id`** フィールドで一致させ、 **`type`** フィールドでフィルタリングして、レコードを適切に更新、削除、挿入します。

-- COMMAND ----------

MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *

-- COMMAND ----------

-- MAGIC %md <i18n value="77cee0a0-f94b-4016-a20b-08e4857d13db"/>
-- MAGIC 
-- MAGIC **`MERGE`** 文により3つのレコードだけが影響を受けたことに注目してください。updatesテーブルのレコードの1つはstudentsテーブル内で一致している **`id`** がありませんでしたが、 **`update`** と印が付いています。 カスタムロジックに基づき、挿入するよりはむしろこのレコードを無視しました。
-- MAGIC 
-- MAGIC 最終的な **`INSERT`** 句に **`update`** の印がついた一致しないレコードを含めるには、上の文をどのように変更しますか？

-- COMMAND ----------

-- MAGIC %md <i18n value="4eca2c53-e457-4964-875e-d39d9205c3c6"/>
-- MAGIC 
-- MAGIC ## テーブルの削除（Dropping a Table）
-- MAGIC 
-- MAGIC ターゲットテーブルについて適切な権限を持っているとすれば、 **`DROP TABLE`** コマンドを使って、レイクハウス内のデータを永久に削除できます。
-- MAGIC 
-- MAGIC **注**：このコースの後半で、テーブルアクセス制御リスト（Table Access Control Lists; ACLs）とデフォルトの権限について説明します。 適切に構成されたレイクハウスでは、ユーザーはプロダクションテーブルを削除**できない**はずです。

-- COMMAND ----------

DROP TABLE students

-- COMMAND ----------

-- MAGIC %md <i18n value="08cbbda5-96b2-4ae8-889f-b1f4c04d1496"/>
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
