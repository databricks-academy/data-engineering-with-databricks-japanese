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
-- MAGIC # Delta Lakeのバージョン管理、最適化、バキューム処理（Delta Lake Versioning, Optimization, and Vacuuming）
-- MAGIC 
-- MAGIC このノートブックでは、Delta Lakeがデータレイクハウスにもたらす、より難解な機能のいくつかを実践的に説明します。
-- MAGIC 
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このラボでは、以下のことが学べます。
-- MAGIC - テーブル履歴の確認
-- MAGIC - 以前のテーブルバージョンを照会して、テーブルを特定のバージョンにロールバックする
-- MAGIC - ファイル圧縮とZ-ORDERインデックスの実行
-- MAGIC - 永久削除の印が付いたファイルをプレビューし、これらの削除をコミットする

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## セットアップ（Setup）
-- MAGIC 次のスクリプトを実行して必要な変数をセットアップし、このノートブックにおける過去の実行を消去します。 このセルを再実行するとラボを再起動できる点に注意してください。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2.4L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Beanコレクションの履歴を再作成する（Recreate the History of your Bean Collection）
-- MAGIC 
-- MAGIC このラボは前回のラボの続きとなります。 以下のセルは、前回のラボの全操作を1つのセルに圧縮します（最後の **`DROP TABLE`** 文以外）。
-- MAGIC 
-- MAGIC 簡単に説明すると、作成された **`beans`** テーブルのスキーマは：
-- MAGIC 
-- MAGIC | フィールド名    | フィールド型  |
-- MAGIC | --------- | ------- |
-- MAGIC | name      | STRING  |
-- MAGIC | color     | STRING  |
-- MAGIC | grams     | FLOAT   |
-- MAGIC | delicious | BOOLEAN |

-- COMMAND ----------

CREATE TABLE beans 
(name STRING, color STRING, grams FLOAT, delicious BOOLEAN);

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false);

INSERT INTO beans VALUES
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false);

UPDATE beans
SET delicious = true
WHERE name = "jelly";

UPDATE beans
SET grams = 1500
WHERE name = 'pinto';

DELETE FROM beans
WHERE delicious = false;

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

MERGE INTO beans a
USING new_beans b
ON a.name=b.name AND a.color = b.color
WHEN MATCHED THEN
  UPDATE SET grams = a.grams + b.grams
WHEN NOT MATCHED AND b.delicious = true THEN
  INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## テーブル履歴を確認する（Review the Table History）
-- MAGIC 
-- MAGIC Delta Lakeのトランザクションログは、テーブルの内容や設定を変更する各トランザクションについての情報を保存します。
-- MAGIC 
-- MAGIC 以下の **`beans`** テーブルの履歴を確認してください。

-- COMMAND ----------

-- ANSWER
DESCRIBE HISTORY beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 以前の全操作が説明通りに完了している場合、テーブルの7つのバージョンが確認できるはずです（**注**：Delta Lakeのバージョン管理は0から始まるので、バージョンの最大値は6です）。
-- MAGIC 
-- MAGIC 操作は次のようになるはずです：
-- MAGIC 
-- MAGIC | バージョン | 操作           |
-- MAGIC | ----- | ------------ |
-- MAGIC | 0     | CREATE TABLE |
-- MAGIC | 1     | WRITE        |
-- MAGIC | 2     | WRITE        |
-- MAGIC | 3     | UPDATE       |
-- MAGIC | 4     | UPDATE       |
-- MAGIC | 5     | DELETE       |
-- MAGIC | 6     | MERGE        |
-- MAGIC 
-- MAGIC  **`operationsParameters`** 列で、更新、削除、マージに使用した述語を確認できます。 **`operationMetrics`** 列は、各操作で追加された行とファイルの数を示しています。
-- MAGIC 
-- MAGIC 時間をとってDelta Lakeの履歴を確認し、どのテーブルバージョンがどのトランザクションと一致しているかを理解してください。
-- MAGIC 
-- MAGIC **注**： **`version`** 列は、特定のトランザクションが完了した時点でのテーブルの状態を指定しています。  **`readVersion`** 列は、実行された操作の対象となったテーブルのバージョンを示しています。 この単純な（並列のトランザクションがない）デモでは、この関係は常に1ずつ増加するはずです。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 特定のバージョンを照会する（Query a Specific Version）
-- MAGIC 
-- MAGIC テーブル履歴を確認した後、一番最初のデータが挿入された後のテーブルの状態を見たいとします。
-- MAGIC 
-- MAGIC 以下のクエリを実行して、その状態を確認しましょう。

-- COMMAND ----------

SELECT * FROM beans VERSION AS OF 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC そして今度は、データの現在の状態を確認します。

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC レコードを削除する前に、beanの重量を確認したいとします。
-- MAGIC 
-- MAGIC 下の文を書き込んで、データが削除される直前のバージョンのテンポラリビューを登録してから、次のセルを実行して、そのビューを照会してください。

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TEMP VIEW pre_delete_vw AS
  SELECT * FROM beans VERSION AS OF 4;

-- COMMAND ----------

SELECT * FROM pre_delete_vw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 以下のセルを実行して、正しいバージョンを取り込んだことを確認してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("pre_delete_vw"), "Make sure you have registered the temporary view with the provided name `pre_delete_vw`"
-- MAGIC assert spark.table("pre_delete_vw").count() == 6, "Make sure you're querying a version of the table with 6 records"
-- MAGIC assert spark.table("pre_delete_vw").selectExpr("int(sum(grams))").first()[0] == 43220, "Make sure you query the version of the table after updates were applied"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 以前のバージョンを復元する（Restore a Previous Version）
-- MAGIC 
-- MAGIC どうやら誤解があったようです。友人がくれたと思い、コレクションにマージしたbeanは、くれるつもりだったものではありませんでした。
-- MAGIC 
-- MAGIC テーブルを、この **`MERGE`** 文が完了する前のバージョンに戻します。

-- COMMAND ----------

-- ANSWER
RESTORE TABLE beans TO VERSION AS OF 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルの履歴を確認します。 以前のバージョンに復元したことで、新たなテーブルバージョンを追加したという事実についてメモします。

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
-- MAGIC assert spark.sql(f"DESCRIBE HISTORY beans").select("operation").first()[0] == "RESTORE", "Make sure you reverted your table with the `RESTORE` keyword"
-- MAGIC assert spark.table("beans").count() == 5, "Make sure you reverted to the version after deleting records but before merging"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## ファイルの圧縮（File Compaction）
-- MAGIC 元に戻す間のトランザクションメトリクスを見て、こんなにも小さなデータコレクションにたくさんのファイルがあることに驚きます。
-- MAGIC 
-- MAGIC このサイズのテーブルにインデックスをつけてもパフォーマンスが改善する可能性は低いのですが、時間が経つにつれbeanのコレクションが飛躍的に増えることを見込んで、 **`名前`** フィールドにZ-ORDERインデックスを追加することにします。
-- MAGIC 
-- MAGIC 以下のセルを使って、ファイル圧縮とZ-ORDERインデックスを実行してください。

-- COMMAND ----------

-- ANSWER
OPTIMIZE beans
ZORDER BY name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC データは1つのファイルに圧縮されたはずです。次のセルを実行することにより、これを手動で確認してください。

-- COMMAND ----------

DESCRIBE DETAIL beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 以下のセルを実行してテーブルを正常に最適化し、インデックスを付けたことを確認してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.sql("DESCRIBE HISTORY beans").first()
-- MAGIC assert last_tx["operation"] == "OPTIMIZE", "Make sure you used the `OPTIMIZE` command to perform file compaction"
-- MAGIC assert last_tx["operationParameters"]["zOrderBy"] == '["name"]', "Use `ZORDER BY name` with your optimize command to index your table"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 古いデータファイルのクリーンアップ（Cleaning Up Stale Data Files）
-- MAGIC 
-- MAGIC おわかりのように、今は全データが1つのデータファイルに保存されていますが、以前のバージョンのテーブルのデータファイルがまだ一緒に保存されています。 テーブルに **`VACUUM`** を実行することで、これらのファイルとテーブルの以前のバージョンへのアクセスを削除したいと思います。
-- MAGIC 
-- MAGIC  **`VACUUM`** を実行することで、テーブルディレクトリのゴミ掃除を行います。 デフォルトでは、7日間の保持閾値が実行されます。
-- MAGIC 
-- MAGIC 以下のセルはSpark設定の一部を変更します。 最初のコマンドは保持閾値のチェックを無効にするので、データの永久削除が実演できます。
-- MAGIC 
-- MAGIC **注**：保持期間の短いプロダクションテーブルをバキューム処理することは、データの破損および/または実行時間の長いクエリの失敗につながるおそれがあります。 これは単なるデモ用で、この設定を無効にする際細心の注意が必要です。
-- MAGIC 
-- MAGIC 二番目のコマンドは **`spark.databricks.delta.vacuum.logging.enabled`** を **`true`** に設定し、確実に **`VACUUM`** 操作がトランザクションログに保存されるようにします。
-- MAGIC 
-- MAGIC **注**：さまざまなクラウドのストレージプロトコルのわずかな違いにより、DBR 9.1の時点では、一部のクラウドについて **`VACUUM`** コマンドのロギングはデフォルトではオンになっていません。

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC データファイルを完全に削除する前に、 **`DRY RUN`** オプションを使ってそれらを手動で確認してください。

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 現バージョンのテーブルにないデータファイルはすべて、上のプレビューに表示されます。
-- MAGIC 
-- MAGIC  **`DRY RUN`** を使わずにコマンドを再び実行し、これらのファイルを永久削除してください。
-- MAGIC 
-- MAGIC **注**：テーブルのすべての以前のバージョンにはもうアクセスできなくなります。

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC  **`VACUUM`** は重要なデータセットにとって非常に破壊的な行為となる可能性があるので、保持期間のチェックをオンに戻すのをおすすめします。 以下のセルを実行して、この設定を再び有効にしてください。

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = true

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブル履歴が、 **`VACUUM`** 操作を完了したユーザー、削除したファイルの数、この操作中に保持チェックが無効だったというログを示すことに注意してください。

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 再度テーブルを照会して、まだ現バージョンが利用可能なことを確認してください。

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" /> Deltaキャッシュは、現在のセッションでクエリされたファイルのコピーを現在アクティブなクラスタにデプロイされたストレージボリュームに保存するため、以前のテーブルバージョンに一時的にアクセスできる可能性があります（しかし、システムをこうした動作を期待するように設計**しない**ほうがいいです）。
-- MAGIC 
-- MAGIC クラスタを再起動することで、これらのキャッシュされたデータを確実に永久パージできます。
-- MAGIC 
-- MAGIC 次のセルのコメントアウトを外して実行することで、この例を確認できます。セルの実行は、（キャッシュの状態により）失敗するかもしれませんし、失敗しないかもしれません。

-- COMMAND ----------

-- SELECT * FROM beans@v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC このラボでは次のことを学びました。
-- MAGIC * 標準的なDelta Lakeテーブルの作成およびデータ操作コマンドの完了
-- MAGIC * テーブル履歴などのテーブルのメタデータの確認
-- MAGIC * Delta Lakeのバージョン管理をスナップショットクエリとロールバックに活用する
-- MAGIC * 小さいファイルを圧縮し、テーブルにインデックスを付ける
-- MAGIC *  **`VACUUM`** を使って、削除の印を付けたファイルを確認し、これらの削除をコミットする

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
