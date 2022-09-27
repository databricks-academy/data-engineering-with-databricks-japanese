# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="405115f0-8ffe-4a64-861b-b4a6772972be"/>
# MAGIC 
# MAGIC # レイクハウスでのエンドツーエンドETL（End-to-End ETL in the Lakehouse）
# MAGIC ## 最終ステップ（Final Steps）
# MAGIC 
# MAGIC このラボの最初のノートブック [DE 12.2.1L - Instructions and Configuration]（$./DE 12.2.1L - 解説と設定）の続きです。
# MAGIC 
# MAGIC すべてが正しく設定されている場合は、次の環境が整っているはずです。
# MAGIC * **Continuous**モードで実行されているDLTパイプライン
# MAGIC * そのパイプラインに2分ごとに新しいデータを供給しているジョブ
# MAGIC * そのパイプラインの出力を分析する一連のDatabricks SQLクエリ

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-12.2.4L

# COMMAND ----------

# MAGIC %md <i18n value="ce3151f0-a3dc-4cd5-9f92-a075c851101a"/>
# MAGIC 
# MAGIC ## クエリを実行して破損しているデータを修復する（Execute a Query to Repair Broken Data）
# MAGIC 
# MAGIC  **`recordings_enriched`** テーブルを定義したコードを確認して、品質チェックに適用されたフィルタを特定します。
# MAGIC 
# MAGIC 次のセルには、この品質チェックで拒否された **`recordings_bronze`** テーブルのすべてのレコードを返すクエリを書きます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC <FILL-IN>

# COMMAND ----------

# MAGIC %md <i18n value="25e86690-7fc4-45ea-b567-c6a229cef220"/>
# MAGIC 
# MAGIC デモンストレーションの目的上、データとシステムを手動で徹底的に確認した結果、通常は正しい心拍数の記録が場合によっては負の値として返されることに気づいたと仮定します。
# MAGIC 
# MAGIC 次のクエリを実行して、負号を削除した同じ行を調べます。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT abs(heartrate), * FROM ${da.db_name}.recordings_bronze WHERE heartrate <= 0

# COMMAND ----------

# MAGIC %md <i18n value="6f853894-27ff-4edb-8e64-ff2fa3c0be39"/>
# MAGIC 
# MAGIC データセットを完成させるために、これらの固定レコードをシルバーの **`recordings_enriched`** テーブルに挿入します。
# MAGIC 
# MAGIC 以下のセルを使用して、この修復を実行するためにDLTパイプラインで使用されるクエリを更新します。
# MAGIC 
# MAGIC **注**：品質チェックによって以前に拒否されたレコードのみを処理するようにコードを更新してください。

# COMMAND ----------

# TODO
# CREATE OR REFRESH STREAMING LIVE TABLE recordings_enriched
#   (CONSTRAINT positive_heartrate EXPECT (heartrate > 0) ON VIOLATION DROP ROW)
# AS SELECT 
#   CAST(a.device_id AS INTEGER) device_id, 
#   CAST(a.mrn AS LONG) mrn, 
#   CAST(a.heartrate AS DOUBLE) heartrate, 
#   CAST(from_unixtime(a.time, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) time,
#   b.name
#   FROM STREAM(live.recordings_bronze) a
#   INNER JOIN STREAM(live.pii) b
#   ON a.mrn = b.mrn

# COMMAND ----------

# MAGIC %md <i18n value="03ebb8bd-781d-4d5d-87ff-f4a842e2419a"/>
# MAGIC 
# MAGIC 以下のセルを使用して、この更新が成功したことを手動またはプログラムで確認します。
# MAGIC 
# MAGIC （ **`recordings_bronze`** のレコードの総数は、 **`recordings_enriched`** のレコードの総数と同じになります）。

# COMMAND ----------

# TODO
<FILL-IN>

# COMMAND ----------

# MAGIC %md <i18n value="9b9685f0-dc1a-490c-92e9-ec068c7064d5"/>
# MAGIC 
# MAGIC ## 本番データの権限を検討する（Consider Production Data Permissions）
# MAGIC 
# MAGIC データの手動修復は成功しましたが、これらのデータセットの所有者として、デフォルトでは、コードを実行している任意の場所からこのデータを変更または削除する権限がある点に注意してください。
# MAGIC 
# MAGIC 別の言い方をすれば、現在の権限だと、誤ったSQLクエリが現在のユーザーの権限で誤って実行された場合（または他のユーザーに同様の権限が付与された場合）、本番テーブルを変更または完全に削除できます。
# MAGIC 
# MAGIC このラボでは、データに対する完全な権限が必要でしたが、コードを開発段階から本番環境に移行する際は、偶発的なデータの変更を回避するため、 <a href="https://docs.databricks.com/administration-guide/users-groups/service-principals.html" target="_blank">サービスプリンシパル</a>を活用してジョブとDLTパイプラインをスケジュールしたほうが安全です。

# COMMAND ----------

# MAGIC %md <i18n value="15d7d859-5788-4c66-88f8-f276dba39347"/>
# MAGIC 
# MAGIC ## 本番インフラストラクチャをシャットダウンする（Shut Down Production Infrastructure）
# MAGIC 
# MAGIC Databricksジョブ、DLTパイプライン、およびスケジュールされたDBSQLクエリとダッシュボードはすべて、本番コードを継続的に実行できるように設計されていることに注意してください。 このエンドツーエンドのデモでは、継続的なデータ処理のためにジョブとパイプラインを設定しました。 これらのワークロードが引き続き実行されないように、Databricksジョブを**Pause**してDLTパイプラインを**Stop**しましょう。 これらのアセットを削除すると、本番インフラストラクチャも確実に終了します。
# MAGIC 
# MAGIC **注**：前のレッスンのDBSQLアセットスケジューリングのすべての手順には、更新スケジュールを明日終了するように設定しましょうと書かれていました。 SQLウェアハウスがそれまでオンのままにならないように、戻ってこれらの更新をキャンセルすることもできます。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
