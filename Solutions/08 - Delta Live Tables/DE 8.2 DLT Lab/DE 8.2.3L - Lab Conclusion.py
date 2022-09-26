# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # ラボ：結論
# MAGIC 次のセルを実行して、ラボ環境を構成します。

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.2.3L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 結果を表示する（Display Results）
# MAGIC 
# MAGIC パイプラインが正常に実行したと仮定して、ゴールドテーブルの内容を表示します。
# MAGIC 
# MAGIC **注**：**ターゲット**に値を指定したため、テーブルは指定されたデータベースに公開されます。 **ターゲット**を指定していない場合、DBFSにある（**保存場所**に対して）基盤となる場所を基にテーブルを照会する必要があります。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.db_name}.daily_patient_avg

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 次のセルを使って、他のファイルの到着をトリガーします。
# MAGIC 
# MAGIC この作業は必要に応じて、さらに2，3回行ってください。
# MAGIC 
# MAGIC 続いてパイプラインを再度実行し、結果を表示します。
# MAGIC 
# MAGIC 必要なだけ上記のセルを再実行し、 **`daily_patient_avg`** テーブルの更新ビューを取得しましょう。

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## まとめ（Wrapping Up）
# MAGIC 
# MAGIC DLT UIからパイプラインを削除したことを確認し、次のセルを実行してラボのセットアップと実行の一部として生成されたファイルとテーブルをクリーンアップします。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 概要（Summary）
# MAGIC 
# MAGIC このラボでは、既存のデータパイプラインをDelta Live TablesのSQLパイプラインに転換し、DLT UIを使ってそのパイプラインをデプロイする方法を学びました。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 追加のトピックとリソース（Additional Topics & Resources）
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/data-engineering/delta-live-tables/index.html" target="_blank">Delta Live Tablesのドキュメント</a>
# MAGIC * <a href="https://youtu.be/6Q8qPZ7c1O0" target="_blank">Delta Live Tablesのデモ</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
