# Databricks notebook source
# MAGIC %run ../../Includes/Classroom-Setup-9.2.4L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # DLTパイプラインの結果を調べる（Exploring the Results of a DLT Pipeline）
# MAGIC 
# MAGIC 次のセルを実行してストレージ場所の出力を一覧表にします：

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/storage")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC **system**ディレクトリは、パイプラインに関連付けられたイベントをキャプチャします。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/storage/system/events")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC これらのイベントログはDeltaテーブルとして保存されます。
# MAGIC 
# MAGIC それではテーブルを照会しましょう。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${da.paths.working_dir}/storage/system/events`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC *テーブル*ディレクトリの内容を見ていきましょう。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/storage/tables")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ゴールドテーブルを照会しましょう。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.db_name}.daily_patient_avg

# COMMAND ----------

DA.cleanup()

