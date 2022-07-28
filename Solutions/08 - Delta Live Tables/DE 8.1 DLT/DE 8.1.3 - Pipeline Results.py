# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # DLTパイプラインの結果を調べる（Exploring the Results of a DLT Pipeline）
# MAGIC 
# MAGIC このノートブックでは、DLTパイプラインの実行結果について学びます。

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.1.3

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC **`system`** ディレクトリは、パイプラインに関連付けられたイベントをキャプチャします。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC これらのイベントログはDeltaテーブルとして保存されます。 それではテーブルを照会しましょう。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${da.paths.storage_location}/system/events`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC *テーブル*ディレクトリの内容を見ていきましょう。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ゴールドテーブルを照会しましょう。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.db_name}.sales_order_in_la

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 次のセルを実行して、このレッスンに関連するテーブルとファイルを削除してください。

# COMMAND ----------

DA.cleanup()

