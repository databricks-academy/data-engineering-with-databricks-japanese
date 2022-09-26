# Databricks notebook source
# MAGIC %run ../../Includes/Classroom-Setup-09.2.4L

# COMMAND ----------

# MAGIC %md <i18n value="1372d675-796a-4bbb-9d83-356d3b1b297e"/>
# DLTパイプラインの結果を調べる（Exploring the Results of a DLT Pipeline）

次のセルを実行してストレージ場所の出力を一覧表にします：

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/storage")
display(files)

# COMMAND ----------

# MAGIC %md <i18n value="9a19b4f8-479b-4dfb-80a8-05e08cfd9eb0"/>
**system**ディレクトリは、パイプラインに関連付けられたイベントをキャプチャします。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/storage/system/events")
display(files)

# COMMAND ----------

# MAGIC %md <i18n value="da333edd-ee6d-4892-9da0-29f38d96e14f"/>
これらのイベントログはDeltaテーブルとして保存されます。

それではテーブルを照会しましょう。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${da.paths.working_dir}/storage/system/events`

# COMMAND ----------

# MAGIC %md <i18n value="303e482b-e3cd-48eb-b9be-5357e07803aa"/>
*テーブル*ディレクトリの内容を見ていきましょう。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/storage/tables")
display(files)

# COMMAND ----------

# MAGIC %md <i18n value="6c5ffd14-74db-4ec2-8764-526da08840ab"/>
ゴールドテーブルを照会しましょう。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.db_name}.daily_patient_avg

# COMMAND ----------

DA.cleanup()
