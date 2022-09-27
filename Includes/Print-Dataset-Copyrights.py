# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

# DA = DBAcademyHelper(**helper_arguments)
DA = DBAcademyHelper(**helper_arguments)
DA.init(install_datasets=True, create_db=False)

# COMMAND ----------

DA.print_copyrights()
