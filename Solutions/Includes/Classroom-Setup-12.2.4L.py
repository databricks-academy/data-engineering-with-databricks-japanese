# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper(lesson="cap_12", **helper_arguments)
# DA.reset_environment() # We don't want to reset the environment
DA.init(install_datasets=True, create_db=False)
DA.conclude_setup()
