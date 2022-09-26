# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper(lesson="jobs_demo_91", **helper_arguments)
# DA.reset_environment() # We don't want to reset the environment
DA.init(install_datasets=True, create_db=True)
DA.conclude_setup()
