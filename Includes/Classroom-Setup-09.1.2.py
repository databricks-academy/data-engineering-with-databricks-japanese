# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper(lesson="jobs_demo_91", **helper_arguments)
DA.reset_environment() # Second in series, but requires reset
DA.init(install_datasets=True, create_db=True)

DA.paths.stream_path = f"{DA.paths.working_dir}/stream"
DA.data_factory = DltDataFactory(DA.paths.stream_path)

DA.conclude_setup()
