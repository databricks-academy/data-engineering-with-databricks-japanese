# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

# Continues where 8.1.1 picks up, don't remove assets
DA = DBAcademyHelper(lesson="dlt_demo_81", **helper_arguments)
# DA.reset_environment()  # We don't want to reset the environment
DA.init(install_datasets=True, create_db=True)

DA.paths.stream_path = f"{DA.paths.working_dir}/stream"
DA.paths.storage_location = f"{DA.paths.working_dir}/storage"

DA.data_factory = DltDataFactory(DA.paths.stream_path)

DA.conclude_setup()
