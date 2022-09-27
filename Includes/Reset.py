# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper(**helper_arguments)
DA.init(install_datasets=False, create_db=False)

DA.cleanup_databases()    # Remove any databases created by this course
DA.cleanup_working_dir()  # Remove any files created in the workspace
DA.cleanup_datasets()     # Remove the local datasets forcing a reinstall
