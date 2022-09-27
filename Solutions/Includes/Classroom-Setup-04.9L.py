# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper(**helper_arguments)
DA.reset_environment()
DA.init(install_datasets=True, create_db=True)

print()
DA.clone_source_table("events", f"{DA.paths.datasets}/ecommerce/delta")
DA.clone_source_table("sales", f"{DA.paths.datasets}/ecommerce/delta")
DA.clone_source_table("users", f"{DA.paths.datasets}/ecommerce/delta")
DA.clone_source_table("transactions", f"{DA.paths.datasets}/ecommerce/delta")

DA.conclude_setup()
