# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper(**helper_arguments) # Create the DA object
DA.reset_environment()                   # Reset by removing databases and files from other lessons
DA.init(install_datasets=True,           # Initialize, install and validate the datasets
        create_db=True)                  # Continue initialization, create the user-db

# Clean out the global_temp database.
for row in spark.sql("SHOW TABLES IN global_temp").select("tableName").collect():
    table_name = row[0]
    spark.sql(f"DROP TABLE global_temp.{table_name}")

DA.conclude_setup()                      # Conclude setup by advertising environmental changes
