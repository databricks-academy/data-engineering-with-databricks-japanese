# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def create_demo_tmp_vw(self):
    print("\nCreating the temp view \"demo_tmp_vw\"")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW demo_tmp_vw(name, value) AS VALUES
        ("Yi", 1),
        ("Ali", 2),
        ("Selina", 3)
        """)

# COMMAND ----------

DA = DBAcademyHelper(**helper_arguments) # Create the DA object
DA.reset_environment()                   # Reset by removing databases and files from other lessons
DA.init(install_datasets=True,           # Initialize, install and validate the datasets
        create_db=False)                 # Continue initialization, create the user-db

DA.create_demo_tmp_vw()                  # Create demo table

DA.conclude_setup()                      # Conclude setup by advertising environmental changes
