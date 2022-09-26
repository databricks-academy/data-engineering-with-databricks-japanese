# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems@63bb19389fd2296f6cc5a74e463ab52ab1548767 \
# MAGIC git+https://github.com/databricks-academy/dbacademy-rest@96351a554ed00b664e7c2d97249805df5611be06 \
# MAGIC git+https://github.com/databricks-academy/dbacademy-helper@ac98761e19f74981b46e577cb39835df267f5da6 \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

# MAGIC %run ./_remote_files

# COMMAND ----------

import time
from dbacademy_gems import dbgems
from dbacademy_helper import DBAcademyHelper, Paths

helper_arguments = {
    "course_code" : "dewd",            # The abreviated version of the course
    "course_name" : "data-engineering-with-databricks",      # The full name of the course, hyphenated
    "data_source_name" : "data-engineering-with-databricks", # Should be the same as the course
    "data_source_version" : "v02",     # New courses would start with 01
    "enable_streaming_support": True,  # This couse uses stream and thus needs checkpoint directories
    "install_min_time" : "5 min",      # The minimum amount of time to install the datasets (e.g. from Oregon)
    "install_max_time" : "15 min",     # The maximum amount of time to install the datasets (e.g. from India)
    "remote_files": remote_files,      # The enumerated list of files in the datasets
}
# Start a timer so we can 
# benchmark execution duration.
setup_start = int(time.time())

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def update_cluster_params(self, params: dict, task_indexes: list):

    if not self.is_smoke_test():
        return params
    
    for task_index in task_indexes:
        # Need to modify the parameters to run run as a smoke-test.
        task = params.get("tasks")[task_index]
        del task["existing_cluster_id"]

        cluster_params =         {
            "num_workers": "0",
            "spark_version": self.client.clusters().get_current_spark_version(),
            "spark_conf": {
              "spark.master": "local[*]"
            },
        }

        instance_pool_id = self.client.clusters().get_current_instance_pool_id()
        if instance_pool_id is not None: cluster_params["instance_pool_id"] = self.client.clusters().get_current_instance_pool_id()
        else:                            cluster_params["node_type_id"] = self.client.clusters().get_current_node_type_id()

        task["new_cluster"] = cluster_params
        
    return params

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def clone_source_table(self, table_name, source_path, source_name=None):
    import time
    start = int(time.time())

    source_name = table_name if source_name is None else source_name
    print(f"Cloning the {table_name} table from {source_path}/{source_name}", end="...")
    
    spark.sql(f"""
        CREATE OR REPLACE TABLE {table_name}
        SHALLOW CLONE delta.`{source_path}/{source_name}`
        """)

    total = spark.read.table(table_name).count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")

# COMMAND ----------

class DltDataFactory:
    def __init__(self, stream_path):
        self.stream_path = stream_path
        self.source = f"{DA.paths.datasets}/healthcare/tracker/streaming"
        try:
            self.curr_mo = 1 + int(max([x[1].split(".")[0] for x in dbutils.fs.ls(self.stream_path)]))
        except:
            self.curr_mo = 1
    
    def load(self, continuous=False):
        if self.curr_mo > 12:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.curr_mo <= 12:
                curr_file = f"{self.curr_mo:02}.json"
                target_dir = f"{self.stream_path}/{curr_file}"
                print(f"Loading the file {curr_file} to the {target_dir}")
                dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
                self.curr_mo += 1
        else:
            curr_file = f"{str(self.curr_mo).zfill(2)}.json"
            target_dir = f"{self.stream_path}/{curr_file}"
            print(f"Loading the file {curr_file} to the {target_dir}")

            dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
            self.curr_mo += 1

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def setup_completed(self):
    print(f"\nSetup completed in {int(time.time())-setup_start} seconds")
