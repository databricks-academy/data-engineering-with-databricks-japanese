# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC %md --i18n-1fb32f72-2ccc-4206-98d9-907287fc3262
# MAGIC 
# MAGIC # Workspace Setup
# MAGIC This notebook should be ran by instructors to prepare the workspace for a class.
# MAGIC 
# MAGIC The key changes this notebook makes includes:
# MAGIC * Updating user-specific grants such that they can create databases/schemas against the current catalog when they are not workspace-admins.
# MAGIC * Configures three cluster policies:
# MAGIC     * **Student's All-Purpose Policy** - which should be used on clusters running standard notebooks.
# MAGIC     * **Jobs-Only Policy** - which should be used on workflows/jobs
# MAGIC     * **DLT-Only Policy** - which should be used on DLT piplines (automatically applied)
# MAGIC * Create or update the shared **Starter Warehouse** for use in Databricks SQL exercises
# MAGIC * Create the Instance Pool **Student Pool** for use by students and the "student" and "jobs" policies.

# COMMAND ----------

# MAGIC %run ./_utility-methods $lesson="Workspace Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC %md --i18n-86c0a995-1251-473e-976c-ba8288c0b2d3
# MAGIC # Get Class Config
# MAGIC The two variables defined by these widgets are used to configure our environment as a means of controlling class cost.

# COMMAND ----------

# Setup the widgets to collect required parameters.

# students_count is the reasonable estiamte to the maximum number of students
dbutils.widgets.text("students_count", "", "Number of Students")

# class_number is the number assigned to this class or alternatively its name
dbutils.widgets.text("class_number", "", "Class Number/Name")

# COMMAND ----------

# MAGIC %md
# MAGIC %md --i18n-b1d39e1d-aa44-4c05-b378-837a1b432128
# MAGIC 
# MAGIC # Init Script & Install Datasets
# MAGIC The main affect of this call is to pre-install the datasets.
# MAGIC 
# MAGIC It has the side effect of create our DA object which includes our REST client.

# COMMAND ----------

#DA = DBAcademyHelper()    # Create the DA object with the specified lesson
DA.cleanup()               # Remove the existing database and files
DA.init(create_db=False)   # True is the default
# DA.install_datasets()      # Install (if necissary) and validate the datasets
DA.conclude_setup()        # Conclude the setup by printing the DA object's final state

# COMMAND ----------

# Initilize the remaining parameters for this script
from dbacademy import dbgems

# Special logic for when we are running under test.
is_smoke_test = spark.conf.get("dbacademy.smoke-test", "false") == "true"

students_count = dbutils.widgets.get("students_count").strip()
user_count = len(DA.client.scim.users.list())

if students_count.isnumeric():
    students_count = int(students_count)
    students_count = max(students_count, user_count)
else:
    students_count = user_count

assert students_count != 1, "You cannot possibly be the only student."    

workspace = dbgems.get_browser_host_name()
if not workspace: workspace = dbgems.get_notebooks_api_endpoint()
org_id = dbgems.get_tag("orgId", "unknown")

import math
autoscale_min = 1 if is_smoke_test else math.ceil(students_count/20)
autoscale_max = 1 if is_smoke_test else math.ceil(students_count/5)

class_number = "Smoke Test" if is_smoke_test else dbutils.widgets.get("class_number")
assert class_number is not None and len(class_number) >= 3, f"The class_number must be specified with min-length of 3"

# COMMAND ----------

# MAGIC %md
# MAGIC %md --i18n-485ff12c-7286-4d14-a90e-3c29d87f8920
# MAGIC 
# MAGIC ## Create Class Instance Pools
# MAGIC The following cell configures the instance pool used for this class

# COMMAND ----------

# Do not change these values.
tags = [("dbacademy.class_number", class_number), 
        ("dbacademy.students_count", students_count),
        ("dbacademy.workspace", workspace),
        ("dbacademy.org_id", org_id)]

pool = DA.client.instance_pools.create_or_update(instance_pool_name = "Student Pool",
                                                 idle_instance_autotermination_minutes = 15, 
                                                 min_idle_instances = 0,
                                                 tags = tags)
instance_pool_id = pool.get("instance_pool_id")
print(instance_pool_id)

# COMMAND ----------

# MAGIC %md
# MAGIC %md --i18n-04ae9a73-8b48-4823-8738-31e337864cf4
# MAGIC 
# MAGIC ## Create The Three Class-Specific Cluster Policies
# MAGIC The following cells create the various cluster policies used by the class

# COMMAND ----------

policy = DA.client.cluster_policies.create_or_update("Student's All-Purpose Policy", {
    "cluster_type": {
        "type": "fixed",
        "value": "all-purpose"
    },
    "autotermination_minutes": {
        "type": "fixed",
        "value": 120,
        "hidden": False
    },
    "spark_conf.spark.databricks.cluster.profile": {
        "type": "fixed",
        "value": "singleNode",
        "hidden": False
    },
    "instance_pool_id": {
        "type": "fixed",
        "value": instance_pool_id,
        "hidden": False
    }
})
policy.get("policy_id")

# COMMAND ----------

policy = DA.client.cluster_policies.create_or_update("Student's Jobs-Only Policy", {
    "cluster_type": {
        "type": "fixed",
        "value": "job"
    },
    "spark_conf.spark.databricks.cluster.profile": {
        "type": "fixed",
        "value": "singleNode",
        "hidden": False
    },
    "instance_pool_id": {
        "type": "fixed",
        "value": instance_pool_id,
        "hidden": False
    },
})
policy.get("policy_id")

# COMMAND ----------

policy = DA.client.cluster_policies.create_or_update("Student's DLT-Only Policy", {
    "cluster_type": {
        "type": "fixed",
        "value": "dlt"
    },
    "spark_conf.spark.databricks.cluster.profile": {
        "type": "fixed",
        "value": "singleNode",
        "hidden": False
    },
    "custom_tags.dbacademy.class_number": {
        "type": "fixed",
        "value": str(class_number),
        "hidden": False
    },
    "custom_tags.dbacademy.students_count": {
        "type": "fixed",
        "value": str(students_count),
        "hidden": False
    },
    "custom_tags.dbacademy.workspace": {
        "type": "fixed",
        "value": workspace,
        "hidden": False
    },
    "custom_tags.dbacademy.org_id": {
        "type": "fixed",
        "value": org_id,
        "hidden": False
    }
})

policy.get("policy_id")

# COMMAND ----------

# MAGIC %md
# MAGIC %md --i18n-2f010a4b-af3c-4b3f-96a0-d8b3556ec728
# MAGIC 
# MAGIC ## Create Class-Shared Databricks SQL Warehouse/Endpoint
# MAGIC Creates a single wharehouse to be used by all students.
# MAGIC 
# MAGIC The configuration is derived from the number of students specified above.

# COMMAND ----------

from dbacademy.dbrest.sql.endpoints import RELIABILITY_OPTIMIZED, CHANNEL_NAME_CURRENT, CLUSTER_SIZE_2X_SMALL

DA.client.sql.endpoints.create_or_update(
    name = "Starter Warehouse",
    cluster_size = CLUSTER_SIZE_2X_SMALL,
    enable_serverless_compute = False, # Due to a bug with Free-Trial workspaces
    min_num_clusters = autoscale_min,
    max_num_clusters = autoscale_max,
    auto_stop_mins = 120,
    enable_photon = True,
    spot_instance_policy = RELIABILITY_OPTIMIZED,
    channel = CHANNEL_NAME_CURRENT,
    tags = {
        "dbacademy.class_number": str(class_number),
        "dbacademy.students_count": str(students_count),
        "dbacademy.workspace": workspace,
        "dbacademy.org_id": org_id
    })

# COMMAND ----------

# MAGIC %md
# MAGIC %md --i18n-74b76ae9-3bbb-4bd4-b1c6-6d76d85a5baa
# MAGIC 
# MAGIC ## Update Grants
# MAGIC This operation executes **`GRANT CREATE ON CATALOG TO users`** to ensure that students can create databases as required by this course when they are not admins.
# MAGIC 
# MAGIC Note: The implementation requires this to execute in another job and as such can take about three minutes to complete.

# COMMAND ----------

# Ensures that all users can create databases on the current catalog 
# for cases wherein the user/student is not an admin.
update_user_specific_grants()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
