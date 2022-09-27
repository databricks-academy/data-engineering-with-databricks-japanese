# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="1fb32f72-2ccc-4206-98d9-907287fc3262"/>
# MAGIC 
# MAGIC # Workspace Setup
# MAGIC This notebook should be run by instructors to prepare the workspace for a class.
# MAGIC 
# MAGIC The key changes this notebook makes includes:
# MAGIC * Updating user-specific grants such that they can create databases/schemas against the current catalog when they are not workspace-admins.
# MAGIC * Configures three cluster policies:
# MAGIC     * **Student's All-Purpose Policy** - which should be used on clusters running standard notebooks.
# MAGIC     * **Student's Jobs-Only Policy** - which should be used on workflows/jobs
# MAGIC     * **Student's DLT-Only Policy** - which should be used on DLT piplines (automatically applied)
# MAGIC * Create or update the shared **Starter Warehouse** for use in Databricks SQL exercises
# MAGIC * Create the Instance Pool **Student's Pool** for use by students and the "student" and "jobs" policies.

# COMMAND ----------

# MAGIC %run ./_utility-methods

# COMMAND ----------

# MAGIC %md <i18n value="86c0a995-1251-473e-976c-ba8288c0b2d3"/>
# MAGIC # Get Class Config
# MAGIC The three variables defined by these widgets are used to configure our environment as a means of controlling class cost.

# COMMAND ----------

# Setup the widgets to collect required parameters.
from dbacademy_helper.workspace_helper import ALL_USERS # no other option for this course
dbutils.widgets.dropdown("configure_for", ALL_USERS, [ALL_USERS], "Configure Workspace For")

# students_count is the reasonable estiamte to the maximum number of students
dbutils.widgets.text("students_count", "", "Number of Students")

# event_name is the name assigned to this event/class or alternatively its class number
dbutils.widgets.text("event_name", "", "Event Name/Class Number")

# COMMAND ----------

# MAGIC %md <i18n value="b1d39e1d-aa44-4c05-b378-837a1b432128"/>
# MAGIC 
# MAGIC # Init Script & Install Datasets
# MAGIC The main affect of this call is to pre-install the datasets.
# MAGIC 
# MAGIC It has the side effect of create our DA object which includes our REST client.

# COMMAND ----------

DA = DBAcademyHelper(**helper_arguments) # Create the DA object
DA.reset_environment()                   # Reset by removing databases and files from other lessons
DA.init(install_datasets=True,           # Initialize, install and validate the datasets
        create_db=False)                 # Continue initialization, create the user-db
DA.conclude_setup()                      # Conclude setup by advertising environmental changes

# COMMAND ----------

# MAGIC %md <i18n value="485ff12c-7286-4d14-a90e-3c29d87f8920"/>
# MAGIC 
# MAGIC ## Create Class Instance Pools
# MAGIC The following cell configures the instance pool used for this class

# COMMAND ----------

instance_pool_id = DA.workspace.clusters.create_instance_pools()

# COMMAND ----------

# MAGIC %md <i18n value="04ae9a73-8b48-4823-8738-31e337864cf4"/>
# MAGIC 
# MAGIC ## Create The Three Class-Specific Cluster Policies
# MAGIC The following cells create the various cluster policies used by the class

# COMMAND ----------

DA.workspace.clusters.create_all_purpose_policy(instance_pool_id)
DA.workspace.clusters.create_jobs_policy(instance_pool_id)
DA.workspace.clusters.create_dlt_policy(instance_pool_id)
None

# COMMAND ----------

# MAGIC %md <i18n value="2f010a4b-af3c-4b3f-96a0-d8b3556ec728"/>
# MAGIC 
# MAGIC ## Create Class-Shared Databricks SQL Warehouse/Endpoint
# MAGIC Creates a single wharehouse to be used by all students.
# MAGIC 
# MAGIC The configuration is derived from the number of students specified above.

# COMMAND ----------

DA.workspace.warehouses.create_shared_sql_warehouse()

# COMMAND ----------

# MAGIC %md <i18n value="a382c82f-6e5a-453c-b612-946e184d576c"/>
# MAGIC 
# MAGIC ## Configure User Entitlements
# MAGIC 
# MAGIC This task simply adds the "**databricks-sql-access**" entitlement to the "**users**" group ensuring that they can access the Databricks SQL view.

# COMMAND ----------

DA.workspace.add_entitlement_workspace_access()
DA.workspace.add_entitlement_databricks_sql_access()

# COMMAND ----------

# MAGIC %md <i18n value="74b76ae9-3bbb-4bd4-b1c6-6d76d85a5baa"/>
# MAGIC 
# MAGIC ## Update Grants
# MAGIC This operation executes **`GRANT CREATE ON CATALOG TO users`** to ensure that students can create databases as required by this course when they are not admins.
# MAGIC 
# MAGIC Note: The implementation requires this to execute in another job and as such can take about three minutes to complete.

# COMMAND ----------

# Ensures that all users can create databases on the current catalog 
# for cases wherein the user/student is not an admin.
job_id = DA.workspace.databases.configure_permissions("Configure-Permissions")

# COMMAND ----------

DA.client.jobs().delete_by_id(job_id)

# COMMAND ----------

DA.setup_completed()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
