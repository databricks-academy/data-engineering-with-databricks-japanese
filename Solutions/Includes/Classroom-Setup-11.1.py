# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def print_sql(self, rows, sql):
    html = f"""<textarea style="width:100%" rows="{rows}"> \n{sql.strip()}</textarea>"""
    displayHTML(html)

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_users_table(self):
    self.print_sql(20, f"""
CREATE DATABASE IF NOT EXISTS {DA.db_name}
LOCATION '{DA.paths.user_db}';

USE {DA.db_name};

CREATE TABLE users (id INT, name STRING, value DOUBLE, state STRING);

INSERT INTO users
VALUES (1, "Yve", 1.0, "CA"),
       (2, "Omar", 2.5, "NY"),
       (3, "Elia", 3.3, "OH"),
       (4, "Rebecca", 4.7, "TX"),
       (5, "Ameena", 5.3, "CA"),
       (6, "Ling", 6.6, "NY"),
       (7, "Pedro", 7.1, "KY");

CREATE VIEW ny_users_vw
AS SELECT * FROM users WHERE state = 'NY';
""")

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_create_database_with_grants(self):
    self.print_sql(7, f"""
CREATE DATABASE {DA.db_name}_derivative;

GRANT USAGE, READ_METADATA, CREATE, MODIFY, SELECT ON DATABASE `{DA.db_name}_derivative` TO `users`;

SHOW GRANT ON DATABASE `{DA.db_name}_derivative`;""")

# COMMAND ----------

DA = DBAcademyHelper(**helper_arguments)
DA.reset_environment()
DA.init(install_datasets=True, create_db=False)
DA.conclude_setup()
