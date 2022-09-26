# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

# The lesson name is specifically named "acls_lab" as it is a significantly user-facing - JDP
DA = DBAcademyHelper(lesson="acls_lab", **helper_arguments)
DA.reset_environment() # Not sequenced, but "acls_lab" is directly referenced in the prose
DA.init(install_datasets=True, create_db=False)
DA.conclude_setup()

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def print_sql(self, rows, sql):
    displayHTML(f"""<body><textarea style="width:100%" rows={rows}> \n{sql.strip()}</textarea></body>""")

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_query(self):
    import re
    import random

    self.print_sql(23, f"""
CREATE DATABASE IF NOT EXISTS {DA.db_name}
LOCATION '{DA.paths.user_db}';

USE {DA.db_name};
    
CREATE TABLE beans 
(name STRING, color STRING, grams FLOAT, delicious BOOLEAN); 

INSERT INTO beans
VALUES ('black', 'black', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
       ('lentils', 'brown', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
       ('jelly', 'rainbow', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
       ('pinto', 'brown', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
       ('green', 'green', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
       ('beanbag chair', 'white', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
       ('lentils', 'green', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
       ('kidney', 'red', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
       ('castor', 'brown', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])});

CREATE VIEW tasty_beans
AS SELECT * FROM beans WHERE delicious = true;
    """)

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_confirmation_query(self, username):
    import re
    # clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
    database = DA.db_name #.replace(DA.clean_username, clean_username)
    
    self.print_sql(11, f"""
USE {database};

SELECT * FROM beans;
SELECT * FROM tasty_beans;
SELECT * FROM beans MINUS SELECT * FROM tasty_beans;

UPDATE beans
SET color = 'pink'
WHERE name = 'black'
""")

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_union_query(self):
    self.print_sql(6, f"""
USE {DA.db_name};

SELECT * FROM beans
UNION ALL TABLE beans;""")

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_derivative_view(self):
    self.print_sql(7, f"""
USE {DA.db_name};

CREATE VIEW our_beans 
AS SELECT * FROM beans
UNION ALL TABLE beans;
""")

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def get_their_db(self, their_username):
    import re
    db_name_prefix = self.to_database_name(username=their_username, course_code=self.course_code)
    
#     da_name, da_hash = self.get_username_hash(their_username)
#     db_name_prefix = f"da-{da_name}@{da_hash}-{self.course_code}"         # Composite all the values to create the "dirty" database name
#     while "__" in db_name_prefix: 
#         db_name_prefix = self.db_name_prefix.replace("__", "_")           # Replace all double underscores with single underscores

    if DA.lesson is None: 
      # No lesson, database name is the same as prefix
      return db_name_prefix                        
    else:
      # Database name includes the lesson name
      return f"{db_name_prefix}_{DA.clean_lesson}"

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_partner_view(self, their_username):
    self.print_sql(7, f"""
USE {self.get_their_db(their_username)};

SELECT name, color, delicious, sum(grams)
FROM our_beans
GROUP BY name, color, delicious;""")

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_delete_query(self, their_username):
    
    self.print_sql(5, f"""
USE {self.get_their_db(their_username)};

DELETE FROM beans
    """)
