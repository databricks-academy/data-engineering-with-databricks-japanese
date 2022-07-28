# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="dlt_lab_82"

# COMMAND ----------

# MAGIC %run ./mount-datasets

# COMMAND ----------

def get_pipeline_config():
    path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    path = "/".join(path.split("/")[:-1]) + "/DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab"
    
    pipeline_name = f"DLT-Lab-82L-{DA.username}"
    source = f"{DA.paths.working_dir}/source/tracker"
    return pipeline_name, path, source
    
def _print_pipeline_config():
    "Provided by DBAcademy, this function renders the configuration of the pipeline as HTML"
    pipeline_name, path, source = get_pipeline_config()
    
    displayHTML(f"""<table style="width:100%">
    <tr>
        <td style="white-space:nowrap; width:1em">Pipeline Name:</td>
        <td><input type="text" value="{pipeline_name}" style="width:100%"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Target:</td>
        <td><input type="text" value="{DA.db_name}" style="width:100%"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Storage Location:</td>
        <td><input type="text" value="{DA.paths.working_dir}/storage" style="width:100%"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Notebook Path:</td>
        <td><input type="text" value="{path}" style="width:100%"></td>
    </tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Source:</td>
        <td><input type="text" value="{source}" style="width:100%"></td>
    </tr>
    </table>""")
    
DA.print_pipeline_config = _print_pipeline_config    

# COMMAND ----------

def _create_pipeline():
    "Provided by DBAcademy, this function creates the prescribed pipline"
    
    from dbacademy.dbrest import DBAcademyRestClient
    client = DBAcademyRestClient()

    pipeline_name, path, source = get_pipeline_config()

    # Delete the existing pipeline if it exists
    client.pipelines().delete_by_name(pipeline_name)

    # Create the new pipeline
    pipeline = client.pipelines().create(
        name = pipeline_name, 
        storage = f"{DA.paths.working_dir}/storage", 
        target = DA.db_name, 
        notebooks = [path],
        configuration = {"source": source})

    DA.pipeline_id = pipeline.get("pipeline_id")
       
DA.create_pipeline = _create_pipeline

# COMMAND ----------

def _validate_pipeline_config():
    "Provided by DBAcademy, this function validates the configuration of the pipeline"
    import json
    
    pipeline_name, path, source = get_pipeline_config()

    pipeline = DA.client.pipelines().get_by_name(pipeline_name)
    assert pipeline is not None, f"The pipline named \"{pipeline_name}\" doesn't exist. Double check the spelling."

    spec = pipeline.get("spec")
    
    storage = spec.get("storage")
    assert storage == f"{DA.paths.working_dir}/storage", f"Invalid storage location. Found \"{storage}\", expected \"{DA.paths.working_dir}/storage\" "
    
    target = spec.get("target")
    assert target == DA.db_name, f"Invalid target. Found \"{target}\", expected \"{DA.db_name}\" "
    
    libraries = spec.get("libraries")
    assert libraries is None or len(libraries) > 0, f"The notebook path must be specified."
    assert len(libraries) == 1, f"More than one library (e.g. notebook) was specified."
    first_library = libraries[0]
    assert first_library.get("notebook") is not None, f"Incorrect library configuration - expected a notebook."
    first_library_path = first_library.get("notebook").get("path")
    assert first_library_path == path, f"Invalid notebook path. Found \"{first_library_path}\", expected \"{path}\" "

    configuration = spec.get("configuration")
#     assert configuration is not None, f"The two configuration parameters were not specified."
#     datasets_path = configuration.get("datasets_path")
#     assert datasets_path == DA.paths.datasets, f"Invalid datasets_path value. Expected \"{DA.paths.datasets}\", found \"{datasets_path}\"."
#     spark_master = configuration.get("spark.master")
#     assert spark_master == f"local[*]", f"Invalid spark.master value. Expected \"local[*]\", found \"{spark_master}\"."
    config_source = None if configuration is None else configuration.get("source")
    assert config_source == source, f"Invalid source value. Expected \"{source}\", found \"{config_source}\"."
    
    cluster = spec.get("clusters")[0]
    autoscale = cluster.get("autoscale")
    assert autoscale is None, f"Autoscaling should be disabled."
    
    # num_workers = cluster.get("num_workers")
    # assert num_workers == 0, f"Expected the number of workers to be 0, found {num_workers}."

    development = spec.get("development")
    assert development == True, f"The pipline mode should be set to \"Development\"."
    
    channel = spec.get("channel")
    assert channel is None or channel == "CURRENT", f"Expected the channel to be Current but found {channel}."
    
    photon = spec.get("photon")
    assert photon == True, f"Expected Photon to be enabled."
    
    continuous = spec.get("continuous")
    assert continuous == False, f"Expected the Pipeline mode to be \"Triggered\", found \"Continuous\"."

    policy = DA.client.cluster_policies.get_by_name("Student's DLT-Only Policy")
    if policy is not None:
        cluster = { 
            "num_workers": 0,
            "label": "default", 
            "policy_id": policy.get("policy_id")
        }
        DA.client.pipelines.create_or_update(pipeline_id=DA.pipeline_id,
                                               name=pipeline_name,
                                               storage = f"{DA.paths.working_dir}/storage", 
                                               target=DA.db_name,
                                               notebooks = [path],
                                               clusters=[cluster],
                                               configuration = {"source": source})
    print("All tests passed!")
    
DA.validate_pipeline_config = _validate_pipeline_config

# COMMAND ----------

def _start_pipeline():
    "Provided by DBAcademy, this function starts the pipline and then blocks until it has completed, failed or was canceled"

    import time
    from dbacademy.dbrest import DBAcademyRestClient
    client = DBAcademyRestClient()

    # Start the pipeline
    start = client.pipelines().start_by_id(DA.pipeline_id)
    update_id = start.get("update_id")

    # Get the status and block until it is done
    update = client.pipelines().get_update_by_id(DA.pipeline_id, update_id)
    state = update.get("update").get("state")

    done = ["COMPLETED", "FAILED", "CANCELED"]
    while state not in done:
        duration = 15
        time.sleep(duration)
        print(f"Current state is {state}, sleeping {duration} seconds.")    
        update = client.pipelines().get_update_by_id(DA.pipeline_id, update_id)
        state = update.get("update").get("state")
    
    print(f"The final state is {state}.")    
    assert state == "COMPLETED", f"Expected the state to be COMPLETED, found {state}"

DA.start_pipeline = _start_pipeline    

# COMMAND ----------

DA.cleanup()
DA.init()

# DA.paths.data_source = "/mnt/training/healthcare"
# DA.paths.storage_location = f"{DA.paths.working_dir}/storage"
# DA.paths.data_landing_location    = f"{DA.paths.working_dir}/source/tracker"

DA.data_factory = DltDataFactory()
DA.conclude_setup()

