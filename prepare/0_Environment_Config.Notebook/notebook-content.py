# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Configuration Notebook
# 
# #### Environments
# This example uses 3 environments:
# - **Dev**
# - **Test**
# - **Prod**
# 
# #### Workspaces
# This example uses 2 workspaces per environment:
# - **Code**: This workspace will be Git integrated and contain all code such as data pipelines, notebooks, etc.
# - **Data**: This workspace will contain all Lakehouses and NOT be Git integrated.
# 
# #### Lakehouses
# This example uses 3 different Lakehouses to implement a medallion architecture:
# - **Landing**: This Lakehouse will contain all raw ingested data including shortcuts to other source systems.
# - **Base**: This Lakehouse will contain deduplicated and cleansed data that will form the basis for the dimensional modelling.
# - **Curated**: This Lakehouse will contain the dimensional model following a classic Star Schema.


# CELL ********************

configuration_ini = """

[Dev]
workspace_code              = c788b694-9566-4587-b385-9c49c392c479
workspace_data              = 1c7b225a-9103-420a-8ffb-cdc785db59a6
workspace_semantic          = 53ae7245-bc28-4260-b903-9f179458341c
lakehouse_landing           = a42faafd-c34e-4999-afda-391c3081850f
lakehouse_base              = 754cc2c7-2283-46e4-8000-178de54f8053
lakehouse_curated           = 50a33a9e-80f1-4299-9410-c268c11a4640
azure_resource_group        = rg-fabric-dev-westeu-001
azure_data_factory          = adf-bhdp-dev
notebook_landingtobase      = 790e88d7-f6e5-428f-9522-7ef031ca23d5
notebook_d365landingtobase  = 1e9d2205-d553-44dd-8f1f-ccdbf4f0e5ae
notebook_daluxlandingtobase = e0d83afa-607a-49c5-9d74-343d03904943
notebook_basetocurated      = 578bd2e0-103b-46a8-b7b9-f5acff909e7c
notebook_curateddependenciesinbase = 637b0b9f-6e58-4d3d-abf2-7bb750845b1e
dataflow_create_views       = 929f7217-f268-4c5b-b6e0-0adb9024d6c5
semantic_model              = 7d3b632a-5251-465e-bcdf-040db5bc6049

[Test]
workspace_code              = f549fb7e-b15b-48fb-8e81-196383d4b726
workspace_data              = 2b00d302-f2e0-4c66-af11-56e7912771bd
workspace_semantic          = 3378028c-9c27-4001-a1cd-55c0cb1ca994
lakehouse_landing           = f012a9c1-888a-4a60-bce8-e49979990d76
lakehouse_base              = ea3f3067-343d-4c58-add0-9ab3e6d18c9c
lakehouse_curated           = 5dc5d356-bdea-424a-8a24-4b637c76c3b3
azure_resource_group        = rg-fabric-test-westeu-001
azure_data_factory          = adf-bhdp-test
notebook_landingtobase      = 7ab0c7fe-7fc3-4af2-857f-2426be862760
notebook_d365landingtobase  = 23667e36-217e-49d0-9194-5052aa1a1775
notebook_daluxlandingtobase = 27bf4660-665d-4572-83f7-f28219709639
notebook_basetocurated      = 3398b36e-b5ef-43d9-8c27-728c5d3ee120
notebook_curateddependenciesinbase = e2fc3621-d9c5-4f9b-aab2-6b0ce08d1be5
dataflow_create_views       = e998bedf-e414-40df-abb4-e6d528eceb95
semantic_model              = c198ac53-d3ac-4e30-a1a9-9fde7a1d8d8f

[Prod]
workspace_code              = aefeba12-3968-4139-88b3-223aecb0a478
workspace_data              = e2edfb2c-2b42-4490-a03f-283669056f52
workspace_semantic          = cff1f908-993c-428e-ae3d-12f4b5720f05
lakehouse_landing           = 58697540-849c-4285-b845-db6e7c74d93a
lakehouse_base              = 2a96dc50-c42f-4ab7-91fd-e4ca9acbed9e
lakehouse_curated           = ca0fb7cf-1bc7-4c19-b807-5fb431fe9455
azure_resource_group        = rg-fabric-prod-westeu-001
azure_data_factory          = adf-bhdp-prod
notebook_landingtobase      = b3b39319-ed47-439b-bcb1-d25bb7d434aa
notebook_d365landingtobase  = 82d9a704-295a-4e52-809c-a6d2f2242bcf
notebook_daluxlandingtobase = 2bff24f0-2d65-4c95-8da7-1efffba0cc5a
notebook_basetocurated      = a7b75f8d-e92e-4133-95e8-b144cc406807
notebook_curateddependenciesinbase = b89e43cf-b6a7-4aab-b0cd-a5af623502c1
dataflow_create_views       = 172582b7-b8c8-4f81-84fe-c73b09e31e16
semantic_model              = 78b82235-3dbf-4c9f-85f0-2fc134e1f285

"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import configparser
import io
from notebookutils import mssparkutils
import sempy.fabric as fabric


class EnvConfig:

    def __init__(self):

        # Initialize configuration
        buffer = io.StringIO(configuration_ini)
        self.conf = configparser.ConfigParser()
        self.conf.read_file(buffer)

        # Set current environment variable
        workspace_id = fabric.get_notebook_workspace_id()
        workspace_name = fabric.resolve_workspace_name(workspace_id)
        self.current_env = self.get_current_env(workspace_id)
        print(f"Current environment:    {self.current_env} ({workspace_name})")

    def get_current_env(self, workspace_id):
        """ 
        This function will deduct which environment is currently used by 
        comparing the current workspace id with the defined list of workspaces.
        Will default to Development environment to accomodate for personal/feature workspaces. 
        """

        # Find which section (Dev/Test/Prod) corresponds to current workspace id
        if workspace_id == self.conf.get("Test", "workspace_code"):
            return "Test"
        elif workspace_id == self.conf.get("Prod", "workspace_code"):
            return "Prod"
        else:
            return "Dev"
    
    def get_parameter(self, parameter):
        """
        This function will return a given parameter for the current environment.
        """
        return self.conf.get(self.current_env, parameter)

    def get_parameters(self):
        """
        This function all parameters for the current environment.
        """
        return self.conf[self.current_env]

    def get_lakehouse_path(self, lakehouse):
        """
        This function will construct a full abfs path to the given lakehouse (landing, base or curated) residing in the data workspace.
        """
        workspace_id = self.conf.get(self.current_env, "workspace_data")
        lakehouse_id = self.conf.get(self.current_env, lakehouse)
        return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"

    def get_lakehouse_name(self, lakehouse):
        """
        This function will return the name of a given lakehouse (landing, base or curated).
        """
        workspace_id = self.conf.get(self.current_env, "workspace_data")
        lakehouse_id = self.conf.get(self.current_env, lakehouse)

        for item in mssparkutils.lakehouse.list(workspaceId=workspace_id):
            if item.id == lakehouse_id:
                return item.displayName
    
        raise ValueError("No lakehouse matching the given value was found.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#initialize Config
env_config = EnvConfig()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare parameter values
import json

# Get names of source and destination Lakehouses
result_json = json.dumps({
    'workspace_code':               env_config.get_parameter("workspace_code"),
    'workspace_data':               env_config.get_parameter("workspace_data"),
    'lakehouse_landing':            env_config.get_parameter("lakehouse_landing"),
    'lakehouse_base':               env_config.get_parameter("lakehouse_base"),
    'lakehouse_curated':            env_config.get_parameter("lakehouse_curated"),
    'azure_resource_group':         env_config.get_parameter("azure_resource_group"),
    'azure_data_factory':           env_config.get_parameter("azure_data_factory"),
    'notebook_landingtobase':       env_config.get_parameter("notebook_landingtobase"),
    'notebook_d365landingtobase':   env_config.get_parameter("notebook_d365landingtobase"),
    'notebook_daluxlandingtobase':  env_config.get_parameter("notebook_daluxlandingtobase"),
    'notebook_basetocurated':       env_config.get_parameter("notebook_basetocurated"),
    'notebook_curateddependenciesinbase': env_config.get_parameter("notebook_curateddependenciesinbase"),
    'dataflow_create_views':        env_config.get_parameter("dataflow_create_views"),
    'workspace_semantic':           env_config.get_parameter("workspace_semantic"),
    'semantic_model':               env_config.get_parameter("semantic_model")
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Return results in nteobook output
mssparkutils.notebook.exit(result_json)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
