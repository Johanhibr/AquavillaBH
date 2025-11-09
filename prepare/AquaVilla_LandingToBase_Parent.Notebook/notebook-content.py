# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0a6bc902-cad5-45de-a8b7-ef928ff544df",
# META       "default_lakehouse_name": "Landing",
# META       "default_lakehouse_workspace_id": "4358b870-4392-403f-8b26-dc95456769dd"
# META     }
# META   }
# META }

# MARKDOWN ********************

# <center>
# 
# # AquaVilla - Landing to Base Parent
# </center>
# 
# Run landing to base in parallel by iterating over metadata and running AquaVilla_LandingToBase_Child in parallel.

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

environment = get_workspace_environment()
connectionString = mssparkutils.credentials.getSecret(f"https://{KEYVAULT_NAME_PREFIX}-{environment}.vault.azure.net/", METADATA_SQLDB_CONNECTION_STRING_SECRET_NAME)

source_objects = None
# source_objects = '[{"SourcePath": "Files/AquaVillaERP/DebtorLocations"}, {"SourcePath": "Files/AquaVillaERP/Debtors"}]'

dag_concurrency_count = 10
dag_timeout_hours = 12
cell_timeout_minutes = 10
cell_retry_count = 1
recreate: bool = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if source_objects is None:
    source_objects = get_source_objects_fromazuresql(connectionString)
else:
    source_objects = parse_notebook_parameter(source_objects)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run landing to base in parallel
# Build DAG from metadata and the child notebook AquaVilla_LandingToBase_Child for each object.
# Executes in parallel by utilizing runMultiple from the mssparkutils library.

# CELL ********************

DAG = {
    "activities": [
        {
            "name": f"Run_{source_object['SourcePath']}",
            "path": "AquaVilla_LandingToBase",
            "timeoutPerCellInSeconds": cell_timeout_minutes * 60,
            "retry": cell_retry_count,
            "args": {
                "source_name": source_object['SourcePath'],
                "recreate": recreate
            }
        } for source_object in source_objects
    ],
    "timeoutInSeconds": dag_timeout_hours * 60 * 60,
    "concurrency": dag_concurrency_count
}

output = mssparkutils.notebook.runMultiple(DAG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
