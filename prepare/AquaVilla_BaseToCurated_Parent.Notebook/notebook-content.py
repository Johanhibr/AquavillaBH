# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2215fdc2-5d05-4516-882c-92b6c0bcd831",
# META       "default_lakehouse_name": "Curated",
# META       "default_lakehouse_workspace_id": "4358b870-4392-403f-8b26-dc95456769dd"
# META     }
# META   }
# META }

# MARKDOWN ********************

# <center>
# 
# # AquaVilla - Base to Curated Parent
# </center>
# 
# Run base to curated by looking up notebooks in the workspace which starts with "Load_Dim", "Load_Fact" or "Load_Bridge".
# Optionally, the parameters can be set with a JSON-string to only run a subset of notebooks. An empty list, i.e. `[]` can be set if no notebook should be run.
# 
# First all dimension notebooks run in parallel, and afterwards all the fact notebooks run.

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# If set, must be a JSON-string. If None, it will automatically scan workspace for notebooks prefixed with `Load_Dim`
dimension_notebooks = None
# dimension_notebooks = '[{"Name": "Load_DimCalendar"}, {"Name":"Load_DimProduct"}]'

# If set, must be a JSON-string. If None, it will automatically scan workspace for notebooks prefixed with `Load_Fact` and `Load_Bridge`
fact_and_bridge_notebooks = None
# fact_and_bridge_notebooks = '[{"Name": "Load_FactRetailSales"}]'


dag_concurrency_count = 10
dag_timeout_hours = 12
cell_timeout_minutes = 10
cell_retry_count = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if dimension_notebooks is None:
    dimension_notebooks = get_workspace_dimension_notebooks()
else:
    dimension_notebooks = [notebook["Name"] for notebook in parse_notebook_parameter(dimension_notebooks)]

if fact_and_bridge_notebooks is None:
    fact_and_bridge_notebooks = get_workspace_fact_and_bridge_notebooks()
else:
    fact_and_bridge_notebooks = [notebook["Name"] for notebook in parse_notebook_parameter(fact_and_bridge_notebooks)]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Define directed acyclic graph (DAG)
# The DAG automatically adds all dimensions as dependencies for the fact tables.

# CELL ********************

dimension_activities = [
    {
        "name": f"Run_{notebook}",
        "path": notebook,
        "timeoutPerCellInSeconds": cell_timeout_minutes * 60,
        "retry": cell_retry_count,
        "args": {
            # "optional_argument": "optional_value"
        }
    } for notebook in dimension_notebooks
]

fact_and_bridge_activities = [
    {
        "name": f"Run_{notebook}",
        "path": notebook,
        "timeoutPerCellInSeconds": cell_timeout_minutes * 60,
        "retry": cell_retry_count,
        "args": {
            # "optional_argument": "optional_value"
        },
        "dependencies": [activity["name"] for activity in dimension_activities]
    } for notebook in fact_and_bridge_notebooks
]

DAG = {
    "activities": dimension_activities + fact_and_bridge_activities,
    "timeoutInSeconds": dag_timeout_hours * 60 * 60,
    "concurrency": dag_concurrency_count
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run DAG in parallel
# Runs all notebooks.

# CELL ********************

output = mssparkutils.notebook.runMultiple(DAG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
