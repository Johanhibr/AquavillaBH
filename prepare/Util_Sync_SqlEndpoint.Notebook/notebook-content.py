# Fabric notebook source


# MARKDOWN ********************


# MARKDOWN ********************

# <center>
# 
# # Util - Synchronize SQL Analytics Endpoint
# </center>
# 
# Run this notebook to synchronize the SQL Analytics Endpoint of a specific lakehouse. The notebook accepts the following parameters:  
# - **workspace_id (str)**: Guid of the workspace for which SQL Analytics endpoint should be synchronized. _Default is **spark.conf.get("trident.workspace.id")**_. This is the guid of the workspace for the attached default lakehouse.
# - **lakehouse_id (str)**: Guid of the lakehouse for which SQL Analytics endpoint should be synchronized. _Default is **spark.conf.get("trident.lakehouse.id")**_. This is the guid of the attached default lakehouse.
# - **standard_time_zone** (str): Timezone used for outputting dates for last table update. _Default is **CET**._
# - **lakehouse_name** (str): Name of the lakehouse for which SQL Analytics endpoint should be synchronized. Only needs to be set if the Lakehouse differs from the default attached lakehouse. _Default is **None**._ 
# 
# Ideally run the notebook using **notebookutils.run** from a parent notebook as shown below.
# ```
# mssparkutils.notebook.run("Util_Sync_SqlEndpoint", arguments={ "useRootDefaultLakehouse": True })
# ```


# CELL ********************

import json
import logging
import time
import pytz
import sempy.fabric as fabric
from datetime import datetime
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException

# Setup logging to show only warnings and errors
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# ## Parameters
# Parameters for defining the lakehouse for which the SQL Analytics Endpoint must be synchronized etc.

# PARAMETERS CELL ********************

workspace_id = spark.conf.get("trident.workspace.id")
lakehouse_id = spark.conf.get("trident.lakehouse.id")

standard_time_zone = "CET" # Central European Time
lakehouse_name = None # Will override the lakehouse_id if set. lakehouse_id will then be derived from the name by scanning the lakehouse items in the workspace.

if lakehouse_name:
    lakehouse_id = sempy.fabric.resolve_item_id(lakehouse_name, "Lakehouse", workspace_id)
   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Functions
# Functions for running the synchronization, displaying converted time etc.
# The sync_sql_endpoint function is based on a blog post by Mark Pryce-Maher.  
# Link: https://medium.com/@sqltidy/delays-in-the-automatically-generated-schema-in-the-sql-analytics-endpoint-of-the-lakehouse-b01c7633035d)

# CELL ********************

def convert_utc_to_timezone(utc_time_str, time_zone:str = standard_time_zone):
    if utc_time_str is None:
        return 'N/A'
    utc_time = datetime.fromisoformat(utc_time_str.replace("Z", "+00:00"))
    return utc_time.astimezone(pytz.timezone(time_zone)).strftime('%Y-%m-%d %H:%M:%S %Z')


# Function to sync SQL endpoint with the Lakehouse
def sync_sql_endpoint(client, workspace_id, lakehouse_id):
    try:
        # Fetch SQL endpoint properties
        lakehouse_info = client.get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()
        sql_endpoint_id = lakehouse_info['properties']['sqlEndpointProperties']['id']
        
        # Set URI for the API call
        uri = f"/v1.0/myorg/lhdatamarts/{sql_endpoint_id}"
        payload = {"commands": [{"$type": "MetadataRefreshCommand"}]}
        
        # Call REST API to initiate the sync
        response = client.post(uri, json=payload)
        if response.status_code != 200:
            logging.error(f"Error initiating sync: {response.status_code} - {response.text}")
            return
        
        data = json.loads(response.text)

        batch_id = data["batchId"]
        progress_state = data["progressState"]

        # URL for checking the sync status
        status_uri = f"/v1.0/myorg/lhdatamarts/{sql_endpoint_id}/batches/{batch_id}"
        
        # Polling until the sync is complete
        while progress_state == 'inProgress':
            time.sleep(1)  # Polling interval
            status_response = client.get(status_uri)
            status_data = status_response.json()
            progress_state = status_data["progressState"]
        
        # Check if the sync completed successfully
        if progress_state == 'success':
            table_details = [
                {
                    'tableName': table['tableName'],
                    'lastSuccessfulUpdate': convert_utc_to_timezone(table.get('lastSuccessfulUpdate')),
                    'tableSyncState': table['tableSyncState'],
                    'sqlSyncState': table['sqlSyncState']
                }
                for table in status_data['operationInformation'][0]['progressDetail']['tablesSyncStatus']
            ]
            
            # Print extracted table details
            for detail in table_details:
                print(f"Table: {detail['tableName']}   Last Update: {detail['lastSuccessfulUpdate']}  "
                      f"Table Sync State: {detail['tableSyncState']}  SQL Sync State: {detail['sqlSyncState']}")
        
        # Handle failure
        elif progress_state == 'failure':
            logging.error(f"Sync failed: {status_data}")
    
    except FabricHTTPException as fe:
        logging.error(f"Fabric HTTP Exception: {fe}")
    except WorkspaceNotFoundException as we:
        logging.error(f"Workspace not found: {we}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Syncronize SQL Analytics Endpoint

# CELL ********************

# Syncronize SQL Analytics Endpoint
client = fabric.FabricRestClient()
sync_sql_endpoint(client, workspace_id, lakehouse_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

