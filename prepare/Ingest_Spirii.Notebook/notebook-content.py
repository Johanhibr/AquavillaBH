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
# META       "default_lakehouse_workspace_id": "4358b870-4392-403f-8b26-dc95456769dd",
# META       "known_lakehouses": [
# META         {
# META           "id": "0a6bc902-cad5-45de-a8b7-ef928ff544df"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
import json
import datetime
import time
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField
from notebookutils import mssparkutils
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run AquaVilla_Functions_Extensions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run AquaVilla_LandingToBase_Custom_Transformations

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

environment = get_workspace_environment()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

API_KEY = get_key_vault_secret('APIKey-Spirii')
connectionString = get_key_vault_secret(METADATA_SQLDB_CONNECTION_STRING_SECRET_NAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

metadata = get_spirii_metadata(connectionString)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

metadata

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Starting Spirii API ingestion process...")

# Configuration
LOAD_TYPE = "incremental"
max_retries = 5
initial_backoff = 2
max_backoff = 60
timeout_value = 120
batch_size = 500

# Extract endpoint names from metadata
endpoints_to_process = [obj['ObjectName'] for obj in metadata['SourceObjects']]
print(f"Will process the following endpoints: {', '.join(endpoints_to_process)}")

# Dictionary to store results for reporting
results = {
    'endpoints_processed': [],
    'total_records': 0,
    'successful_batches': 0,
    'max_timestamps': {},
    'last_cursors': {}
}

# Process each endpoint
for API_ENDPOINT in endpoints_to_process:
    print(f"\n{'=' * 80}")
    print(f"Processing endpoint: {API_ENDPOINT} (Load Type: {LOAD_TYPE})")
    print(f"{'=' * 80}")
    
    endpoint_metadata = next((obj for obj in metadata['SourceObjects'] if obj['ObjectName'] == API_ENDPOINT), None)
    if not endpoint_metadata:
        print(f"WARNING: Could not find metadata for {API_ENDPOINT}, skipping")
        continue
    
    # Force full load for locations endpoint
    effective_load_type = LOAD_TYPE
    if API_ENDPOINT == "locations":
        effective_load_type = "full"
        if LOAD_TYPE == "incremental":
            print(f"WARNING: Locations endpoint forced to full load (data cannot handle incremental)")
    
    # Process the endpoint
    endpoint_result = spirii_process_endpoint(API_ENDPOINT, endpoint_metadata, effective_load_type)
    
    print(f"\n--- Ingestion Complete for '{API_ENDPOINT}' ({effective_load_type} load) ---")
    print(f"Total records retrieved: {endpoint_result['total_records']}")
    print(f"Total batches processed: {endpoint_result['batch_count']}")
    print(f"Successful batches: {endpoint_result['successful_batches']}")
    print(f"Data stored in: {endpoint_result['folder_path']}")
    
    # Update metadata LastValueLoaded if ingestion was successful
    if endpoint_result['successful_batches'] > 0:
        try:
            update_last_value_loaded_now(
                connection_string=connectionString,
                connection_name="SpiriiAPI",
                endpoints=[API_ENDPOINT],
                source_objects_metadata=metadata,
                minutes=60,
                format_strategy="infer"
            )
            print(f"Updated LastValueLoaded for '{API_ENDPOINT}' to now() - 60 minutes in meta DB.")
        except Exception as e:
            print(f"WARNING: Failed to update LastValueLoaded for '{API_ENDPOINT}': {e}")

    # Store results for final summary
    results['endpoints_processed'].append(API_ENDPOINT)
    results['total_records'] += endpoint_result['total_records']
    results['successful_batches'] += endpoint_result['successful_batches']
    results['last_cursors'][API_ENDPOINT] = endpoint_result['next_cursor']
    
    # Convert max timestamp to expected format if found
    if endpoint_result['max_timestamp_found'] and effective_load_type == "incremental":
        try:
            dt = datetime.fromisoformat(endpoint_result['max_timestamp_found'].replace('Z', '+00:00'))
            formatted_timestamp = dt.strftime('%Y%m%d%H%M%S')
            print(f"Maximum timestamp found: {endpoint_result['max_timestamp_found']}")
            print(f"Formatted for next incremental load: {formatted_timestamp}")
            results['max_timestamps'][API_ENDPOINT] = formatted_timestamp
        except Exception as e:
            print(f"Could not parse max timestamp: {str(e)}")
            results['max_timestamps'][API_ENDPOINT] = None
    else:
        results['max_timestamps'][API_ENDPOINT] = None

# Print final summary
print("\n\n" + "=" * 80)
print("FINAL SUMMARY OF SPIRII API INGESTION")
print("=" * 80)
print(f"Load type: {LOAD_TYPE}")
print(f"Endpoints processed: {', '.join(results['endpoints_processed'])}")
print(f"Total records retrieved across all endpoints: {results['total_records']}")
print(f"Total successful batches: {results['successful_batches']}")
print("\nLast cursors for each endpoint:")
for endpoint, cursor in results['last_cursors'].items():
    print(f"  - {endpoint}: {cursor}")
print("\nMax timestamps for incremental loads:")
for endpoint, timestamp in results['max_timestamps'].items():
    if timestamp:
        print(f"  - {endpoint}: {timestamp}")
    else:
        print(f"  - {endpoint}: None")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
