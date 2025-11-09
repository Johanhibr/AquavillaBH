# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a42faafd-c34e-4999-afda-391c3081850f",
# META       "default_lakehouse_name": "Landing",
# META       "default_lakehouse_workspace_id": "1c7b225a-9103-420a-8ffb-cdc785db59a6",
# META       "known_lakehouses": [
# META         {
# META           "id": "a42faafd-c34e-4999-afda-391c3081850f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# <center>
# 
# # DaluxAPI - Landing to Base
# 
# </center>
# 
# This notebook automates the process of loading raw JSON data from the Landing layer to the Base layer of the Data Platform.
# 
# The process is based on metadata in an Azure SQL DB.

# CELL ********************

from pyspark.sql.functions import col, explode, lit, when, expr, size, first, udf, concat_ws
from notebookutils import mssparkutils
import re

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

# # Custom made functions for JSON object processing

# def flatten(df):
#     # Initial flattening pass
#     flat_cols = []
#     for column_name, dtype in df.dtypes:
#         if dtype.startswith("struct"):
#             # If it's a struct, flatten it
#             flat_cols += [col(f"{column_name}.{nested}").alias(f"{column_name}_{nested}") 
#                           for nested in df.select(f"{column_name}.*").columns]
#         else:
#             # Otherwise, add the column as is
#             flat_cols.append(col(column_name))
    
#     # Create a new DataFrame with the flattened columns
#     df_flat = df.select(flat_cols)
    
#     # Check if there are still struct fields left to flatten
#     has_struct = any(dtype.startswith("struct") for _, dtype in df_flat.dtypes)
    
#     # If there are struct fields, recursively flatten the DataFrame
#     if has_struct:
#         return flatten(df_flat)
    
#     # Return the fully flattened DataFrame
#     return df_flat

# # Function to remove invalid characters and clean "items_data_" prefix from column names
# def clean_column_names(df: DataFrame) -> DataFrame:
#     for col_name in df.columns:
#         # Remove the "items_data_" prefix if it exists
#         clean_col_name = re.sub(r'^items_data_', '', col_name)
        
#         # Remove invalid characters from the column name
#         clean_col_name = re.sub(r'[ -\/.,;{}()\n\t=]', '', clean_col_name)

#         # Translate DK characters (æøå) in the column name
#         dk_char_mapping = {'æ':'ae','ø':'oe','å':'aa'}
#         for k,v in dk_char_mapping.items():
#             clean_col_name = clean_col_name.replace(k,v)
        
#         # Rename the column if the cleaned name is different
#         if clean_col_name != col_name:
#             df = df.withColumnRenamed(col_name, clean_col_name)
    
#     return df

# def handle_user_defined_fields(df):
#     # Check if the userDefinedFields column exists
#     if "items_data_userDefinedFields" in df.columns:
#         # Check if the userDefinedFields is not null or empty
#         df = df.withColumn("userDefinedFields_exists", 
#                            when((col("items_data_userDefinedFields").isNotNull()) & (F.size(col("items_data_userDefinedFields")) > 0), True)
#                            .otherwise(False))
        
#         # Proceed only if the userDefinedFields array exists and is not empty
#         if df.filter(col("userDefinedFields_exists")).count() > 0:
#             # Explode the userDefinedFields array into multiple rows
#             exploded_df = df.withColumn("userDefinedField", explode(col("items_data_userDefinedFields")))
            
#             # Now pivot the 'name' to create new columns without prefix
#             pivot_df = exploded_df.groupBy(*[c for c in exploded_df.columns if c != 'userDefinedField']) \
#                                   .pivot("userDefinedField.name") \
#                                   .agg(first("userDefinedField.value"))
            
#             # Rename the columns by prefixing them with "userDefinedFields_"
#             for col_name in pivot_df.columns:
#                 if col_name not in df.columns:  # Only rename newly created columns
#                     pivot_df = pivot_df.withColumnRenamed(col_name, f"userDefinedFields_{col_name}")
            
#             # Drop the temporary existence check column
#             pivot_df = pivot_df.drop("userDefinedFields_exists")
#             return pivot_df
#         else:
#             # If no userDefinedFields exist or are empty, return the original DataFrame
#             return df.drop("userDefinedFields_exists")
#     else:
#         # If the userDefinedFields column doesn't exist, just return the original DataFrame
#         return df

# # Helper function to extract date (year/month/day) from the file path
# def extract_date_from_path(path):
#     match = re.search(r'year=(\d{4})/month=(\d{2})/day=(\d{2})', path)
#     if match:
#         year, month, day = match.groups()
#         return f"{year}-{month}-{day}"
#     return None

# # Function to recursively get the latest file based on the folder date and file modification time
# def get_latest_file_by_date_and_modification(root_path):
#     # Recursively list all files and directories under the root path
#     all_files = []
#     directories_to_scan = [root_path]
    
#     while directories_to_scan:
#         current_dir = directories_to_scan.pop()
#         items = mssparkutils.fs.ls(current_dir)
        
#         for item in items:
#             if item.isDir:  # If it's a directory, add it to the list to scan
#                 directories_to_scan.append(item.path)
#             else:  # If it's a file, append it along with its modification time
#                 all_files.append((item.path, item.modifyTime))
    
#     # Filter out files with date pattern (year=yyyy/month=mm/day=dd)
#     dated_files = [(file, extract_date_from_path(file), mod_time) for file, mod_time in all_files if extract_date_from_path(file)]
    
#     # Sort the files based on extracted date first and then modification time (newest first)
#     dated_files.sort(key=lambda x: (x[1], x[2]), reverse=True)
    
#     # Return the path of the latest file, if any exist
#     if dated_files:
#         return dated_files[0][0]  # Return the file path of the latest file
#     else:
#         return None

# def handle_userRegion_column(df):
#     # Check if "items_data_userRegion" column exists
#     if "items_data_userRegion" in df.columns:
#         # Apply concat_ws to flatten the array into a comma-separated string
#         df = df.withColumn("userRegion", concat_ws(",", col("items_data_userRegion")))
#     else:
#         # Optionally, if the column doesn't exist, do nothing or print a message
#         print("Column 'items_data_userRegion' does not exist, skipping concat_ws.")
    
#     return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Define meta columns to be dropped after processing

# cols_to_drop = [
#     'items_data_userDefinedFields', 
#     'links',
#     'items_links',
#     'items_data_userRegion',
#     'metadata_bookmark', 
#     'metadata_limit', 
#     'metadata_nextBookmark', 
#     'metadata_totalItems', 
#     'responseCode', 
#     'responseMessage',
#     'landing_created_date',
#     'landing_load_type',
#     'year',
#     'month',
#     'day',
#     'date',
#     ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

environment = get_workspace_environment()
source_name: str = None
recreate: bool = None       # Enables overriding recreate flag. I.e. through pipeline execution. Note! This will delete and recreate the destination tables
destination_lakehouse: str = 'Base'

connectionString = mssparkutils.credentials.getSecret(f"https://{KEYVAULT_NAME_PREFIX}-{environment}.vault.azure.net/", METADATA_SQLDB_CONNECTION_STRING_SECRET_NAME)
unfiltered_meta_data = get_metadata_base_fromazuresql(connectionString, source_name, recreate)


#Filtering out all metadata that does not relate to Dalux
meta_data = {key: value for key, value in unfiltered_meta_data.items() if "dalux" in key.lower()}





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Printing meta_data to see structure
unfiltered_meta_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##############################################################################
## LOAD LANDING TO BASE FOR NESTED JSON OBJECTS - ITERATE METADATA DEFINITION
##############################################################################

# Tracking loop attempts to avoid indefinite while-loops
max_full_loops = 5  # Define maximum number of full loops
current_loop = 0    # Track the current loop count

while not all_tables_loaded(meta_data):
    print(f"Starting loop {current_loop + 1} of {max_full_loops}")

    for sourceName in meta_data:
        try:
            source = meta_data[sourceName]
            print(f"Handling {sourceName}")

            # # Return the absolute path for the latest available full-load file
            # latest_file = get_latest_file_by_date_and_modification(sourceName)
            # print(f"Processing data from full-load file: " + latest_file[latest_file.rfind("/") + 1:])

            # # Custom code for handling JSON files. Nested schemas are identified.
            # sourceSchema = spark.read.json(latest_file).schema
            # source_df_unexploded = flatten(spark.read.schema(sourceSchema).json(latest_file).withColumn("items", explode(col("items"))))
            # source_df_exploded = handle_user_defined_fields(source_df_unexploded)
            # source_df_userRegion = handle_userRegion_column(source_df_exploded)
            # source_df_drop = source_df_userRegion.drop(*cols_to_drop)
            # source_df = clean_column_names(source_df_drop)

            source_df = process_dalux_json_file(sourceName)

            for destination_table in source['Destinations']:
                destination = source['Destinations'][destination_table]

                if recreate:
                    print(f"Overwrite recreate flag by parameter ({recreate})")
                    destination['RecreateFlag'] = recreate

                if (destination['IsLoaded']):
                    print (f"{sourceName} to {destination_table} is loaded")
                    print(f"------------------------------------------------------------------------")

                    continue

                print(f"Processing destination {destination_table}")

                dependencies_loaded = True
                for dependencyName in destination['dependencies']:
                    if not is_dependency_loaded(meta_data, dependencyName):
                        print (f"Dependency {dependencyName} for {destination_table} not loaded")
                        dependencies_loaded = False

                if not dependencies_loaded:
                    print(f"Postponing {destination_table} missing dependencies")
                    continue

                destination_df = source_df
                
                if list(filter(None, destination['ProjectedColumns'])):
                    print(destination['ProjectedColumns'])
                    projectedColumns = destination['ProjectedColumns'].copy()
                else:
                    projectedColumns = list(source_df.columns)

                for dependencyName in destination['dependencies']:
                    dependency = find_dependency(meta_data, dependencyName)
                    print(f"Loading dependecy table {dependencyName}")
                    dependency_df = spark.read.table(f"{destination_lakehouse}.{dependencyName}")
                    print(f"Replacing columns: {dependency['ProjectedColumns']} with {dependencyName}_id")
                    destination_df = destination_df.join(dependency_df, dependency['ProjectedColumns'], 'left')
                    destination_df = destination_df.withColumnRenamed('Id', f"{dependencyName}_id")
                    destination_df
                    projectedColumns.remove('{' + dependencyName + '}')
                    projectedColumns.insert(0, f"{dependencyName}_id")

                special_character_escaped_columns = [f"`{column}`" for column in projectedColumns]
                destination_df = destination_df.select(special_character_escaped_columns)

                # Drop duplicate
                print("De-duplicating")
                destination_df = destination_df.dropDuplicates()

                # Replace null/blank values
                print("Fill na with 0 and Tomt")
                destination_df = destination_df.na.fill(0) # Fill all empty integers with 0 (zero)
                destination_df = destination_df.na.fill("Tomt") # Fill all empty strings with "Tomt"

                # Translate columns based on translation definition
                if destination['Translations']:
                    jsonstr = re.sub(r'''(["'0-9.]\s*),\s*}''', r'\1}', destination['Translations'])
                    translations = json.loads(jsonstr)
                    print("Renaming columns based on translation definition")
                    destination_df = rename_columns(destination_df, translations)

                readBehavior: str = source['SourceReadBehavior'].lower()
                full_load = True if readBehavior == 'full' else False         

                writeBehavior: str = destination['DestinationWriteBehavior'].lower()
                match writeBehavior:
                    case 'overwrite':
                        write_to_delta_overwrite(df = destination_df, destination_lakehouse = destination_lakehouse, destination_table = destination_table, recreate = destination['RecreateFlag']) 
                    case 'append':
                        write_to_delta_append(df = destination_df, destination_lakehouse = destination_lakehouse, destination_table = destination_table, recreate = destination['RecreateFlag']) 
                    case 'scd1':
                        write_to_delta_scd1(
                            df = destination_df, 
                            destination_lakehouse = destination_lakehouse, 
                            destination_table = destination_table, 
                            keys = destination['Keys'], 
                            attribute_columns = projectedColumns, 
                            sequence_columns = (destination['ModifyDateColumn']), 
                            recreate = destination['RecreateFlag'],
                            full_load = full_load
                        )     
                    case 'scd2':
                        write_to_delta_scd2(
                            df = destination_df, 
                            destination_lakehouse = destination_lakehouse, 
                            destination_table = destination_table, 
                            keys = destination['Keys'], 
                            attribute_columns = projectedColumns, 
                            sequence_columns = (destination['ModifyDateColumn']), 
                            recreate = destination['RecreateFlag'],
                            full_load = full_load
                        ) 
                    case _:
                        raise Exception(f"Unsupported write behavior: {writeBehavior}. Verify that metadata is correct.")

                destination['IsLoaded'] = True
                print(f"------------------------------------------------------------------------")    
        except Exception as e:
            print(f"Failed processing {sourceName} due to error {e}")
            print(f"------------------------------------------------------------------------") 
    
    # Increment the loop counter after each full pass through all tables
    current_loop += 1

    # Check if maximum loop count has been reached
    if current_loop >= max_full_loops:
        print(f"Reached maximum of {max_full_loops} full loops. Exiting loop to avoid indefinite retry.")
        break  # Break out of the while loop if the max count is reached

# Final check if the loop ended due to loop limit or successful loading
if all_tables_loaded(meta_data):
    print("All tables loaded successfully.")
else:
    print(f"Some tables failed to load after {max_full_loops} attempts.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
