# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c83ca59e-f468-4305-a323-a324e0fb54b4",
# META       "default_lakehouse_name": "Base",
# META       "default_lakehouse_workspace_id": "4358b870-4392-403f-8b26-dc95456769dd"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # D365FO_LandingToBase
# This notebook reads all D365FO delta tables that are included in Dataverse Fabric Link and added as shortcuts in Landing Lakehouse and writes those tables to the Base Lakehouse.

# PARAMETERS CELL ********************

load_base_tables = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Import AquaVilla Functions

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Set Lakehouse and Environment parameters

# CELL ********************

environment = get_workspace_environment()

LandingLakehouse = {
        'dev':'abfss://4358b870-4392-403f-8b26-dc95456769dd@onelake.dfs.fabric.microsoft.com/0a6bc902-cad5-45de-a8b7-ef928ff544df/Tables',
        'test':'abfss://b3de0feb-959b-4c81-b458-19c53bacd973@onelake.dfs.fabric.microsoft.com/0836a5ea-e097-4abe-83d9-daefb0b991b9/Tables',
        'prod':'abfss://163f2225-2696-4741-8fb1-c8f0bdf9d4b8@onelake.dfs.fabric.microsoft.com/949b6636-a504-4d96-9271-2f60e1544314/Tables'
        }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Notebook Parameters

# CELL ********************

d365fo_shortcut_table_prefix = "d365fo_"
onelake_d365fo_tables_dir_path: str = LandingLakehouse[environment]
shortcuts_lakehouse_name: str = "Landing"
destination_lakehouse: str = 'Base'
d365fo_default_write_behaviour: str = 'overwrite'
d365fo_default_recreate_flag: bool = True
d365fo_default_keys = ['Id']
d365fo_default_sequence_columns = ['modifieddatetime']

# Below exceptions will oversteer the default settings above when writing to destination.
default_exceptions = {
    # 'd365fo_custtable': {
    #     'd365fo_default_write_behaviour': 'scd2'
    # }
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Get List of D365FO tables synced to d365fo Link to Fabric
# Based on folders in onelake generates a dictionary with all table names.

# CELL ********************

from notebookutils import mssparkutils

onelake_d365fo_tables_dir_items = mssparkutils.fs.ls(onelake_d365fo_tables_dir_path)

d365fo_shortcut_tables = []

for dir_item in onelake_d365fo_tables_dir_items:
    if dir_item.name.startswith(d365fo_shortcut_table_prefix): #check if shortcut table is prefixed with d365fo
        d365fo_shortcut_tables.append(dir_item.name)

if load_base_tables != None:
    load_base_tables = eval(load_base_tables) # Convert string param to list

    #Overwrite all tables w. only the ones from parameter input
    d365fo_shortcut_tables = [x.replace('Base.','') for x in load_base_tables]

print(f'#### D365FO Table Shortcuts found ({len(d365fo_shortcut_tables)}) ####')
print(d365fo_shortcut_tables)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write D365FO tables from Landing Shortcuts to Base Tables

# CELL ********************

from pyspark.sql.functions import when, col, year
import datetime as dt

# Function for specific table
def d365fotable_to_base(
    shortcuts_lakehouse_name: str,
    d365fo_shortcut_table: str,
    d365fo_default_write_behaviour: str,
    d365fo_default_recreate_flag: bool,
    d365fo_default_keys,
    d365fo_default_sequence_columns,
    default_exceptions: str
):
    
    # Read Source Table
    source_df = spark.read.table(f"{shortcuts_lakehouse_name}.{d365fo_shortcut_table}")

    # Do not include records deleted in D365FO
    source_df = source_df.filter("IsDelete IS NOT true")

    # Skip empty tables
    rowCount = source_df.count()
    if rowCount == 0:
        print(f"{rowCount} rows. Skipping.")
    else:

        print(f"{rowCount} rows.")
        # Destination DF and Table Name
        destination_df = source_df
        destination_table = d365fo_shortcut_table

        # Drop duplicate
        print("De-duplicating")
        destination_df = destination_df.dropDuplicates()
        
        # Replace null/blank values
        print("Fill na with 0 and Tomt")
        destination_df = destination_df.na.fill(0) # Fill all empty integers with 0 (zero)
        destination_df = destination_df.na.fill("Tomt") # Fill all empty strings with "Tomt"

        # Column Cleaning:
        #     Cleaning CreatedOn Column. Replace datetime values from source on before 1900, eg. like 1/5/0001 12:00:00 AM
        if "CreatedOn" in destination_df.columns:
            destination_df = destination_df.withColumn('CreatedOn', when(year(col('CreatedOn')) < 1900, dt.date(1900, 1, 1)).otherwise(col('CreatedOn')))

        # Below uses default variable values unless default exceptions have been specified for the particular table.
        writeBehavior: str = d365fo_default_write_behaviour if default_exceptions.get(d365fo_shortcut_table, {}).get('d365fo_default_write_behaviour') is None else default_exceptions[d365fo_shortcut_table]['d365fo_default_write_behaviour']
        recreate_flag: bool = d365fo_default_recreate_flag if default_exceptions.get(d365fo_shortcut_table, {}).get('d365fo_default_recreate_flag') is None else default_exceptions[d365fo_shortcut_table]['d365fo_default_recreate_flag']
        keys: [] = d365fo_default_keys if default_exceptions.get(d365fo_shortcut_table, {}).get('d365fo_default_keys') is None else default_exceptions[d365fo_shortcut_table]['d365fo_default_keys']
        sequence_columns: [] = d365fo_default_sequence_columns if default_exceptions.get(d365fo_shortcut_table, {}).get('d365fo_default_sequence_columns') is None else default_exceptions[d365fo_shortcut_table]['d365fo_default_sequence_columns']
        projectedColumns = list(source_df.columns)

        match writeBehavior:
            case 'overwrite':
                write_to_delta_overwrite(df = destination_df, destination_lakehouse = destination_lakehouse, destination_table = destination_table, recreate = recreate_flag)
            case 'append':
                    write_to_delta_append(df = destination_df, destination_lakehouse = destination_lakehouse, destination_table = destination_table, recreate = recreate_flag) 
            case 'scd1':
                write_to_delta_scd1(
                    df = destination_df, 
                    destination_lakehouse = destination_lakehouse, 
                    destination_table = destination_table, 
                    keys = keys, 
                    attribute_columns = projectedColumns, 
                    sequence_columns = (sequence_columns), 
                    recreate = recreate_flag
                )     
            case 'scd2':
                write_to_delta_scd2(
                    df = destination_df, 
                    destination_lakehouse = destination_lakehouse, 
                    destination_table = destination_table, 
                    keys = keys, 
                    attribute_columns = projectedColumns, 
                    sequence_columns = (sequence_columns), 
                    recreate = recreate_flag
                ) 
            case _:
                raise Exception(f"Unsupported write behavior: {writeBehavior}. Verify that metadata is correct.")

        print(f"------------------------------------------------------------------------")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


tableProcessErrorTables = []
tableProcessErrors = []

for d365fo_shortcut_table in d365fo_shortcut_tables:
    
    print(f"#############################################################################################################################################")
    print(f"Processing {d365fo_shortcut_table} from d365fo shortcut delta table in lakehouse '{shortcuts_lakehouse_name}' to '{destination_lakehouse}'")
    print(f"#############################################################################################################################################")

    try:
        d365fotable_to_base(
            shortcuts_lakehouse_name = shortcuts_lakehouse_name,
            d365fo_shortcut_table = d365fo_shortcut_table,
            d365fo_default_write_behaviour= d365fo_default_write_behaviour,
            d365fo_default_recreate_flag= d365fo_default_recreate_flag,
            d365fo_default_keys=d365fo_default_keys,
            d365fo_default_sequence_columns=d365fo_default_keys,
            default_exceptions = default_exceptions
        )
    except Exception as e:
        # Collect tables with processing errors, for later retry
        tableProcessErrorTables.append(f"{d365fo_shortcut_table}")
        tableProcessErrors.append(f"{d365fo_shortcut_table}: {e}")

if len(tableProcessErrorTables) == 0:   
    print(f"Completed")
else:
    # Retry processing once for tables that failed
    print(f"Error Tables: {tableProcessErrorTables}")
    print(f"Errors: {tableProcessErrors}")

    for d365fo_shortcut_table in tableProcessErrorTables:
    
        print(f"#############################################################################################################################################")
        print(f"Retrying processing of {d365fo_shortcut_table} from d365fo shortcut delta table in lakehouse '{shortcuts_lakehouse_name}' to '{destination_lakehouse}'")
        print(f"#############################################################################################################################################")

        d365fotable_to_base(
            shortcuts_lakehouse_name = shortcuts_lakehouse_name,
            d365fo_shortcut_table = d365fo_shortcut_table,
            d365fo_default_write_behaviour= d365fo_default_write_behaviour,
            d365fo_default_recreate_flag= d365fo_default_recreate_flag,
            d365fo_default_keys=d365fo_default_keys,
            d365fo_default_sequence_columns=d365fo_default_keys,
            default_exceptions = default_exceptions
        )

    print(f"Completed after retries.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
