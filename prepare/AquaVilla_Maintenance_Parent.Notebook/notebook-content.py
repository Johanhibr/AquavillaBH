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
# # AquaVilla - Maintenance Parent
# </center>
# 
# Finds all tables in all lake houses and runs standard maintenance, e.g. compact and vacuum.

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delta_tables = get_workspace_delta_tables()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DAG = {
    "activities": [
        {
            "name": f"Run_Maintenance_{table['name']}",
            "path": "AquaVilla_Maintenance",
            "timeoutPerCellInSeconds": 10 * 60,
            "args": {
                "delta_table_path": table["table_path"],
                "vacuum_retention_days": 30
            }
        } for table in delta_tables
    ],
    "concurrency": 5
}

output = mssparkutils.notebook.runMultiple(DAG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
