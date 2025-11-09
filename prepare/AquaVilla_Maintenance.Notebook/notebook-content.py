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
# # AquaVilla - Maintenance
# </center>
# 
# Runs optimize and vacuum.

# CELL ********************

delta_table_path: str
vacuum_retention_days: int = 30

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta import DeltaTable
import pyspark.sql.functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delta_table = DeltaTable.forPath(
    spark,
    delta_table_path
)

compaction_result = delta_table.optimize().executeCompaction()
vacuum_stats = delta_table.vacuum(
    retentionHours=vacuum_retention_days * 24,
    dryRun=False
)

try: 
    display(compaction_result.select("*", F.col("metrics.*")))
    display(vacuum_stats)
except:
    print("Could not display maintenance results.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
