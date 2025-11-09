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

# # DimBuilding & DimProperty Bridge
# This notebook creates a bridge linking DimBuilding and DimProperty together on their FlexPropertyNumber.

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Notebook specific parameters
fact_name: str = 'BuildingProperty'                # Set the name of the fact. Table name is automatically prefixed with fact_type variable
fact_type: str = 'Bridge_'                         # Options: Fact / Bridge

# Optional input parameters
destination_lakehouse: str = 'Curated'          # Name of the destination lakehouse. I.e. Curated, Gold etc.
write_pattern: str = 'overwrite'                # Options: overwrite, append. Default: overwrite
recreate: bool = False                          # Options:True, False. Default: False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_building_property_df AS
# MAGIC 
# MAGIC SELECT
# MAGIC 	building_key as building_key
# MAGIC 	,property_key as property_key
# MAGIC FROM Curated.dim_building db
# MAGIC INNER JOIN Curated.dim_property dp
# MAGIC 	ON db.building_flex_property_nr = dp.property_no
# MAGIC 	and dp.lh_is_deleted = 0
# MAGIC WHERE db.lh_is_deleted = 0


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_df = spark.table('temp_building_property_df')

written_df = load_fact(
    df = fact_df, 
    destination_lakehouse = destination_lakehouse,
    destination_table = fact_name,
    destination_table_prefix = fact_type, 
    write_pattern = write_pattern,
    recreate = recreate
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
