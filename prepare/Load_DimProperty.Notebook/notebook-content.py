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

# # Property dimension
# This notebook creates the Property dimension.

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Notebook specific parameters
dimension_name: str = 'Property'          # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

# Optional input parameters
destination_lakehouse: str = 'Curated'  # Name of the destination lakehouse. I.e. Curated, Gold etc.
write_pattern: str = 'SCD1'             # Options: SCD1, SDC2. Default: SCD1
full_load: bool = True                 # Options: True, False. Default: False
recreate: bool = False                  # Options:True, False. Default: False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_property_df AS
# MAGIC 
# MAGIC SELECT
# MAGIC     CONCAT(property.buildingid, '-', UPPER(property.dataareaid)) AS property_key,
# MAGIC     property.buildingid AS property_no,
# MAGIC     property.txt AS property_name,
# MAGIC     propertytype.txt AS property_type,
# MAGIC     propertygroup.buildinggroupid AS property_location,
# MAGIC     CASE
# MAGIC         WHEN property.buildingstatus = 1 THEN 'Aktiv'
# MAGIC         ELSE 'Ikke Aktiv' 
# MAGIC     END AS property_status
# MAGIC FROM Base.d365fo_flxbuildingtable AS property
# MAGIC LEFT JOIN Base.d365fo_flxbuildingtypetable AS propertytype
# MAGIC     ON property.buidingtypeid = propertytype.buildingtypeid
# MAGIC         and property.dataareaid = propertytype.dataareaid
# MAGIC LEFT JOIN Base.d365fo_flxbuildinggrouptable AS propertygroup
# MAGIC     ON property.buildinggroupid = propertygroup.buildinggroupid
# MAGIC         and property.dataareaid = propertygroup.dataareaid

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_property_df')

written_df = load_dimension(
    df = dim_df, 
    destination_lakehouse = destination_lakehouse,
    destination_table = dimension_name, 
    write_pattern = write_pattern,
    full_load = full_load,
    recreate = recreate
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
