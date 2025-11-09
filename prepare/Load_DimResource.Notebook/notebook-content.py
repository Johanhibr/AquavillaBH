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

# # Resource dimension
# This notebook creates a Resource dimension.

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Notebook specific parameters
dimension_name: str = 'Resource'           # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_resource_df AS 
# MAGIC 
# MAGIC SELECT
# MAGIC      CONCAT(wrk.wrkctrid, '-', UPPER(dataareaid))                          AS resource_key
# MAGIC     ,wrk.wrkctrid                          AS resource_no
# MAGIC     ,wrk.name                              AS resource_name
# MAGIC     ,CONCAT(wrk.wrkctrid, ' - ', wrk.name) AS resource_no_and_name
# MAGIC     ,CASE
# MAGIC         WHEN wrk.wrkctrtype = 0 THEN 'Leverandør'
# MAGIC         WHEN wrk.wrkctrtype = 1 THEN 'Personale'
# MAGIC         WHEN wrk.wrkctrtype = 2 THEN 'Maskine'
# MAGIC         WHEN wrk.wrkctrtype = 3 THEN 'Værktøj'
# MAGIC         WHEN wrk.wrkctrtype = 4 THEN 'Lokation'
# MAGIC         WHEN wrk.wrkctrtype = 5 THEN 'Ressourcegruppe'
# MAGIC         WHEN wrk.wrkctrtype = 6 THEN 'Facilitet'
# MAGIC         ELSE '?'
# MAGIC     END                                    AS resource_type
# MAGIC FROM
# MAGIC     Base.d365fo_wrkctrtable AS wrk


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_resource_df')

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
