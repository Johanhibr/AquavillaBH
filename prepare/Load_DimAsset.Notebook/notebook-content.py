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

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Notebook specific parameters
dimension_name: str = 'Asset'          # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

# Optional input parameters
destination_lakehouse: str = 'Curated'  # Name of the destination lakehouse. I.e. Curated, Gold etc.
write_pattern: str = 'SCD1'             # Options: SCD1, SDC2. Default: SCD1
full_load: bool = True                 # Options: True, False. Default: False
recreate: bool = False                  # Options: True, False. Default: False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_asset_df AS
# MAGIC 
# MAGIC SELECT
# MAGIC      CONCAT(at.assetid, '-', UPPER(at.dataareaid))         AS asset_key
# MAGIC     ,at.assetid         AS asset_code
# MAGIC     ,at.name            AS asset_description
# MAGIC     ,CONCAT(at.assetid, ' - ', at.name)     AS asset_code_description
# MAGIC     ,at.sortingid       AS asset_sorting_level_1
# MAGIC     ,at.sortingid2      AS asset_sorting_level_2
# MAGIC     ,at.sortingid3      AS asset_sorting_level_3
# MAGIC     ,davs.formaalvalue      AS default_dimension_business_unit
# MAGIC     ,davs.baerervalue       AS default_dimension_area
# MAGIC     ,davs.afdelingvalue     AS default_dimension_department
# MAGIC FROM Base.d365fo_assettable at
# MAGIC LEFT JOIN Base.d365fo_assetbook ab
# MAGIC     ON at.assetid = ab.assetid
# MAGIC         and at.dataareaid = ab.dataareaid
# MAGIC LEFT JOIN Base.d365fo_dimensionattributevalueset davs
# MAGIC     ON ab.defaultdimension = davs.recid

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_asset_df')

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
