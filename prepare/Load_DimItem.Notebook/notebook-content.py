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

# # Product dimension
# This notebook creates a product dimension joining data from both the product tabel and the product group table.

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Notebook specific parameters
dimension_name: str = 'Item'           # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_item_df AS
# MAGIC 
# MAGIC SELECT
# MAGIC      CONCAT(it.itemid, '-', UPPER(it.dataareaid))                  AS item_key
# MAGIC     ,it.itemid                  AS item_number
# MAGIC     ,ecpt.name                  AS item_name
# MAGIC     ,iig.name                   AS item_group_name
# MAGIC     ,CASE
# MAGIC         WHEN    iig.name LIKE '%Affaldssug%' OR iig.name LIKE '%Metrobidrag%'
# MAGIC             THEN 'Opkrævning, Andet'
# MAGIC         ELSE 'Lejeopkrævninger'
# MAGIC     END                         AS item_collection_group_name
# MAGIC FROM
# MAGIC     Base.d365fo_inventtable AS it
# MAGIC LEFT JOIN 
# MAGIC     Base.d365fo_ecoresproducttranslation AS ecpt
# MAGIC     ON it.product = ecpt.product
# MAGIC LEFT JOIN
# MAGIC     Base.d365fo_inventitemgroupitem AS iigi
# MAGIC     ON it.itemid = iigi.itemid
# MAGIC LEFT JOIN
# MAGIC     Base.d365fo_inventitemgroup AS iig
# MAGIC     ON iigi.itemgroupid = iig.itemgroupid
# MAGIC     AND it.dataareaid = iig.dataareaid


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_item_df')

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
