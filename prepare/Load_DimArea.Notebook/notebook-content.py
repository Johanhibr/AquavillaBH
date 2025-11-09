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
# META       "default_lakehouse_workspace_id": "4358b870-4392-403f-8b26-dc95456769dd",
# META       "known_lakehouses": [
# META         {
# META           "id": "2215fdc2-5d05-4516-882c-92b6c0bcd831"
# META         }
# META       ]
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
dimension_name: str = 'Area'          # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

# Optional input parameters
destination_lakehouse: str = 'Curated'  # Name of the destination lakehouse. I.e. Curated, Gold etc.
write_pattern: str = 'SCD1'             # Options: SCD1, SDC2. Default: SCD1
full_load: bool = True                 # Options: True, False. Default: False
recreate: bool = False  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_area_df AS 
# MAGIC 
# MAGIC SELECT
# MAGIC      dav.displayvalue                             AS area_key
# MAGIC     ,dav.displayvalue                             AS area_code
# MAGIC     ,dpt.name                                     AS area_description
# MAGIC     ,concat(dav.displayvalue, '-',dpt.name)       AS area_code_and_description
# MAGIC     -- Hierarchy level 1
# MAGIC     ,CASE
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 0 AND 999 THEN '0100'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 1000 AND 1999 THEN '1000'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 2000 AND 2999 THEN '2000'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 3000 AND 3999 THEN '3000'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 4000 AND 4999 THEN '4000'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 5000 AND 5999 THEN '5000'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 6000 AND 6999 THEN '6000'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 7000 AND 7999 THEN '7000'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 8000 AND 8999 THEN '8000'
# MAGIC          ELSE NULL
# MAGIC          END AS area_hierarchy_level_1_code
# MAGIC     ,CASE
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 0 AND 999 THEN 'Generelt'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 1000 AND 1999 THEN 'Ørestad'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 2000 AND 2999 THEN 'Sydhavnen'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 3000 AND 3999 THEN 'Inderhavnen'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 4000 AND 4999 THEN 'Søndre Frihavn'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 5000 AND 5999 THEN 'Indre Nordhavn'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 6000 AND 6999 THEN 'Nordhavnen'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 7000 AND 7999 THEN 'Ydre Nordhavn (opfyldsområdet)'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 8000 AND 8999 THEN 'Øst-havnen'
# MAGIC          ELSE NULL
# MAGIC          END AS area_hierarchy_level_1_description
# MAGIC 
# MAGIC        -- Hierarchy level 2
# MAGIC        ,dav.displayvalue                          AS area_hierarchy_level_2_code
# MAGIC        ,dpt.name                                  AS area_hierarchy_level_2_description
# MAGIC 
# MAGIC        -- Parking hierarchy level 1
# MAGIC     ,CASE
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) IN (4400) THEN '4400'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 1000 AND 1999 THEN '1000'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 5000 AND 7999 THEN '6000'
# MAGIC          ELSE '0100'
# MAGIC          END AS area_parking_hierarchy_level_1_code
# MAGIC     ,CASE
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) IN (4400) THEN 'Marmormolen'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 1000 AND 1999 THEN 'Ørestad'
# MAGIC          WHEN TRY_CAST(dav.displayvalue AS INT) BETWEEN 5000 AND 7999 THEN 'Nordhavn'
# MAGIC          ELSE 'Øvrige'
# MAGIC          END AS area_parking_hierarchy_level_1_description
# MAGIC 
# MAGIC        -- Parking hierarchy level 2
# MAGIC        ,dav.displayvalue                          AS area_parking_hierarchy_level_2_code
# MAGIC        ,dpt.name                                  AS area_parking_hierarchy_level_2_description
# MAGIC 
# MAGIC FROM Base.d365fo_dimensionattributevalue AS dav
# MAGIC JOIN Base.d365fo_dimensionattribute AS da
# MAGIC     ON dav.dimensionattribute = da.recid
# MAGIC LEFT JOIN Base.d365fo_omoperatingunit AS oou
# MAGIC   ON dav.entityinstance= oou.recid
# MAGIC LEFT JOIN Base.d365fo_dirpartytable AS dpt
# MAGIC   ON oou.recid = dpt.recid
# MAGIC   AND oou.partition = dpt.partition
# MAGIC WHERE da.name = 'Baerer'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_area_df')

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
