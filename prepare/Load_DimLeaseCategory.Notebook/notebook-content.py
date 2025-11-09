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
dimension_name: str = 'Lease_Category'          # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_d365fo_dirpartytable_clean AS
# MAGIC 
# MAGIC --Inital cleaning of inconsistent dash delimiter: '–' != '-'
# MAGIC SELECT
# MAGIC     recid
# MAGIC     ,partition
# MAGIC     ,REPLACE(name,'–','-') as name
# MAGIC FROM Base.d365fo_dirpartytable

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_lease_category_df AS
# MAGIC 
# MAGIC SELECT
# MAGIC      davp.displayvalue                          AS lease_category_key
# MAGIC     ,davp.displayvalue                          AS lease_category_code
# MAGIC     ,dptp.name                                  AS lease_category_description
# MAGIC     ,TRIM(substring_index(dptp.name, '-', 1))   AS lease_category_level_1
# MAGIC     ,TRIM(substring_index(substring_index(dptp.name, '-', -1),'(',1))
# MAGIC                                                 AS lease_category_level_2
# MAGIC     ,CASE
# MAGIC         WHEN LOCATE('(',dptp.name)>0 THEN TRIM(substring_index(substring_index(dptp.name, '(', -1),')',1))
# MAGIC         ELSE 'Tomt'
# MAGIC     END                                         AS lease_category_level_3
# MAGIC     ,CONCAT(davp.displayvalue,' - ',dptp.name)  AS lease_category_code_and_description
# MAGIC 
# MAGIC FROM Base.d365fo_dimensionattributevalue davp
# MAGIC JOIN Base.d365fo_dimensionattribute AS dap
# MAGIC     ON davp.dimensionattribute = dap.recid
# MAGIC LEFT JOIN Base.d365fo_omoperatingunit AS ooup
# MAGIC     ON davp.entityinstance= ooup.recid
# MAGIC LEFT JOIN temp_d365fo_dirpartytable_clean AS dptp
# MAGIC     ON ooup.recid = dptp.recid
# MAGIC     AND ooup.partition = dptp.partition
# MAGIC WHERE
# MAGIC     dap.name = 'Udlejning'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_lease_category_df')

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
