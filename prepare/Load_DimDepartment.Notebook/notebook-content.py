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
dimension_name: str = 'Department'          # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_department_df AS 
# MAGIC 
# MAGIC SELECT
# MAGIC      dav.displayvalue                           AS department_key
# MAGIC     ,dav.displayvalue                           AS department_no
# MAGIC     ,INT(dav.displayvalue)                      AS department_no_sort
# MAGIC     ,dpt.name                                   AS department
# MAGIC     ,concat(dav.displayvalue, '-',dpt.name)     AS department_no_and_department
# MAGIC     ,CASE
# MAGIC         WHEN LENGTH(dav.displayvalue) = 4 THEN SUBSTRING(dav.displayvalue,0,3)
# MAGIC         ELSE NULL
# MAGIC      END                                        AS department_parent_no
# MAGIC     ,CASE
# MAGIC         WHEN LENGTH(dav.displayvalue) = 4 THEN CONCAT(SUBSTRING(dav.displayvalue,0,3), '*-', parent.name)
# MAGIC         WHEN ISNOTNULL(child.displayvalue) THEN CONCAT(dav.displayvalue, '*-', dpt.name)
# MAGIC         WHEN LENGTH(dav.displayvalue) = 3 THEN CONCAT(dav.displayvalue, '-', dpt.name)
# MAGIC         ELSE NULL
# MAGIC      END                                        AS department_level_1_no_and_department
# MAGIC FROM Base.d365fo_dimensionattributevalue AS dav
# MAGIC JOIN Base.d365fo_dimensionattribute AS da
# MAGIC     ON dav.dimensionattribute = da.recid
# MAGIC LEFT JOIN Base.d365fo_omoperatingunit AS oou
# MAGIC     ON dav.entityinstance= oou.recid
# MAGIC LEFT JOIN Base.d365fo_dirpartytable AS dpt
# MAGIC   ON oou.recid = dpt.recid
# MAGIC   AND oou.partition = dpt.partition
# MAGIC LEFT JOIN 
# MAGIC     (
# MAGIC     Select
# MAGIC          davp.displayvalue
# MAGIC         ,dptp.name
# MAGIC     FROM Base.d365fo_dimensionattributevalue davp
# MAGIC     JOIN Base.d365fo_dimensionattribute AS dap
# MAGIC         ON davp.dimensionattribute = dap.recid
# MAGIC     LEFT JOIN Base.d365fo_omoperatingunit AS ooup
# MAGIC         ON davp.entityinstance= ooup.recid
# MAGIC     LEFT JOIN Base.d365fo_dirpartytable AS dptp
# MAGIC         ON ooup.recid = dptp.recid
# MAGIC         AND ooup.partition = dptp.partition
# MAGIC     WHERE
# MAGIC         dap.name = 'Afdeling'
# MAGIC     ) AS parent
# MAGIC         ON
# MAGIC             CASE
# MAGIC                 WHEN LENGTH(dav.displayvalue) = 4 THEN SUBSTRING(dav.displayvalue,0,3)
# MAGIC                 ELSE NULL
# MAGIC             END            = parent.displayvalue
# MAGIC LEFT JOIN 
# MAGIC     (
# MAGIC     Select
# MAGIC          davp.displayvalue
# MAGIC         ,dptp.name
# MAGIC     FROM Base.d365fo_dimensionattributevalue davp
# MAGIC     JOIN Base.d365fo_dimensionattribute AS dap
# MAGIC         ON davp.dimensionattribute = dap.recid
# MAGIC     LEFT JOIN Base.d365fo_omoperatingunit AS ooup
# MAGIC         ON davp.entityinstance= ooup.recid
# MAGIC     LEFT JOIN Base.d365fo_dirpartytable AS dptp
# MAGIC         ON ooup.recid = dptp.recid
# MAGIC         AND ooup.partition = dptp.partition
# MAGIC     WHERE
# MAGIC         dap.name = 'Afdeling'
# MAGIC     ) AS child
# MAGIC         ON
# MAGIC             CASE
# MAGIC                 WHEN LENGTH(child.displayvalue) = 4 THEN SUBSTRING(child.displayvalue,0,3)
# MAGIC                 ELSE NULL
# MAGIC             END            = dav.displayvalue
# MAGIC WHERE
# MAGIC     da.name = 'Afdeling'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_department_df')

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
