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
dimension_name: str = 'Project_Category'          # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_project_category_df AS 
# MAGIC 
# MAGIC SELECT
# MAGIC      CONCAT(UPPER(pc.categoryid), '-', UPPER(dataareaid))      AS project_category_key
# MAGIC     ,pc.categoryid      AS project_category_identifier
# MAGIC     ,pc.name            AS project_category_name
# MAGIC     ,pc.categorygroupid AS project_category_group_name
# MAGIC     ,CASE
# MAGIC         WHEN LEFT(pc.name, 2) = 40 THEN 'Vedligehold'
# MAGIC         WHEN LEFT(pc.name, 2) = 41 THEN 'Administration'
# MAGIC         WHEN LEFT(pc.name, 2) = 42 THEN 'Personale'
# MAGIC         ELSE 'Øvrige'
# MAGIC     END AS project_category_cost_type
# MAGIC 
# MAGIC FROM Base.d365fo_projcategory AS pc

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_project_category_df')

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
