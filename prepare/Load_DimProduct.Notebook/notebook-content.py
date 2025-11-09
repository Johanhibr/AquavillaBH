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
# This notebook creates a product dimension.

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Notebook specific parameters
dimension_name: str = 'Product'           # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_product_df AS
# MAGIC 
# MAGIC SELECT 
# MAGIC      p.Id                                               AS product_key
# MAGIC     ,p.Id                                               AS product_code
# MAGIC     ,p.Navn                                             AS product_name
# MAGIC     ,COALESCE(t.Type, 'Ikke mappet i Excel-fil')        AS product_type
# MAGIC     ,COALESCE(t.Kategori, 'Ikke mappet i Excel-fil')    AS product_category
# MAGIC     ,p.Bil                                              AS product_allows_car_flag
# MAGIC     ,p.Motorcykel                                       AS product_allows_motorcycle_flag
# MAGIC     ,p.Beboer                                           AS product_resident_flag
# MAGIC     ,p.Erhverv                                          AS product_business_flag
# MAGIC     ,p.Abonnement                                       AS product_subscription_flag
# MAGIC     ,p.Aktiv                                            AS product_active_flag
# MAGIC FROM
# MAGIC     Base.parkincph_produkt AS p
# MAGIC LEFT JOIN
# MAGIC     Base.sharepointsitepowerbi_parkincph_type AS t
# MAGIC     ON p.Navn = t.Produkt
# MAGIC WHERE p.lh_is_current = 1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_product_df')

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
