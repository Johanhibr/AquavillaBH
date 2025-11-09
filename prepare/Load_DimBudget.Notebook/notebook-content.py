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
dimension_name: str = 'Budget'          # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

# Optional input parameters
destination_lakehouse: str = 'Curated'  # Name of the destination lakehouse. I.e. Curated, Gold etc.
write_pattern: str = 'SCD1'             # Options: SCD1, SDC2. Default: SCD1
full_load: bool = True                 # Options: True, False. Default: False
recreate: bool = False                   # Options: True, False. Default: False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_budget_df AS 
# MAGIC 
# MAGIC SELECT DISTINCT
# MAGIC      CONCAT(bth.budgetmodelid,'-',bth.budgettransactioncode, '-', UPPER(bth.budgetmodeldataareaid))               AS budget_key
# MAGIC     ,bth.budgetmodelid               AS budget_model
# MAGIC     ,btc.name                        AS budget_transaction_code
# MAGIC FROM Base.d365fo_budgettransactionheader bth
# MAGIC LEFT JOIN Base.d365fo_budgettransactioncode btc
# MAGIC     ON bth.budgettransactioncode = btc.recid

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_budget_df')

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
