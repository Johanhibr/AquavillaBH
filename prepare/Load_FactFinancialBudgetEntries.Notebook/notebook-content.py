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
# META         },
# META         {
# META           "id": "c83ca59e-f468-4305-a323-a324e0fb54b4"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Financial Budget Entries Fact

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Notebook specific parameters
fact_name: str = 'Financial_Budget_Entries'       # Set the name of the fact. Table name is automatically prefixed with fact_type variable
fact_type: str = 'Fact_'                         # Options: Fact / Bridge

# Optional input parameters
destination_lakehouse: str = 'Curated'          # Name of the destination lakehouse. I.e. Curated, Gold etc.
write_pattern: str = 'overwrite'                # Options: overwrite, append. Default: overwrite
recreate: bool = True                           # Options:True, False. Default: False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_financial_budget_entries_df AS
# MAGIC 
# MAGIC SELECT
# MAGIC     -- Foreign Keys
# MAGIC      CAST(date_format(btl.date, 'yMMdd') AS INT)    AS calendar_key
# MAGIC     ,davc.afdelingvalue                             AS department_key
# MAGIC     ,davc.formaalvalue                              AS business_unit_key
# MAGIC     ,ma.mainaccountid                               AS financial_account_key
# MAGIC     ,CONCAT(bth.budgetmodelid,'-',bth.budgettransactioncode, '-', bth.budgetmodeldataareaid) AS budget_key
# MAGIC     ,UPPER(bth.budgetmodeldataareaid)               as company_key
# MAGIC     ,CONCAT(davc.projektvalue, '-', UPPER(bth.budgetmodeldataareaid))  as project_key
# MAGIC     ,CONCAT(UPPER(davc.kategorivalue), '-', UPPER(bth.budgetmodeldataareaid)) as project_category_key
# MAGIC     -- Atributes
# MAGIC     ,btl.comment                                    AS comment
# MAGIC 
# MAGIC     -- Measures
# MAGIC     ,btl.accountingcurrencyamount                   AS accounting_currency_amount
# MAGIC     
# MAGIC FROM Base.d365fo_budgettransactionline btl
# MAGIC LEFT JOIN
# MAGIC     Base.d365fo_dimensionattributevaluecombination davc
# MAGIC         ON btl.ledgerdimension = davc.recid
# MAGIC LEFT JOIN
# MAGIC     Base.d365fo_budgettransactionheader bth
# MAGIC         ON btl.budgettransactionheader = bth.recid
# MAGIC LEFT JOIN
# MAGIC     Base.d365fo_mainaccount ma
# MAGIC         ON davc.mainaccount = ma.recid

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_df = spark.table('temp_financial_budget_entries_df')

written_df = load_fact(
    df = fact_df, 
    destination_lakehouse = destination_lakehouse,
    destination_table = fact_name,
    destination_table_prefix = fact_type, 
    write_pattern = write_pattern,
    recreate = recreate
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
