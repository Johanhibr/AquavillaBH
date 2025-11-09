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

# # Lease Contract Transactions fact

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Notebook specific parameters
fact_name: str = 'LeaseContractTransactions'    # Set the name of the fact. Table name is automatically prefixed with fact_type variable
fact_type: str = 'Fact_'                         # Options: Fact / Bridge

# Optional input parameters
destination_lakehouse: str = 'Curated'  # Name of the destination lakehouse. I.e. Curated, Gold etc.
write_pattern: str = 'overwrite'        # Options: overwrite, append. Default: overwrite
recreate: bool = False                   # Options:True, False. Default: False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_lease_contract_transactions_df AS
# MAGIC 
# MAGIC SELECT
# MAGIC     -- Foregin keys
# MAGIC     CASE
# MAGIC         WHEN CAST(clt.accrualdate as DATE) <> '1900-01-01' THEN CAST(date_format(clt.accrualdate, 'yMMdd') as INT)
# MAGIC         ELSE CAST(date_format(clt.datestart, 'yMMdd') as INT)
# MAGIC     END                                             AS calendar_key
# MAGIC     ,CAST(davs.afdelingvalue as STRING)             AS department_key
# MAGIC     ,CAST(davs.baerervalue as INT)                  AS area_key
# MAGIC     ,davs.formaalvalue                              AS business_unit_key
# MAGIC     ,davs.udlejningvalue                            AS lease_category_key
# MAGIC     ,CONCAT(davs.ejendomvalue, '-', UPPER(clt.dataareaid))                              AS property_key
# MAGIC     ,clt.floor                                      AS building_floor_key
# MAGIC     ,CONCAT(clt.contractid, '-', UPPER(clt.dataareaid))                                 AS lease_contract_key
# MAGIC     ,CONCAT(clt.custaccount, '-', UPPER(clt.dataareaid))   AS customer_tenant_key
# MAGIC     ,CONCAT(clt.itemid, '-', UPPER(clt.dataareaid))                                     AS item_key
# MAGIC     ,CONCAT(clt.buildinglease, '-' UPPER(clt.dataareaid))                              AS property_unit_key
# MAGIC     ,UPPER(clt.dataareaid)                          AS company_key
# MAGIC 
# MAGIC     -- Transaction attributes
# MAGIC     ,clt.salesunit                                  AS sales_unit
# MAGIC 
# MAGIC     -- Measures
# MAGIC     ,clt.amountmst                                  AS amount
# MAGIC     ,clt.salesinvoicedqty                           AS quantity
# MAGIC 
# MAGIC FROM 
# MAGIC     Base.d365fo_flxcontractleasetrans clt 
# MAGIC LEFT JOIN
# MAGIC     Base.d365fo_dimensionattributevalueset davs
# MAGIC     ON clt.defaultdimension = davs.recid


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_df = spark.table('temp_lease_contract_transactions_df')

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
