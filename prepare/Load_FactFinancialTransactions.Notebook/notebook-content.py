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
fact_name: str = 'Financial_Transactions'          # Set the name of the fact. Table name is automatically prefixed with fact_type variable
fact_type: str = 'Fact_'                         # Options: Fact / Bridge

# Optional input parameters
destination_lakehouse: str = 'Curated'          # Name of the destination lakehouse. I.e. Curated, Gold etc.
write_pattern: str = 'overwrite'                # Options: overwrite, append. Default: overwrite
recreate: bool = False                           # Options:True, False. Default: False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_financial_transactions_df AS
# MAGIC 
# MAGIC SELECT
# MAGIC      CASE
# MAGIC         WHEN len(MONTH(gje.accountingdate)) = 1 and len(DAY(gje.accountingdate)) = 1 THEN
# MAGIC             CAST(CONCAT(YEAR(gje.accountingdate), 0, MONTH(gje.accountingdate), 0,DAY(gje.accountingdate)) AS INT)
# MAGIC         WHEN len(MONTH(gje.accountingdate)) = 1 and len(DAY(gje.accountingdate)) = 2 THEN
# MAGIC             CAST(CONCAT(YEAR(gje.accountingdate), 0, MONTH(gje.accountingdate), DAY(gje.accountingdate)) AS INT)
# MAGIC         WHEN len(MONTH(gje.accountingdate)) = 2 and len(DAY(gje.accountingdate)) = 1 THEN
# MAGIC             CAST(CONCAT(YEAR(gje.accountingdate), MONTH(gje.accountingdate), 0,DAY(gje.accountingdate)) AS INT)
# MAGIC         ELSE
# MAGIC             CAST(CONCAT(YEAR(gje.accountingdate), MONTH(gje.accountingdate), DAY(gje.accountingdate)) AS INT)
# MAGIC      END                                                                                    AS calendar_key
# MAGIC 
# MAGIC     ,davc.afdelingvalue                                                                     AS department_key
# MAGIC     ,davc.baerervalue                                                                       AS area_key
# MAGIC     ,davc.formaalvalue                                                                      AS business_unit_key
# MAGIC     ,davc.udlejningvalue                                                                    AS lease_category_key
# MAGIC     ,CONCAT(davc.ejendomvalue, '-', UPPER(gje.subledgervoucherdataareaid))                  AS property_key
# MAGIC     ,CONCAT(davc.projektvalue, '-', UPPER(gje.subledgervoucherdataareaid))                  AS project_key
# MAGIC     ,CONCAT(davc.anlaegvalue, '-', UPPER(gje.subledgervoucherdataareaid))                   AS asset_key
# MAGIC     ,ma.mainaccountid                                                                       AS financial_account_key
# MAGIC     ,gjae.transactioncurrencycode                                                           AS currency_key
# MAGIC     ,UPPER(gje.subledgervoucherdataareaid)                                                  AS company_key
# MAGIC     ,gjae.text                                                                              AS transaction_text
# MAGIC     ,gje.subledgervoucher                                                                   AS voucher_number 
# MAGIC     ,stringmap.value                                                                        AS posting_type
# MAGIC     ,gjae.transactioncurrencyamount                                                         AS transaction_currency_amount
# MAGIC     ,gjae.accountingcurrencyamount                                                          AS accounting_currency_amount
# MAGIC FROM Base.d365fo_generaljournalaccountentry AS gjae
# MAGIC     LEFT JOIN Base.d365fo_generaljournalentry AS gje
# MAGIC         ON gjae.generaljournalentry = gje.recid
# MAGIC         AND gjae.partition = gje.partition
# MAGIC     LEFT JOIN Base.d365fo_dimensionattributevaluecombination AS davc
# MAGIC         ON gjae.ledgerdimension = davc.recid
# MAGIC         AND gjae.partition = davc.partition
# MAGIC     LEFT JOIN Base.d365fo_mainaccount AS ma
# MAGIC         ON gjae.mainaccount = ma.recid
# MAGIC         AND gjae.partition = ma.partition
# MAGIC     LEFT JOIN Base.d365fo_stringmap AS stringmap
# MAGIC         ON gjae.postingtype = stringmap.displayorder - 1
# MAGIC 		AND stringmap.objecttypecode = 'mserp_generaljournalaccountentrybientity'
# MAGIC 		AND stringmap.attributename = 'mserp_postingtype'
# MAGIC WHERE
# MAGIC     gjae.postingtype <> 19 /* Excludes all transactions of with field "Bogføringstype" of "Overførsel af ultimo- og primoposteringer". 
# MAGIC     This is to avoid that year-ending postings mess up the financial reports for income statement and balance sheet. */

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_df = spark.table('temp_financial_transactions_df')

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
