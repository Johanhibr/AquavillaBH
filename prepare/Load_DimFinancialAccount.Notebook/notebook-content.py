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
dimension_name: str = 'Financial_Account'           # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

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

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

data = [("Drift",0),
    ("Indtægt",1),
    ("Udgift",2),
    ("Balance",3),
    ("Aktiv",4),
    ("Passiv",5),
    ("Egenkapital",6),
    ("Sum",7),
    ("Rapportering",8),
    ("Common",9),
  ]

schema = StructType([ 
    StructField("name",StringType(),True), 
    StructField("code",IntegerType(),True), 
  ])
 
DfBusinesslevel1 = spark.createDataFrame(data=data,schema=schema)
DfBusinesslevel1.createOrReplaceTempView("temp_account_type_mapping")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_manual_input_account_hierarchy_cleaned AS 
# MAGIC SELECT 
# MAGIC     *
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC          Hovedkonto                  AS account_no
# MAGIC         ,Niveau1                     AS financial_account_reporting_level1
# MAGIC         ,Niveau2                     AS financial_account_reporting_level2
# MAGIC         ,Niveau1_Sortering           AS financial_account_reporting_level1_sort
# MAGIC         ,Niveau2_Sortering           AS financial_account_reporting_level2_sort
# MAGIC         ,row_number() OVER(PARTITION BY Hovedkonto ORDER BY Hovedkonto) AS rn --Window function to avoid duplicates on account number
# MAGIC     FROM
# MAGIC         Base.sharepointsitepowerbi_finanskontohieraki
# MAGIC )
# MAGIC WHERE rn = 1 -- Avoid duplicates on account number

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_financial_account_df AS
# MAGIC SELECT
# MAGIC      ma.mainaccountid                       AS financial_account_key
# MAGIC     ,ma.mainaccountid                       AS financial_account_number
# MAGIC     ,ma.name                                AS financial_account_name
# MAGIC     ,CONCAT(ma.mainaccountid," ",ma.name)   AS financial_account
# MAGIC     ,CASE
# MAGIC         WHEN ma.mainaccountid <= 49990 THEN 'Resultatopgørelse'
# MAGIC         ELSE 'Balance'
# MAGIC      END                                    AS financial_account_group
# MAGIC     ,atm.name                               AS financial_account_type
# MAGIC     ,ma.type                                AS financial_account_type_sort
# MAGIC     ,mac.accountcategory                    AS financial_account_category
# MAGIC     ,mac.accountcategorydisplayorder        AS financial_account_category_sort
# MAGIC     ,CASE
# MAGIC         WHEN ma.mainaccountid = 15112 THEN 'Beboer'
# MAGIC         WHEN ma.mainaccountid IN (15106, 15110, 15111, 15113, 15114, 20510) THEN 'Erhverv'
# MAGIC         WHEN ma.mainaccountid IN (15101, 15104, 15105) THEN 'Korttid'
# MAGIC         WHEN ma.mainaccountid IN (20110, 20111) THEN 'Salg af el'
# MAGIC         ELSE 'Ikke parkering'
# MAGIC     END                                      AS financial_account_parking_product_type
# MAGIC     ,sharepoint.financial_account_reporting_level1
# MAGIC     ,sharepoint.financial_account_reporting_level2
# MAGIC     ,sharepoint.financial_account_reporting_level1_sort
# MAGIC     ,sharepoint.financial_account_reporting_level2_sort
# MAGIC FROM Base.d365fo_mainaccount AS ma
# MAGIC     LEFT JOIN temp_account_type_mapping AS atm
# MAGIC         ON ma.type = atm.code
# MAGIC     LEFT JOIN Base.d365fo_mainaccountcategory AS mac
# MAGIC         ON ma.accountcategoryref = mac.accountcategoryref
# MAGIC     LEFT JOIN temp_manual_input_account_hierarchy_cleaned sharepoint
# MAGIC         ON ma.mainaccountid = sharepoint.account_no
# MAGIC     LEFT JOIN Base.d365fo_ledgerchartofaccounts AS coa
# MAGIC         ON ma.ledgerchartofaccounts = coa.recid
# MAGIC WHERE coa.name = 'By & Havn'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_financial_account_df')

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
