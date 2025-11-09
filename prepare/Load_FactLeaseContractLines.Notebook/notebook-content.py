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
fact_name: str = 'Lease_Contract_Lines' # Set the name of the fact. Table name is automatically prefixed with fact_type variable
fact_type: str = 'Fact_'                 # Options: Fact / Bridge

# Optional input parameters
destination_lakehouse: str = 'Curated'  # Name of the destination lakehouse. I.e. Curated, Gold etc.
write_pattern: str = 'overwrite'        # Options: overwrite, append. Default: overwrite
recreate: bool = False                   # Options:True, False. Default: False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Manually insert display values of status based on code

# CELL ********************

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

data = [("Kladde",0),
    ("Afventer godkendelse",1),
    ("Aktiv",2),
    ("Suspenderet",3),
    ("Skal udløbe",4),
    ("Udløbet",5),
    ("Opsætningssrelationen for min./maks. til en udgiftspulje",6)
  ]

schema = StructType([ 
    StructField("StatusDiplay",StringType(),True), 
    StructField("StatusCode",IntegerType(),True), 
  ])
 
DfBusinesslevel1 = spark.createDataFrame(data=data,schema=schema)
DfBusinesslevel1.createOrReplaceTempView("DfStatusMapping")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_lease_contract_lines_df AS
# MAGIC SELECT DISTINCT
# MAGIC      MIN(c.calendar_key) OVER (PARTITION BY c.month_code)               AS calendar_key
# MAGIC     ,davs.afdelingvalue                                                 AS department_key
# MAGIC     ,davs.baerervalue                                                   AS area_key
# MAGIC     ,davs.formaalvalue                                                  AS business_unit_key
# MAGIC     ,davs.udlejningvalue                                                AS lease_category_key
# MAGIC     ,CONCAT(davs.ejendomvalue, '-', UPPER(flct.dataareaid))             AS property_key
# MAGIC     ,flc.buildingfloor                                                  AS building_floor_key
# MAGIC     ,CONCAT(flc.buildingunitid, '-', UPPER(flct.dataareaid))            AS property_unit_key
# MAGIC     ,CONCAT(flc.contractid, '-', UPPER(flct.dataareaid))                AS lease_contract_key
# MAGIC     ,CONCAT(flct.custaccount, '-', UPPER(flct.dataareaid))              AS customer_tenant_key
# MAGIC     ,CONCAT(flc.itemid, '-', UPPER(flct.dataareaid))                    AS item_key
# MAGIC     ,UPPER(flc.dataareaid)                                              AS company_key
# MAGIC     ,flc.txt                                                            AS description
# MAGIC     ,flc.unitid                                                         AS unit
# MAGIC     ,flc.qty                                                            AS monthly_quantity
# MAGIC     ,dfsm.StatusDiplay                                                  AS status
# MAGIC     ,CASE
# MAGIC         WHEN flc.billingfrequencyperiodid = 'Engangs'       THEN flc.amount
# MAGIC         WHEN flc.billingfrequencyperiodid = 'Månedlig'      THEN flc.amount             
# MAGIC         WHEN flc.billingfrequencyperiodid = 'Kvartal'       THEN flc.amount/3
# MAGIC         WHEN flc.billingfrequencyperiodid = 'Halvårligt'    THEN flc.amount/6
# MAGIC         WHEN flc.billingfrequencyperiodid = 'Årligt'        THEN flc.amount/12
# MAGIC     END                                                                 AS monthly_amount -- Here we make sure field is always in monthly amount, by dividing amount with number of months in invoicing frquency
# MAGIC     ,CASE
# MAGIC         WHEN flc.unitid = 'm2' THEN flc.qty
# MAGIC         ELSE NULL 
# MAGIC      END                                                                AS monthly_square_meters
# MAGIC FROM Base.d365fo_flxcontractlease AS flc
# MAGIC JOIN Base.d365fo_dimensionattributevalueset AS davs
# MAGIC     ON flc.defaultdimension = davs.recid
# MAGIC JOIN Base.d365fo_flxcontracttable AS flct
# MAGIC     ON flc.contractid = flct.contractid AND
# MAGIC        flc.dataareaid = flct.dataareaid
# MAGIC JOIN DfStatusMapping AS dfsm
# MAGIC     ON flc.status = dfsm.StatusCode
# MAGIC JOIN  Curated.dim_calendar AS c
# MAGIC     ON c.calendar_date BETWEEN
# MAGIC                             flc.fromdate
# MAGIC                             AND CASE
# MAGIC                                     WHEN flc.todate = '1900-01-01 00:00:00' THEN ( SELECT MAX(calendar_date) FROM Curated.dim_calendar  )
# MAGIC                                     ELSE flc.todate
# MAGIC                                 END -- We fill in end date for active contract lines, as active contract lines dont have an end date set.

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_df = spark.table('temp_lease_contract_lines_df')

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
