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

# # Project Transactions Fact
# This notebook creates a fact table holding project transactions.

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Notebook specific parameters
fact_name: str = 'ProjectTransactions'          # Set the name of the fact. Table name is automatically prefixed with fact_type variable
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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_distinct_buildings_df AS
# MAGIC 
# MAGIC SELECT distinct
# MAGIC     combined.ProjectID, combined.BuildingID, proj.dataareaid
# MAGIC FROM (
# MAGIC     SELECT userDefinedFields_ProjektnrEjendomme1 AS ProjectID, id AS BuildingID FROM Base.dalux_buildings
# MAGIC     UNION
# MAGIC     SELECT userDefinedFields_ProjektnrEjendomme2 AS ProjectID, id AS BuildingID FROM Base.dalux_buildings
# MAGIC     UNION
# MAGIC     SELECT userDefinedFields_ProjektnrEjendomme3 AS ProjectID, id AS BuildingID FROM Base.dalux_buildings
# MAGIC ) AS combined
# MAGIC left join Base.d365fo_projpostedtranstable as proj
# MAGIC 	on proj.projid = combined.ProjectID
# MAGIC WHERE ProjectID IS NOT NULL AND ProjectID NOT IN ('N/A', 'Tomt', '')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_project_transactions_df AS
# MAGIC 
# MAGIC SELECT
# MAGIC     -- Foreign Keys
# MAGIC     CAST(date_format(proj.transdate, 'yMMdd') AS INT) AS calendar_key,
# MAGIC     dimval.afdelingvalue AS department_key,
# MAGIC     dimval.baerervalue AS area_key,
# MAGIC     dimval.formaalvalue AS business_unit_key,
# MAGIC     CONCAT(dimval.ejendomvalue, '-', UPPER(proj.dataareaid)) AS property_key,
# MAGIC     CONCAT(proj.projid, '-', UPPER(proj.dataareaid)) AS project_key,
# MAGIC     CONCAT(UPPER(proj.categoryid), '-', UPPER(proj.dataareaid)) AS project_category_key,
# MAGIC     CONCAT(proj.vendoraccount, '-', UPPER(proj.dataareaid)) AS vendor_key,
# MAGIC     CONCAT(buildingproj.BuildingID, '-', UPPER(proj.dataareaid)) AS building_key,
# MAGIC     mainacc.mainaccountid AS financial_account_key,
# MAGIC     CONCAT(wrk.wrkctrid, '-', UPPER(proj.dataareaid)) AS resource_key,
# MAGIC     proj.currencyid AS currency_key,
# MAGIC     UPPER(proj.dataareaid) AS company_key,
# MAGIC 
# MAGIC     -- Transaction Attributes
# MAGIC     CASE
# MAGIC         WHEN proj.projtranstype = 1 THEN 'Gebyr'
# MAGIC         WHEN proj.projtranstype = 2 THEN 'Time'
# MAGIC         WHEN proj.projtranstype = 3 THEN 'Udgift'
# MAGIC         WHEN proj.projtranstype = 4 THEN 'Vare'
# MAGIC         WHEN proj.projtranstype = 5 THEN 'Aconto'
# MAGIC         WHEN proj.projtranstype = 6 THEN 'IGVF'
# MAGIC         WHEN proj.projtranstype = 7 THEN 'Indirekte omkostningskomponent'
# MAGIC         WHEN proj.projtranstype = 8 THEN 'Tilbageholdelse'
# MAGIC         ELSE CONCAT('Ukendt: ', proj.projtranstype)
# MAGIC     END AS transaction_type,
# MAGIC     proj.txt AS description,
# MAGIC     projtrans.voucherjournal AS voucher_number,
# MAGIC 
# MAGIC     -- Measures
# MAGIC     CAST(proj.totalcostamountcur AS DECIMAL(18, 4)) AS project_transactions_cost_amount,
# MAGIC     CAST(proj.qty AS DECIMAL(12, 4)) AS project_transactions_quantity
# MAGIC FROM
# MAGIC     Base.d365fo_projpostedtranstable AS proj
# MAGIC LEFT JOIN
# MAGIC     Base.d365fo_projcosttrans AS projtrans
# MAGIC         ON proj.transidref = projtrans.transid AND
# MAGIC            proj.dataareaid = projtrans.dataareaid
# MAGIC LEFT JOIN
# MAGIC     Base.d365fo_dimensionattributevalueset AS dimval
# MAGIC         ON proj.defaultdimension = dimval.recid
# MAGIC LEFT JOIN
# MAGIC     temp_distinct_buildings_df AS buildingproj
# MAGIC         ON proj.projid = buildingproj.ProjectID AND
# MAGIC           proj.dataareaid = buildingproj.dataareaid
# MAGIC LEFT JOIN
# MAGIC     Base.d365fo_projcategory AS projcat
# MAGIC         ON proj.categoryid = projcat.categoryid AND
# MAGIC            proj.dataareaid = projcat.dataareaid
# MAGIC LEFT JOIN
# MAGIC     (select distinct mainaccountid from Base.d365fo_mainaccount) mainacc 
# MAGIC         ON substring(projcat.name, 1, 5) = mainacc.mainaccountid
# MAGIC LEFT JOIN
# MAGIC     Base.d365fo_wrkctrtable AS wrk
# MAGIC         ON proj.resource = wrk.recid

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_df = spark.table('temp_project_transactions_df')

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
