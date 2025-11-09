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

# # Project Investment Budget Fact

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Notebook specific parameters
fact_name: str = 'ProjectInvestmentBudget'      # Set the name of the fact. Table name is automatically prefixed with fact_type variable
fact_type: str = 'Fact_'                         # Options: Fact / Bridge

# Optional input parameters
destination_lakehouse: str = 'Curated'          # Name of the destination lakehouse. I.e. Curated, Gold etc.
write_pattern: str = 'overwrite'                # Options: overwrite, append. Default: overwrite
recreate: bool = False                          # Options:True, False. Default: False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df to rename åøæ columns to make them SparkSQL compatible.
df = spark.sql("SELECT * FROM Base.sharepointsitepowerbi_investeringsprojektbudgetter")
df = df.withColumnRenamed("restbudget_tidl_år", "restbudget_tidl_aar").withColumnRenamed("indeks_regul_tidl_år", "indeks_regul_tidl_aar").withColumnRenamed("overf_anlæg_auto_tidl_år", "overf_anlaeg_auto_tidl_aar").withColumnRenamed("overf_til_anlæg_93_tidl_år", "overf_til_anlaeg_93_tidl_aar").withColumnRenamed("anl_ført_4xxxx_auto", "anl_foert_4xxxx_auto").withColumnRenamed("anl_ført_auto", "anl_foert_auto").withColumnRenamed("anl_ført_4xxxx_gr_93", "anl_foert_4xxxx_gr_93").withColumnRenamed("anl_ført_gr_93", "anl_foert_gr_93")

df.createOrReplaceTempView("temp_project_investment_budget_columnsfixed_df")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_project_investment_budget_df AS
# MAGIC 
# MAGIC SELECT
# MAGIC     -- Foreign Keys
# MAGIC      CONCAT(i.projektnr, '-', UPPER(proj.dataareaid))                 AS project_key
# MAGIC     ,dimval.afdelingvalue                                            AS department_key
# MAGIC     ,dimval.baerervalue                                              AS area_key
# MAGIC     ,dimval.formaalvalue                                             AS business_unit_key
# MAGIC     ,CONCAT(dimval.ejendomvalue, '-', UPPER(proj.dataareaid))         AS property_key
# MAGIC 
# MAGIC     -- Attributes
# MAGIC     ,CASE 
# MAGIC         WHEN TRIM(i.`bemærkninger`) = '' THEN 'Tomt'
# MAGIC         ELSE TRIM(i.`bemærkninger`)               
# MAGIC     END                                             AS comment
# MAGIC     ,i.`budgetår`                                   AS budget_year
# MAGIC 
# MAGIC     -- Measures
# MAGIC     /* Note: Amounts in excel-file are written in thousands */
# MAGIC     ,COALESCE(CAST(i.restbudget_tidl_aar AS INT),0) * 1000              AS InvestmentRemaningBudgetPreviousYearsAmount
# MAGIC     ,COALESCE(CAST(i.opr_bevilling AS INT),0) * 1000                   AS InvestmentBudgetAmount
# MAGIC     ,COALESCE(CAST(i.flytte_projektreserver AS INT),0) * 1000          AS InvestmentTransferReserversAmount
# MAGIC     ,COALESCE(CAST(i.reserver AS INT),0) * 1000                        AS InvestmentReservesAmount
# MAGIC     ,COALESCE(CAST(i.indeks_regul_tidl_aar AS INT),0) * 1000            AS InvestmentIndexRegulationPreviousYear
# MAGIC     ,COALESCE(CAST(i.indeksregul_daa AS INT),0) * 1000                 AS InvestmentIndexRegulationDaa
# MAGIC     ,COALESCE(CAST(i.seneste_forventning_ej_til_best AS INT),0) * 1000 AS InvestmentEstimateAmount
# MAGIC     ,COALESCE(CAST(i.overf_anlaeg_auto_tidl_aar AS INT),0) * 1000        AS InvestmentTransferAssetAutoPreviousYear
# MAGIC     ,COALESCE(CAST(i.overf_til_anlaeg_93_tidl_aar AS INT),0) * 1000      AS InvestmentTransferToAsset93PreviousYear
# MAGIC     ,COALESCE(CAST(i.anl_foert_4xxxx_auto AS INT),0) * 1000             AS InvestmentAssetTransfer4xxxxAuto
# MAGIC     ,COALESCE(CAST(i.anl_foert_auto AS INT),0) * 1000                   AS InvestmentAssetTransferAuto
# MAGIC     ,COALESCE(CAST(i.anl_foert_4xxxx_gr_93 AS INT),0) * 1000            AS InvestmentAssetTransfer4xxxxGroup93
# MAGIC     ,COALESCE(CAST(i.anl_foert_gr_93 AS INT),0) * 1000                  AS InvestmentAssetTransferGroup93
# MAGIC 
# MAGIC FROM temp_project_investment_budget_columnsfixed_df i
# MAGIC LEFT JOIN
# MAGIC     Base.d365fo_projtable proj
# MAGIC         ON i.projektnr = proj.projid
# MAGIC         and upper(proj.dataareaid) = 'BH'
# MAGIC LEFT JOIN
# MAGIC     Base.d365fo_dimensionattributevalueset AS dimval
# MAGIC         ON proj.defaultdimension = dimval.recid
# MAGIC 
# MAGIC WHERE i.projektnr <> 'Tomt'
# MAGIC AND dimval.lh_is_deleted = false

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_df = spark.table('temp_project_investment_budget_df')

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
