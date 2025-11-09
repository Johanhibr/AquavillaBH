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
dimension_name: str = 'Lease_Contract'          # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_lease_contract_df AS
# MAGIC SELECT
# MAGIC      CONCAT(fct.contractid, '-', UPPER(fct.dataareaid))                     AS lease_contract_key
# MAGIC     ,fct.contractid                     AS lease_contract_no
# MAGIC     ,fct.description                    AS lease_contract_description
# MAGIC     ,fcst.contractstatusid              AS lease_contract_status
# MAGIC     ,fctt.contracttype                  AS lease_contract_type
# MAGIC     ,fpt.periodid                       AS lease_contract_billing_frequenecy
# MAGIC     ,CASE fct.dateofstart           
# MAGIC         WHEN '1900-01-01T00:00:00Z' THEN null 
# MAGIC         ELSE fct.dateofstart 
# MAGIC     END                                 AS lease_contract_start_date
# MAGIC     ,CASE fct.limiteddate           
# MAGIC         WHEN '1900-01-01T00:00:00Z' THEN null 
# MAGIC         ELSE fct.limiteddate 
# MAGIC     END                                 AS lease_contract_end_date
# MAGIC     ,CASE fct.dateleaserev
# MAGIC         WHEN '1900-01-01T00:00:00Z' THEN null
# MAGIC         ELSE fct.dateleaserev
# MAGIC     END                                 AS lease_contract_revision_date
# MAGIC     ,CASE fct.nottermiabledatecust  
# MAGIC         WHEN '1900-01-01T00:00:00Z' THEN null 
# MAGIC         ELSE fct.nottermiabledatecust
# MAGIC     END                                 AS lease_contract_irrevocability_tenant_expiry_date
# MAGIC     ,CASE fct.nottermiabledate  
# MAGIC         WHEN '1900-01-01T00:00:00Z' THEN null 
# MAGIC         ELSE fct.nottermiabledate 
# MAGIC     END                                 AS lease_contract_irrevocability_landlord_expiry_date
# MAGIC     ,fct.leasewarninglender             AS lease_contract_termination_notice_tenant_months
# MAGIC     ,fct.leasewarninglenderstartup      AS lease_contract_termination_notice_tenant_startup_months
# MAGIC     ,CASE fct.noticestartupend 
# MAGIC         WHEN '1900-01-01T00:00:00Z' THEN null 
# MAGIC         ELSE fct.noticestartupend
# MAGIC     END                                 AS lease_contract_termination_notice_tenant_startup_end_date
# MAGIC     ,fct.leasewarningletter             AS lease_contract_termination_notice_landlord_months
# MAGIC     ,davs.ejendomvalue                  AS lease_contract_default_dimension_property
# MAGIC     ,davs.formaalvalue                  AS lease_contract_default_dimension_business_unit
# MAGIC     ,davs.baerervalue                   AS lease_contract_default_dimension_area
# MAGIC     ,davs.afdelingvalue                 AS lease_contract_default_dimension_department
# MAGIC     ,davs.udlejningvalue                AS lease_contract_default_dimension_lease_category
# MAGIC     ,CASE fct.limiteddateforecast  
# MAGIC         WHEN '1900-01-01T00:00:00Z' THEN null 
# MAGIC         ELSE fct.limiteddateforecast
# MAGIC     END                                 AS lease_contract_forecast_expiration_date
# MAGIC FROM Base.d365fo_flxcontracttable AS fct
# MAGIC LEFT JOIN Base.d365fo_flxcontractstatustable AS fcst
# MAGIC     ON fct.contractstatusid = fcst.contractstatusid
# MAGIC         and fct.dataareaid = fcst.dataareaid
# MAGIC LEFT JOIN Base.d365fo_flxcontracttypetable AS fctt
# MAGIC     ON fct.contracttype = fctt.contracttype
# MAGIC         and fct.dataareaid = fctt.dataareaid
# MAGIC LEFT JOIN Base.d365fo_flxperiodtable AS fpt
# MAGIC     ON fct.billingfrequencyperiodid = fpt.periodid
# MAGIC         and fct.dataareaid = fpt.dataareaid
# MAGIC LEFT JOIN Base.d365fo_dimensionattributevalueset AS davs
# MAGIC     ON fct.defaultdimension = davs.recid

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_lease_contract_df')
dim_df = dim_df.na.fill("Tomt")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
