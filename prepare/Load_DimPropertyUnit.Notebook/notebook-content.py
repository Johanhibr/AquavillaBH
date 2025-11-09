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
dimension_name: str = 'Property_Unit'          # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

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
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_property_unit_df AS 
# MAGIC 
# MAGIC SELECT
# MAGIC      CONCAT(fbu.buildingunitid, '-', UPPER(fbu.dataareaid))                          AS property_unit_key
# MAGIC     ,CONCAT(fbu.buildingid, '-', UPPER(fbu.dataareaid))                              AS property_key
# MAGIC     ,fbu.floor                                   AS building_floor_key
# MAGIC     ,fbu.buildingunitid                          AS property_unit_code
# MAGIC 
# MAGIC     ,CASE
# MAGIC         WHEN LEN(fbu.buildingunitid) - LEN(REPLACE(fbu.buildingunitid, '-', '')) = 2
# MAGIC             THEN TRIM(substring_index(fbu.buildingunitid, '-', 2))
# MAGIC         ELSE fbu.buildingunitid
# MAGIC     END                                          AS property_unit_address
# MAGIC 
# MAGIC     ,CASE
# MAGIC         WHEN LEN(fbu.buildingunitid) - LEN(REPLACE(fbu.buildingunitid, '-', '')) = 2
# MAGIC             THEN TRIM(substring_index(fbu.buildingunitid, '-', -1))
# MAGIC         ELSE '01'
# MAGIC     END                                          AS property_unit_count
# MAGIC 
# MAGIC     ,fbu.propertyunitidentifiers                 AS property_unit_identifier
# MAGIC     ,fbu.txt                                     AS property_unit_description
# MAGIC     ,CASE
# MAGIC         WHEN fbust.status = 0 THEN 'Disponibel'
# MAGIC         WHEN fbust.status = 1 THEN 'Ikke til rådighed'
# MAGIC         WHEN fbust.status = 2 THEN 'Udlejet'
# MAGIC         ELSE 'Tomt'
# MAGIC     END                                          AS property_unit_status
# MAGIC     ,CASE
# MAGIC         WHEN fmutt.txt IS NULL THEN 'Tomt'
# MAGIC         ELSE fmutt.txt                           
# MAGIC     END                                          AS property_unit_main_type
# MAGIC     ,CASE
# MAGIC         WHEN futt.txt IS NULL THEN 'Tomt'
# MAGIC         ELSE futt.txt
# MAGIC     END                                          AS property_unit_type
# MAGIC 
# MAGIC     ,davs.ejendomvalue                           AS default_dimension_property
# MAGIC     ,davs.formaalvalue                           AS default_dimension_business_unit
# MAGIC     ,davs.baerervalue                            AS default_dimension_area
# MAGIC     ,davs.afdelingvalue                          AS default_dimension_department
# MAGIC     ,davs.udlejningvalue                         AS default_dimension_lease_category
# MAGIC 
# MAGIC FROM
# MAGIC     Base.d365fo_flxbuildingunit fbu
# MAGIC LEFT JOIN
# MAGIC     Base.d365fo_flxmainunittypetable fmutt
# MAGIC     ON fbu.mainunittypeid = fmutt.mainunittypeid
# MAGIC         and fbu.dataareaid = fmutt.dataareaid
# MAGIC LEFT JOIN
# MAGIC     Base.d365fo_flxunittypetable futt
# MAGIC     ON fbu.unittypeid = futt.unittypeid
# MAGIC         and fbu.dataareaid = futt.dataareaid
# MAGIC LEFT JOIN
# MAGIC     Base.d365fo_flxbuildingunitstatustable fbust
# MAGIC     ON 
# MAGIC         fbu.buildingunitid = fbust.buildingunitid AND
# MAGIC         fbu.buildingid = fbust.buildingid AND
# MAGIC         fbu.floor = fbust.floor AND
# MAGIC         fbu.dataareaid = fbust.dataareaid
# MAGIC LEFT JOIN Base.d365fo_dimensionattributevalueset davs
# MAGIC     ON fbu.defaultdimension = davs.recid
# MAGIC 
# MAGIC WHERE current_date() BETWEEN CAST(fbust.validfrom as DATE) AND COALESCE(CAST(fbust.validto as DATE), current_date())


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_property_unit_df')
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
