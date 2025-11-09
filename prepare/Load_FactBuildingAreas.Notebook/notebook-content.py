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
fact_name: str = 'BuildingAreas'                # Set the name of the fact. Table name is automatically prefixed with fact_type variable
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

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_building_areas_date_logic_df AS
# MAGIC 
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         -- If a row has a created date not equal to the day historic DALUX data started, then use the created date
# MAGIC         WHEN CAST(db.lh_created_date as timestamp) <> CAST(a.min_date as timestamp)
# MAGIC             THEN CAST(db.lh_created_date AS DATE)
# MAGIC         -- If construction year is before 2010, or missing, use 1. January 2010
# MAGIC         WHEN CAST(lh_valid_from as DATE) = CAST('1900-01-01' as DATE) AND (db.userDefinedFields_Opfoerelsesaar <= 2010 OR db.userDefinedFields_Opfoerelsesaar = 'N/A')
# MAGIC             THEN CAST('2010-01-01' as DATE)
# MAGIC         -- If construction year is after 2010, use the 1. January of that year
# MAGIC         WHEN CAST(lh_valid_from as DATE) = CAST('1900-01-01' as DATE) AND (db.userDefinedFields_Opfoerelsesaar > 2010)
# MAGIC             THEN make_date(YEAR(db.userDefinedFields_Opfoerelsesaar),1,1)
# MAGIC     END as start_date
# MAGIC     ,CASE
# MAGIC         WHEN CAST(db.lh_valid_to as DATE) = CAST('9999-12-31' as DATE)
# MAGIC             THEN make_date(YEAR(current_date()),MONTH(current_date())+1,1)
# MAGIC         ELSE
# MAGIC             CAST(db.lh_valid_to as DATE)
# MAGIC     END as end_date
# MAGIC     ,prop.dataareaid
# MAGIC     ,db.*
# MAGIC     
# MAGIC FROM
# MAGIC     Base.dalux_buildings AS db
# MAGIC LEFT JOIN Base.d365fo_flxbuildingtable prop
# MAGIC 	ON lpad(CAST(db.`userDefinedFields_FlexPropertynr` as STRING), 3, '0') = prop.buildingid
# MAGIC CROSS JOIN
# MAGIC     (SELECT MIN(db.lh_created_date) min_date FROM Base.dalux_buildings db) as a

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_building_areas_df AS
# MAGIC 
# MAGIC SELECT
# MAGIC     -- Foreign Keys
# MAGIC      CAST(date_format(make_date(c.year,c.month_no,1), 'yMMdd') as INT)  AS calendar_key
# MAGIC     ,TRY_CAST(substring_index(db.estateName, '_', 1) AS STRING)         AS area_key -- 'Lynetten' & 'Sommerhuset' cannot be converted to int.. -> NULL
# MAGIC     ,CONCAT(db.id, '-', UPPER(db.dataareaid))                           AS building_key
# MAGIC 
# MAGIC     -- Measures
# MAGIC     ,last_value(db.grossArea)                                           AS gross_area
# MAGIC     ,last_value(db.netArea)                                             AS net_area
# MAGIC 
# MAGIC FROM temp_building_areas_date_logic_df db
# MAGIC CROSS JOIN Curated.dim_calendar c
# MAGIC     -- Keep only rows that are active on current month
# MAGIC     ON c.calendar_date BETWEEN db.start_date AND db.end_date
# MAGIC     AND c.calendar_date <= current_date()
# MAGIC GROUP BY CONCAT(db.id, '-', UPPER(db.dataareaid)), c.year, c.month_no, db.estateName


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_df = spark.table('temp_building_areas_df')

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
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
