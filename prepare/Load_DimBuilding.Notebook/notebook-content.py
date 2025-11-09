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
dimension_name: str = 'Building'          # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

# Optional input parameters
destination_lakehouse: str = 'Curated'  # Name of the destination lakehouse. I.e. Curated, Gold etc.
write_pattern: str = 'SCD1'             # Options: SCD1, SDC2. Default: SCD1
full_load: bool = True                # Options: True, False. Default: False
recreate: bool = False 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_building_df AS 
# MAGIC SELECT
# MAGIC      COALESCE(CONCAT(B.id, '-', UPPER(prop.dataareaid)), CONCAT(B.id, '-BH')) AS building_key
# MAGIC     ,B.name                                                       AS building_name
# MAGIC     ,B.alternativeName                                            AS building_alternative_name
# MAGIC     ,L.name                                                       AS building_hierarchy_level_1
# MAGIC     ,E.name                                                       AS building_hierarchy_level_2
# MAGIC     ,B.road                                                       AS building_street_name
# MAGIC     ,B.number                                                     AS building_street_no
# MAGIC     ,B.zipCode                                                    AS building_zip_code
# MAGIC 	,B.city                                                       AS building_city
# MAGIC 	,CASE B.owned WHEN 1 THEN 'Ja' ELSE 'Nej' END                 AS building_owned
# MAGIC 	,B.`userDefinedFields_Opfoerelsesaar`                         AS building_year_of_construction
# MAGIC 	,B.`userDefinedFields_Status`                                 AS building_status
# MAGIC 	,B.`userDefinedFields_Type`                                   AS building_type
# MAGIC 	,B.`userDefinedFields_Primaeranvendelse`                      AS building_primary_use
# MAGIC 	,B.`userDefinedFields_Sekundaeranvendelse`                    AS building_secondary_use
# MAGIC 	,B.`userDefinedFields_Tertiaeranvendelse`                     AS building_tertiary_use
# MAGIC 	,B.`userDefinedFields_Kategori`                               AS building_category
# MAGIC 	,B.`userDefinedFields_Forsikringsselskab`                     AS building_insurance_company
# MAGIC 	,B.`userDefinedFields_Matrikelnr`                             AS building_registration_number
# MAGIC 	,B.`userDefinedFields_Fredetbevaringsvaerdig`                 AS building_protected_presevation
# MAGIC 	,B.`userDefinedFields_Beregningsgrundlag1risikosumforfredet`  AS building_calculation_basis_1_risk_sum_for_protection
# MAGIC     ,B.`userDefinedFields_Bemaerkninger`                          AS building_notes
# MAGIC 	,B.`userDefinedFields_SikringafbygningABAanlaegmv`            AS building_security
# MAGIC 	,lpad(CAST(B.`userDefinedFields_FlexPropertynr` as STRING), 3, '0') AS building_flex_property_nr
# MAGIC     ,CONCAT('https://fm.dalux.com/redirection?customerId=byoghavn&buildingId=',B.id) AS building_dalux_link
# MAGIC FROM Base.dalux_buildings AS B
# MAGIC LEFT JOIN Base.dalux_estates AS E
# MAGIC     ON B.estateID = E.id
# MAGIC LEFT JOIN Base.dalux_locations AS L
# MAGIC     ON E.locationID = L.id
# MAGIC LEFT JOIN Base.d365fo_flxbuildingtable prop
# MAGIC 	ON lpad(CAST(B.`userDefinedFields_FlexPropertynr` as STRING), 3, '0') = prop.buildingid
# MAGIC WHERE 1 = 1
# MAGIC 	AND B.lh_is_deleted = 0
# MAGIC 	AND B.lh_is_current = 1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_building_df')

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
