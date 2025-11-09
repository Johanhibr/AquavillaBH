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
# META         },
# META         {
# META           "id": "c83ca59e-f468-4305-a323-a324e0fb54b4"
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
dimension_name: str = 'Chargepoint'          # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

# Optional input parameters
destination_lakehouse: str = 'Curated'  # Name of the destination lakehouse. I.e. Curated, Gold etc.
write_pattern: str = 'SCD1'             # Options: SCD1, SDC2. Default: SCD1
full_load: bool = True                  # Options: True, False. Default: False
recreate: bool = False                  # Options: True, False. Default: False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_chargepoint_df AS
# MAGIC 
# MAGIC -- First source - Chargepoint existing in the Locations table
# MAGIC SELECT DISTINCT
# MAGIC     REGEXP_REPLACE(evses_evseId, '\\*[0-9]+$', '')  AS chargepoint_key,
# MAGIC     REGEXP_REPLACE(evses_evseId, '\\*[0-9]+$', '')  AS chargepoint_code,
# MAGIC     REGEXP_REPLACE(evses_evseId, '\\*[0-9]+$', '')  AS chargepoint_name,
# MAGIC     operator_name                                   AS chargepoint_operator_name,
# MAGIC     evses_connectors_maxElectricPower               AS chargepoint_max_power,
# MAGIC     CASE
# MAGIC         WHEN parkingType = 'PARKING_GARAGE' THEN 'Parking lot (indoor)'
# MAGIC         WHEN parkingType = 'ON_DRIVEWAY' THEN 'Public road'
# MAGIC         WHEN parkingType = 'PARKING_LOT' THEN 'Parking lot'
# MAGIC         WHEN parkingType = 'ON_STREET' THEN 'Public road'
# MAGIC         WHEN parkingType = 'UNDERGROUND_GARAGE' THEN 'Parking lot (underground)'
# MAGIC     END                                             AS chargepoint_location_type,
# MAGIC     name                                            AS chargepoint_location_name,
# MAGIC     address                                         AS chargepoint_location_address,
# MAGIC     postalCode                                      AS chargepoint_location_zip_code
# MAGIC FROM Base.spirii_locations
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC -- Second source - only Chargepoints NOT existing in Locations
# MAGIC SELECT DISTINCT
# MAGIC     REGEXP_REPLACE(evseId, '\\*[0-9]+$', '')        AS chargepoint_key,
# MAGIC     REGEXP_REPLACE(evseId, '\\*[0-9]+$', '')        AS chargepoint_code,
# MAGIC     REGEXP_REPLACE(evseId, '\\*[0-9]+$', '')        AS chargepoint_name,
# MAGIC     'By og Havn'                                    AS chargepoint_operator_name,
# MAGIC     22                                              AS chargepoint_max_power,
# MAGIC     location_type                                   AS chargepoint_location_type,
# MAGIC     location_name                                   AS chargepoint_location_name,
# MAGIC     NULL                                            AS chargepoint_location_address,
# MAGIC     location_zipCode                                AS chargepoint_location_zip_code
# MAGIC FROM Base.spirii_transactions AS tran
# MAGIC WHERE location_name IS NOT NULL
# MAGIC   AND NOT EXISTS (
# MAGIC       SELECT 1
# MAGIC       FROM Base.spirii_locations AS loc
# MAGIC       WHERE REGEXP_REPLACE(loc.evses_evseId, '\\*[0-9]+$', '') = REGEXP_REPLACE(tran.evseId, '\\*[0-9]+$', '')
# MAGIC   )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_chargepoint_df')

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
