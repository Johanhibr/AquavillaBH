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

# MARKDOWN ********************

# # Charges Fact
# This notebook creates a fact table holding charge records and corresponding transactions from Spirii.

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Notebook specific parameters
fact_name: str = 'Charges'                      # Set the name of the fact. Table name is automatically prefixed with fact_type variable
fact_type: str = 'Fact_'                        # Options: Fact / Bridge

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_assets_df AS
# MAGIC WITH normalized AS (
# MAGIC     SELECT DISTINCT
# MAGIC         a.userDefinedFields_IDnummer,
# MAGIC         a.buildingID,
# MAGIC         CASE
# MAGIC             WHEN regexp_replace(a.userDefinedFields_IDnummer, '\\*1$', '') RLIKE '^DK\\.SPI\\.'
# MAGIC                 THEN CONCAT(
# MAGIC                     'DK.SPI.',
# MAGIC                     regexp_replace(
# MAGIC                         regexp_replace(
# MAGIC                             regexp_replace(a.userDefinedFields_IDnummer, '\\*1$', ''),
# MAGIC                             '^DK\\.SPI\\.',
# MAGIC                             ''
# MAGIC                         ),
# MAGIC                         '[.-]',
# MAGIC                         ''
# MAGIC                     )
# MAGIC                 )
# MAGIC             ELSE regexp_replace(a.userDefinedFields_IDnummer, '\\*1$', '')
# MAGIC         END AS evses_evseId_formatted
# MAGIC     FROM Base.dalux_assets AS a
# MAGIC     LEFT JOIN Base.dalux_buildings AS b ON b.id = a.buildingID
# MAGIC     WHERE a.classificationName = 'Ladeboks'
# MAGIC       AND a.userDefinedFields_IDnummer NOT LIKE '%XXXXXX%'
# MAGIC       AND a.userDefinedFields_IDnummer LIKE '%SPI%'
# MAGIC       AND a.userDefinedFields_IDnummer IS NOT NULL
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC     n.userDefinedFields_IDnummer,
# MAGIC     CASE
# MAGIC         WHEN n.buildingID IS NULL AND n.evses_evseId_formatted LIKE '%0390%' THEN 170
# MAGIC         ELSE n.buildingID
# MAGIC     END AS buildingID,
# MAGIC     n.evses_evseId_formatted,
# MAGIC     -- area_key from location name or manual mapping
# MAGIC     COALESCE(
# MAGIC         CASE
# MAGIC             WHEN L.name IS NOT NULL AND TRIM(L.name) RLIKE '^[0-9]{4}'
# MAGIC                 THEN SUBSTRING(TRIM(L.name), 1, 4)
# MAGIC             ELSE NULL
# MAGIC         END,
# MAGIC         CASE
# MAGIC             WHEN buildingID <> 0 THEN NULL
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%015%' THEN '6000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%011%' THEN '6000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%282%' THEN '6000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%017%' THEN '6000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%281%' THEN '6000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%009%' THEN '6000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%275%' THEN '6000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%005%' THEN '6000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%172%' THEN '4000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%173%' THEN '4000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%174%' THEN '4000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%171%' THEN '4000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%176%' THEN '8000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%284%' THEN '8000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%280%' THEN '8000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%260%' THEN '8000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%179%' THEN '8000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%175%' THEN '8000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%272%' THEN '8000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%177%' THEN '8000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%258%' THEN '3000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%265%' THEN '3000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%278%' THEN '3000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%266%' THEN '3000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%289%' THEN '3000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%274%' THEN '3000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%259%' THEN '3000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%267%' THEN '3000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%270%' THEN '3000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%290%' THEN '3000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%020%' THEN '7000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%349%' THEN '7000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%338%' THEN '7000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%004%' THEN '7000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%276%' THEN '7000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%013%' THEN '7000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%339%' THEN '7000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%340%' THEN '7000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%348%' THEN '7000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%288%' THEN '7000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%341%' THEN '7000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%077%' THEN '1000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%078%' THEN '1000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%042%' THEN '8000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%040%' THEN '8000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%041%' THEN '8000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%039%' THEN '8000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%998%' THEN '8000'
# MAGIC             WHEN n.evses_evseId_formatted LIKE '%0390%' THEN '1000'
# MAGIC             ELSE NULL
# MAGIC         END
# MAGIC     ) AS area_key
# MAGIC FROM normalized n
# MAGIC LEFT JOIN Base.dalux_buildings B ON n.buildingID = B.id
# MAGIC LEFT JOIN Base.dalux_estates E ON B.estateID = E.id
# MAGIC LEFT JOIN Base.dalux_locations L ON E.locationID = L.id;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_chargepoints_union_df AS
# MAGIC SELECT DISTINCT
# MAGIC     userDefinedFields_IDnummer,
# MAGIC     buildingID,
# MAGIC     evses_evseId_formatted,
# MAGIC     area_key
# MAGIC FROM temp_assets_df
# MAGIC 
# MAGIC UNION
# MAGIC 
# MAGIC SELECT
# MAGIC     NULL AS userDefinedFields_IDnummer,
# MAGIC     NULL AS buildingID,
# MAGIC     REGEXP_REPLACE(ev.evses_evseId, '\\*[0-9]+$', '') AS evses_evseId_formatted,
# MAGIC     NULL AS area_key
# MAGIC FROM Base.spirii_locations ev
# MAGIC WHERE
# MAGIC     REGEXP_REPLACE(ev.evses_evseId, '\\*[0-9]+$', '') NOT IN (
# MAGIC         SELECT evses_evseId_formatted FROM temp_assets_df
# MAGIC     );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_charges_df AS
# MAGIC 
# MAGIC SELECT
# MAGIC     id AS transaction_id,
# MAGIC     -- Foreign Keys
# MAGIC     regexp_replace(TRANS.evseId, '\\*[0-9]+$', '') AS chargepoint_key,
# MAGIC     CONCAT(ASSET.buildingID, '-BH') AS building_key,
# MAGIC     -- ASSET.floorID AS building_floor_key, /*ID doesnt match existing DIM values. No correlation*/
# MAGIC     -- ASSET.roomID AS room_key, /*DIM WAS NEVER IMPLEMENTED*/
# MAGIC     ASSET.area_key AS area_key,
# MAGIC     CAST(date_format(TRANS.startedAt, 'yyyyMMdd') AS INT) AS calendar_key,
# MAGIC     CAST(date_format(TRANS.startedAt, 'yyyyMMdd') AS INT) AS calendar_start_key,
# MAGIC     CAST(date_format(TRANS.endedAt, 'yyyyMMdd') AS INT) AS calendar_end_key,
# MAGIC     DATEDIFF(SECOND, CAST(TRANS.startedAt AS DATE), startedAt) AS time_start_key,
# MAGIC     DATEDIFF(SECOND, CAST(TRANS.endedAt AS DATE), endedAt) AS time_end_key,
# MAGIC     TRANS.price_currency AS currency_key,
# MAGIC 
# MAGIC     -- Measures
# MAGIC     TRANS.price_amount AS charge_price_amount,
# MAGIC     TRANS.price_amountExVat AS charge_price_ex_vat_amount,
# MAGIC     TRANS.price_perKwh AS charge_price_per_kwh,
# MAGIC     CAST(TRANS.duration_charging AS LONG) AS charge_duration_seconds,
# MAGIC     CAST(TRANS.duration_idle AS LONG) AS charge_duration_idle_seconds,
# MAGIC     CAST(TRANS.duration_total AS LONG) AS charge_duration_total_seconds,
# MAGIC     TRANS.chargingDetails_consumed AS charge_consumed_kwh,
# MAGIC     TRANS.chargingDetails_co2Emitted AS charge_co2_emitted,
# MAGIC 
# MAGIC     -- Attributes
# MAGIC     TRANS.roamingDetails_roamingOperator AS charge_roaming_operator,
# MAGIC     CASE
# MAGIC         WHEN TRANS.roamingDetails_roamingOperator IS NULL OR TRANS.roamingDetails_roamingOperator = '' THEN 'Spirii'
# MAGIC         WHEN TRANS.roamingDetails_roamingOperator = 'DK-CLE' THEN 'Clever'
# MAGIC         ELSE 'Andre operatører'
# MAGIC     END AS charge_roaming_operator_filter,
# MAGIC     CASE 
# MAGIC         WHEN TRANS.voucherGroup_name IS NOT NULL THEN 'Ja'
# MAGIC         ELSE 'Nej'
# MAGIC     END AS charge_is_voucher_customer_flag,
# MAGIC     CASE
# MAGIC         WHEN TRANS.voucherGroup_name LIKE '%Tyske Ambassade%' THEN 'Ambassade'
# MAGIC         WHEN TRANS.voucherGroup_name LIKE '%GreenMobility%' THEN 'Green Mobility'
# MAGIC         WHEN TRANS.voucherGroup_name LIKE '%Levant%' THEN 'LevantKaj 1'
# MAGIC         WHEN TRANS.voucherGroup_name IN ('By&Havn', 'By & Havn sommerhus') THEN 'By & Havn'
# MAGIC         ELSE TRANS.voucherGroup_name
# MAGIC     END AS charge_voucher_group_name
# MAGIC FROM Base.spirii_transactions AS TRANS
# MAGIC     LEFT JOIN temp_chargepoints_union_df AS ASSET
# MAGIC         ON regexp_replace(TRANS.evseId, '\\*[0-9]+$', '') = ASSET.evses_evseId_formatted


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_df = spark.table('temp_charges_df')

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
