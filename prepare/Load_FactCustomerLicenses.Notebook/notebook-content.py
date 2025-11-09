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

# # Customer Licenses Fact
# This notebook creates a fact table holding licenses from PARKinCPH.

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Notebook specific parameters
fact_name: str = 'CustomerLicenses'                      # Set the name of the fact. Table name is automatically prefixed with fact_type variable
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
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_customerlicenses_created_vw AS
# MAGIC 
# MAGIC 
# MAGIC SELECT 
# MAGIC     -- Foreign Keys    
# MAGIC      CAST(date_format(pl.GyldigFra, 'yyyyMMdd') AS INT)                                     AS calendar_key
# MAGIC     ,ko.ProduktId                                                                           AS product_key
# MAGIC     ,COALESCE(CONCAT(ct.accountnum ,'-', UPPER(ct.dataareaid)),v.Id,b.BrugerId,mh.BrugerId) AS customer_key
# MAGIC     ,o.Omraadekode                                                                          AS area_key
# MAGIC     ,CASE
# MAGIC         WHEN pl.Handicapparkering = 1 THEN 'Handicap Beboer Licens'
# MAGIC         
# MAGIC         WHEN (p.Abonnement = 1 AND p.AnprProdukt = 1 AND pl.AnprOprettet = 1)
# MAGIC                 OR p.Abonnement = 0      THEN 'Korttid'
# MAGIC 
# MAGIC         ELSE pt.Kategori
# MAGIC 
# MAGIC       END                                                                                   AS parking_product_category_key 
# MAGIC 
# MAGIC 
# MAGIC     -- Attrbutes
# MAGIC     ,pl.DanskNummerplade                                                                    AS customer_licenses_is_danish_plate
# MAGIC     ,pl.Handicapparkering                                                                   AS customer_licenses_is_disabled_parking
# MAGIC     ,pl.KoeretoejType                                                                       AS customer_licenses_vehicle_type
# MAGIC     ,pl.ReserveretPlads                                                                     AS customer_licenses_reserved_spot
# MAGIC     ,ko.Koebsnummer                                                                         AS customer_licenses_order_number
# MAGIC     ,pl.Registreringsnummer                                                                 AS customer_licenses_registration_number
# MAGIC     ,'Created'                                                                              AS customer_licenses_record_type
# MAGIC 
# MAGIC     --Measures
# MAGIC     ,1                                                                                      AS customer_licenses_quantity
# MAGIC 
# MAGIC FROM 
# MAGIC     Base.parkincph_parkeringslicens AS pl
# MAGIC INNER JOIN 
# MAGIC     Base.parkincph_koebsordre AS ko ON ko.Id = pl.KoebsordreId
# MAGIC LEFT JOIN
# MAGIC     Base.parkincph_virksomhed AS v ON v.Id = pl.VirksomhedId
# MAGIC LEFT JOIN 
# MAGIC     Base.parkincph_beboer AS b on b.Id = pl.BeboerId
# MAGIC LEFT JOIN 
# MAGIC     Base.parkincph_medarbejderhaandvaerker AS mh ON mh.Id = pl.MedarbejderHaandvaerkerId
# MAGIC LEFT JOIN
# MAGIC     Base.parkincph_produktomraade AS po ON po.Id = pl.ProduktOmraadeId
# MAGIC LEFT JOIN 
# MAGIC     Base.parkincph_omraade AS o ON o.Id = po.OmraadeId
# MAGIC LEFT JOIN 
# MAGIC     Base.parkincph_produkt AS p ON p.Id = ko.ProduktId
# MAGIC LEFT JOIN 
# MAGIC     Base.sharepointsitepowerbi_parkincph_type AS pt ON pt.Produkt = p.Navn
# MAGIC LEFT JOIN 
# MAGIC     Base.d365fo_custtable AS ct ON ct.accountnum = TRIM(v.Kundenummer) 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_customerlicenses_expired_vw AS
# MAGIC 
# MAGIC 
# MAGIC SELECT 
# MAGIC     -- Foreign Keys    
# MAGIC     CASE
# MAGIC         WHEN 
# MAGIC             (p.Abonnement = 1 AND p.AnprProdukt = 1 AND pl.AnprOprettet = 1)
# MAGIC                 OR p.Abonnement = 0 
# MAGIC         THEN 
# MAGIC             CAST(date_format(COALESCE(pl.SpaerretTidspunkt,pl.GyldigTil), 'yyyyMMdd') AS INT)
# MAGIC 
# MAGIC         ELSE 
# MAGIC             CAST(date_format(dateadd(COALESCE(pl.SpaerretTidspunkt,pl.GyldigTil,pl.GyldigFra),1), 'yyyyMMdd') AS INT)
# MAGIC 
# MAGIC      END                                                                                            AS calendar_key
# MAGIC     ,ko.ProduktId                                                                                   AS product_key
# MAGIC     ,COALESCE(CONCAT(ct.accountnum ,'-', UPPER(ct.dataareaid)),v.Id,b.BrugerId,mh.BrugerId)         AS customer_key
# MAGIC     ,o.Omraadekode                                                                                  AS area_key
# MAGIC     ,CASE
# MAGIC         WHEN pl.Handicapparkering = 1 THEN 'Handicap Beboer Licens'
# MAGIC         
# MAGIC         WHEN (p.Abonnement = 1 AND p.AnprProdukt = 1 AND pl.AnprOprettet = 1)
# MAGIC                 OR p.Abonnement = 0      THEN 'Korttid'
# MAGIC 
# MAGIC         ELSE pt.Kategori
# MAGIC 
# MAGIC       END                                                                                           AS parking_product_category_key 
# MAGIC 
# MAGIC     -- Attrbutes
# MAGIC     ,pl.DanskNummerplade                                                                            AS customer_licenses_is_danish_plate
# MAGIC     ,pl.Handicapparkering                                                                           AS customer_licenses_is_disabled_parking
# MAGIC     ,pl.KoeretoejType                                                                               AS customer_licenses_vehicle_type
# MAGIC     ,pl.ReserveretPlads                                                                             AS customer_licenses_reserved_spot
# MAGIC     ,ko.Koebsnummer                                                                                 AS customer_licenses_order_number
# MAGIC     ,pl.Registreringsnummer                                                                         AS customer_licenses_registration_number
# MAGIC     ,'Expired'                                                                                      AS customer_licenses_record_type
# MAGIC 
# MAGIC     --Measures
# MAGIC     ,-1                                                                                             AS customer_licenses_quantity
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC FROM 
# MAGIC     Base.parkincph_parkeringslicens AS pl
# MAGIC INNER JOIN 
# MAGIC     Base.parkincph_koebsordre AS ko ON ko.Id = pl.KoebsordreId
# MAGIC LEFT JOIN
# MAGIC     Base.parkincph_virksomhed AS v ON v.Id = pl.VirksomhedId
# MAGIC LEFT JOIN 
# MAGIC     Base.parkincph_beboer AS b on b.Id = pl.BeboerId
# MAGIC LEFT JOIN 
# MAGIC     Base.parkincph_medarbejderhaandvaerker AS mh ON mh.Id = pl.MedarbejderHaandvaerkerId
# MAGIC LEFT JOIN
# MAGIC     Base.parkincph_produktomraade AS po ON po.Id = pl.ProduktOmraadeId
# MAGIC LEFT JOIN 
# MAGIC     Base.parkincph_omraade AS o ON o.Id = po.OmraadeId
# MAGIC LEFT JOIN 
# MAGIC     Base.parkincph_produkt AS p ON p.Id = ko.ProduktId
# MAGIC LEFT JOIN 
# MAGIC     Base.sharepointsitepowerbi_parkincph_type AS pt ON pt.Produkt = p.Navn
# MAGIC LEFT JOIN 
# MAGIC     Base.d365fo_custtable AS ct ON ct.accountnum = TRIM(v.Kundenummer) 
# MAGIC 
# MAGIC WHERE 1=1
# MAGIC     AND (pl.GyldigTil IS NOT NULL OR pl.SpaerretTidspunkt IS NOT NULL OR pl.Spaerret = 1)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Return view as data frame
temp_customerlicenses_created_df = spark.table('temp_customerlicenses_created_vw')
temp_customerlicenses_expired_df = spark.table('temp_customerlicenses_expired_vw')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Union data frames
dataframes = [temp_customerlicenses_created_df, temp_customerlicenses_expired_df]

temp_customerlicenses_df = dataframes[0]
for df in dataframes[1:]:
    temp_customerlicenses_df = temp_customerlicenses_df.union(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

written_df = load_fact(
    df = temp_customerlicenses_df, 
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
