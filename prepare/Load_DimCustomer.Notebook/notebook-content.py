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
dimension_name: str = 'Customer'          # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_customer_df AS
# MAGIC WITH parking_d365_company_customer AS (
# MAGIC     SELECT
# MAGIC          CONCAT(ct.accountnum ,'-', UPPER(ct.dataareaid))                       AS customer_key
# MAGIC         ,ct.accountnum                                                          AS customer_no
# MAGIC         ,dpt.name                                                               AS customer_name
# MAGIC         ,ct.custclassificationid                                                AS customer_classification
# MAGIC         ,ct.custgroup                                                           AS customer_group
# MAGIC         ,REPLACE(lpa.address, '\n',', ')                                        AS customer_full_address
# MAGIC         ,REPLACE(lpa.street, '\n',', ')                                         AS customer_street
# MAGIC         ,lpa.zipcode                                                            AS customer_zip
# MAGIC         ,lpa.city                                                               AS customer_city
# MAGIC         ,lpa.countryregionid                                                    AS customer_country_code
# MAGIC         ,'D365FO'                                                               AS customer_source
# MAGIC         ,false AS Deleted
# MAGIC     FROM
# MAGIC         Base.d365fo_custtable AS ct
# MAGIC     LEFT JOIN
# MAGIC         Base.d365fo_dirpartytable AS dpt
# MAGIC         ON ct.party = dpt.recid
# MAGIC     LEFT JOIN 
# MAGIC         Base.d365fo_logisticspostaladdress AS lpa
# MAGIC         ON dpt.primaryaddresslocation = lpa.location
# MAGIC         AND current_timestamp() BETWEEN lpa.validfrom AND lpa.validto
# MAGIC ),
# MAGIC 
# MAGIC parking_parkincph_company_customer AS (
# MAGIC     SELECT DISTINCT
# MAGIC          Id                                                                     AS customer_key
# MAGIC         ,Id                                                                     AS customer_no
# MAGIC         ,Navn                                                                   AS customer_name
# MAGIC         ,'Selskab'                                                              AS customer_classification
# MAGIC         ,'Park'                                                                 AS customer_group
# MAGIC         ,CONCAT(Adresse, ', ', Postnummer, ', DNK')                             AS customer_full_address
# MAGIC         ,Adresse                                                                AS customer_street
# MAGIC         ,Postnummer                                                             AS customer_zip
# MAGIC         ,By                                                                     AS customer_city
# MAGIC         ,'DNK'                                                                  AS customer_country_code
# MAGIC         ,'PARKinCPH'                                                            AS customer_source
# MAGIC         ,Deleted
# MAGIC     FROM Base.parkincph_virksomhed
# MAGIC     WHERE 1 = 1
# MAGIC         AND lh_is_current = 1
# MAGIC         AND lh_is_deleted = 0
# MAGIC         AND TRIM(Kundenummer) NOT IN (SELECT accountnum FROM Base.d365fo_custtable ) --inkluder kun kunder fra PARKinCPH der IKKE eksisterer i D365FO 
# MAGIC 
# MAGIC ),
# MAGIC 
# MAGIC parking_parkincph_private_customer AS (
# MAGIC     SELECT DISTINCT
# MAGIC          br.Id                                                                  AS customer_key
# MAGIC         ,br.Id                                                                  AS customer_no
# MAGIC         ,br.Name                                                                AS customer_name
# MAGIC         ,'Privat'                                                               AS customer_classification
# MAGIC         ,'Park'                                                                 AS customer_group
# MAGIC         ,CONCAT(wv.Vejnavn, ' ', be.Husnummer, ', ', be.Postnummer, ', DNK')    AS customer_full_address
# MAGIC         ,CONCAT(wv.Vejnavn, ' ', be.Husnummer)                                  AS customer_street
# MAGIC         ,be.Postnummer                                                          AS customer_zip
# MAGIC         ,be.By                                                                  AS customer_city
# MAGIC         ,'DNK'                                                                  AS customer_country_code
# MAGIC         ,'PARKinCPH'                                                            AS customer_source
# MAGIC         ,br.Deleted
# MAGIC     FROM Base.parkincph_bruger AS br
# MAGIC         LEFT JOIN Base.parkincph_beboer AS be
# MAGIC             ON br.Id = be.BrugerId
# MAGIC             AND be.By <> 'deleted'
# MAGIC             AND be.lh_is_current = 1
# MAGIC         LEFT JOIN Base.parkincph_whitelistetvej AS wv
# MAGIC             ON be.WhitelistetVejId = wv.Id
# MAGIC             AND wv.lh_is_current = 1
# MAGIC     WHERE 1 = 1
# MAGIC         AND br.Deleted = 0
# MAGIC         AND br.lh_is_current = 1
# MAGIC         AND br.lh_is_deleted = 0
# MAGIC )
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM parking_d365_company_customer
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM parking_parkincph_company_customer
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM parking_parkincph_private_customer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_customer_df')

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
