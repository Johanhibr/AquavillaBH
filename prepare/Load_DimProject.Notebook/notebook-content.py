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
# META           "id": "c83ca59e-f468-4305-a323-a324e0fb54b4"
# META         },
# META         {
# META           "id": "2215fdc2-5d05-4516-882c-92b6c0bcd831"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Project dimension
# This notebook creates a project dimension.

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Notebook specific parameters
dimension_name: str = 'Project'           # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

# Optional input parameters
destination_lakehouse: str = 'Curated'  # Name of the destination lakehouse. I.e. Curated, Gold etc.
write_pattern: str = 'SCD1'             # Options: SCD1, SDC2. Default: SCD1
full_load: bool = True                 # Options: True, False. Default: False
recreate: bool = True                  # Options:True, False. Default: False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_project_hierarchy_concatination AS 
# MAGIC 
# MAGIC -- Joining on self multiple times, as regular recursive CTEs are unavailable in Spark SQL.
# MAGIC SELECT 
# MAGIC     CONCAT(proj.projid, '-', UPPER(proj.dataareaid)) AS project_key,
# MAGIC     proj.projid AS project_no,
# MAGIC     proj.name AS project_name,
# MAGIC     CONCAT(proj.projid, ': ', proj.name) AS project_no_and_name,
# MAGIC     group.name AS project_group,
# MAGIC     CASE 
# MAGIC         WHEN stage.stage = 'Finished' THEN 'Udført' 
# MAGIC         WHEN stage.stage = 'In progress' THEN 'Igangværende' 
# MAGIC     END AS project_stage,
# MAGIC     Pstatus.stage AS project_status,  
# MAGIC     proj.parentid AS project_parent_project_no,
# MAGIC     proj.dataareaid,
# MAGIC -- Concatenate hierarchy levels into a paragraph-separated string (using Paragraph symbol to ensure uniqueness)
# MAGIC -- Reversing hierarchy based on BUG 1026 requiring highest level at lvl 1 and so on (13/11/2024 - JHB)
# MAGIC CONCAT_WS('§',
# MAGIC         CASE WHEN projLVL15.projid IS NOT NULL THEN CONCAT(projLVL15.projid, ': ', projLVL15.name) END,
# MAGIC         CASE WHEN projLVL14.projid IS NOT NULL THEN CONCAT(projLVL14.projid, ': ', projLVL14.name) END,
# MAGIC         CASE WHEN projLVL13.projid IS NOT NULL THEN CONCAT(projLVL13.projid, ': ', projLVL13.name) END,
# MAGIC         CASE WHEN projLVL12.projid IS NOT NULL THEN CONCAT(projLVL12.projid, ': ', projLVL12.name) END,
# MAGIC         CASE WHEN projLVL11.projid IS NOT NULL THEN CONCAT(projLVL11.projid, ': ', projLVL11.name) END,
# MAGIC         CASE WHEN projLVL10.projid IS NOT NULL THEN CONCAT(projLVL10.projid, ': ', projLVL10.name) END,
# MAGIC         CASE WHEN projLVL9.projid IS NOT NULL THEN CONCAT(projLVL9.projid, ': ', projLVL9.name) END,
# MAGIC         CASE WHEN projLVL8.projid IS NOT NULL THEN CONCAT(projLVL8.projid, ': ', projLVL8.name) END,
# MAGIC         CASE WHEN projLVL7.projid IS NOT NULL THEN CONCAT(projLVL7.projid, ': ', projLVL7.name) END,
# MAGIC         CASE WHEN projLVL6.projid IS NOT NULL THEN CONCAT(projLVL6.projid, ': ', projLVL6.name) END,
# MAGIC         CASE WHEN projLVL5.projid IS NOT NULL THEN CONCAT(projLVL5.projid, ': ', projLVL5.name) END,
# MAGIC         CASE WHEN projLVL4.projid IS NOT NULL THEN CONCAT(projLVL4.projid, ': ', projLVL4.name) END,
# MAGIC         CASE WHEN projLVL3.projid IS NOT NULL THEN CONCAT(projLVL3.projid, ': ', projLVL3.name) END,
# MAGIC         CASE WHEN projLVL2.projid IS NOT NULL THEN CONCAT(projLVL2.projid, ': ', projLVL2.name) END,
# MAGIC         CASE WHEN projLVL1.projid IS NOT NULL THEN CONCAT(projLVL1.projid, ': ', projLVL1.name) END,
# MAGIC         CONCAT(proj.projid, ': ', proj.name)
# MAGIC     ) AS project_hierarchy_string
# MAGIC FROM Base.d365fo_projtable AS proj
# MAGIC     LEFT JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC             projgroupid,
# MAGIC             name,
# MAGIC             dataareaid
# MAGIC         FROM Base.d365fo_projgroup
# MAGIC         ) AS group
# MAGIC         ON proj.projgroupid = group.projgroupid AND proj.dataareaid = group.dataareaid
# MAGIC     LEFT JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC 	        status,
# MAGIC 	        stage,
# MAGIC             dataareaid
# MAGIC         FROM Base.d365fo_projstagetable
# MAGIC 	        WHERE language = 'en-GB'
# MAGIC         ) as stage
# MAGIC         ON proj.status = stage.status AND proj.dataareaid = stage.dataareaid
# MAGIC     LEFT JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC 	        status,
# MAGIC 	        stage,
# MAGIC             dataareaid
# MAGIC         FROM Base.d365fo_projstagetable
# MAGIC 	        WHERE language = 'da-DK'
# MAGIC         ) as Pstatus
# MAGIC         ON proj.status = Pstatus.status AND proj.dataareaid = Pstatus.dataareaid        
# MAGIC     LEFT JOIN Base.d365fo_projtable AS projLVL1
# MAGIC         ON proj.parentid = projLVL1.projid AND proj.dataareaid = projLVL1.dataareaid
# MAGIC     LEFT JOIN Base.d365fo_projtable AS projLVL2 
# MAGIC         ON projLVL1.parentid = projLVL2.projid AND proj.dataareaid = projLVL2.dataareaid
# MAGIC     LEFT JOIN Base.d365fo_projtable AS projLVL3
# MAGIC         ON projLVL2.parentid = projLVL3.projid AND proj.dataareaid = projLVL3.dataareaid
# MAGIC     LEFT JOIN Base.d365fo_projtable AS projLVL4
# MAGIC         ON projLVL3.parentid = projLVL4.projid AND proj.dataareaid = projLVL4.dataareaid
# MAGIC     LEFT JOIN Base.d365fo_projtable AS projLVL5
# MAGIC         ON projLVL4.parentid = projLVL5.projid AND proj.dataareaid = projLVL5.dataareaid
# MAGIC     LEFT JOIN Base.d365fo_projtable AS projLVL6
# MAGIC         ON projLVL5.parentid = projLVL6.projid  AND proj.dataareaid = projLVL6.dataareaid
# MAGIC     LEFT JOIN Base.d365fo_projtable AS projLVL7
# MAGIC         ON projLVL6.parentid = projLVL7.projid  AND proj.dataareaid = projLVL7.dataareaid
# MAGIC     LEFT JOIN Base.d365fo_projtable AS projLVL8
# MAGIC         ON projLVL7.parentid = projLVL8.projid  AND proj.dataareaid = projLVL8.dataareaid
# MAGIC     LEFT JOIN Base.d365fo_projtable AS projLVL9
# MAGIC         ON projLVL8.parentid = projLVL9.projid  AND proj.dataareaid = projLVL9.dataareaid
# MAGIC     LEFT JOIN Base.d365fo_projtable AS projLVL10
# MAGIC         ON projLVL9.parentid = projLVL10.projid  AND proj.dataareaid = projLVL10.dataareaid
# MAGIC     LEFT JOIN Base.d365fo_projtable AS projLVL11
# MAGIC         ON projLVL10.parentid = projLVL11.projid AND proj.dataareaid = projLVL11.dataareaid
# MAGIC     LEFT JOIN Base.d365fo_projtable AS projLVL12
# MAGIC         ON projLVL11.parentid = projLVL12.projid  AND proj.dataareaid = projLVL12.dataareaid
# MAGIC     LEFT JOIN Base.d365fo_projtable AS projLVL13
# MAGIC         ON projLVL12.parentid = projLVL13.projid  AND proj.dataareaid = projLVL13.dataareaid
# MAGIC     LEFT JOIN Base.d365fo_projtable AS projLVL14
# MAGIC         ON projLVL13.parentid = projLVL14.projid  AND proj.dataareaid = projLVL14.dataareaid
# MAGIC     LEFT JOIN Base.d365fo_projtable AS projLVL15
# MAGIC         ON projLVL14.parentid = projLVL15.projid  AND proj.dataareaid = projLVL15.dataareaid

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_manual_input_project_masterdata_cleaned AS 
# MAGIC SELECT 
# MAGIC     project_no,
# MAGIC     project_responsible
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         projektnr AS project_no,
# MAGIC         projektansvarlig AS project_responsible,
# MAGIC         row_number() OVER(PARTITION BY projektnr ORDER BY projektnr) AS rn /* Window function to avoid duplicates on project no */
# MAGIC     FROM
# MAGIC         Base.sharepointsitepowerbi_investeringsprojektstamoplysninger
# MAGIC )
# MAGIC WHERE rn = 1 /* Avoid duplicates on project no */

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_project_investment_level_df AS
# MAGIC 
# MAGIC SELECT 
# MAGIC     *
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC          projektnr                          as project_key
# MAGIC         ,niveau_1                           as project_investment_report_level_1
# MAGIC         ,CAST(sortering_niveau_1 as INT)    as project_investment_report_level_1_sort
# MAGIC         ,row_number() OVER(PARTITION BY projektnr ORDER BY projektnr) AS rn --Window function to avoid duplicates on project number
# MAGIC     FROM
# MAGIC         Base.sharepointsitepowerbi_investeringsprojektbudgetter
# MAGIC     )
# MAGIC WHERE rn = 1 -- Avoid duplicates on project number

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_project_df AS
# MAGIC 
# MAGIC -- Splitting the project hierarchy string into respective levels and adding information from manual files
# MAGIC SELECT 
# MAGIC     tphc.project_key,
# MAGIC     tphc.project_no,
# MAGIC     tphc.project_name,
# MAGIC     tphc.project_no_and_name,
# MAGIC     tphc.project_group,
# MAGIC     tphc.project_stage,
# MAGIC     project_status,
# MAGIC     tphc.project_parent_project_no,
# MAGIC     SPLIT(tphc.project_hierarchy_string, '§')[0] AS project_hierarchy_level_1,
# MAGIC     SPLIT(tphc.project_hierarchy_string, '§')[1] AS project_hierarchy_level_2,
# MAGIC     SPLIT(tphc.project_hierarchy_string, '§')[2] AS project_hierarchy_level_3,
# MAGIC     SPLIT(tphc.project_hierarchy_string, '§')[3] AS project_hierarchy_level_4,
# MAGIC     SPLIT(tphc.project_hierarchy_string, '§')[4] AS project_hierarchy_level_5,
# MAGIC     SPLIT(tphc.project_hierarchy_string, '§')[5] AS project_hierarchy_level_6,
# MAGIC     SPLIT(tphc.project_hierarchy_string, '§')[6] AS project_hierarchy_level_7,
# MAGIC     SPLIT(tphc.project_hierarchy_string, '§')[7] AS project_hierarchy_level_8,
# MAGIC     SPLIT(tphc.project_hierarchy_string, '§')[8] AS project_hierarchy_level_9,
# MAGIC     SPLIT(tphc.project_hierarchy_string, '§')[9] AS project_hierarchy_level_10,
# MAGIC     SPLIT(tphc.project_hierarchy_string, '§')[10] AS project_hierarchy_level_11,
# MAGIC     SPLIT(tphc.project_hierarchy_string, '§')[11] AS project_hierarchy_level_12,
# MAGIC     SPLIT(tphc.project_hierarchy_string, '§')[12] AS project_hierarchy_level_13,
# MAGIC     SPLIT(tphc.project_hierarchy_string, '§')[13] AS project_hierarchy_level_14,
# MAGIC     SPLIT(tphc.project_hierarchy_string, '§')[14] AS project_hierarchy_level_15,
# MAGIC     COALESCE(mipm.project_responsible, 'Tomt')   AS project_responsible,
# MAGIC     COALESCE(pil.project_investment_report_level_1, 'Tomt')     AS project_investment_report_level_1,
# MAGIC     COALESCE(pil.project_investment_report_level_1_sort, -1)    AS project_investment_report_level_1_sort
# MAGIC 
# MAGIC FROM temp_project_hierarchy_concatination AS tphc
# MAGIC     LEFT JOIN temp_manual_input_project_masterdata_cleaned AS mipm
# MAGIC         ON tphc.project_no = mipm.project_no
# MAGIC             and upper(tphc.dataareaid) = 'BH'
# MAGIC     LEFT JOIN temp_project_investment_level_df pil
# MAGIC         ON tphc.project_key = pil.project_key
# MAGIC             and upper(tphc.dataareaid) = 'BH'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_project_df')

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
