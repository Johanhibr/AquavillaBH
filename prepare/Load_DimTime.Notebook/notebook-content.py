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

# # Time dimension
# This notebook creates a time dimension.

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Notebook specific parameters
dimension_name: str = 'Time'        # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

# Optional input parameters
destination_lakehouse: str = 'Curated'  # Name of the destination lakehouse. I.e. Curated, Gold etc.
write_pattern: str = 'SCD1'             # Options: SCD1, SDC2. Default: SCD1
full_load: bool = True                 # Options: True, False. Default: False
recreate: bool = False                  # Options:True, False. Default: False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timedelta
from pyspark.sql.functions import sequence, to_date, explode, col

# Set start_time and end time of the day with arbitrary date in order to make use of timestamp function
start_time = '1970-01-01 00:00:00'
end_time = '1970-01-01 23:59:59'

# Generate 1 second time intervals
dfTime = spark.sql(f"""
  SELECT explode(sequence(
     timestamp('{start_time}')
    ,timestamp('{end_time}')
    ,interval 1 second
)) AS time_point
""")

dfTime.createOrReplaceTempView("timeDim");

dim_df = spark.sql(f"""
  SELECT 
     unix_timestamp(time_point) - unix_timestamp('{start_time}')                                                  AS time_key
    ,hour(time_point)                                                                                             AS hour_24
    ,CASE WHEN hour(time_point) % 12 = 0 THEN 12 ELSE hour(time_point) % 12 END                                   AS hour_12
    ,CASE WHEN hour(time_point) < 12 THEN 'AM' ELSE 'PM' END                                                      AS am_pm
    ,minute(time_point)                                                                                           AS minute
    ,second(time_point)                                                                                           AS second
    ,date_format(time_point, 'HH:mm:ss')                                                                          AS time_in_24h_format
    ,date_format(time_point, 'hh:mm:ss a')                                                                        AS time_in_12h_format
    ,date_format(time_point, 'HH:mm')                                                                             AS hour_minute
    ,CASE
      WHEN hour(time_point) >= 0 AND hour(time_point) < 9 THEN 'Kl. 00-09'
      WHEN hour(time_point) >= 9 AND hour(time_point) < 13 THEN 'Kl. 09-13'
      WHEN hour(time_point) >= 13 AND hour(time_point) < 17 THEN 'Kl. 13-17'
      WHEN hour(time_point) >= 17 AND hour(time_point) < 21 THEN 'Kl. 17-21'
      WHEN hour(time_point) >= 21 AND hour(time_point) < 23 THEN 'Kl. 21-00'
      ELSE 'Ukendt'
      END                                                                                                         AS time_grouping
    ,CASE
      WHEN hour(time_point) >= 7 AND hour(time_point) < 17 THEN 'Dag'
      ELSE 'Nat'
     END                                                                                                          AS time_of_day
    ,CONCAT(lpad(hour(time_point), 2, '0'), ':00')                                                                AS hour_bucket
    ,hour(time_point) * 60 + minute(time_point)                                                                   AS minute_of_day
    ,CONCAT(lpad(hour(time_point), 2, '0'), ':', lpad(CAST(floor(minute(time_point) / 15) * 15 AS INT), 2, '0'))  AS quarter_hour
    ,CONCAT(lpad(hour(time_point), 2, '0'), ':', lpad(CAST(floor(minute(time_point) / 30) * 30 AS INT), 2, '0'))  AS half_hour
  FROM      
    timeDim
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

written_df = load_scd1_dimension(
    df = dim_df, 
    destination_lakehouse = destination_lakehouse,
    destination_table = dimension_name,
    destination_table_prefix = 'dim_', 
    full_load = full_load,
    recreate = recreate,
    with_identity_column = False,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Set dummy records to null for records where -1 is a valid value

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE Curated.dim_time
# MAGIC      SET
# MAGIC       hour_24 = NULL
# MAGIC      ,hour_12 = NULL
# MAGIC      ,minute = NULL
# MAGIC      ,second = NULL
# MAGIC      ,minute_of_day = NULL
# MAGIC    WHERE time_key = -1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
