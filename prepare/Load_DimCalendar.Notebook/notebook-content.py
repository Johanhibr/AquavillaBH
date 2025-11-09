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

# # Calendar dimension
# This notebook creates a generic calendar dimension.

# CELL ********************

%run AquaVilla_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Notebook specific parameters
dimension_name: str = 'Calendar'        # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

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

# Set fixed start date and rolling end date of calendar dimension
start_date = '2010-01-01'
end_date = str((datetime.now() + timedelta(days=(365*10))).year) + '-12-31'

dfCal = spark.sql(f"SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as date").withColumn("date", explode(col("date")))
dfCal.createOrReplaceTempView("calDim");

dim_df = spark.sql("""
  SELECT 
    CAST(date_format(date, 'yyyyMMdd') AS INT) AS calendar_key, 
    date AS calendar_date,
    CASE QUARTER(date)
      WHEN 1 THEN 'Q1'
      WHEN 2 THEN 'Q2'
      WHEN 3 THEN 'Q3'
      WHEN 4 THEN 'Q4'
    END AS quarter_no,
    DATE_FORMAT(date, 'yyyyMM') AS month_code, 
    MONTH(date) AS month_no,
    initcap(to_csv(named_struct('date', date), map('dateFormat', 'MMMM', 'locale', 'da-DK'))) AS month_name,
    CASE initcap(to_csv(named_struct('date', date), map('dateFormat', 'MMMM', 'locale', 'da-DK')))
      WHEN 'Januar' THEN 'Jan'
      WHEN 'Februar' THEN 'Feb'
      WHEN 'Marts' THEN 'Mar'
      WHEN 'April' THEN 'Apr'
      WHEN 'Maj' THEN 'Maj'
      WHEN 'Juni' THEN 'Jun'
      WHEN 'Juli' THEN 'Jul'
      WHEN 'August' THEN 'Aug'
      WHEN 'September' THEN 'Sep'
      WHEN 'Oktober' THEN 'Okt'
      WHEN 'November' THEN 'Nov'
      WHEN 'December' THEN 'Dec'
      ELSE 'Ukendt'
    END AS month_name_short,
    CONCAT(
      CAST(year(date) AS STRING),
      ' ',
      CASE initcap(to_csv(named_struct('date', date), map('dateFormat', 'MMMM', 'locale', 'da-DK')))
        WHEN 'Januar' THEN 'Jan'
        WHEN 'Februar' THEN 'Feb'
        WHEN 'Marts' THEN 'Mar'
        WHEN 'April' THEN 'Apr'
        WHEN 'Maj' THEN 'Maj'
        WHEN 'Juni' THEN 'Jun'
        WHEN 'Juli' THEN 'Jul'
        WHEN 'August' THEN 'Aug'
        WHEN 'September' THEN 'Sep'
        WHEN 'Oktober' THEN 'Okt'
        WHEN 'November' THEN 'Nov'
        WHEN 'December' THEN 'Dec'
        ELSE 'Ukendt'
      END
    ) AS year_month_short,
    WEEKOFYEAR(date) AS week_no,
    DAYOFWEEK(date-1) AS weekday_no,
    initcap(to_csv(named_struct('date', date), map('dateFormat', 'EEEE', 'locale', 'da-DK'))) AS weekday_name,
    DAYOFMONTH(date) AS day_of_month,
    YEAR(date) AS year,
    datediff(day, current_date(), date)                                 AS calendar_day_counter,
    datediff(month, trunc(current_date(), 'MM'), trunc(date, 'MM'))     AS calendar_month_counter,
    datediff(year, trunc(current_date(), 'YY'), trunc(date, 'YY'))      AS calendar_year_counter,
    datediff(week,
    date_sub(current_date(), ((dayofweek(current_date()) + 5) % 7 + 1)-1),
    date_sub(date, ((dayofweek(date) + 5) % 7 + 1)-1))                  AS calendar_week_counter,
    (year(date)*4 + quarter(date)) -
    (year(current_date())*4 + quarter(current_date()))                  AS calendar_quarter_counter,
    CONCAT(/* Interval Start */ INT(YEAR(current_date()) + (ROUND((YEAR(date) - YEAR(current_date()) - 3) / 5) * 5) + 1) , " - ", /* Interval End */ INT(YEAR(current_date()) + (ROUND((YEAR(date) - YEAR(current_date()) + 2) / 5) * 5) ) ) AS 5_year_period 
  FROM 
    calDim
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
# MAGIC UPDATE Curated.dim_calendar
# MAGIC      SET
# MAGIC       calendar_day_counter = NULL
# MAGIC      ,calendar_month_counter = NULL
# MAGIC      ,calendar_year_counter = NULL
# MAGIC      ,calendar_week_counter = NULL
# MAGIC      ,calendar_quarter_counter = NULL
# MAGIC    WHERE calendar_key = -1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
