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
dimension_name: str = 'Business_Unit'          # Set the name of the dimension. Table name is automatically prefixed with Dim, i.e. value 'Customer' creates a DimCustomer table.

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

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

data = [("Fælles",10),
    ("Arealudvikling",20),
    ("Stormsikring",21),
    ("Stormsikring",60),
    ("Udlejning",30),
    ("Udlejning",31),
    ("Udlejning",32),
    ("Udlejning",33),
    ("Udlejning",34),
    ("Havnedrift",35),
    ("Havnedrift",36),
    ("Havnedrift",50),
    ("Havnedrift",51),
    ("Havnedrift",52),
    ("Havnedrift",53),
    ("Parkering",40)
  ]

schema = StructType([ 
    StructField("BusinessUnitLevel1Name",StringType(),True), 
    StructField("BusinessUnitLevel2Code",IntegerType(),True), 
  ])
 
DfBusinesslevel1 = spark.createDataFrame(data=data,schema=schema)
DfBusinesslevel1.createOrReplaceTempView("DfBusinesslevel1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_business_unit_df AS 
# MAGIC 
# MAGIC SELECT
# MAGIC      dav.displayvalue                               AS business_unit_key
# MAGIC     ,dav.displayvalue                               AS business_unit_no
# MAGIC     ,dat.description                                AS business_unit_description
# MAGIC     ,dfbl1.BusinessUnitLevel1Name                   AS business_unit_level_1_description
# MAGIC     ,concat(dav.displayvalue, '-',dat.description)  AS business_unit_no_and_description
# MAGIC FROM Base.d365fo_dimensionattributevalue AS dav
# MAGIC JOIN Base.d365fo_dimensionattribute AS da
# MAGIC     ON dav.dimensionattribute = da.recid
# MAGIC LEFT JOIN Base.d365fo_dimensionfinancialtag AS dat
# MAGIC   ON dav.entityinstance = dat.recid
# MAGIC LEFT JOIN DfBusinesslevel1 dfbl1
# MAGIC     ON dfbl1.BusinessUnitLevel2Code = dav.displayvalue
# MAGIC WHERE da.name = 'Formaal'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_business_unit_df')

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
