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

# ### Example of how to explode a nested JSON to more data frames

# CELL ********************


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit


def split_orders_transformation(df: DataFrame) -> list[DataFrame]:

    customers_mapping = {
        "customer.id": "customer_id",
        "customer.name": "name",
        "customer.email": "email",
        "customer.phone": "phone",
    }

    address_mapping = {
        "customer.id": "customer_id",
        "customer.address.street": "street",
        "customer.address.city": "city",
        "customer.address.state": "state",
        "customer.address.zip": "zip",
        "customer.address.country": "country",
    }

    orders_mappings = {
        "order.orderId": "order_id",
        "order.date": "date",
        "order.total": "total",
    }

    order_items_orders_mappings = {
        "item.itemId": "item_id",
        "item.name": "item_name",
        "item.price": "price",
        "item.quantity": "quantity",
    }

    customer_keys = ["customer_id"]
    address_keys = ["customer_id"]
    orders_keys = ["order_id"]
    item_keys = ["item_id"]

    customer_df = df.select(
        [col(column).alias(alias) for column, alias in customers_mapping.items()]
    )

    address_df = df.select(
        [col(column).alias(alias) for column, alias in address_mapping.items()]
    )

    orders_df = df.select(
        col("customer.id").alias("customer_id"),
        explode(col("customer.orders")).alias("order"),
    ).select(
        [col("customer_id")]
        + [col(column).alias(alias) for column, alias in orders_mappings.items()]
    )

    items_df = (
        df.select(
            col("customer.id").alias("customer_id"),
            explode(col("customer.orders")).alias("order"),
        )
        .select(
            col("customer_id"),
            col("order.orderId").alias("order_id"),
            explode(col("order.items")).alias("item"),
        )
        .select(
            [col("customer_id"), col("order_id")]
            + [
                col(column).alias(alias)
                for column, alias in order_items_orders_mappings.items()
            ]
        )
    )

    return [
        ("customer", customer_df, customer_keys),
        ("customer_address", address_df, address_keys),
        ("customer_orders", orders_df, orders_keys),
        ("customer_orders_items", items_df, item_keys),
    ]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Example of how to use services like AI Foundry, Cognitive services etc. to translate audio to text and do sentiment analysis.

# CELL ********************

from pyspark.sql.functions import udf, col
from synapse.ml.io.http import HTTPTransformer, http_udf
from requests import Request
from pyspark.sql.functions import lit
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col
import os

from synapse.ml.core.platform import *

from synapse.ml.services import *

# A general Azure AI services key for Text Analytics, Vision and Document Intelligence (or use separate keys that belong to each service)
service_key = "<YOUR-KEY-VALUE>"  # Replace <YOUR-KEY-VALUE> with your Azure AI service key, check prerequisites for more details
service_location = "westeurope"


def sentiment_analysis_from_wav(df: DataFrame):

    print("Setting up sentiment analysis from wav audio files")

    speech_to_text = (
        SpeechToTextSDK()
        .setSubscriptionKey(service_key)
        .setLocation(service_location)
        .setOutputCol("text")
        .setAudioDataCol("content")
        .setLanguage("en-US")
        .setFormat("detailed")
    )

    text_df = (
        speech_to_text.transform(df)
        .drop("content")
        .withColumn("text", col("text.DisplayText"))
    )

    sentiment = (
        TextSentiment()
        .setTextCol("text")
        .setLocation(service_location)
        .setSubscriptionKey(service_key)
        .setOutputCol("sentiment")
        .setErrorCol("sentiment_error")
        .setLanguage("en-US")
    )

    # Show the results of your text query in a table format
    text_with_sentiment_df = sentiment.transform(text_df).withColumn(
        "sentiment", col("sentiment.document.sentiment")
    )

    return [("AquaVilla_Customer_Feedback", text_with_sentiment_df, [])]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Example of how to explode Business Central entities. It should work with for all of them.

# CELL ********************

from pyspark.sql.functions import explode

bc_entity_keys_mapping = {
    "Currency": ["SystemId"],
    "Customer": ["Id"]
}

def explode_bc_transformation(entity: str, df: DataFrame) -> list[DataFrame]:
    
    keys = bc_entity_keys_mapping.get(entity)
    if keys is None:
        keys = []

    df = df.withColumn("value", explode("value")).select("value.*", "company")

    return [(entity, df, keys)]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Example of how to explode XML, while we are waiting for Fabric to adopt Spark 4.0

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, explode, col
from pyspark.sql.types import (
    ArrayType, StructType, StructField,
    LongType, StringType
)
import pandas as pd
import xml.etree.ElementTree as ET

customer_schema = StructType([
    StructField("CustomerID", LongType(), nullable=False),
    StructField("FirstName",  StringType(), nullable=True),
    StructField("LastName",   StringType(), nullable=True),
    StructField("Email",      StringType(), nullable=True),
    StructField("Phone",      StringType(), nullable=True),
    StructField("Address", StructType([
        StructField("Street",     StringType(), nullable=True),
        StructField("City",       StringType(), nullable=True),
        StructField("State",      StringType(), nullable=True),
        StructField("PostalCode", StringType(), nullable=True),
        StructField("Country",    StringType(), nullable=True),
    ]), nullable=True),
])

@pandas_udf(ArrayType(customer_schema))
def parse_customers(xml_series: pd.Series) -> pd.Series:
    def parse_xml(xml_str):
        if not xml_str:
            return []
        try:
            root = ET.fromstring(xml_str)
            out = []
            for cust in root.findall("Customer"):
                addr = cust.find("Address")
                rec = {
                    "CustomerID": int(cust.findtext("CustomerID") or 0),
                    "FirstName":  cust.findtext("FirstName"),
                    "LastName":   cust.findtext("LastName"),
                    "Email":      cust.findtext("Email"),
                    "Phone":      cust.findtext("Phone"),
                    "Address": {
                        "Street":     addr.findtext("Street") if addr is not None else None,
                        "City":       addr.findtext("City") if addr is not None else None,
                        "State":      addr.findtext("State") if addr is not None else None,
                        "PostalCode": addr.findtext("PostalCode") if addr is not None else None,
                        "Country":    addr.findtext("Country") if addr is not None else None,
                    }
                }
                out.append(rec)
            return out
        except Exception:
            return []
    return xml_series.apply(parse_xml)

def customers_transformation(df):
    customer_df = (
        df.withColumn("customers", parse_customers(col("xml")))
          .withColumn("customer", explode(col("customers")))
          .select(
              col("customer.CustomerID"),
              col("customer.FirstName"),
              col("customer.LastName"),
              col("customer.Email"),
              col("customer.Phone"),
              col("customer.Address.Street"),
              col("customer.Address.City"),
              col("customer.Address.State"),
              col("customer.Address.PostalCode"),
              col("customer.Address.Country"),
          )
    )
    return [
        ("customers_large", customer_df, ["CustomerID"]),
    ]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### DaluxAPI JSON Processing Functions
# 
# These functions handle the transformation of nested JSON data from Dalux APIs.

# CELL ********************

from pyspark.sql.functions import col, explode, lit, when, expr, size, first, udf, concat_ws
from pyspark.sql import DataFrame
import re
import json
from notebookutils import mssparkutils

# Custom functions for JSON object processing from DaluxAPI
def flatten(df):
    # Initial flattening pass
    flat_cols = []
    for column_name, dtype in df.dtypes:
        if dtype.startswith("struct"):
            # If it's a struct, flatten it
            flat_cols += [col(f"{column_name}.{nested}").alias(f"{column_name}_{nested}") 
                          for nested in df.select(f"{column_name}.*").columns]
        else:
            # Otherwise, add the column as is
            flat_cols.append(col(column_name))
    
    # Create a new DataFrame with the flattened columns
    df_flat = df.select(flat_cols)
    
    # Check if there are still struct fields left to flatten
    has_struct = any(dtype.startswith("struct") for _, dtype in df_flat.dtypes)
    
    # If there are struct fields, recursively flatten the DataFrame
    if has_struct:
        return flatten(df_flat)
    
    # Return the fully flattened DataFrame
    return df_flat

# Function to remove invalid characters and clean "items_data_" prefix from column names
def clean_column_names(df: DataFrame) -> DataFrame:
    for col_name in df.columns:
        # Remove the "items_data_" prefix if it exists
        clean_col_name = re.sub(r'^items_data_', '', col_name)
        
        # Remove invalid characters from the column name
        clean_col_name = re.sub(r'[ -\/.,;{}()\n\t=]', '', clean_col_name)

        # Translate DK characters (æøå) in the column name
        dk_char_mapping = {'æ':'ae','ø':'oe','å':'aa'}
        for k,v in dk_char_mapping.items():
            clean_col_name = clean_col_name.replace(k,v)
        
        # Rename the column if the cleaned name is different
        if clean_col_name != col_name:
            df = df.withColumnRenamed(col_name, clean_col_name)
    
    return df

def handle_user_defined_fields(df):
    # Check if the userDefinedFields column exists
    if "items_data_userDefinedFields" in df.columns:
        # Check if the userDefinedFields is not null or empty
        df = df.withColumn("userDefinedFields_exists", 
                           when((col("items_data_userDefinedFields").isNotNull()) & (size(col("items_data_userDefinedFields")) > 0), True)
                           .otherwise(False))
        
        # Proceed only if the userDefinedFields array exists and is not empty
        if df.filter(col("userDefinedFields_exists")).count() > 0:
            # Explode the userDefinedFields array into multiple rows
            exploded_df = df.withColumn("userDefinedField", explode(col("items_data_userDefinedFields")))
            
            # Now pivot the 'name' to create new columns without prefix
            pivot_df = exploded_df.groupBy(*[c for c in exploded_df.columns if c != 'userDefinedField']) \
                                  .pivot("userDefinedField.name") \
                                  .agg(first("userDefinedField.value"))
            
            # Rename the columns by prefixing them with "userDefinedFields_"
            for col_name in pivot_df.columns:
                if col_name not in df.columns:  # Only rename newly created columns
                    pivot_df = pivot_df.withColumnRenamed(col_name, f"userDefinedFields_{col_name}")
            
            # Drop the temporary existence check column
            pivot_df = pivot_df.drop("userDefinedFields_exists")
            return pivot_df
        else:
            # If no userDefinedFields exist or are empty, return the original DataFrame
            return df.drop("userDefinedFields_exists")
    else:
        # If the userDefinedFields column doesn't exist, just return the original DataFrame
        return df

# Helper function to extract date (year/month/day) from the file path
def extract_date_from_path(path):
    match = re.search(r'year=(\d{4})/month=(\d{2})/day=(\d{2})', path)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month}-{day}"
    return None

# Function to recursively get the latest file based on the folder date and file modification time
def get_latest_file_by_date_and_modification(root_path):
    # Recursively list all files and directories under the root path
    all_files = []
    directories_to_scan = [root_path]
    
    while directories_to_scan:
        current_dir = directories_to_scan.pop()
        items = mssparkutils.fs.ls(current_dir)
        
        for item in items:
            if item.isDir:  # If it's a directory, add it to the list to scan
                directories_to_scan.append(item.path)
            else:  # If it's a file, append it along with its modification time
                all_files.append((item.path, item.modifyTime))
    
    # Filter out files with date pattern (year=yyyy/month=mm/day=dd)
    dated_files = [(file, extract_date_from_path(file), mod_time) for file, mod_time in all_files if extract_date_from_path(file)]
    
    # Sort the files based on extracted date first and then modification time (newest first)
    dated_files.sort(key=lambda x: (x[1], x[2]), reverse=True)
    
    # Return the path of the latest file, if any exist
    if dated_files:
        return dated_files[0][0]  # Return the file path of the latest file
    else:
        return None

def handle_userRegion_column(df):
    # Check if "items_data_userRegion" column exists
    if "items_data_userRegion" in df.columns:
        # Apply concat_ws to flatten the array into a comma-separated string
        df = df.withColumn("userRegion", concat_ws(",", col("items_data_userRegion")))
    else:
        # Optionally, if the column doesn't exist, do nothing or print a message
        print("Column 'items_data_userRegion' does not exist, skipping concat_ws.")
    
    return df

def process_dalux_data(raw_df: DataFrame) -> DataFrame:
    """
    Process raw Dalux API JSON data through all necessary transformation steps.
    
    This function applies all the necessary transformations to convert raw nested
    JSON data from Dalux APIs into a flattened, clean DataFrame ready for analysis.
    
    Args:
        raw_df: Raw DataFrame containing the Dalux JSON data with nested structures
        
    Returns:
        DataFrame: A processed DataFrame with flattened structure and cleaned column names
    """
    # Define columns to drop after processing
    cols_to_drop = [
        'items_data_userDefinedFields', 
        'links',
        'items_links',
        'items_data_userRegion',
        'metadata_bookmark', 
        'metadata_limit', 
        'metadata_nextBookmark', 
        'metadata_totalItems', 
        'responseCode', 
        'responseMessage',
        'landing_created_date',
        'landing_load_type',
        'year',
        'month',
        'day',
        'date',
        # Dalux Asset specifikke under her
        'userDefinedFields_Forsyningskabler',
        'userDefinedFields_AMPtilraadighed',
        'userDefinedFields_ActiveRoomTempSetpt?',
        'userDefinedFields_Adgangsforhold',
        'userDefinedFields_Aflaesningsform',
        'userDefinedFields_Afmaerkningsnummer',
        'userDefinedFields_AfmaerkningsnummerFyr',
        'userDefinedFields_Aftagernr',
        'userDefinedFields_AirflowMeter?',
        'userDefinedFields_Ankerklods',
        'userDefinedFields_Antal',
        'userDefinedFields_Bline',
        'userDefinedFields_BROmregnetgeografiskposition',
        'userDefinedFields_BSnummerHOFOR',
        'userDefinedFields_BallastKg',
        'userDefinedFields_Bemaerkninger',
        'userDefinedFields_Betaling',
        'userDefinedFields_Betjening',
        'userDefinedFields_BoilerHeatingCapacitykW',
        'userDefinedFields_Bolvaerkstype',
        'userDefinedFields_Bredde',
        'userDefinedFields_Braendetid',
        'userDefinedFields_ByHavnsejendom',
        'userDefinedFields_Boejetype',
        'userDefinedFields_ChillerkWkWDesign',
        'userDefinedFields_Dagmaerkebaakevedfyret',
        'userDefinedFields_DanskFyrlistenummer',
        'userDefinedFields_Datumomregnetgeografiskposition',
        'userDefinedFields_Debitornummer',
        'userDefinedFields_Driftsansvarlig',
        'userDefinedFields_Daekseltype',
        'userDefinedFields_Ejer',
        'userDefinedFields_Ejerskab',
        'userDefinedFields_EndoflifeAPI',
        'userDefinedFields_Erderenredningsline',
        'userDefinedFields_Erderhaandtagpaakaj',
        'userDefinedFields_Erstigentilgaengelig',
        'userDefinedFields_ExhaustFanRatedFlowLs',
        'userDefinedFields_ExhaustFanRatedPowerkW',
        'userDefinedFields_Fabrikat',
        'userDefinedFields_FanRatedFlowLs',
        'userDefinedFields_FanRatedPowerkW',
        'userDefinedFields_Farve',
        'userDefinedFields_FarvepaaStige',
        'userDefinedFields_FilterDifferentialPressureMaxPa',
        'userDefinedFields_Form',
        'userDefinedFields_ForsikretmedAMP',
        'userDefinedFields_Forsikring',
        'userDefinedFields_Forsyner',
        'userDefinedFields_Forsyningskabler',
        'userDefinedFields_Forventetlevetidaar',
        'userDefinedFields_FuelOilconsumptionmeter',
        'userDefinedFields_FyrbygningensellerpaelensFARVE',
        'userDefinedFields_FyrbygningensellerpaelensFORM',
        'userDefinedFields_FyrbygningensellerpaelensHØJDE',
        'userDefinedFields_FyretetableretogtaendtdatoogevtEfSnr',
        'userDefinedFields_Fyretklartilattaendesaendres',
        'userDefinedFields_FyretaendretdatoogevtEfSnr',
        'userDefinedFields_FyretsXkoordinat',
        'userDefinedFields_FyretsYkoordinat',
        'userDefinedFields_Fyretsdagligetilsynsfoerende',
        'userDefinedFields_Fyretsejer',
        'userDefinedFields_Fyretsfunktion',
        'userDefinedFields_Fyretslysevne',
        'userDefinedFields_FyretslysevneogsynsviddeiCandelacd',
        'userDefinedFields_Fyretslysevneogsynsviddeisoemilsm',
        'userDefinedFields_Fyretslysvinklersehjaelpetekst',
        'userDefinedFields_Fyretsnavn',
        'userDefinedFields_Fyrkarakter:Karakter',
        'userDefinedFields_Fyrkarakter:Lysmoerke',
        'userDefinedFields_GMSModulnummer',
        'userDefinedFields_Garantipaaarbejdeaar',
        'userDefinedFields_HEXEfficiencyFractionkW',
        'userDefinedFields_Hvordanerstigenbeskyttet',
        'userDefinedFields_InstalationsdatoÅrstal',
        'userDefinedFields_InstallationsaarAPI',
        'userDefinedFields_Kasse',
        'userDefinedFields_KommentarAPI',
        'userDefinedFields_Kontakoplysninger',
        'userDefinedFields_Konteringsnummer',
        'userDefinedFields_Kontroldato',
        'userDefinedFields_Koteplacering',
        'userDefinedFields_Kundenummer',
        'userDefinedFields_Kaedekontrolmaaltimm',
        'userDefinedFields_Kaededimension',
        'userDefinedFields_Kaedelaengde',
        'userDefinedFields_Lanternensflammehoejde',
        'userDefinedFields_LanterneudstyrFabrikant',
        'userDefinedFields_LanterneudstyrType',
        'userDefinedFields_Lejemaal',
        'userDefinedFields_LgOmregnetgeografiskposition',
        'userDefinedFields_Lyspaaredningsstiger',
        'userDefinedFields_Lysboeje',
        'userDefinedFields_LysgiverensartogspecifikationEnergiforsyning:',
        'userDefinedFields_LysgiverensartogspecifikationEventuelreserve',
        'userDefinedFields_LysgiverensartogspecifikationLampetype',
        'userDefinedFields_Lysgiverensartogspecifikationampere',
        'userDefinedFields_Lysgiverensartogspecifikationvolt',
        'userDefinedFields_Lysgiverensartogspecifikationwatt',
        'userDefinedFields_Lyskarakter',
        'userDefinedFields_Laengde',
        'userDefinedFields_Materiale',
        'userDefinedFields_MaterialeAPI',
        'userDefinedFields_MaxAMP',
        'userDefinedFields_Modelnummer',
        'userDefinedFields_Moringkontrolmaaltimm',
        'userDefinedFields_Maaleraflaeses',
        'userDefinedFields_Maalernr',
        'userDefinedFields_Maalertype',
        'userDefinedFields_MaaltmedGPS',
        'userDefinedFields_Maanedforacontoregning',
        'userDefinedFields_Maanedforaarsregning',
        'userDefinedFields_Noegletype',
        'userDefinedFields_Omraade',
        'userDefinedFields_Oplandpaapumpe',
        'userDefinedFields_Overfladebehandling',
        'userDefinedFields_OverholdelseafstandardforByHavn',
        'userDefinedFields_Powermeter',
        'userDefinedFields_Produktnavn',
        'userDefinedFields_PumpRatedFlowLs',
        'userDefinedFields_PumpRatedPowerkW',
        'userDefinedFields_SikkerhedAPI',
        'userDefinedFields_Sjaekelkontrolmaaltimm',
        'userDefinedFields_SkifteintervalÅrstal',
        'userDefinedFields_SkumringsrelaeFabrikant',
        'userDefinedFields_SkumringsrelaeType',
        'userDefinedFields_SourceAHU',
        'userDefinedFields_SpatialMapID',
        'userDefinedFields_Stage',
        'userDefinedFields_StandAPI',
        'userDefinedFields_Stoerrelse',
        'userDefinedFields_Supplerendeafmaerkningsellernavigationsudstyretableretvedelleriforbindelsemedfyret',
        'userDefinedFields_SupplyFanRatedFlowLs',
        'userDefinedFields_SupplyFanRatedPowerkW',
        'userDefinedFields_Systemdatumofficieltgodkendtkoordinatprojektion',
        'userDefinedFields_Soekortnr',
        'userDefinedFields_TegngiverFabrikant',
        'userDefinedFields_TegngiverType',
        'userDefinedFields_TilstandBelaegning',
        'userDefinedFields_TilstandEltavler',
        'userDefinedFields_TilstandSikkerhed',
        'userDefinedFields_Topbetegnelse',
        'userDefinedFields_Type',
        'userDefinedFields_TypeModel',
        'userDefinedFields_Typeafredingsline',
        'userDefinedFields_Typeafstige',
        'userDefinedFields_Taendskab',
        'userDefinedFields_Underreperation',
        'userDefinedFields_Vanddybe',
        'userDefinedFields_x_sys34',
        'userDefinedFields_y_sys34'
    ]
    
    # Step 1: Explode items array and flatten nested structures
    df_flattened = flatten(raw_df.withColumn("items", explode(col("items"))))
    if 'userDefinedFields_Kontroldato' in df_flattened.columns:
        df_flattened = df_flattened.drop('userDefinedFields_Kontroldato')

    # Step 2: Handle special columns
    df_user_fields = handle_user_defined_fields(df_flattened)
    if 'userDefinedFields_Kontroldato' in df_user_fields.columns:
        df_user_fields = df_user_fields.drop('userDefinedFields_Kontroldato')

    df_user_region = handle_userRegion_column(df_user_fields)
    if 'userDefinedFields_Kontroldato' in df_user_region.columns:
        df_user_region = df_user_region.drop('userDefinedFields_Kontroldato')

    # Step 3: Drop unwanted columns
    # Only drop columns that actually exist in the DataFrame
    cols_to_drop_filtered = [c for c in cols_to_drop if c in df_user_region.columns]
    df_dropped = df_user_region.drop(*cols_to_drop_filtered)
    if 'userDefinedFields_Kontroldato' in df_dropped.columns:
        df_dropped = df_dropped.drop('userDefinedFields_Kontroldato')

    # Step 4: Clean column names
    df_cleaned = clean_column_names(df_dropped)
    if 'userDefinedFields_Kontroldato' in df_cleaned.columns:
        df_cleaned = df_cleaned.drop('userDefinedFields_Kontroldato')

    return df_cleaned

def process_dalux_data_buildings(raw_df: DataFrame) -> DataFrame:

    df_cleaned = process_dalux_data(raw_df)

    return [
        ("dalux_buildings", df_cleaned, ["id"])
    ]

def process_dalux_data_estates(raw_df: DataFrame) -> DataFrame:

    df_cleaned = process_dalux_data(raw_df)

    return [
        ("dalux_estates", df_cleaned, ["id"])
    ]

def process_dalux_data_floors(raw_df: DataFrame) -> DataFrame:

    df_cleaned = process_dalux_data(raw_df)

    return [
        ("dalux_floors", df_cleaned, ["id"])
    ]

def process_dalux_data_locations(raw_df: DataFrame) -> DataFrame:

    df_cleaned = process_dalux_data(raw_df)

    return [
        ("dalux_locations", df_cleaned, ["id"])
    ]

def process_dalux_data_lots(raw_df: DataFrame) -> DataFrame:

    df_cleaned = process_dalux_data(raw_df)

    return [
        ("dalux_lots", df_cleaned, ["id"])
    ]

def process_dalux_data_rooms(raw_df: DataFrame) -> DataFrame:

    df_cleaned = process_dalux_data(raw_df)

    return [
        ("dalux_rooms", df_cleaned, ["id"])
    ]

def process_dalux_data_assets(raw_df: DataFrame) -> DataFrame:
    
    df_cleaned = process_dalux_data(raw_df)

    return [
        ("dalux_assets", df_cleaned, ["id"])
    ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Spirii JSON Processing Functions
# These functions handle the transformation of nested JSON data from Spirii APIs.

# CELL ********************

def flatten_spirii(df):

    flat_cols = []
    nested_cols = []

    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            nested_cols.append(field.name)
        elif isinstance(field.dataType, ArrayType):
            nested_cols.append(field.name)
        else:
            flat_cols.append(field.name)

    # Select all columns
    df_flat = df.select(flat_cols + nested_cols)

    # Handle nested columns
    for col_name in nested_cols:
        dtype = [f.dataType for f in df.schema.fields if f.name == col_name][0]
        if isinstance(dtype, StructType):
            # Flatten struct columns
            new_cols = [col(f"{col_name}.{k.name}").alias(f"{col_name}_{k.name}") for k in dtype.fields]
            df_flat = df_flat.select("*", *new_cols).drop(col_name)
        elif isinstance(dtype, ArrayType):
            # Explode arrays
            df_flat = df_flat.withColumn(col_name, explode_outer(col(col_name)))

    # Check if there are still nested columns to flatten
    if any(isinstance(f.dataType, (StructType, ArrayType)) for f in df_flat.schema.fields):
        return flatten_spirii(df_flat)
    else:
        return df_flat


def process_spirii_data(df: DataFrame) -> DataFrame:

 # Define columns to drop after processing
    cols_to_drop = [
        #locations
        'directions_language',
        #transactions
        'roamingDetails_sessionPayload_AuthorizationStatus',
        'roamingDetails_sessionPayload_CPOPartnerSessionID',
        'roamingDetails_sessionPayload_EMPPartnerSessionID',
        'roamingDetails_sessionPayload_EvseID',
        'roamingDetails_sessionPayload_PartnerProductID',
        'roamingDetails_sessionPayload_ProviderID',
        'roamingDetails_sessionPayload_SessionID',
        'roamingDetails_sessionPayload_createdManually',
        'roamingDetails_sessionPayload_StatusCode_AdditionalInfo',
        'roamingDetails_sessionPayload_StatusCode_Code',
        'roamingDetails_sessionPayload_StatusCode_Description',
        'roamingDetails_sessionPayload_Identification_RemoteIdentification_EvcoID',
        'roamingDetails_sessionPayload_AuthorizationStopIdentifications_RFIDIdentification_EvcoID',
        'roamingDetails_sessionPayload_AuthorizationStopIdentifications_RFIDIdentification_ExpiryDate',
        'roamingDetails_sessionPayload_AuthorizationStopIdentifications_RFIDIdentification_PrintedNumber',
        'roamingDetails_sessionPayload_AuthorizationStopIdentifications_RFIDIdentification_RFID',
        'roamingDetails_sessionPayload_AuthorizationStopIdentifications_RFIDIdentification_UID',
        'roamingDetails_sessionPayload_AuthorizationStopIdentifications_RFIDMifareFamilyIdentification_UID',
        'roamingDetails_sessionPayload_AuthorizationStopIdentifications_RemoteIdentification_EvcoID',
        #charge-records
        'price_breakdown_steps_amount',
        'price_breakdown_steps_consumed',
        'price_breakdown_steps_end',
        'price_breakdown_steps_spotPrice',
        'price_breakdown_steps_start',
        'price_breakdown_steps_spotPriceDetails_margin',
        'price_breakdown_steps_spotPriceDetails_noMargin'
            ]


    # Step 1: Explode items array and flatten nested structures
    df = df.withColumn("source_file", input_file_name())
    
    df_flat = flatten_spirii(df)

    # Step 2: Drop unwanted columns
    # Only drop columns that actually exist in the DataFrame
    cols_to_drop_filtered = [c for c in cols_to_drop if c in df_flat.columns]
    df_dropped = df_flat.drop(*cols_to_drop_filtered)

    # Step 3: Deduplicate
    df_cleaned = df_dropped.dropDuplicates()

    return df_cleaned

def process_spirii_data_locations(df: DataFrame) -> DataFrame:

    #Date inherited from filename used as ModifyDateColumn in meta.ObjectDefinitionsBase
    df = df.withColumn("TransformModifiedDate", to_date(regexp_extract(input_file_name(), r".*_(\d{8})_\d+\.json$", 1),"yyyyMMdd"))

    #Only include locations with charge boxes
    df = df.filter(size(col("evses"))>0)

    df_cleaned = process_spirii_data(df)

    return [
        ("spirii_locations", df_cleaned, ["evses_evseId"])
    ]

def process_spirii_data_transactions(df: DataFrame) -> DataFrame:

    df_cleaned = process_spirii_data(df)

    return [
        ("spirii_transactions", df_cleaned, ["id"])
    ]

def process_spirii_data_chargerecords(df: DataFrame) -> DataFrame:

    df_cleaned = process_spirii_data(df)

    return [
        ("spirii_chargerecords", df_cleaned, ["_id"])
    ]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
