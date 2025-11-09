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

# <center>
# 
# ![AquaVilla400.png](attachment:c102a520-c68e-4c82-b5e2-911b331652f1.png)
# 
# # AquaVilla - Functions version 1.10.0
# </center>
# 
# The AquaVilla_Functions notebook holds general functions used for reading dataframes, writing dataframes as well as utils for manipulating dataframes etc.
# 
# Use the %run command to include the function notebook in any relevant Spark session. After it has been included you can refer to any of the functions in the notebook. Ie. get_meta_from_sql_server, read_excel etc.
# ```
# %run AquaVilla_Functions
# ```


# ATTACHMENTS ********************

# ATTA {
# ATTA   "c102a520-c68e-4c82-b5e2-911b331652f1.png": {
# ATTA     "image/png": "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"
# ATTA   }
# ATTA }

# MARKDOWN ********************

# ## Configuration import
# Imports framework configuration

# CELL ********************

%run AquaVilla_Config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Module import
# Import modules used by generic framework functions etc.

# CELL ********************

from datetime import datetime, date, timedelta
from enum import Enum
import json
import re
import os
import pprint
from typing import Dict, List, Literal
import warnings
from pyspark.sql.functions import col, input_file_name, regexp_extract, udf, lit
from pyspark.sql.types import ArrayType, StringType
import urllib.parse


import pandas as pd
import pyodbc
from pyspark.sql import DataFrame, Row, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    LongType,
    ByteType,
    IntegerType,
    ShortType,
    FloatType,
    DoubleType,
    BinaryType,
    BooleanType,
    StringType,
    DateType,
    DecimalType,
)
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from notebookutils import mssparkutils
import sempy

warnings.filterwarnings("ignore")


class SCDOption(Enum):
    """
    Enum representing different types of Slowly Changing Dimension (SCD) strategies.

    SCD1: Type 1 Slowly Changing Dimension strategy.
    SCD2: Type 2 Slowly Changing Dimension strategy.
    """

    SCD1 = "SCD1"
    SCD2 = "SCD2"


class AttrDict(dict):
    """
    Dictionary subclass allowing attribute-style access to its keys.

    This class provides attribute-style access to its keys, allowing for more
    concise and readable code. If the key is present in the dictionary,
    accessing it as an attribute will return the corresponding value.

    Example:
        attrs = AttrDict({'a': 1, 'b': 2})
        print(attrs.a)  # Output: 1
        print(attrs.b)  # Output: 2
    """

    def __getattr__(self, attr):
        if attr in self:
            value = self[attr]
            if isinstance(value, dict):
                return AttrDict(value)
            return value
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{attr}'"
        )

    def __hash__(self):
        return hash(frozenset(self.items()))


## Spark settings for optimization
spark.conf.set("spark.sql.caseSensitive", True)
spark.conf.set("spark.sql.parquet.vorder.enabled", True)  # Enable v-order write (default true)
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", True)  # Enable automatic delta optimized write
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") #Handling old-style int96 date types, in case if that is the data type
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") #Making sure we are always writing correct type og date types


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Private: Constants

# CELL ********************


default_audit_columns_dict = AttrDict(default_audit_columns_config)

default_audit_columns = [
    default_audit_columns_dict.created_date.name,
    default_audit_columns_dict.modified_date.name,
    default_audit_columns_dict.is_deleted.name,
]

default_scd2_columns_dict = AttrDict(default_scd2_columns_config)

default_scd2_columns = [
    default_scd2_columns_dict.valid_from.name,
    default_scd2_columns_dict.valid_to.name,
    default_scd2_columns_dict.is_current.name,
]

default_validation_columns_dict = AttrDict(default_validation_columns_config)

default_landing_audit_columns_dict = AttrDict(default_landing_audit_columns_config)

default_landing_partition_columns_dict = AttrDict(default_landing_partition_columns_config)

default_landing_audit_columns = [
    default_landing_audit_columns_dict.created_date.name,
    default_landing_audit_columns_dict.key_columns.name,
    default_landing_audit_columns_dict.load_type.name,
]

default_landing_partition_columns = [
    default_landing_partition_columns_dict.year.name,
    default_landing_partition_columns_dict.month.name,
    default_landing_partition_columns_dict.day.name,
    default_landing_partition_columns_dict.run_id.name,
    default_landing_partition_columns_dict.load_type.name,                
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Private: Handling and building meta data
# Functions for reading data from files. The reader functions currently supports Parquet, Excel, JSON and CSV file types.
# For parquet it supports full read  as well as reading the last full partition + the following deltas.
# This cell also contains a function for reading metadata from the metadata database.

# CELL ********************

_META_DATA_SQL_COLUMNS = [
    "ID",
    "SourcePath",
    "SourceType",
    "SourceReadBehavior",
    "ReadOptions",
    "DestinationTable",
    "DestinationWriteBehavior",
    "Keys",
    "ModifyDateColumn",
    "PartitionBy",
    "ProjectedColumns",
    "Translations",
    "ReadOptions",
    "CustomTransformation",
    "RecreateFlag",
    "IncludeFlag",
]


def add_read_auto(df: DataFrame, checkpoint: datetime) -> tuple[DataFrame, datetime]:
    
    required_partitions = ["load_type", "year", "month", "day", "run_id"]

    if not all(partition in df.columns for partition in required_partitions):
        raise Exception(f"The landing files are missing the required partitions: {required_partitions}.\nPlease ensure you're using the an Ingestion Framework outputting to the folder structure:\n`<SourceObjectName>/data/load_type=<load_type>/year=<year>/month=<month>/day=<day>/run_id=<run_id>`")

    print(f"Auto-loading files based on latest checkpoint: {checkpoint}")

    # Coarse filter on year, month and day
    df = df.filter(
        F.make_date(F.col("year"), F.col("month"), F.col("day")) >= checkpoint.date()
    )

    # Add a column for the ingest timestamp, which is used for the fine grained filtering
    df = df.withColumn(
        "__ingest_timestamp", F.to_timestamp(
            F.concat(
                F.format_string("%04d", F.col("year")),
                F.format_string("%02d", F.col("month")),
                F.format_string("%02d", F.col("day")),
                F.lit(" "),
                F.format_string("%09d" ,F.col("run_id")),
            ),
            "yyyyMMdd HHmmssSSS"
        )
    )

    df = df.filter(
        F.col("__ingest_timestamp") > checkpoint
    )

    max_full_ingest_timestamp, max_delta_ingest_timestamp = (
        df.select(
            F.max(F.when(F.col("load_type") == "full", F.col("__ingest_timestamp"))),
            F.max(F.when(F.col("load_type") == "delta", F.col("__ingest_timestamp")))
        ).collect()[0]
    )

    if max_full_ingest_timestamp is None and max_delta_ingest_timestamp is None:
        print("No new data found since", checkpoint)

    # If there has been a full load (F) after checkpoint we filter on latest full load + subsequent delta loads (D).
    # For instance, at the third run, we only keep the 2F load and the 3D load. 1D and 2D are filtered out.
    # At the fourth run, we only load 4D and 5D.
    # 
    # Time:             |-----|-----|-----|-----|-----|-----|-----|-----
    # Load number/type: 1F          1D    2D    2F    3D    4D    5D     
    # Checkpoint:       |-----------|-------------------|-------------|-
    #                   ^           ^                   ^             ^
    #                   |           |                   |             |
    #                   |           |                   |             |
    #                   1.          2.                  3.            4.
    if max_full_ingest_timestamp:

        # Although the year, month and day filters are not strictly required, and may seem redundant as we're already filtering on the created date,
        # It helps greatly, since they are partition filters. 
        df = (
            df.filter(
                (
                    (F.col("load_type") == "delta") &
                    (F.col("__ingest_timestamp") > max_full_ingest_timestamp)
                ) |
                (
                    (F.col("load_type") == "full") &
                    (F.col("__ingest_timestamp") == max_full_ingest_timestamp)
                )
            )
        )

    new_checkpoint = max(
        max_full_ingest_timestamp or datetime(1900, 1, 1),
        max_delta_ingest_timestamp or datetime(1900, 1, 1),
        checkpoint or datetime(1900, 1, 1)
    )
    
    df = df.drop("__ingest_timestamp", *required_partitions)
    return df, new_checkpoint


def _read_auto_parquet(path: str, checkpoint: datetime) -> tuple[DataFrame, datetime]:

    print("Reading all files after checkpoint", checkpoint)

    df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(f"{path}/data")
    )

    return add_read_auto(df, checkpoint)


def _read_auto_csv(path: str, read_options: dict[str, str], checkpoint: datetime) -> tuple[DataFrame, datetime]:

    print("Reading all files after checkpoint", checkpoint)
    schema_path = f"/lakehouse/default/{path}/meta/schema.json"
    data_path = f"{path}/data"

    print(f"Reading csv with {read_options}")


    if read_options.get("inferSchema", False) == True:
        df = (
            spark.read
            .options(**read_options)
            .csv(data_path)
        )

        return add_read_auto(df, checkpoint)

    if not os.path.isfile(schema_path):
        raise Exception(f"""No schema file found at `{schema_path}`, and infer schema is not set, or set to false.\nEnsure you've selected the right default lakehouse.\nIf you want to infer schema, set `"inferSchema":true` in the ReadOptions.""")
    
    with open(schema_path, 'r') as file:
        schema_as_json_string: str = file.read()

    schema = StructType.fromJson(json.loads(schema_as_json_string))

    df = (
        spark.read
        .schema(schema)
        .options(**read_options)
        .csv(data_path)
    )

    return add_read_auto(df, checkpoint)



def _read_excel(path):
    pd_df = pd.read_excel(f"/lakehouse/default/{path}")
    return spark.createDataFrame(pd_df)


def _read_auto_json(path: str, checkpoint: datetime) -> tuple[DataFrame, datetime]:

    print("Reading all files after checkpoint", checkpoint)
    data_path = f"{path}/data"

    print(f"Reading json")

    df = spark.read.option("multiline", "true").json(data_path)

    return add_read_auto(df, checkpoint)


def _read_auto_wav(path: str, checkpoint: datetime) -> tuple[DataFrame, datetime]:

    print("Reading all files after checkpoint", checkpoint)
    data_path = f"{path}/data"

    print(f"Reading wav files")

    df = spark.read.format("binaryFile") \
        .option("pathGlobFilter", "*.wav") \
        .option("recursiveFileLookup", "false") \
        .load(data_path) \
        .withColumnRenamed("path", "url")

    return add_read_auto(df, checkpoint)


def _read_auto_bc_json(path: str, checkpoint: datetime) -> tuple[DataFrame, datetime]:

    print("Reading all files after checkpoint", checkpoint)
    base_path = f"/lakehouse/default/{path.split('*')[0]}"
    print(f"Base path: {base_path}")

    companies = [entry.path.split('/')[-1] for entry in os.scandir(base_path) if entry.is_dir()] 
    print("Companies found:", companies)

    unioned_df = None
    last_checkpoint = None

    for company in companies:
        data_path = f"{path.replace('*', company)}/data"
        print(f"Data location for {company}: {data_path}")

        if not os.path.exists(f"/lakehouse/default/{data_path}"):
            print(f"{company} is missing data for {data_path}")
            continue

        df = spark.read.option("multiline", "true").json(data_path)

        df, last_checkpoint = add_read_auto(df, checkpoint)

#        df = df.drop(col("`@odata`.`context`"))
        df = df.withColumn("company", lit(company))

        if df.schema["value"].dataType == ArrayType(StringType(), True):
            print('Datafile is empty, skipping')
            continue

        if unioned_df is None:
            unioned_df = df
        else:
            unioned_df = unioned_df.union(df)            

    return unioned_df, last_checkpoint


def _read_auto_xml(path: str, checkpoint: datetime) -> tuple[DataFrame, datetime]:

    df = spark.read.format("Text") \
            .option("wholetext", True)\
            .option("pathGlobFilter", "*.xml") \
            .option("recursiveFileLookup", "false") \
            .load(path) \
            .withColumnRenamed("value", "xml")

    return add_read_auto(df, checkpoint)


def register_custom_readers(source: str, read_behavior: str, source_type: str, read_options, checkpoint: datetime) -> dict:
    return {}

def _get_reader_function(row: pyodbc.Row, checkpoint: datetime):
    source = row[_col("SourcePath")]
    read_behavior = row[_col("SourceReadBehavior")].lower()
    source_type = row[_col("SourceType")].lower()
    read_options = row[_col("ReadOptions")]

    if (read_behavior== "auto"):
        source_type = f"auto_{source_type}"

    if (read_behavior == "full") and (source_type == "parquet"):
        checkpoint = datetime(1900, 1, 1)
        source_type = "auto_parquet"

    if (source_type == "delta"):
        source_type = f"delta_{read_behavior}"

    if read_options:
        read_options_json = re.sub(r"""(["'0-9.]\s*),\s*}""", r"\1}", read_options)
        read_options = json.loads(read_options_json)

    custom_readers = register_custom_readers(source, read_behavior, source_type, read_options, checkpoint)

    readers = {
        "auto_parquet": lambda: _read_auto_parquet(source, checkpoint),
        "parquet": lambda: (spark.read.parquet(source), datetime(1900, 1, 1)),
        "auto_csv": lambda: _read_auto_csv(source, read_options, checkpoint),
        "csv": lambda: (spark.read.options(**read_options).csv(source), datetime(1900,1,1)),
        "xls": lambda: (_read_excel(source), datetime(1900, 1, 1)),
        "xlsx": lambda: (_read_excel(source), datetime(1900, 1, 1)),
        "json": lambda: (spark.read.option("multiline", "true").options(**read_options).json(source), datetime(1900, 1, 1)),
        "auto_json": lambda: _read_auto_json(source, checkpoint),
        "auto_wav": lambda: _read_auto_wav(source, checkpoint),
        "delta_file": lambda: (spark.read.format("delta").load(source), datetime(1900, 1, 1)),
        "delta_table": lambda: (spark.read.format("delta").table(source), datetime(1900, 1, 1)),
        "auto_bc_json": lambda: _read_auto_bc_json(source, checkpoint),
        "auto_xml_text": lambda: _read_auto_xml(source, checkpoint),
    }

    readers = readers | custom_readers

    return readers[source_type]


def _col(name: str) -> int:
    try:
        return _get_available_meta_data_sql_columns().index(name)
    except ValueError:
        return None

def _get_transformation(row: pyodbc.Row):
    if _col("CustomTransformation") is None or row[_col("CustomTransformation")] is None or  row[_col("CustomTransformation")] == '':
        return None

    function_name = row[_col("CustomTransformation")]
    function = globals().get(function_name)

    if function is None:
        raise KeyError(f"Can't find custom transformation function: {function_name}")

    if function.__code__.co_argcount == 1:
        return lambda dataframe: function(dataframe)
    else:
        return lambda entity, dataframe: function(entity, dataframe)



def _initialize_source_from_row(row: pyodbc.Row, checkpoint: datetime) -> dict:
    source = row[_col("SourcePath")]

    return {
        "ID": row[_col("ID")],
        "SourcePath": source,
        "ReadOptions": row[_col("ReadOptions")],
        "Read": _get_reader_function(row, checkpoint),
        "Destinations": {},
        "Transform": _get_transformation(row),
    }


def get_table_property(table: str, property_key: str) -> Dict:
    if spark.catalog.tableExists(f"{table}"):
        delta_table = DeltaTable.forName(spark, table)
        delta_properties_df = delta_table.detail()
        return delta_properties_df.select("properties").collect()[0]["properties"].get(property_key)
    return None


def set_table_property(table: str, property_key: str, property_value: str):
    spark.sql(f"""
        ALTER TABLE {table}
        SET TBLPROPERTIES (
            '{property_key}' = '{property_value}'
        )
    """)

def unset_table_property(table: str, property_key: str):
    spark.sql(f"""
        ALTER TABLE {table}
        UNSET TBLPROPERTIES (
            '{property_key}'
        )
    """)

def reset_table_checkpoint(table: str):
    checkpoint = datetime(1900, 1, 1).isoformat()
    set_table_property(table, "aquavilla.checkpoint.ingestTimestamp", checkpoint)   


def _add_destination_from_row(row: pyodbc.Row, recreate: bool = None) -> dict:
    projected_columns = (
        row[_col("ProjectedColumns")].split(";")
        if row[_col("ProjectedColumns")] is not None
        and row[_col("ProjectedColumns")] != ""
        else []
    )
    keys = row[_col("Keys")].split(";")

    dependencies = [
        column.replace("{", "").replace("}", "")
        for column in projected_columns
        if column.startswith("{") and column.endswith("}")
    ]

    if not list(filter(None, projected_columns)):
        projected_columns = []

    if not list(filter(None, keys)):
        keys = []

    return {
        "IsLoaded": False,
        "dependencies": dependencies,
        "DestinationWriteBehavior": row[_col("DestinationWriteBehavior")],
        "Keys": keys,
        "ModifyDateColumn": row[_col("ModifyDateColumn")],
        "PartitionBy": row[_col("PartitionBy")],
        "ProjectedColumns": projected_columns,
        "Translations": row[_col("Translations")],
        "RecreateFlag": recreate if recreate is not None else row[_col("RecreateFlag")],
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## DW utility functions
# DW functions includes various functions for looking up dimension information, assigning surrogate keys to fact dataframes etc.

# CELL ********************

############################################################
## Get all dimensions and their business and surrogate keys
############################################################
def _get_dimension_informations(lakehousename: str) -> dict:
    """
    Scans a lakehouse for dimensions, and resolves keys.

    Args:
        lakehousename (str):        Lakehouse from where to read dimension information

    Returns:
        pyspark.sql.DataFrame:      DataFrame containing information about all dimensions in the curated catalog that follow the layer structure convention.
    """

    dimension_informations = {}

    for table in (
        sqlContext.tables(lakehousename)
        .filter(f"tableName like '{DIMENSION_PREFIX}%' and namespace = '{lakehousename}'")
        .collect()
    ):
        table_name = table["namespace"] + "." + table["tableName"]

        # Collect information
        dimension_name = table["tableName"].replace(DIMENSION_PREFIX, "")
        # table_details.first()

        columns = spark.catalog.listColumns(table_name)

        primary_key_columns = [
            col.name
            for col in columns
            if col.name.endswith(DEFAULT_BUSINESSKEY_POSTFIX)
        ]
        surrogate_key_columns = [
            col.name
            for col in columns
            if col.name.endswith(DEFAULT_SURROGATEKEY_POSTFIX)
        ]

        all_column_names = [col.name for col in columns]

        scd2_flag: bool = set(default_scd2_columns).issubset(all_column_names)

        dimension_informations[dimension_name] = {
            "fully_qualified_dimension_name": table_name,
            "dimension_name": dimension_name,
            "primary_key_columns": primary_key_columns,
            "surrogate_key_columns": surrogate_key_columns,
            "scd2_flag": scd2_flag,
        }

    return dimension_informations


############################################################
## Get dimension mapping from input dataframe such as facts
############################################################


def _part_key_column(key_column: str, dimension_informations: dict = None) -> (str, str, str):
    """
    Splits the key column into the respective parts: foreign table name, role and key identifier.
    It iteratively searches the dimensions available in `_get_dimension_informations()` by `dimension_name` backwards.

    Args:
        key_column (str):           Key column to split into parts

    Returns:
        tuple (str, str, str):      Tuple of strings, where the first item is the foreign table, the second part is the role (if any) and the third part is the key identifier.

    Raises:
        ValueError:                 If the `key_column` is not properly formatted.
        ValueError:                 If the dimension referenced in `key_column` could not be resolved, i.e. does not exist in `_get_dimension_informations()`
    
    Examples:
        Given we have a dimension called `product`.
        >>> _part_key_column("product_key")
        ("product", None, "key")
        
        Given we have a dimension called `resort`.
        >>> _part_key_column("resort_manager_key")
        ("resort", "manager", "key")         
    """
    if dimension_informations is None:
        dimension_informations = _get_dimension_informations(LAKEHOUSENAME_CURATED)

    parts = key_column.lower().split("_")

    # E.g. parts can't be split on `_`.
    if len(parts) == 1:
        raise ValueError(f"Invalid format for key_column `{key_column}`. Must have the format `{{dimension}}_{{key}}` or `{{dimension}}_{{role}}_{{key}}`, e.g. `product_key`.")

    key_identifier = parts[-1]
    possible_foreign_table_name = "_".join(parts[0:-1])
    role_parts: list[str] = []

    available_foreign_table_names = [dimension_details["dimension_name"] for dimension_details in dimension_informations.values()]

    searched_foreign_table_names: list[str] = []

    # Iterate backwards over parts 
    for i in range(len(parts) - 1, 0, -1):
        possible_foreign_table_name = "_".join(parts[:i])

        if possible_foreign_table_name in available_foreign_table_names:
            break

        if i == 0:
            raise ValueError(f"Could not find a dimension matching the foreign key `{key_column}`. Searched for: {searched_foreign_table_names}")
        
        searched_foreign_table_names.append(possible_foreign_table_name)
        role_parts.append(parts[i-1])

    foreign_table = possible_foreign_table_name
    
    # Reverse list and join to a string
    role = "_".join(role_parts[::-1]) or None

    return foreign_table, role, key_identifier


def _is_a_fact_foreign_key_reference(
    column: str, role: str, fact_foreign_key_columns: list, dimension_informations: dict
) -> bool:
    foreign_table, _, _ = _part_key_column(column, dimension_informations)

    if (
        role is not None
        and f"{foreign_table}_{role}{DEFAULT_BUSINESSKEY_POSTFIX}"
        in fact_foreign_key_columns
    ):
        return True

    if role is None and column in fact_foreign_key_columns:
        return True

    return False


def _get_dimension_mapping(input_df, lakehousename: str) -> Dict:
    """
    Given an input DataFrame, this function returns a dictionary of mappings between fact tables and dimension tables.
    The function looks up dimensions in the lakehouse based on naming conventions and iterates through the DataFrame and list to detect the mappings.
    If a mapping is found, it is added to the dimension_mapping dictionary.

    Args:
        input_df (pyspark.sql.DataFrame):   The input DataFrame containing foreign key columns.

    Raises:
        ValueError:                         If a dimension cannot be found for an input column.

    Returns:
        dict:                               A dictionary of mappings between fact tables and dimension tables.
    """
    fact_foreign_key_columns = [
        col
        for col in input_df.schema.fieldNames()
        if col.endswith(DEFAULT_BUSINESSKEY_POSTFIX)
    ]

    dimension_informations = _get_dimension_informations(lakehousename)

    dimension_mapping = {}

    for fact_foreign_key_column in fact_foreign_key_columns:
        dimension_name_from_foreign_key, role, _ = _part_key_column(
            fact_foreign_key_column,
            dimension_informations
        )

        dimension: dict = dimension_informations.get(dimension_name_from_foreign_key)
        if dimension is None:
            print(
                f"Unable to find dimension for single input column called {fact_foreign_key_column}. Are you missing a dimension? Otherwise it might be used in a compound key."
            )
            continue

        dimension_name = dimension["dimension_name"]

        dimension_with_role = f"{dimension_name}_{role}" if role else dimension_name

        dimension_primary_key_columns = dimension["primary_key_columns"]
        dimension_surrogate_key_columns = (
            dimension["surrogate_key_columns"]
            if len(dimension["surrogate_key_columns"]) > 0
            else None
        )

        join_conditions: dict = {}
        unmapped_dimension_key_columns = []
        for column in dimension_primary_key_columns:

            if (
                _is_a_fact_foreign_key_reference(column, role, fact_foreign_key_columns, dimension_informations)
                is False
            ):
                unmapped_dimension_key_columns.append(column)
                continue

            left_side_column: str = column
            right_side_column: str = (
                column if dimension_surrogate_key_columns is not None else None
            )

            foreign_table, _, _ = _part_key_column(column, dimension_informations)

            if role is not None:
                left_side_column = (
                    f"{foreign_table}_{role}{DEFAULT_BUSINESSKEY_POSTFIX}"
                )

            join_conditions[left_side_column] = right_side_column

        mapping = {
            "fully_qualified_dimension_name": dimension[
                "fully_qualified_dimension_name"
            ],
            "role": role,
            "join_conditions": join_conditions,
            "unmapped_dimension_key_columns": unmapped_dimension_key_columns,
            "scd2_flag": dimension["scd2_flag"],
            "surrogate_key_column": dimension_surrogate_key_columns,
        }

        dimension_mapping[dimension_with_role] = mapping

        print(f"mapping for {dimension_with_role}")
        pprint.pp(mapping)

    return dimension_mapping


############################################################
## Lookup dimension surrogate keys and assign to dataframe
############################################################
def _map_surrogate_keys_from_dimensions(
    input_df: DataFrame,
    dimension_mapping: dict = None,
    sequence_column: str = None,
    remove_foreign_key_columns: bool = True,
) -> DataFrame:
    """
    Looks up surrogate keys for dimensions based on input DataFrame and dimension mapping.

    Args:
        input_df (pyspark.sql.DataFrame):               Input DataFrame containing foreign key columns to be replaced with surrogate keys.
        dimension_mapping (dict, optional):             Dictionary containing mapping of dimension names to fully qualified dimension names, join conditions, and surrogate key column names. Defaults to None.
        sequence_column (str, optional):                Name of the sequence column used for SCD2 lookups. Defaults to None.
        remove_foreign_key_columns (bool, optional):    Whether to remove mapped foreign key columns from the output DataFrame. Defaults to True.

    Returns:
        pyspark.sql.DataFrame:                          Output DataFrame with foreign key columns replaced with surrogate keys.
    """

    if dimension_mapping is None:
        dimension_mapping = _get_dimension_mapping(input_df, LAKEHOUSENAME_CURATED)

    result_df = input_df

    drop_keys = []

    for dimension_name, dimension_info in reversed(dimension_mapping.items()):

        print(f"handling {dimension_name}")

        if dimension_info["surrogate_key_column"] is None:
            continue

        surrogate_key_column = dimension_info["surrogate_key_column"][0]
        dimension_role = dimension_info["role"]
        surrogate_key_column_alias = (
            surrogate_key_column.rsplit("_", 1)[0]
            + f"_{dimension_role}{DEFAULT_SURROGATEKEY_POSTFIX}"
            if dimension_role
            else surrogate_key_column
        )

        drop_keys += dimension_info["join_conditions"].keys()

        dimension_df = spark.read.table(
            dimension_info["fully_qualified_dimension_name"]
        )

        join_conditions = [
            result_df[col_name] == dimension_df[dimension_col_name]
            for col_name, dimension_col_name in dimension_info[
                "join_conditions"
            ].items()
        ]

        if dimension_info["scd2_flag"]:
            if DEFAULT_CALENDAR_KEY_COLUMN in result_df.columns:
                print(
                    f"Looking up SCD2 {default_scd2_columns_dict.valid_from.name} <= {DEFAULT_CALENDAR_KEY_COLUMN} < {default_scd2_columns_dict.valid_to.name} "
                )

                dimension_df = dimension_df.withColumn(
                    "_lh_from_date_int",
                    F.expr(
                        f"int(date_format({default_scd2_columns_dict.valid_from.name}, 'yyyyMMdd'))"
                    ),
                )
                dimension_df = dimension_df.withColumn(
                    "_lh_to_date_int",
                    F.expr(
                        f"int(date_format({default_scd2_columns_dict.valid_to.name}, 'yyyyMMdd'))"
                    ),
                )

                join_conditions.append(
                    dimension_df["_lh_from_date_int"]
                    <= result_df[DEFAULT_CALENDAR_KEY_COLUMN]
                )
                join_conditions.append(
                    result_df[DEFAULT_CALENDAR_KEY_COLUMN]
                    < dimension_df["_lh_to_date_int"]
                )
            else:
                raise ValueError(
                    f"A main date key with name {DEFAULT_CALENDAR_KEY_COLUMN} must present to map SCD2 dimensions"
                )

        if surrogate_key_column_alias in result_df.columns:
            print(f"Dropping {surrogate_key_column_alias}")
            result_df = result_df.drop(surrogate_key_column_alias)

        result_df = result_df.join(dimension_df, on=join_conditions, how="left").select(
            [dimension_df[surrogate_key_column].alias(surrogate_key_column_alias)]
            + [result_df[column] for column in result_df.columns]
        )

        result_df = result_df.fillna(
            DEFAULT_SURROGATE_KEY_VALUE, subset=[f"{surrogate_key_column_alias}"]
        )

    if remove_foreign_key_columns:
        drop_keys_unique = list(set(drop_keys))
        print(f"Removing mapped foreign key columns: {drop_keys_unique}")
        result_df = result_df.drop(*drop_keys_unique)

    return result_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Private: Keys and attributes resolver funtions
# # When loading data key often need to be resolved. These functions are located here.

# CELL ********************


def _resolve_columns_classes(
    df: DataFrame, table: str, scd_option: SCDOption
) -> (List[str], List[str], List[str]):
    print("Resolving keys and attributes")

    df_columns = df.columns

    primary_key_columns = [
        col for col in df_columns if col.endswith(DEFAULT_BUSINESSKEY_POSTFIX)
    ]

    if not primary_key_columns:
        raise TypeError(
            f"No primary keys found, there should be a least one column with a post fix '{DEFAULT_BUSINESSKEY_POSTFIX}'. Most likely {table.capitalize()}{DEFAULT_BUSINESSKEY_POSTFIX}"
        )

    surrogate_key_columns = [
        col for col in df_columns if col.endswith(DEFAULT_SURROGATEKEY_POSTFIX)
    ]

    excluded_columns = (
        primary_key_columns + surrogate_key_columns + default_audit_columns
    )

    if scd_option is SCDOption.SCD2:
        excluded_columns += default_scd2_columns

    attribute_columns = [
        column for column in df.columns if column not in excluded_columns
    ]

    print(f"Found primary keys: {','.join(primary_key_columns)}")
    print(f"Found surrogate keys: {','.join(surrogate_key_columns)}")
    print(f"Found attribute columns: {','.join(attribute_columns)}")

    return primary_key_columns, surrogate_key_columns, attribute_columns


def _set_columns_classes(
    df: DataFrame,
    scd_option: SCDOption,
    primary_key_columns: List[str] = None,
    attribute_columns: List[str] = None,
    surrogate_key_columns: List[str] = None,
) -> (List[str], List[str], List[str]):
    print("Setting keys and attributes")

    if primary_key_columns is None:
        primary_key_columns = []

    if attribute_columns is None:
        attribute_columns = []


    if surrogate_key_columns is None:
        surrogate_key_columns = []

    excluded_columns = (
        primary_key_columns + surrogate_key_columns + default_audit_columns
    )

    if scd_option is SCDOption.SCD2:
        excluded_columns += default_scd2_columns

    attribute_columns = [
        column for column in df.columns if column not in excluded_columns
    ]

    print(f"Using primary keys: {','.join(primary_key_columns)}")
    print(f"Using surrogate keys: {','.join(surrogate_key_columns)}")
    print(f"Using attribute columns: {','.join(attribute_columns)}")

    return primary_key_columns, surrogate_key_columns, attribute_columns

def _escape_identifier(identifier: str) -> str:
    return "`" + identifier.strip("`") + "`"

def _quote_tsql_identifier(identifier: str) -> str:
    """
    Quotes a T-SQL identifier by surrounding it with square brackets.

    Args:
        - identifier (str):     The identifier to quote.
    Returns:
        - str:                  The quoted identifier.
    """
    return "[" + identifier.strip("[]") + "]"

def _snake_to_capital_case(snake_str: str) -> str:
    """
    Converts a snake_case string to Capital Case.

    Args:
        - snake_str (str):      The snake_case string to convert.

    Returns:
        - str:                  The converted Capital Case string.
    """
    return " ".join(word.capitalize() for word in snake_str.split("_"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Private: Functions for support our BPF

# CELL ********************


def _get_next_autoincrement(database: str, table_prefix: str, table: str, column_name: str) -> int:
    max_id: int = 1
    if spark.catalog.tableExists(f"{database}.{table_prefix}{table}"):
        target_df = spark.read.table(f"{database}.{table_prefix}{table}")
        target_max_df = target_df.select(F.max(F.col(column_name)).alias("MaxId"))
        max_id = (target_max_df.first().MaxId or 0) + 1
    return max_id


def _drop_duplicates(
    df: DataFrame,
    partition_by_columns: str | list[str],
    order_by_columns: str | list[str]
) -> DataFrame:
    
    if isinstance(partition_by_columns, str):
        partition_by_columns = [partition_by_columns]
    
    if isinstance(order_by_columns, str):
        order_by_columns = [order_by_columns]

    first_duplicate = (
        df.groupBy(*partition_by_columns)
        .agg(F.count("*").alias("__lh_duplicate_count"))
        .filter(F.col("__lh_duplicate_count") > 1)
        .limit(1)
    )
    if first_duplicate.count() == 0:
        return df

    print(f"Dataframe has duplicates. Keeping latest {partition_by_columns} based on {order_by_columns}")
    
    window_spec = (
        Window.partitionBy(partition_by_columns)
        .orderBy(
            [
                F.col(column).desc() for column in order_by_columns
            ]
        )
    )

    return (
        df.withColumn("__lh_row_number", F.row_number().over(window_spec))
        .filter(F.col("__lh_row_number") == 1)
        .drop("__lh_row_number")
    )


def _add_identity_column(
    df: DataFrame,
    database: str = None,
    table_prefix: str = None,
    table: str = None,
    identity_column: str = DEFAULT_IDENTITY_COLUMN,
) -> DataFrame:
    max_id: int = 1

    if database and table:
        table_prefix = table_prefix or ""
        max_id = _get_next_autoincrement(database, table_prefix, table, identity_column)

    rdd_with_index = df.rdd.zipWithIndex()
    rdd_with_index = rdd_with_index.map(lambda row: Row(*row[0], row[1] + max_id))

    new_schema = StructType(df.schema.fields + [StructField(identity_column, LongType(), False)])

    return rdd_with_index.toDF(schema=new_schema)


def _append_surrogatekey_if_not_exists(
    df: DataFrame, database: str, table_prefix: str, table: str, is_new_delta_table: bool
) -> DataFrame:
    surrogate_key_column: str = f"{table}{DEFAULT_SURROGATEKEY_POSTFIX}"
    columns = df.columns

    if surrogate_key_column in df.columns:
        return

    if is_new_delta_table:
        return _add_identity_column(
            df=df,
            identity_column=surrogate_key_column,
        )

    return _add_identity_column(
        df=df,
        database=database,
        table_prefix=table_prefix,
        table=table,
        identity_column=surrogate_key_column,
    )


def _prepend_surrogatekey_if_not_exists(
    df: DataFrame, database: str, table_prefix: str, table: str, is_new_delta_table: bool
) -> DataFrame:
    surrogate_key_column: str = f"{table}{DEFAULT_SURROGATEKEY_POSTFIX}"
    columns = df.columns.copy()

    df = _append_surrogatekey_if_not_exists(
        df=df, database=database, table_prefix=table_prefix, table=table, is_new_delta_table=is_new_delta_table
    )
    return df.select(surrogate_key_column, *columns)



def _add_dummy_record(df: DataFrame) -> DataFrame:
    data_list = []

    for field in df.schema.fields:
        data_list.append(default_type_mappings_dict[type(field.dataType)])

    dummy_df = spark.createDataFrame(data=[data_list], schema=df.schema)
    return df.union(dummy_df)


def _add_audit_columns_if_not_exists(
    df: DataFrame,
    audit_columns: dict = default_audit_columns_dict,
) -> DataFrame:
    for _, settings in audit_columns.items():
        if settings["name"] not in df.columns:
            df = df.withColumn(
                settings["name"],
                settings["sql_default_value"].cast(settings["sql_data_type"]),
            )

    return df


def _add_scd2_audit_columns_if_not_exists(
    df: DataFrame,
    scd2_columns: dict = default_scd2_columns_dict,
) -> DataFrame:
    for _, settings in scd2_columns.items():
        if settings["name"] not in df.columns:
            df = df.withColumn(
                settings["name"],
                settings["sql_default_value"].cast(settings["sql_data_type"]),
            )

    return df


def _handle_schema_evolution(
    df: DataFrame,
    lakehouse: str,
    table_prefix: str,
    table: str,
) -> DeltaTable:
    prefixed_table_name = _escape_identifier(table_prefix + table)
    
    dt_target = DeltaTable.forName(spark, f"{lakehouse}.{prefixed_table_name}")

    missing_columns = [column for column in df.dtypes if column[0] not in dt_target.toDF().columns]
    if len(missing_columns) > 0:
        print(f"Adding columns: {(missing_columns)}")
        spark.sql(f"""
            ALTER TABLE {lakehouse}.{prefixed_table_name} 
            ADD COLUMNS 
                (
                    {", ".join([f"`{col[0]}` {col[1]}" for col in missing_columns])}
                )
        """)
        dt_target = DeltaTable.forName(spark, f"{lakehouse}.{prefixed_table_name}")

    return dt_target

def _ensure_no_duplicates(
    df: DataFrame,
    key_columns: list[str]
) -> None:
    # Check for duplicates in source data on primary_key + sequence column.
    ## Duplicates are not allowed as it breaks the merge
    first_duplicate = (
        df.groupBy(*key_columns)
        .agg(F.count("*").alias("__lh_duplicate_count"))
        .filter(F.col("__lh_duplicate_count") > 1)
        .limit(1)
        .collect()
    )

    source_df_has_duplicates = len(first_duplicate) > 0
    if source_df_has_duplicates:
        duplicate = first_duplicate[0]
        duplicate_error_row = "\n".join(
            [
                f"{col}: {duplicate[col]}"
                for col in key_columns
            ]
        )
        raise ValueError(
            f"Source has duplicates based on:\n{duplicate_error_row}\nRemove duplicates before upserting."
        )        


def _load_table_scd1(
    df: DataFrame,
    lakehouse: str,
    table_prefix: str,
    table: str,
    full_load: bool,
    is_new_delta_table: bool,
    column_classes_function,
) -> DataFrame:

    primary_key_columns, surrogate_key_columns, attribute_columns = (
        column_classes_function()
    )

    lakehouse = _escape_identifier(lakehouse)
    primary_key_columns = [_escape_identifier(col) for col in primary_key_columns]
    surrogate_key_columns = [_escape_identifier(col) for col in surrogate_key_columns]
    attribute_columns = [_escape_identifier(col) for col in attribute_columns]


    # Raise error, if there are duplicates.
    _ensure_no_duplicates(
        df,
        primary_key_columns
    )

    if is_new_delta_table:
        _write_to_delta_append(
            df=df,
            destination_lakehouse=lakehouse,
            destination_table_prefix=table_prefix,
            destination_table=table,
            recreate=True,
        )

        print(
            f"New table created {lakehouse}.{table_prefix}{table} using SCD1 write pattern."
        )

        return df

    dt_target = _handle_schema_evolution(df, lakehouse, table_prefix, table)

    target_alias = "target"
    source_alias = "source"    

    merge_condition = " AND ".join(
        [
            f"{target_alias}.{column} = {source_alias}.{column}" 
            for column in primary_key_columns
        ]
    )

    update_condition = f" OR ".join(
        [
            f"NOT ({target_alias}.{column} <=> {source_alias}.{column})"
            for column in attribute_columns + [default_audit_columns_dict.is_deleted.name]
        ]
    )

    update_values = {
        default_audit_columns_dict.modified_date.name: F.current_timestamp(),
        **{
            f"{target_alias}.{column}": f"{source_alias}.{column}"
            for column in attribute_columns + [default_audit_columns_dict.is_deleted.name, ]
        }
    }

    insert_values = {
        f"{target_alias}.{column}": f"{source_alias}.{column}"
        for column in primary_key_columns + surrogate_key_columns + attribute_columns + default_audit_columns
    }


    dmb = (
        dt_target.alias(target_alias)
        .merge(
            source=df.alias(source_alias),
            condition=merge_condition,
        )
        .whenMatchedUpdate(
            condition=update_condition,
            set=update_values,
        )
        .whenNotMatchedInsert(
            values=insert_values
        )
    )

    if full_load:
        delete_values = {
            f"{target_alias}.{default_audit_columns_dict.is_deleted.name}": "true",
            f"{target_alias}.{default_audit_columns_dict.modified_date.name}": F.current_timestamp(),
        }

        # Never mark the -1 (Unknown) record as deleted.
        exclude_unknown_deletion_condition = " AND ".join(
            [
                f"{target_alias}.{column} != {DEFAULT_SURROGATE_KEY_VALUE}"
                for column in surrogate_key_columns
            ]
        )

        delete_condition = f"""
            {target_alias}.{default_audit_columns_dict.is_deleted.name} = false
            AND {exclude_unknown_deletion_condition}
        """

        dmb = dmb.whenNotMatchedBySourceUpdate(
            condition=delete_condition,
            set=delete_values,
        )

    dmb.execute()
    print("Done")
    return df


def _load_table_scd2(
    df: DataFrame,
    lakehouse: str,
    table_prefix: str,
    table: str,
    full_load: bool,
    is_new_delta_table: bool,
    column_classes_function,
    valid_from_column: str = None,
) -> DataFrame:

    primary_key_columns, surrogate_key_columns, attribute_columns = (
        column_classes_function()
    )

    # Raise error, if there are duplicates.
    _ensure_no_duplicates(
        df,
        primary_key_columns + [valid_from_column or default_scd2_columns_dict.valid_from.name] 
    )    

    if is_new_delta_table:
        # Create an empty table which is then merged into.
        _write_to_delta_append(
            df=df.limit(0),
            destination_lakehouse=lakehouse,
            destination_table_prefix=table_prefix,
            destination_table=table,
            recreate=True,
        )

        print(
            f"New table created {lakehouse}.{table_prefix}{table} using SCD2 write pattern."
        )

    # Overwrite the `valid_from` audit column with the user-chosen column, if provided.
    # This will do a "SCD2 by column" operation.
    # If not provided, do a "SCD2 by time" operation.
    if valid_from_column:
        df = df.withColumn(
            default_scd2_columns_dict.valid_from.name,
            F.col(valid_from_column),
        )
    else:
        df = df.withColumn(
            default_scd2_columns_dict.valid_from.name,
            F.col(default_audit_columns_dict.modified_date.name)
        )        

    dt_target = _handle_schema_evolution(df, lakehouse, table_prefix, table) 
    
    target_df = dt_target.toDF()

    target_alias = "target"
    source_alias = "source"

    # A left semi join is performed to find records in the target table which has a match in the source data frame given the date constraints.
    # These records in target potentially needs their valid_from, valid_to and is_current updated.
    # The join type "left_semi" cannot currently be used with Spark Connect, which breaks unit tests, so this alternative join is used instead.
    target_records_to_be_updated_df = (
        target_df.alias(target_alias)
        .join(
            other=df.alias(source_alias),
            on=primary_key_columns,
            how="left",
        )
        .filter(f"""
            ({target_alias}.{default_scd2_columns_dict.valid_from.name} > {source_alias}.{default_scd2_columns_dict.valid_from.name}) 
            OR (
                {source_alias}.{default_scd2_columns_dict.valid_from.name} > {target_alias}.{default_scd2_columns_dict.valid_from.name} 
                AND {source_alias}.{default_scd2_columns_dict.valid_from.name} <= {target_alias}.{default_scd2_columns_dict.valid_to.name}
            )
        """)
        .selectExpr(f"{target_alias}.*")
        .join(
            other=df,
            on=primary_key_columns + [default_scd2_columns_dict.valid_from.name],
            how="left_anti",
        )
        # If source df contains records older than current record
        # and it contains records newer than current record, then the above join creates duplicates.
        # They can safely (and must) be dropped.
        .dropDuplicates(
            subset=primary_key_columns + [default_scd2_columns_dict.valid_from.name]
        )
    )

    data_columns = [
        column for column 
        in attribute_columns
        if column not in 
            default_landing_audit_columns 
            + default_landing_partition_columns 
            + default_audit_columns 
            + default_scd2_columns
    ] + [default_audit_columns_dict.is_deleted.name]

    # Then combine with incoming source records with existing records
    to_be_applied_df = (
        df.alias(source_alias)
        .join(
            other=target_records_to_be_updated_df.alias(target_alias),
            on=primary_key_columns,
            how="left",
        )
        .filter(f"""
            (
                ({target_alias}.{default_scd2_columns_dict.valid_from.name} > {source_alias}.{default_scd2_columns_dict.valid_from.name}) 
                OR (
                    {source_alias}.{default_scd2_columns_dict.valid_from.name} > {target_alias}.{default_scd2_columns_dict.valid_from.name} 
                    AND {source_alias}.{default_scd2_columns_dict.valid_from.name} <= {target_alias}.{default_scd2_columns_dict.valid_to.name}
                )
            ) AND (
                {" OR ".join([f"(NOT {target_alias}.{column} <=> {source_alias}.{column})" for column in data_columns])}
            ) OR (
                {" AND ".join([f"{target_alias}.{column} IS NULL" for column in primary_key_columns])}
            )
        """)
        .selectExpr(f"{source_alias}.*")
        .unionByName(target_records_to_be_updated_df, allowMissingColumns=True)
        .dropDuplicates(
            subset=primary_key_columns + [default_scd2_columns_dict.valid_from.name]
        )        
    )

    # Recalculate valid_to and is_current
    window_spec = (
        Window.partitionBy(primary_key_columns)
        .orderBy(
            F.desc(default_scd2_columns_dict.valid_from.name)
        )
    )

    staged_df = (
        # Recalculate the valid_to column
        to_be_applied_df.withColumn( 
            default_scd2_columns_dict.valid_to.name,
            F.lag(
                col=default_scd2_columns_dict.valid_from.name,
                offset=1,
                # default=default_scd2_columns_dict.valid_to.name
            ).over(window_spec)
        )
        # Use the latest (null) `valid_to` date to infer `is_current` column.
        .withColumn(
            default_scd2_columns_dict.is_current.name,
            F.when(
                F.col(default_scd2_columns_dict.valid_to.name).isNull(),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        # Subtract a microsecond if it's not the latest row to ensure no `valid_from` / `valid_to` overlaps
        # If it is the latest row, set the default value.
        .withColumn(
            default_scd2_columns_dict.valid_to.name,
            F.when(
                F.col(default_scd2_columns_dict.valid_to.name).isNull(),
                default_scd2_columns_dict.valid_to.default_value
            ).otherwise(F.expr(f"{default_scd2_columns_dict.valid_to.name} - INTERVAL 0.000001 SECOND"))
        )
    )

    print("Starting merge")

    merge_predicate = " AND ".join(
        [
            f"{target_alias}.{column} = {source_alias}.{column}"
            for column in primary_key_columns + [default_scd2_columns_dict.valid_from.name]
        ]
    )

    # We only want to trigger an update if an actual data column is updated, so audit columns are excluded
    columns_to_trigger_update = [
        column for column 
        in attribute_columns
        if column not in 
            default_landing_audit_columns 
            + default_landing_partition_columns 
            + default_audit_columns 
            + default_scd2_columns
    ]

    # However, `is_current`, `valid_to` and `is_deleted` can change due to a recalculation of SCD2, and must be trigger an update. 
    audit_columns_to_trigger_update = [
        default_scd2_columns_dict.is_current.name,
        default_scd2_columns_dict.valid_to.name,
        default_audit_columns_dict.is_deleted.name,
    ]
    update_condition = " OR ".join(
        [
            f"NOT ({target_alias}.{column} <=> {source_alias}.{column})" 
            for column in columns_to_trigger_update + audit_columns_to_trigger_update
        ]
    )

    # Only update `is_current`, `valid_to` and `is_deleted` on existing records.
    update_values = {
        default_audit_columns_dict.modified_date.name: F.current_timestamp(),
        **{
            f"{target_alias}.{column}": f"{source_alias}.{column}"
            for column in audit_columns_to_trigger_update
        }
    }

    # New records must have all columns included.
    insert_values = {
        f"{target_alias}.{column}": f"{source_alias}.{column}"
        for column in primary_key_columns + surrogate_key_columns + attribute_columns + default_audit_columns + default_scd2_columns
    }


    dmb = (
        dt_target.alias(target_alias)
        .merge(
            source=staged_df.alias(source_alias),
            condition=merge_predicate,
        )
        .whenMatchedUpdate(
            condition=update_condition,
            set=update_values,
        )
        .whenNotMatchedInsert(
            values=insert_values
        )
    )

    if full_load:
        # If a record is deleted, only set its `is_deleted`.
        delete_values = {
            f"{target_alias}.{default_audit_columns_dict.is_deleted.name}": "true",
            f"{target_alias}.{default_audit_columns_dict.modified_date.name}": F.current_timestamp(),
        }

        # Never mark the -1 (Unknown) record as deleted.
        exclude_unknown_deletion_condition = " AND ".join(
            [
                f"{target_alias}.{column} != {DEFAULT_SURROGATE_KEY_VALUE}"
                for column in surrogate_key_columns
            ]
        )

        # Only mark as deleted, if it's not already deleted, and it's not the -1 (Unknown) record.
        delete_condition = f"""
            {target_alias}.{default_audit_columns_dict.is_deleted.name} = false
            AND {exclude_unknown_deletion_condition}
        """

        dmb = dmb.whenNotMatchedBySourceUpdate(
            condition=delete_condition,
            set=delete_values,
        )

    dmb.execute()
    print("Done")
    return df

def _rearrange_key_columns(df: DataFrame, key_columns: list[str]) -> DataFrame:
    return df.select(
        *key_columns,
        *[column for column in df.columns if column not in key_columns]
    )


def _write_to_delta_append(
    df: DataFrame,
    destination_lakehouse: str,
    destination_table_prefix: str,
    destination_table: str,
    recreate: bool = False,
    column_classes_function=None,
) -> DataFrame:

    if column_classes_function is not None:
        primary_key_columns, surrogate_key_columns, _ = column_classes_function()
        key_columns = primary_key_columns + surrogate_key_columns
        df = _rearrange_key_columns(df, key_columns)

    if recreate and spark.catalog.tableExists(
        f"{destination_lakehouse}.{destination_table_prefix}{destination_table}"
    ):
        spark.sql(f"DROP TABLE {destination_lakehouse}.{destination_table_prefix}{destination_table}")

    print(
        f"Appending to {destination_lakehouse}.{destination_table_prefix}{destination_table}. Recreate={recreate}"
    )
    df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(
        f"{destination_lakehouse}.{destination_table_prefix}{destination_table}"
    )

    print("Done")

    return df


def _write_to_delta_overwrite(
    df: DataFrame,
    destination_lakehouse: str,
    destination_table_prefix: str,
    destination_table: str,
    recreate: bool = False,
    column_classes_function=None,
) -> DataFrame:

    if column_classes_function is not None:
        primary_key_columns, surrogate_key_columns, _ = column_classes_function()
        key_columns = primary_key_columns + surrogate_key_columns
        df = _rearrange_key_columns(df, key_columns)


    if recreate is True:
        spark.sql(f"DROP TABLE IF EXISTS {destination_lakehouse}.{destination_table_prefix}{destination_table}")
        write_to_delta_append(df, destination_lakehouse, destination_table, destination_table_prefix, recreate)
        return


    print(
        f"Overwriting {destination_lakehouse}.{destination_table_prefix}{destination_table}. Recreate={recreate}"
    )
    df.write.format("delta").option("mergeSchema", "true").mode(
        "overwrite"
    ).saveAsTable(f"{destination_lakehouse}.{destination_table_prefix}{destination_table}")

    print("Done")

    return df


def _write_to_delta_incremental(
    df: DataFrame,
    destination_lakehouse: str,
    destination_table: str,
    destination_table_prefix: str,
    incremental_column: str,
    recreate: bool = False,
) -> DataFrame:
    """
    Write DataFrame to a Delta Lake table by incrementally appending data.
    Only records with an `incremental_column` value greater than the target table's maximum `incremental_column` value are appended.

    Parameters:
    - df (pyspark.sql.DataFrame):     The DataFrame containing the data to be written.
    - destination_lakehouse (str):    Name of the destination lakehouse.
    - destination_table (str):        The name of the destination table.
    - destination_table_prefix (str): The name prefix of the destination table.
    - incremental_column (str):       The name of the column used to determine the delta between source and target.
    - recreate (bool, optional):      Whether to recreate the table if it already exists. Defaults to False.

    Returns:
    None
    """

    if recreate is True:
        _write_to_delta_append(df, destination_lakehouse, destination_table_prefix, destination_table, recreate)
        return

    lakehouse = _escape_identifier(destination_lakehouse)
    table = _escape_identifier(destination_table_prefix + destination_table)

    target_max_timestamp = spark.table(f"{lakehouse}.{table}").select(
        F.max(incremental_column)
    ).collect()[0][0]

    df = df.filter(F.col(incremental_column) > target_max_timestamp)

    print(
        f"Writing incrementally to {destination_lakehouse}.{destination_table_prefix}{destination_table}. incremental_column={incremental_column}"
    )

    return _write_to_delta_append(
        df=df,
        destination_lakehouse=destination_lakehouse,
        destination_table=destination_table,
        destination_table_prefix=destination_table_prefix,
        recreate=recreate,
    )


def _write_to_delta_replace(
    df: DataFrame,
    destination_lakehouse: str,
    destination_table: str,
    destination_table_prefix: str,
    replace_between_column: str,
    recreate: bool = False,
) -> DataFrame:
    """
    Write DataFrame to a Delta Lake table by replacing existing data.
    All records in the target table between the minimum and maximum value (inclusive) of `replace_between_column` of the source DataFrame are overwritten.

    Parameters:
    - df (pyspark.sql.DataFrame):     The DataFrame containing the data to be written.
    - destination_lakehouse (str):    Name of the destination lakehouse.
    - destination_table (str):        The name of the destination table.
    - destination_table_prefix (str): The name prefix of the destination table.
    - replace_between_column (str):   The name of the column used to determine which records to replace.
    - recreate (bool, optional):      Whether to recreate the table if it already exists. Defaults to False.

    Returns:
    None
    """


    if recreate is True:
        _write_to_delta_append(df, destination_lakehouse, destination_table_prefix, destination_table, recreate)
        return

    lakehouse = _escape_identifier(destination_lakehouse)
    table = _escape_identifier(destination_table_prefix + destination_table)
    replace_between_column = _escape_identifier(replace_between_column)

    source_min_timestamp, source_max_timestamp = df.select(
        F.min(replace_between_column), F.max(replace_between_column)
    ).collect()[0]

    print(
        f"Replacing {lakehouse}.{table}. replace_between_column={replace_between_column}"
    )

    (   
        df.write
        .format("delta")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"{replace_between_column} >= '{source_min_timestamp}' AND {replace_between_column} <= '{source_max_timestamp}'")
        .mode("overwrite")
        .saveAsTable(f"{lakehouse}.{table}")
    )
    print("Done")


def _load_table_dataverse_merge(
    df: DataFrame,
    lakehouse: str,
    table_prefix: str,
    table: str,
    is_new_delta_table: bool,
    column_classes_function,
) -> DataFrame:

    primary_key_columns, surrogate_key_columns, attribute_columns = (
        column_classes_function()
    )

    lakehouse = _escape_identifier(lakehouse)
    prefixed_table_name = _escape_identifier(table_prefix + table)
    primary_key_columns = [_escape_identifier(col) for col in primary_key_columns]
    surrogate_key_columns = [_escape_identifier(col) for col in surrogate_key_columns]
    attribute_columns = [_escape_identifier(col) for col in attribute_columns]


    if is_new_delta_table:
        _write_to_delta_append(
            df=df,
            destination_lakehouse=lakehouse,
            destination_table_prefix=table_prefix,
            destination_table=table,
            recreate=True,
        )

        print(
            f"New table created {lakehouse}.{table_prefix}{table} using SCD1 write pattern."
        )

        return df

    dt_target = DeltaTable.forName(spark, f"{lakehouse}.{prefixed_table_name}")

    dmb = (
        dt_target.alias("target")
        .merge(
            source=df.alias("updates"),
            condition=f"""{" AND ".join(f"target.{key} = updates.{key}" for key in primary_key_columns)}""",
        )
        .whenMatchedUpdate(
            condition="updates.IsDelete = 1 OR updates.sysrowversion > target.sysrowversion",
            set={
                **{
                    f"target.{column}": f"updates.{column}"
                    for column in attribute_columns
                },
                f"target.{default_audit_columns_dict.modified_date.name}": F.current_timestamp(),
                f"target.{default_audit_columns_dict.is_deleted.name}": "false",
            },
        )
        .whenNotMatchedInsert(
            values={
                **{
                    f"target.{column}": f"updates.{column}"
                    for column in attribute_columns
                },
                **{
                    f"target.{column}": f"updates.{column}"
                    for column in surrogate_key_columns
                },
                **{
                    f"target.{column}": f"updates.{column}"
                    for column in primary_key_columns
                },
                **{
                    f"target.{column}": f"updates.{column}"
                    for column in default_audit_columns
                },
            }
        )
    )

    dmb.execute()
    print("Done")
    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Private: Miscellaneous utility functions

# CELL ********************

def _pack_pystr_as_c_struct(string: str) -> bytes:
    """
    Packs a Python string into a C-style struct with interleaved null bytes.

    The function converts the input string into its UTF-8 byte representation,
    interleaves each byte with a null byte (`\x00`), and prepends the total length
    of the resulting byte sequence as a 4-byte little-endian integer.

    Args:
        string (str): The input string to be packed.

    Returns:
        bytes: A byte sequence where each character's UTF-8 byte is followed by a null byte,
               prefixed by a 4-byte integer indicating the total length.
    """    

    bytes_str = b""
    for i in string.encode("utf-8"):
        bytes_str += bytes({i})
        bytes_str += bytes(1)

    import struct
    return struct.pack("=i", len(bytes_str)) + bytes_str


def _contains_credentials(connection_string: str) -> bool:
    """Checks if the connection string contains user credentials.

    This function uses regular expressions to detect whether the provided
    connection string includes a user identifier (either 'UID' or 'User ID')
    and a password (either 'PWD' or 'Password').

    Args:
        connection_string (str): The database connection string to check.

    Returns:
        bool: True if connection string contains a user identifier or password.
    """
    uid_pattern = re.compile(r'\b(?:UID|USER\s*ID)\s*=', re.IGNORECASE)
    pwd_pattern = re.compile(r'\b(?:PWD|PASSWORD)\s*=', re.IGNORECASE)

    has_uid = bool(uid_pattern.search(connection_string))
    has_pwd = bool(pwd_pattern.search(connection_string))
    return has_uid or has_pwd    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## API: Public functions for Landing to Base
# # Mainly used for Landing to Base, but as public these might be handy for something else.

# CELL ********************


def add_validation_columns(df: DataFrame, validation_columns: dict = None) -> DataFrame:
    """
    Adds validation columns to a DataFrame if they are not already present.

    Args:
        - df (pyspark.sql.DataFrame):       The DataFrame to add audit columns to.
        - validation_columns (dict):        A dictionary containing the names and data types of the validation columns to add.

    Returns:
        - pyspark.sql.DataFrame:            The DataFrame with the new audit columns added.
    """

    if validation_columns is None:
        validation_columns = default_validation_columns_dict

    select_lists = []

    for _, settings in validation_columns.items:
        if settings["name"] not in df.columns:
            select_lists.append(
                f"CAST({settings['sql_default_value']} AS {settings['sql_data_type']}) as {settings['name']}"
            )

    return df.selectExpr("*", *select_lists)


def rename_columns(df: DataFrame, translations: dict) -> DataFrame:
    """
    Renames columns in a PySpark DataFrame according to a given dictionary of translations.

    Args:
        - df (pyspark.sql.DataFrame):       The DataFrame to rename columns in.
        - translations (dict):              A dictionary where the keys are the old column names and the values are the new column names.

    Returns:
        pyspark.sql.DataFrame:              The DataFrame with renamed columns.
    """
    for old_name, new_name in translations.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    return df


def get_last_row_of_primary_keys(
    df: DataFrame, primary_keys: list[str], sequence_by: str
) -> DataFrame:
    """
    Returns the last row of each primary key based on the sequence_by column.

    Args:
        - df (pyspark.sql.DataFrame):       The input DataFrame.
        - primary_keys (list[str]):         A list of primary key column names.
        - sequence_by (str):                The column name to sequence by.

    Returns:
        pyspark.sql.DataFrame:              A DataFrame containing the last row of each primary key.
    """
    # Setup, convert all column names to lowercase
    primary_keys = [key.lower() for key in primary_keys]
    df_columns = [column.lower() for column in df.columns]
    sequence_by = sequence_by.lower()

    # Find the latest change for each key based on the timestamp
    # Note: For nested structs, max on struct is computed as max on first struct field,
    # if equal fall back to second fields, and so on.
    other_columns = [
        ("`" + column + "`")
        for column in df_columns
        if column not in (primary_keys + [sequence_by])
    ]

    return (
        df.selectExpr(
            *[("`" + key + "`") for key in primary_keys],
            f"struct({sequence_by}, {', '.join(other_columns)}) as otherCols",
        )
        .groupBy(*primary_keys)
        .agg(max(_col("otherCols")).alias("latest"))
        .select(*primary_keys, "latest.*")
    )


def get_service_principal_access_token(tenant_id: str, client_id: str, client_secret: str, resource: str) -> str:
    """
    Obtains an access token for a service principal using OAuth 2.0 client credentials flow.

    Args:
        tenant_id (str): The Azure AD tenant ID.
        client_id (str): The client ID of the service principal.
        client_secret (str): The client secret of the service principal.
        resource (str): The resource (API) for which the token is requested.

    Returns:
        str: The access token for the specified resource.

    Raises:
        requests.HTTPError: If the request to obtain the token fails.
    """

    authority = f"https://login.microsoftonline.com/{tenant_id}"

    body = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "resource": resource,
        "authority": authority
    }

    import requests
    response = requests.post(f"{authority}/oauth2/token", data=body)
    response.raise_for_status()

    access_token = response.json()['access_token']
    return access_token


def get_key_vault_secret(secret_name: str) -> str:
    """
    Retrieves a secret from an Azure Key Vault.

    This function constructs the Key Vault URI based on the current workspace environment.

    Args:
        secret_name (str): The name of the secret to retrieve from Azure Key Vault.

    Returns:
        str: The value of the requested secret.
    """
   
    environment = get_workspace_environment()
    key_vault_uri = f"https://{KEYVAULT_NAME_PREFIX}-{environment}.vault.azure.net/"

    secret_value = notebookutils.credentials.getSecret(key_vault_uri, secret_name)
    return secret_value


def get_service_principal_secrets() -> tuple[str, str]:
    """
    Retrieves the client ID and client secret for the service principal from Azure Key Vault.

    This function fetches the service principal credentials stored as secrets in Azure Key Vault.

    Returns:
        tuple[str, str]: A tuple containing:
            - client_id (str): The client ID of the service principal.
            - client_secret (str): The client secret of the service principal.    
    """

    client_id = get_key_vault_secret(SERVICE_PRINCIPAL_CLIENT_ID_SECRET_NAME)
    client_secret = get_key_vault_secret(SERVICE_PRINCIPAL_CLIENT_SECRET_SECRET_NAME)

    return client_id, client_secret


def get_pyodbc_attrs() -> dict:
    """
    Constructs the ODBC connection attributes required for authentication using an Azure AD access token.

    This function retrieves the necessary credentials, obtains an access token using the service principal,
    and formats it as a C-style structure for use in ODBC authentication.

    Returns:
        dict: A dictionary containing ODBC attributes:
            - 1256 (SQL_COPT_SS_ACCESS_TOKEN): The structured access token for authentication.
            - 113 (SQL_ATTR_CONNECTION_TIMEOUT): The connection timeout set to 60 seconds.

    Raises:
        Exception: If any step in retrieving secrets, obtaining the access token, or struct packing fails.
    
    """    
    tenant_id = get_key_vault_secret(TENANT_ID_SECRET_NAME)

    client_id, client_secret = get_service_principal_secrets()

    access_token = get_service_principal_access_token(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        resource="https://database.windows.net/"
    )

    access_token_struct = _pack_pystr_as_c_struct(access_token)

    attrs_before={
        1256: access_token_struct, # 1256 is the ODBC attribute for access token (SQL_COPT_SS_ACCESS_TOKEN)
        113: 60 # SQL_ATTR_CONNECTION_TIMEOUT required when trying to connect a sleeping serverless database
    } 

    return attrs_before

def _get_available_meta_data_sql_columns(pyodbc_connection: pyodbc.Connection = None) -> list:

    if _get_available_meta_data_sql_columns._available_meta_data_sql_columns or pyodbc_connection is None:
        return _get_available_meta_data_sql_columns._available_meta_data_sql_columns

    cursor = pyodbc_connection.cursor()
    rows = cursor.columns(table='ObjectDefinitionsBase', schema='meta').fetchall()
    cursor.close()

    meta_column_names = [row.column_name for row in rows]
    missing_cols = [column for column in _META_DATA_SQL_COLUMNS if column not in meta_column_names]
    _get_available_meta_data_sql_columns._available_meta_data_sql_columns = [column for column in meta_column_names if column in _META_DATA_SQL_COLUMNS]

    if missing_cols:
        print(f'Warning: Missing meta-data columns {missing_cols}, meta-data table should be upgraded.')

    return _get_available_meta_data_sql_columns._available_meta_data_sql_columns

_get_available_meta_data_sql_columns._available_meta_data_sql_columns = []

def execute_sql_endpoint_query(
    query: str,
    workspace_name: str = None,
    database_name: str = None,
    database_type: Literal["Lakehouse", "Warehouse", "Database"] = "Lakehouse",
) -> None | list[dict]:
    """
    Executes a SQL query on an Fabric SQL Database, Lakehouse or Warehouse using the provided workspace and database names.

    Args:
        - query (str):                  The SQL query to execute.
        - workspace_name (str):         The name of the Fabric workspace. If None, the default lakehouse is used.
        - database_name (str):          The name of the database, lakehouse or warehouse. If None the value of `LAKEHOUSENAME_CURATED` is used.
        - database_type (str):          The type of the sql endpoint. Can be `Lakehouse`, `Warehouse` or `Database`. Default is `Lakehouse`.

    Returns:
        None | list[dict]:              The result of the executed query as a list of dictionaries. If query returns an empty result, None is returned.
    """

    workspace_id = sempy.fabric.get_workspace_id() if workspace_name is None else sempy.fabric.resolve_workspace_id(workspace_name) 
    database_name = database_name or LAKEHOUSENAME_CURATED

    item_id = sempy.fabric.resolve_item_id(database_name, database_type)

    client = sempy.fabric.FabricRestClient()

    item_type = database_type.lower() + "s"

    sql_endpoint_uri = (
        client.get(f"v1/workspaces/{workspace_id}/{item_type}/{item_id}").json()
        .get("properties")
        .get("sqlEndpointProperties")
        .get("connectionString")
    )

    access_token = notebookutils.credentials.getToken("pbi")

    access_token_struct = _pack_pystr_as_c_struct(access_token)

    attrs_before={
        1256: access_token_struct, # 1256 is the ODBC attribute for access token (SQL_COPT_SS_ACCESS_TOKEN)
        113: 60 # SQL_ATTR_CONNECTION_TIMEOUT required when trying to connect a sleeping serverless database
    }
    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={sql_endpoint_uri};DATABASE={database_name}"

    with pyodbc.connect(conn_str, attrs_before=attrs_before) as conn, conn.cursor() as cursor:

        cursor.execute(query)

        if cursor.rowcount <= 0:
            return None
        
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    return rows

def get_source_objects_fromazuresql(connection_string: str) -> list[dict]:
    """
    Retrieve source objects from an Azure SQL database.

    This function retrives only metadata of source object names from an Azure SQL database

    Args:
        - connection_string (str):     The connection string to the Azure SQL database.

    Returns:
        - list[dict]:                   A list of dictionaries representing the source objects table.
    """
    if not _contains_credentials(connection_string):
        attrs_before = get_pyodbc_attrs()
    else:
        attrs_before = {}

    with pyodbc.connect(connection_string, attrs_before=attrs_before) as conn:
        available_meta_data_sql_columns = _get_available_meta_data_sql_columns(conn)
        columns = [f"[{x}]" for x in available_meta_data_sql_columns]

        query = f"SELECT {','.join(columns)} FROM [meta].[ObjectDefinitionsBase] WHERE IncludeFlag=1"


        cursor = conn.cursor()
        cursor.execute(query)
        
        columns = [column[0] for column in cursor.description]
        
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        cursor.close()

    return rows


def get_metadata_base_fromazuresql(connection_string: str, source_name: str = None, recreate: bool = None) -> dict:
    """
    Retrieve metadata base from an Azure SQL database.

    This function retrieves metadata base information from an Azure SQL database
    using the provided connection string. It connects to the database and fetches
    the metadata information, returning it as a dictionary.

    Args:
        - connection_string (str):      The connection string to the Azure SQL database.

    Returns:
        - dict:                         A dictionary containing the metadata base information retrieved from the Azure SQL database.
    """    

    if not _contains_credentials(connection_string):
        attrs_before = get_pyodbc_attrs()
    else:
        attrs_before = {}

    with pyodbc.connect(connection_string, attrs_before=attrs_before) as conn:
        available_meta_data_sql_columns = _get_available_meta_data_sql_columns(conn)
        columns = [f"[{x}]" for x in available_meta_data_sql_columns]
        query = f"SELECT {','.join(columns)} FROM [meta].[ObjectDefinitionsBase] WHERE IncludeFlag=1"
        if source_name:
            query += f" AND SourcePath = '{source_name}'"


        result = conn.execute(query)

        meta_data = {}

        for row in result:

            source = row[_col("SourcePath")]
            destination = row[_col("DestinationTable")]
            
            checkpoint = get_table_property(f"Base.{destination}", "aquavilla.checkpoint.ingestTimestamp")
            
            if not checkpoint or recreate:
                print(f"{'No checkpoint found' if not checkpoint else 'Recreate is set to True'} for {destination}. Defaulting to {checkpoint}")
                checkpoint = datetime(1900, 1, 1).isoformat()
            
            checkpoint = datetime.fromisoformat(checkpoint)

            if source not in meta_data:
                meta_data[source] = _initialize_source_from_row(row, checkpoint)   

            meta_data[source]["Destinations"][destination] = _add_destination_from_row(row, recreate)

    return meta_data


def is_dependency_loaded(meta_data: dict, name: str) -> bool:
    """
    Check if a dependency with the given name is loaded.

    Args:
        - meta_data (dict):         A dictionary containing metadata information.
        - name (str):               The name of the dependency to check.

    Returns:
        - bool:                     True if the dependency is loaded, False otherwise.
    """

    for source in meta_data:
        if name in meta_data[source]["Destinations"]:
            return meta_data[source]["Destinations"][name]["IsLoaded"]
    return True


def all_tables_loaded(meta_data: dict) -> bool:
    """
    Check if all tables are loaded.

    Args:
        - meta_data (dict):         A dictionary containing metadata information.

    Returns:
        - bool:                     True if all tables are loaded, False otherwise.
    """

    for source in meta_data:
        for name in meta_data[source]["Destinations"]:
            if meta_data[source]["Destinations"][name]["IsLoaded"] is False:
                return False
    return True


def find_dependency(meta_data: dict, name: str) -> dict:
    """
    Find a dependency with the given name.

    Args:
        - meta_data (dict):         A dictionary containing metadata information.
        - name (str):               The name of the dependency to find.

    Returns:
        - dict or None:             The metadata information of the dependency if found, None otherwise.
    """

    for source in meta_data:
        if name in meta_data[source]["Destinations"]:
            return meta_data[source]["Destinations"][name]
    return None


############################################################
## WRITE TO DELTA - APPEND
############################################################


def write_to_delta_append(
    df: DataFrame,
    destination_lakehouse: str,
    destination_table: str,
    destination_table_prefix: str = "",
    recreate: bool = False,
) -> DataFrame:
    """
    Write DataFrame to a Delta Lake table by appending data.

    Args:
        - df (pyspark.sql.DataFrame):       The DataFrame containing the data to be written.
        - destination_lakehouse (str):      The path to the Delta Lakehouse where the table is located.
        - destination_table (str):          The name of the Delta Lake table to write data to.
        - recreate (bool, optional):        Whether to recreate the table if it already exists.Defaults to False.

    Returns:
        None
    """

    destination_table = destination_table.lower()

    if DEFAULT_IDENTITY_COLUMN not in [column[0] for column in df.dtypes]:
        df = _add_identity_column(
            df, destination_lakehouse, destination_table_prefix, destination_table, DEFAULT_IDENTITY_COLUMN
        )

    df = _add_audit_columns_if_not_exists(df=df)

    return _write_to_delta_append(
        df=df,
        destination_lakehouse=destination_lakehouse,
        destination_table_prefix=destination_table_prefix,
        destination_table=destination_table,
        recreate=recreate,
        column_classes_function=lambda: _set_columns_classes(
            df=df,
            scd_option=SCDOption.SCD1,
            primary_key_columns=[],
            attribute_columns=df.columns,
            surrogate_key_columns=[DEFAULT_IDENTITY_COLUMN],
        ),
    )


############################################################
## WRITE TO DELTA - OVERWRITE
############################################################
def write_to_delta_overwrite(
    df: DataFrame,
    destination_lakehouse: str,
    destination_table: str,
    destination_table_prefix: str = "",
    recreate: bool = False,
) -> DataFrame:
    """
    Write DataFrame to a Delta Lake table by overwriting existing data.

    Args:
        - df (pyspark.sql.DataFrame):       The DataFrame containing the data to be written.
        - destination_lakehouse (str):      The path to the Delta Lakehouse where the table is located.
        - destination_table (str):          The name of the Delta Lake table to write data to.
        - recreate (bool, optional):        Whether to recreate the table if it already exists. Defaults to False.

    Returns:
        None
    """

    destination_table = destination_table.lower()

    if DEFAULT_IDENTITY_COLUMN not in [column[0] for column in df.dtypes]:
        df = _add_identity_column(
            df, destination_lakehouse, destination_table_prefix, destination_table, DEFAULT_IDENTITY_COLUMN
        )

    df = _add_audit_columns_if_not_exists(df=df)

    return _write_to_delta_overwrite(
        df=df,
        destination_lakehouse=destination_lakehouse,
        destination_table=destination_table,
        destination_table_prefix=destination_table_prefix,
        recreate=recreate,
        column_classes_function=lambda: _set_columns_classes(
            df=df,
            scd_option=SCDOption.SCD1,
            primary_key_columns=[],
            attribute_columns=df.columns,
            surrogate_key_columns=[DEFAULT_IDENTITY_COLUMN],
        ),
    )


############################################################
## WRITE TO DELTA - SCD1 (Upsert Type 1)
############################################################
def write_to_delta_scd1(
    df: DataFrame,
    destination_lakehouse: str,
    destination_table: str,
    destination_table_prefix: str = "",
    keys: List[str] = None,
    attribute_columns: List[str] = None,
    sequence_columns: List[str] = None,
    recreate: bool = False,
    full_load: bool = False,
) -> DataFrame:
    """
    Write DataFrame to a Delta Lake table using Slowly Changing Dimension Type 1 (SCD1) strategy.

    Args:
        - df (pyspark.sql.DataFrame):               The DataFrame containing the data to be written.
        - destination_lakehouse (str):              The path to the Delta Lakehouse where the table is located.
        - destination_table (str):                  The name of the Delta Lake table to write data to.
        - keys (List[str], optional):               List of primary key columns for identifying records. Defaults to None.
        - attribute_columns (List[str], optional):  List of attribute columns to track changes. Defaults to None.
        - sequence_columns (List[str], optional):   List of sequence columns for versioning. Defaults to None.
        - recreate (bool, optional):                Whether to recreate the table if it already exists. Defaults to False.
        - full_load (bool, optional):               Whether to perform a full load. Defaults to False.

    Returns:
        None
    """

    destination_table = destination_table.lower()

    is_new_delta_table: bool = recreate is True or not spark.catalog.tableExists(
        f"{destination_lakehouse}.{destination_table_prefix}{destination_table}"
    )

    if keys and sequence_columns:
        df = _drop_duplicates(
            df=df,
            partition_by_columns=keys,
            order_by_columns=sequence_columns,
        )

    if DEFAULT_IDENTITY_COLUMN not in [column[0] for column in df.dtypes]:
        df = _add_identity_column(
            df, destination_lakehouse, destination_table_prefix, destination_table, DEFAULT_IDENTITY_COLUMN
        )

    df = _add_audit_columns_if_not_exists(df=df)

    return _load_table_scd1(
        df=df,
        lakehouse=destination_lakehouse,
        table=destination_table,
        table_prefix=destination_table_prefix,
        full_load=full_load,
        is_new_delta_table=is_new_delta_table,
        column_classes_function=lambda: _set_columns_classes(
            df=df,
            scd_option=SCDOption.SCD1,
            primary_key_columns=keys,
            attribute_columns=attribute_columns,
            surrogate_key_columns=[DEFAULT_IDENTITY_COLUMN],
        ),
    )

############################################################
## WRITE TO DELTA - DATAVERSE SCD1 (Upsert Type 1)
############################################################
def write_to_delta_dataverse_merge(
    df: DataFrame,
    destination_lakehouse: str,
    destination_table: str,
    destination_table_prefix: str = "",
    keys: List[str] = None,
    attribute_columns: List[str] = None,
    sequence_columns: List[str] = None,
    recreate: bool = False,
) -> DataFrame:
    """
    Write DataFrame to a Delta Lake table using Slowly Changing Dimension Type 1 (SCD1) for Dataverse strategy.

    Args:
        - df (pyspark.sql.DataFrame):               The DataFrame containing the data to be written.
        - destination_lakehouse (str):              The path to the Delta Lakehouse where the table is located.
        - destination_table (str):                  The name of the Delta Lake table to write data to.
        - keys (List[str], optional):               List of primary key columns for identifying records. Defaults to None.
        - attribute_columns (List[str], optional):  List of attribute columns to track changes. Defaults to None.
        - sequence_columns (List[str], optional):   List of sequence columns for versioning. Defaults to None.
        - recreate (bool, optional):                Whether to recreate the table if it already exists. Defaults to False.

    Returns:
        None
    """

    destination_table = destination_table.lower()

    is_new_delta_table: bool = recreate is True or not spark.catalog.tableExists(
        f"{destination_lakehouse}.{destination_table_prefix}{destination_table}"
    )

    if keys and sequence_columns:
        df = _drop_duplicates(
            df=df,
            partition_by_columns=keys,
            order_by_columns=sequence_columns,
        )

    if DEFAULT_IDENTITY_COLUMN not in [column[0] for column in df.dtypes]:
        df = _add_identity_column(
            df, destination_lakehouse, destination_table_prefix, destination_table, DEFAULT_IDENTITY_COLUMN
        )

    df = _add_audit_columns_if_not_exists(df=df)

    return _load_table_dataverse_merge(
        df=df,
        lakehouse=destination_lakehouse,
        table=destination_table,
        table_prefix=destination_table_prefix,
        is_new_delta_table=is_new_delta_table,
        column_classes_function=lambda: _set_columns_classes(
            df=df,
            scd_option=SCDOption.SCD1,
            primary_key_columns=keys,
            attribute_columns=attribute_columns,
            surrogate_key_columns=[DEFAULT_IDENTITY_COLUMN],
        ),
    )    


############################################################
## WRITE TO DELTA - SCD2 (Upsert Type 2)
############################################################
def write_to_delta_scd2(
    df: DataFrame,
    *,
    destination_lakehouse: str,
    destination_table: str,
    destination_table_prefix: str = "",
    keys: List[str],
    valid_from_column: str = None,
    attribute_columns: List[str] = None,
    sequence_columns: List[str] = None,
    recreate: bool = False,
    full_load: bool = False,
) -> DataFrame:
    """
    Write DataFrame to a Delta Lake table using Slowly Changing Dimension Type 2 (SCD2) strategy.

    Args:
        - df (pyspark.sql.DataFrame):               The DataFrame containing the data to be written.
        - destination_lakehouse (str):              The path to the Delta Lakehouse where the table is located.
        - destination_table (str):                  The name of the Delta Lake table to write data to.
        - keys (List[str]):                         List of primary key columns for identifying records.
        - valid_from_column (str, optional):        The name of the column to base the SCD2 off. Should be the `modified timestamp` column of the source or similar. If not provided, it will use the current timestamp.
        - attribute_columns (List[str],             optional): List of attribute columns to track changes. Defaults to None.
        - sequence_columns (List[str], optional):   List of sequence columns for versioning. Defaults to None.
        - recreate (bool, optional):                Whether to recreate the table if it already exists. Defaults to False.
        - full_load (bool, optional):               Whether to perform a full load. Defaults to False.

    Returns:
        None
    """
    destination_table = destination_table.lower()

    is_new_delta_table: bool = recreate is True or not spark.catalog.tableExists(
        f"{destination_lakehouse}.{destination_table_prefix}{destination_table}"
    )

    if valid_from_column:
        partition_by_columns = keys + [valid_from_column]
    else:
        partition_by_columns = keys

    if keys and sequence_columns:
        df = _drop_duplicates(
            df=df,
            partition_by_columns=partition_by_columns,
            order_by_columns=sequence_columns,
        )    

    if DEFAULT_IDENTITY_COLUMN not in [column[0] for column in df.dtypes]:
        df = _add_identity_column(
            df, destination_lakehouse, destination_table_prefix, destination_table, DEFAULT_IDENTITY_COLUMN
        )
    df = _add_scd2_audit_columns_if_not_exists(df=df)
    df = _add_audit_columns_if_not_exists(df=df)

    return _load_table_scd2(
        df=df,
        lakehouse=destination_lakehouse,
        table=destination_table,
        table_prefix=destination_table_prefix,
        valid_from_column=valid_from_column,
        full_load=full_load,
        is_new_delta_table=is_new_delta_table,
        column_classes_function=lambda: _set_columns_classes(
            df=df,
            scd_option=SCDOption.SCD2,
            primary_key_columns=keys,
            attribute_columns=attribute_columns,
            surrogate_key_columns=[DEFAULT_IDENTITY_COLUMN],
        ),
    )

def create_default_view_definition(
    table: str,
    schema: str = None,
    output_as_cell: bool = True,
    include_audit_columns: bool = True,
) -> str:
    """
    Create a default view definition for a table in the specified schema.
    Args:
        - lakehouse (str): The name of the lakehouse.
        - schema (str):    The name of the schema. Default is None. Use None if lakehouse is not schema enabled.
        - table (str):     The name of the table.
        - output_as_cell (bool): Whether to output the view definition as a cell. Default is True.
        - include_audit_columns (bool): Whether to include audit columns in the view definition. Default is False.
    Returns:
        str: The SQL view definition string
    """
    import textwrap

    # Assume lakehouse is not schema enabled, if schema is not set
    if schema is None:
        spark_table_name = _escape_identifier(table)
        spark_full_table_name = f"{spark_table_name}"

        tsql_table_name = _quote_tsql_identifier(table)
        tsql_view_name = _quote_tsql_identifier(VIEW_PREFIX + table)
        tsql_schema_name = "[dbo]"
        tsql_full_view_name = f"{tsql_schema_name}.{tsql_view_name}"
        tsql_full_table_name = f"{tsql_table_name}"
        
    else:
        spark_table_name = _escape_identifier(table)
        spark_schema_name = _escape_identifier(spark_schema_name)
        spark_full_table_name = f"{spark_schema_name}.{spark_table_name}"

        tsql_table_name = _quote_tsql_identifier(table)
        tsql_view_name = _quote_tsql_identifier(VIEW_PREFIX + table)
        tsql_schema_name = schema
        tsql_full_view_name = f"{tsql_schema_name}.{tsql_view_name}"
        tsql_full_table_name = f"{tsql_schema_name}.{tsql_table_name}"



    columns = spark.read.table(spark_full_table_name).columns
    audit_columns = default_audit_columns + default_scd2_columns

    if not include_audit_columns:
        columns = [
            column for column in columns if column not in audit_columns
        ]

    columns = [
        f"{_quote_tsql_identifier(column)} AS {_quote_tsql_identifier(_snake_to_capital_case(column))}" 
        for column in columns
    ]

    # Format the columns for SQL
    columns_sql = f",\n{20 * ' '}".join([column for column in columns]) 

    view_definition = f'''
                CREATE OR ALTER VIEW {tsql_full_view_name} AS
                SELECT {columns_sql}
                FROM
                    {tsql_full_table_name}
                '''

    if output_as_cell:
        code = textwrap.dedent(f'''
            create_view_sql = """{view_definition}"""

            execute_sql_endpoint_query(create_view_sql)
        ''')

        ipython = get_ipython()
        ipython.set_next_input(code)


    return textwrap.dedent(view_definition).strip()

    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## API: Load dimension and fact functions
# Functions used when loading data to the Curated layer.
# The load_dimension function adds a surrogate key column as well as a unknown (dummy) dimension member.
# Loading dimensions currently only supports upsert (SCD1).
# The load_fact function supports loading of facts and bridges and automatically detects foreign key columns to dimensions and replaces the business keys with foreign keys. The function also supports key lookup for role playing dimensions. Compound keys are currently not supported.

# CELL ********************


def load_scd1_dimension(
    df: DataFrame,
    destination_lakehouse: str,
    destination_table: str,
    destination_table_prefix: str,
    full_load: bool = False,
    recreate: bool = False,
    with_identity_column: bool = True,
) -> DataFrame:
    """
    Load a slowly changing dimension (SCD Type 1) into a lakehouse.

    Parameters:
    - df (pyspark.sql.DataFrame):               The DataFrame containing the dimension data.
    - destination_lakehouse (str):              Name of the destination lakehouse.
    - destination_table (str):                  The name of the destination table.
    - full_load (bool, optional):               Whether to perform a full load of the dimension. Default is False.
    - recreate (bool, optional):                Whether to recreate the destination table if it already exists. Default is False.
    - with_identity_column (bool, optional):    Whether to include an identity column in the destination table. Default is True.

    Returns:
    - pyspark.sql.DataFrame:                    The DataFrame representing the loaded dimension data.
    """

    destination_table = destination_table.lower()

    is_new_delta_table: bool = recreate is True or not spark.catalog.tableExists(
        f"{destination_lakehouse}.{destination_table_prefix}{destination_table}"
    )

    if with_identity_column:
        df = _prepend_surrogatekey_if_not_exists(
            df=df,
            database=destination_lakehouse,
            table_prefix=destination_table_prefix,
            table=destination_table,
            is_new_delta_table=is_new_delta_table,
        )

    df = _add_audit_columns_if_not_exists(df=df)

    if is_new_delta_table:
        print("Is a new Delta table adding dummy record")
        df = _add_dummy_record(df)

    return _load_table_scd1(
        df=df,
        lakehouse=destination_lakehouse,
        table=destination_table,
        table_prefix=destination_table_prefix,
        full_load=full_load,
        is_new_delta_table=is_new_delta_table,
        column_classes_function=lambda: _resolve_columns_classes(
            df=df, table=destination_table, scd_option=SCDOption.SCD1
        ),
    )


def load_scd2_dimension(
    df: DataFrame,
    destination_lakehouse: str,
    destination_table: str,
    destination_table_prefix: str,
    valid_from_column: str = None,
    full_load: bool = False,
    recreate: bool = False,
    with_identity_column: bool = True,
) -> DataFrame:
    """
    Load a slowly changing dimension (SCD Type 2) into a lakehouse.

    Parameters:
    - df (pyspark.sql.DataFrame):               The DataFrame containing the dimension data.
    - destination_lakehouse (str):              Name of the destination lakehouse.
    - destination_table (str):                  The name of the destination table.
    - valid_from_column (str, optional):        If specified, this column will be used to build the SCD2 history. Otherwise current timestamp is used.
    - full_load (bool, optional):               Whether to perform a full load of the dimension. Default is False.
    - recreate (bool, optional):                Whether to recreate the destination table if it already exists. Default is False.
    - with_identity_column (bool, optional):    Whether to include an identity column in the destination table. Default is True.

    Returns:
    - pyspark.sql.DataFrame:                    The DataFrame representing the loaded dimension data.
    """

    destination_table = destination_table.lower()

    is_new_delta_table: bool = recreate is True or not spark.catalog.tableExists(
        f"{destination_lakehouse}.{destination_table_prefix}{destination_table}"
    )

    if with_identity_column:
        df = _prepend_surrogatekey_if_not_exists(
            df=df,
            database=destination_lakehouse,
            table=destination_table,
            table_prefix=destination_table_prefix,
            is_new_delta_table=is_new_delta_table,
        )

    df = _add_audit_columns_if_not_exists(df=df)
    df = _add_scd2_audit_columns_if_not_exists(df=df)

    if is_new_delta_table:
        print("Is a new Delta table adding dummy record")
        df = _add_dummy_record(df)

    return _load_table_scd2(
        df=df,
        lakehouse=destination_lakehouse,
        table=destination_table,
        table_prefix=destination_table_prefix,
        valid_from_column=valid_from_column,
        full_load=full_load,
        is_new_delta_table=is_new_delta_table,
        column_classes_function=lambda: _resolve_columns_classes(
            df=df, table=destination_table, scd_option=SCDOption.SCD2
        ),
    )


############################################################
## LOAD DIMENSION
############################################################
def load_dimension(
    df: DataFrame,
    destination_lakehouse: str,
    destination_table: str,
    destination_table_prefix: str = DIMENSION_PREFIX,
    write_pattern: Literal["SCD1", "SCD2"] = "SCD1",
    valid_from_column: str = None,
    full_load: bool = False,
    recreate: bool = False,
) -> DataFrame:
    """
    Load a slowly changing dimension into a lakehouse. A farcade function for either load_scd1_dimension or load_scd2_dimension.

    Parameters:
    - df (pyspark.sql.DataFrame):               The DataFrame containing the dimension data.
    - destination_lakehouse (str):              Name of the destination lakehouse.
    - destination_table (str):                  The name of the destination table.
    - write_pattern (str, optional)             Type of SCD
    - valid_from_column (str, optional):        If specified, and write_pattern is SCD2, this column to build the SCD2 history. Otherwise current timestamp is used.
    - full_load (bool, optional):               Whether to perform a full load of the dimension. Default is False.
    - recreate (bool, optional):                Whether to recreate the destination table if it already exists. Default is False.

    Returns:
    - pyspark.sql.DataFrame:                    The DataFrame representing the loaded dimension data.
    """

    match write_pattern.lower():
        case "scd1":
            print("Writing SCD1")
            return load_scd1_dimension(
                df=df,
                destination_lakehouse=destination_lakehouse,
                destination_table=destination_table,
                destination_table_prefix=destination_table_prefix,
                full_load=full_load,
                recreate=recreate,
                with_identity_column=True,
            )
        case "scd2":
            print("Writing SCD2")
            return load_scd2_dimension(
                df=df,
                destination_lakehouse=destination_lakehouse,
                destination_table=destination_table,
                destination_table_prefix=destination_table_prefix,
                valid_from_column=valid_from_column,
                full_load=full_load,
                recreate=recreate,
                with_identity_column=True,
            )
        case _:
            raise Exception(
                f"Unsupported write pattern for dimension load: {write_pattern}. Verify settings in notebook."
            )


############################################################
## LOAD FACT/BRIDGE
############################################################
def load_fact(
    df: DataFrame,
    destination_lakehouse: str,
    destination_table: str = None,
    destination_table_prefix: str = "fact_",
    write_pattern: Literal["overwrite", "append", "merge", "incremental", "replace"] = "overwrite",
    recreate: bool = False,
    primary_key_columns: List[str] = None,
    with_identity_column: bool = True,
    ignore_auto_mapping: bool = False,
    incremental_column: str = "calendar_key",

) -> DataFrame:
    """
    Load DataFrame as a fact table into a Delta Lakehouse.

    Args:
        - df (pyspark.sql.DataFrame):                   The DataFrame containing the data to be loaded.
        - destination_lakehouse (str):                  The path to the Delta Lakehouse where the table is located.
        - destination_table (str, optional):            The name of the Delta Lake table to write data to. If not provided, a default table name will be used based on the destination_table_prefix. Defaults to None.
        - destination_table_prefix (str, optional):     The prefix to be used for the default table name. Defaults to "Fact".
        - write_pattern (str, optional):                The write pattern to use when writing data to the Delta Lake table. Possible values are "overwrite", "append", "ignore", "error". Defaults to "overwrite".
        - recreate (bool, optional):                    Whether to recreate the table if it already exists. Defaults to False.
        - with_identity_column (bool, optional):        Whether to include an identity column in the destination table. Default is True.
        - ignore_auto_mapping (bool, optional):         Ignore key lookups and naming convention
        - primary_key_columns (List[str], optional):    List of primary key columns for `merge` write pattern. Defaults to None.
        - incremental_column (str, optional):           The name of the column used by the `incremental` and `replace` write patterns. Defaults to "calendar_key".

    Returns:
        - pyspark.sql.DataFrame:                    The DataFrame containing the loaded data.
    """

    destination_table = destination_table.lower()
    destination_table_prefix = destination_table_prefix.lower()

    is_new_delta_table: bool = recreate is True or not spark.catalog.tableExists(
        f"{destination_lakehouse}.{destination_table_prefix}{destination_table}"
    )

    if not ignore_auto_mapping:
        df = _map_surrogate_keys_from_dimensions(df)

    df = _add_audit_columns_if_not_exists(df=df)

    if with_identity_column:
        df = _prepend_surrogatekey_if_not_exists(
            df=df,
            database=destination_lakehouse,
            table=destination_table,
            table_prefix=destination_table_prefix,
            is_new_delta_table=is_new_delta_table,
        )  

    match write_pattern.lower():
        case "overwrite":
            _write_to_delta_overwrite(
                df=df,
                destination_lakehouse=destination_lakehouse,
                destination_table=destination_table,
                destination_table_prefix=destination_table_prefix,
                recreate=is_new_delta_table,
            )
        case "append":
            _write_to_delta_append(
                df=df,
                destination_lakehouse=destination_lakehouse,
                destination_table=destination_table,
                destination_table_prefix=destination_table_prefix,
                recreate=is_new_delta_table,
            )
        case "merge":
            _load_table_scd1(
                df=df,
                lakehouse=destination_lakehouse,
                table=destination_table,
                table_prefix=destination_table_prefix,
                is_new_delta_table=is_new_delta_table,
                full_load=False,
                column_classes_function=lambda: _set_columns_classes(
                    df=df,
                    scd_option=SCDOption.SCD1,
                    primary_key_columns=primary_key_columns,
                    surrogate_key_columns=[f"{destination_table}{DEFAULT_SURROGATEKEY_POSTFIX}"]
                )
            )
        case "incremental":
            _write_to_delta_incremental(
                df=df,
                destination_lakehouse=destination_lakehouse,
                destination_table=destination_table,
                destination_table_prefix=destination_table_prefix,
                recreate=is_new_delta_table,
                incremental_column=incremental_column,
            )
        case "replace":
            _write_to_delta_replace(
                df=df,
                destination_lakehouse=destination_lakehouse,
                destination_table=destination_table,
                destination_table_prefix=destination_table_prefix,
                recreate=is_new_delta_table,
                replace_between_column=incremental_column,
            )            
        case _:
            raise Exception(
                f"Unsupported write pattern for fact/bridge load: {write_pattern}. Verify settings in notebook."
            )

    return df

def get_workspace_environment() -> str:
    """
    Gets the environment of the executed notebook based off the environment tag in the workspace name.

    Args:
        - None

    Returns:
        - str:          The current environment of the workspace.

    Raises:
        - Exception:    If environment for workspace could not be resolved.
    """

    workspace_id = sempy.fabric.get_workspace_id()
    workspace_name = sempy.fabric.resolve_workspace_name(workspace_id)

    matched_environments = [env["name"] for env in ENVIRONMENTS if env["workspace_tag"] in workspace_name]

    if not matched_environments:
        workspace_tags = ", ".join(["`" + env["workspace_tag"] + "`" for env in ENVIRONMENTS])
        raise Exception(
            f"Could not resolve environment for workspace `{workspace_name}`.\nThe workspace name must include a valid environment tag, e.g. `Lakehouse - Data - [dev]`.\nValid environment tags are {workspace_tags}.\nValid environment tags can be configured in the configuration variable ENVIRONMENTS."
        )

    if len(matched_environments) > 1:
        raise Exception(
            f"Could not resolve environment for workspace `{workspace_name}`. The workspace name matches multiple workspace tags defined in the configuration variable ENVIRONMENTS."
        )

    return matched_environments[0]



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_workspace_notebooks():
    workspace_id = sempy.get_notebook_workspace_id()
    client = sempy.fabric.FabricRestClient()
    response = client.get(f"v1/workspaces/{workspace_id}/notebooks")

    response.raise_for_status()
    
    return [notebook["displayName"] for notebook in response.json()["value"]]


def get_workspace_fact_and_bridge_notebooks():
    return [notebook for notebook in get_workspace_notebooks() if notebook.lower().startswith(("load_fact","load_bridge"))]


def get_workspace_dimension_notebooks():
    return [notebook for notebook in get_workspace_notebooks() if notebook.lower().startswith("load_dim")]    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_lakehouse_delta_tables(workspace_id: str, lakehouse_id: str):

    client = sempy.fabric.FabricRestClient()
    response = client.get(f"v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables")
    response.raise_for_status()

    delta_tables = []
    
    while True:

        content = response.json()
        tables = content["data"]

        delta_tables += [
            {
                "name": table["name"],
                "table_path": table["location"]
            }    
            for table in tables
        ]
        
        continuation_uri = content["continuationUri"]

        if continuation_uri is None:
            break

        print("Paging next ... ")
        response = client.get(continuation_uri)
        response.raise_for_status()
    
    return delta_tables



def get_workspace_delta_tables():

    environment = get_workspace_environment()

    workspace_name = mssparkutils.credentials.getSecret(
        f"https://{KEYVAULT_NAME_PREFIX}-{environment.lower()}.vault.azure.net/", 
       FABRIC_DATA_WORKSPACE_SECRET_NAME
    )    

    client = sempy.fabric.FabricRestClient()
    workspace_id = sempy.fabric.resolve_workspace_id(workspace_name)

    reponse = client.get(f"v1/workspaces/{workspace_id}/lakehouses")
    reponse.raise_for_status()
    workspace_lakehouses = reponse.json()["value"]

    delta_tables = []
    for lakehouse in workspace_lakehouses:

        lakehouse_id = lakehouse["id"]
        delta_tables += get_lakehouse_delta_tables(
            workspace_id,
            lakehouse_id
        )
        

    return delta_tables

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## API: Public miscellaneous utility functions

# CELL ********************

def parse_notebook_parameter(parameter: str) -> list[str]:
    """
    Deserializes a notebook parameter from a JSON-string to a Python object.
    Asserts if the resulting object adheres to expected format.

    Args:
        - parameter (str): The string parameter to deserialized

    Returns:
        - (list[dict]): The deserialized object
    """    

    if not isinstance(parameter, str):
        return parameter
    
    try:
        result = json.loads(parameter)

        if not isinstance(result, list):
            raise TypeError()

        if not all(isinstance(item, dict) for item in result):
            raise TypeError()

    except (json.JSONDecodeError, TypeError) as e:
        print("""Parameter must be a valid JSON-string expressed as a list of objects, i.e. `[{"name": "Load_DimCalendar"}, {"name": "Load_DimCustomer"}, {"name": "Load_DimCalendar"}]`""")
        raise e
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
