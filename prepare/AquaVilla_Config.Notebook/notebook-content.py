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
# # AquaVilla - Config
# </center>
# 
# The AquaVilla_Config notebook holds general configuration variables used in AquaVilla.
# 
# ```
# %run AquaVilla_Config
# ```


# ATTACHMENTS ********************

# ATTA {
# ATTA   "c102a520-c68e-4c82-b5e2-911b331652f1.png": {
# ATTA     "image/png": "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"
# ATTA   }
# ATTA }

# MARKDOWN ********************

# ## Module import
# Import modules used by generic framework functions etc.

# CELL ********************

from datetime import datetime
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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Required configuration
# Must always be changed to match the setup.

# CELL ********************

# The name of the key vault containing secrets.
# Do not include environment suffix, e.g. "-dev", it is automatically appended in the code based on the environment executing the notebook.
KEYVAULT_NAME_PREFIX = "kv-bhdp"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 

# MARKDOWN ********************

# ## Basic configuration
# Can be changed to match the setup.

# CELL ********************

# The name of the key vault secret containing the name of the workspace of the lakehouses.
FABRIC_DATA_WORKSPACE_SECRET_NAME = "Fabric-WorkspaceName-Store"

# The name of the key vault secret containing the name of the connection string to the metadata sql db.
METADATA_SQLDB_CONNECTION_STRING_SECRET_NAME = "ConnectionString-Metadata"

# The name of the key vault secret containing the tenant id.
TENANT_ID_SECRET_NAME = "SPTenantID"

# The name of the key vault secret containing the client id of the service principal used for connecting to the metadata database.
SERVICE_PRINCIPAL_CLIENT_ID_SECRET_NAME = "SPClientID"

# The name of the key vault secret containing the client secret of the service principal used for connecting to the metadata database.
SERVICE_PRINCIPAL_CLIENT_SECRET_SECRET_NAME = "SPClientSecret"

# The environments used in the solution. 
# `name` should match the suffix of the resouces.
# `workspace_tag` is how the environments are tagged in the name of the Fabric workspace.
ENVIRONMENTS = [
    {
        "name": "dev",
        "workspace_tag": "[dev]"
    },
    {
        "name": "test",
        "workspace_tag": "[tst]"
    },
    {
        "name": "prod",
        "workspace_tag": "[prd]"
    }
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Advanced configuration
# Change if you know what you're doing.

# CELL ********************

default_type_mappings_dict = {
    LongType: -1,
    ByteType: -1,
    IntegerType: -1,
    ShortType: -1,
    FloatType: 0.00,
    DoubleType: 0.00,
    BinaryType: False,
    BooleanType: False,
    StringType: "?",
    DateType: datetime.strptime("1900-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
    TimestampType: datetime.strptime("1900-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
    DecimalType: None,
}

default_landing_audit_columns_config = {
    "created_date": {
        "name": "landing_created_date",
        "data_type": TimestampType()
    },
    "key_columns": {
        "name": "landing_key_columns",
        "data_type": StringType(),
    },
    "load_type": {
        "name": "landing_load_type",
        "data_type": StringType()
    },
}

default_landing_partition_columns_config = {
    "year": {
        "name": "year",
        "data_type": IntegerType(),
    },
    "month": {
        "name": "month",
        "data_type": IntegerType(),
    },
    "day": {
        "name": "day",
        "data_type": IntegerType(),
    },
    "run_id": {
        "name": "run_id",
        "data_type": LongType()
    },
    "load_type": {
        "name": "load_type",
        "data_type": StringType(),
    },     
}

default_audit_columns_config = {
    "created_date": {
        "name": "lh_created_date",
        "data_type": TimestampType(),
        "default_value": datetime.now(),
        "sql_data_type": "timestamp",
        "sql_default_value": F.current_timestamp(),
    },
    "modified_date": {
        "name": "lh_modified_date",
        "data_type": TimestampType(),
        "default_value": datetime.now(),
        "sql_data_type": "timestamp",
        "sql_default_value": F.current_timestamp(),
    },
    "is_deleted": {
        "name": "lh_is_deleted",
        "data_type": BooleanType(),
        "default_value": "false",
        "sql_data_type": "boolean",
        "sql_default_value": F.lit(False),
    },
}

default_scd2_columns_config = {
    "valid_from": {
        "name": "lh_valid_from",
        "data_type": TimestampType(),
        "default_value": datetime.strptime(
            "1900-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"
        ),
        "sql_data_type": "timestamp",
        "sql_default_value": F.lit(
            datetime.strptime("1900-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        ),
    },
    "valid_to": {
        "name": "lh_valid_to",
        "data_type": TimestampType(),
        "default_value": datetime.strptime(
            "9999-12-31 00:00:00", "%Y-%m-%d %H:%M:%S"
        ),
        "sql_data_type": "timestamp",
        "sql_default_value": F.lit(
            datetime.strptime("9999-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
        ),
    },
    "is_current": {
        "name": "lh_is_current",
        "data_type": BooleanType(),
        "default_value": "true",
        "sql_data_type": "boolean",
        "sql_default_value": F.lit(True),
    },
}

default_validation_columns_config = {
    "quarantine_rules": {
        "name": "lh_quarantine_rules",
        "data_type": StringType(),
        "default_value": None,
        "sql_data_type": "string",
        "sql_default_value": "NULL",
    },
    "is_quarantined": {
        "name": "lh_is_quarantined",
        "data_type": BooleanType(),
        "default_value": "false",
        "sql_data_type": "boolean",
        "sql_default_value": "false",
    },
}

DEFAULT_IDENTITY_COLUMN = "lh_identity"
DEFAULT_SURROGATEKEY_POSTFIX = "_id"
DEFAULT_BUSINESSKEY_POSTFIX = "_key"
DIMENSION_PREFIX = "dim_"
VIEW_PREFIX = "vw_"
DEFAULT_SURROGATE_KEY_VALUE = -1
LAKEHOUSENAME_CURATED = "curated"
DEFAULT_CALENDAR_KEY_COLUMN = "calendar_key"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
