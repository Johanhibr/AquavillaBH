# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c83ca59e-f468-4305-a323-a324e0fb54b4",
# META       "default_lakehouse_name": "Base",
# META       "default_lakehouse_workspace_id": "4358b870-4392-403f-8b26-dc95456769dd"
# META     }
# META   }
# META }

# MARKDOWN ********************

# <center>
# 
# # AquaVilla - Base to Curated async
# </center>
# 
# **NB!: We are looking into feedback regarding race conditions, where content from running notebooks gets mixed up**
# 
# This notebook orchestrates the loading from Base layer to Curated layer. The notebook loads all dimensions, facts and bridges by running the individual notebooks for each task. Notebooks are run by using the run method of the mssparkutils.notebook class, i.e.
# 
# ```
# runOutput = mssparkutils.notebook.run("LoadDimCalendar", 300)
# print(runOutput)
# ```
# 
# Or utilizing runMultiple with either an array of strings or array of activities which can include timeout settings, arguments, retry information and dependencies.
# 
# ```
# mssparkutils.notebook.runMultiple(["LoadDimCalendar","LoadDimProduct"])
# ```
# 
# &nbsp; 
#     
# In a Lakehouse architecture, the Curated layer houses data that is structured in “project-specific” databases, making it readily available for consumption. This integration of data from various sources may result in a shift in data ownership. As for the Gold layer, data is denormalized and read-optimized data model with fewer joins, such as a Kimball-style star schema, depending on the specific use cases.
# 
# Data in the Curated layer is categorized by :
# <table border="0" style="border:none;" width="100%"><tr><td style="border:none;" width="50%">
# 
# - Business level data
# - Aggregated data
# - Modelled data (Kimball)
# - Data ready for consumers
# </td><td align="center" style="border:none;">
# 
# </td></table>


# PARAMETERS CELL ********************

full_load: bool = False             # Options: True, False. Default: False
recreate: bool = False              # Options:True, False. Default: False

# Specify only specific loads to be run. Default (None): Run all
# If you want to exclude any dim/fact tables from running, then give empty list: "[]"
load_dim_tables = None                    # Example: "['FactProjectTransactions','FactFinancialTransactions','FactFinancialBudgetEntries']"
load_fact_tables = None                   # Example: "['DimProject','DimFinancialBudgetEntries']"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Load Dimension tables
# Run multiple notebooks by utilizing runMultiple.

# CELL ********************

dimensionRuns = {
    "activities" : [
        {
            "name": "Load_DimArea", 
            "path": "Load_DimArea", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimBuilding", 
            "path": "Load_DimBuilding", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimBusinessUnit", 
            "path": "Load_DimBusinessUnit", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimCustomer", 
            "path": "Load_DimCustomer", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimProperty", 
            "path": "Load_DimProperty", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimCalendar", 
            "path": "Load_DimCalendar", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimDepartment", 
            "path": "Load_DimDepartment", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimLeaseContract", 
            "path": "Load_DimLeaseContract", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimLeaseCategory", 
            "path": "Load_DimLeaseCategory", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimProjectCategory", 
            "path": "Load_DimProjectCategory", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimProject", 
            "path": "Load_DimProject", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimBuildingFloor", 
            "path": "Load_DimBuildingFloor", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimVendor", 
            "path": "Load_DimVendor", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimItem", 
            "path": "Load_DimItem", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimPropertyUnit", 
            "path": "Load_DimPropertyUnit", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimFinancialAccount", 
            "path": "Load_DimFinancialAccount", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimResource", 
            "path": "Load_DimResource", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimBudget", 
            "path": "Load_DimBudget", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimCurrency", 
            "path": "Load_DimCurrency", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimAsset", 
            "path": "Load_DimAsset", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_DimCompany", 
            "path": "Load_DimCompany", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        }
    ]
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter 
if load_dim_tables != None:
    load_dim_tables = eval(load_dim_tables) # Convert string param to list
    load_dim_tables = ['Load_'+x for x in load_dim_tables]
    #Overwrite dimensionRuns
    dimensionRuns['activities'] = [x for x in dimensionRuns['activities'] if x['name'] in load_dim_tables]

print( f"Loading {len( dimensionRuns['activities'] )} dimension activities" )
print(dimensionRuns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

output = mssparkutils.notebook.runMultiple(dimensionRuns)
#print(runOutput)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Load Fact tables

# CELL ********************

factRuns = {
    "activities" : [
        {
            "name": "Load_FactProjectTransactions",
            "path": "Load_FactProjectTransactions",
            "timeoutPerCellInSeconds": 300,
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_FactLeaseContractTransactions",
            "path": "Load_FactLeaseContractTransactions",
            "timeoutPerCellInSeconds": 300,
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_FactLeaseContractLines", 
            "path": "Load_FactLeaseContractLines", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_FactBuildingAreas", 
            "path": "Load_FactBuildingAreas", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_FactFinancialBudgetEntries", 
            "path": "Load_FactFinancialBudgetEntries", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_FactFinancialTransactions", 
            "path": "Load_FactFinancialTransactions", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        },
        {
            "name": "Load_FactProjectInvestmentBudget", 
            "path": "Load_FactProjectInvestmentBudget", 
            "timeoutPerCellInSeconds": 300, 
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "args": {"recreate": recreate}
        }
    ]
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter 
if load_fact_tables != None:
    load_fact_tables = eval(load_fact_tables) # Convert string param to list
    load_fact_tables = ['Load_'+x for x in load_fact_tables]
    #Overwrite dimensionRuns
    factRuns['activities'] = [x for x in factRuns['activities'] if x['name'] in load_fact_tables]

print( f"Loading {len( factRuns['activities'] )} fact activities" )
print(factRuns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

output = mssparkutils.notebook.runMultiple(factRuns)
#print(runOutput)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Load Bridge tables

# CELL ********************

# Example using run function to run a single notebook
#runOutput = mssparkutils.notebook.run("Load_BridgeSample", 300, { "recreate": recreate} )
#print(runOutput)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
