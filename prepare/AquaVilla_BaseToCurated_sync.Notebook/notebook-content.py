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
# META       "default_lakehouse_workspace_id": "4358b870-4392-403f-8b26-dc95456769dd"
# META     }
# META   }
# META }

# MARKDOWN ********************

# <center>
# 
# # AquaVilla - Base to Curated sync
# </center>
# 
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

# MARKDOWN ********************

# ## Step 1: Load Dimension tables
# Run multiple notebooks by utilizing runMultiple.

# CELL ********************

runOutput = mssparkutils.notebook.run("Load_DimCalendar", 300)
print(runOutput)

runOutput = mssparkutils.notebook.run("Load_DimProduct", 300)
print(runOutput)

runOutput = mssparkutils.notebook.run("Load_DimResort", 300)
print(runOutput)

runOutput = mssparkutils.notebook.run("Load_DimRoom", 300)
print(runOutput)


# MARKDOWN ********************

# ## Step 2: Load Fact tables

# CELL ********************

runOutput = mssparkutils.notebook.run("Load_FactRetailSales", 300)
print(runOutput)


# MARKDOWN ********************

# ## Step 3: Load Bridge tables

# CELL ********************

# Example using run function to run a single notebook
#runOutput = mssparkutils.notebook.run("Load_BridgeSample", 300, { "recreate": recreate} )
#print(runOutput)
