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

# # Currated Tables to Base table dependencies
# 
# Mapping of currated load scripts and return the lineage tables in Base


# PARAMETERS CELL ********************

load_fact_tables = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import itertools
import json

#Could replace this with dynamic RegEx?:
table_dependencies_dict = {
    'FactProjectTransactions':      ['Base.d365fo_projpostedtranstable'
                                    ,'Base.d365fo_projcosttrans'
                                    ,'Base.d365fo_dimensionattributevalueset'
                                    ,'Base.d365fo_projcategory'
                                    ,'Base.d365fo_mainaccount'
                                    ,'Base.d365fo_wrkctrtable'],
    'FactFinancialTransactions':    ['Base.d365fo_generaljournalaccountentry'
                                    ,'Base.d365fo_generaljournalentry'
                                    ,'Base.d365fo_dimensionattributevaluecombination'
                                    ,'Base.d365fo_mainaccount'],
    'FactFinancialBudgetEntries':   ['Base.d365fo_budgettransactionline'
                                    ,'Base.d365fo_dimensionattributevaluecombination'
                                    ,'Base.d365fo_budgettransactionheader'
                                    ,'Base.d365fo_mainaccount']
}

def table_base_dependencies(load_fact_tables):
    if load_fact_tables == None:
        fact_tables = list(table_dependencies_dict.keys()) # Default list of tables to load if none specified
    else:
        fact_tables = eval(load_fact_tables)
    print('Returning base table w. dependencies to:', list(fact_tables))
    base_tables = list(set(itertools.chain(*[table_dependencies_dict[x] for x in fact_tables])))

    return {
             'fact_tables':         fact_tables
            ,'base_tables':         base_tables
            }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dependencies = table_base_dependencies(load_fact_tables)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils
# Prepare parameter values

# Get names of source and destination Lakehouses

result_json = json.dumps({
    'load_fact_tables':               str(dependencies['fact_tables']),
    'load_base_tables':               str(dependencies['base_tables'])
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Return results in notebook output
mssparkutils.notebook.exit(result_json)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
