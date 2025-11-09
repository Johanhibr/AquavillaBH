# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a42faafd-c34e-4999-afda-391c3081850f",
# META       "default_lakehouse_name": "Landing",
# META       "default_lakehouse_workspace_id": "1c7b225a-9103-420a-8ffb-cdc785db59a6",
# META       "known_lakehouses": [
# META         {
# META           "id": "a42faafd-c34e-4999-afda-391c3081850f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils
import os


os.path.exists("/lakehouse/default/Files/landing/daluxapi")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import shutil
from datetime import datetime
 
# First test the notebook to see how the new folder structure would be applied.
# When you have checked it's correct set this parameter to false, and run the script again.
dry_run = True
 
# Source base directory
src_base_dir = "/lakehouse/default/Files/landing/sharepointsitepowerbi" # <- Replace with your folder name
 
# Define a function to move files to the new structure
def move_files_to_new_structure(src_base_dir, dest_base_dir, dry_run=True):
    # Iterate over all directories in the source base directory
    for load_type in ['full', 'delta']:
        load_type_dir = os.path.join(src_base_dir, load_type)
      
        # Check if the load_type directory exists (either 'full' or 'delta')
        if not os.path.exists(load_type_dir):
            continue
 
        # Walk through the directory structure to find year, month, day folders
        for year in os.listdir(load_type_dir):
            year_dir = os.path.join(load_type_dir, year)
            if not os.path.isdir(year_dir) or not year.startswith('year='):
                continue
          
            for month in os.listdir(year_dir):
                month_dir = os.path.join(year_dir, month)
                if not os.path.isdir(month_dir) or not month.startswith('month='):
                    continue
 
                for day in os.listdir(month_dir):
                    day_dir = os.path.join(month_dir, day)
                    if not os.path.isdir(day_dir) or not day.startswith('day='):
                        continue
                  
 
 
                    # Move all Parquet files from the current day folder to the new folder
                    for file_name in os.listdir(day_dir):
 
                        if file_name.endswith(('.parquet','.json','.csv')):
 
                            src_file = os.path.join(day_dir, file_name)
 
                            modified_time = os.path.getmtime(src_file)
 
                            modified_datetime = datetime.fromtimestamp(modified_time)
 
                            run_id = modified_datetime.strftime("%H%M%S%f")[:9]
                            # Construct the new directory structure
                            new_dir = os.path.join(
                                dest_base_dir,
                                f"load_type={load_type}",
                                year,
                                month,
                                day,
                                f"run_id={run_id}"
                            )
 
                            # Create the new directory if it doesn't exist
                            if not dry_run:
                                os.makedirs(new_dir, exist_ok=True)
 
 
 
                            dest_file = os.path.join(new_dir, file_name)
 
                            src_file_formatted = "/".join(src_file.split("/")[4:])
                            dest_file_formatted = "/".join(dest_file.split("/")[4:])
 
                            # Move the file to the new location
                            if not dry_run:
                                shutil.move(src_file, dest_file)
                                print(f"Moved:\n    {src_file_formatted} \n    ->\n    {dest_file_formatted}\n")
                              
                            print(f"Will move:\n    {src_file_formatted} \n    ->\n    {dest_file_formatted}\n")
 
                    if not dry_run:
                        shutil.rmtree(day_dir)
 
file_dirs = [os.path.join(src_base_dir, path, "data") for path in os.listdir(src_base_dir) if os.path.isdir(os.path.join(src_base_dir, path))]
 
# Call the function to move files
for file_dir in file_dirs:
    move_files_to_new_structure(file_dir, file_dir)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
