#---------------------------------------------------------
# Default values
#---------------------------------------------------------
default_stages_in_scope = "orchestrate"
default_environment = "dev"

#---------------------------------------------------------
# Main script
#---------------------------------------------------------
import os, sys, argparse, json
from datetime import datetime

start_time = datetime.now()

dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
os.chdir(dir)
sys.path.append(os.getcwd())

import modules.fabric_functions as fabfunc
import modules.misc_functions as miscfunc
import modules.auth_functions as authfunc

# Get arguments 
parser = argparse.ArgumentParser(description="Fabric release arguments")
parser.add_argument("--fabric_token", required=False, default=None, help="Microsoft Entra ID token for Fabric API based on SPN or UPN.")
parser.add_argument("--env", required=False, default=default_environment, help="Name of environment to release.")
parser.add_argument("--stages", required=False, default=default_stages_in_scope, help="Comma seperated list of stages to deploy. Can also be single stage.")
parser.add_argument("--dplist", required=False, default="", help="Comma seperated list of Data Pipeline names which are to be updated.")
parser.add_argument("--dpstartswith", required=False, default="Controller", help="Text which the Data Pipeline name should start with.")

args = parser.parse_args()
fabric_token = args.fabric_token
environment = args.env
dplist = args.dplist
dpstartswith = args.dpstartswith
included_stages_list = [stage.strip().lower() for stage in args.stages.split(",")]

if not fabric_token:
    credential = authfunc.create_credentials_from_user()
    fabric_token = credential.get_token("https://api.fabric.microsoft.com/.default").token

# Load JSON files and merge
env_file_path = os.path.join(os.path.dirname(__file__), f'../environments/fabric_solution_{environment}.json')

if not os.path.exists(env_file_path):
    raise ValueError(f"Environment file was not found. {env_file_path}")

with open(env_file_path, 'r') as file:
    env_definition = json.load(file)

    miscfunc.print_header(f"Updating Data Pipeline metadata for {environment} environment")

    solution_name = env_definition.get("name")
    stages = env_definition.get("parameters").get("stages")

    stages_deploy_order = ["Prepare", "Ingest", "Orchestrate"]
    sorted_stages = {key: stages[key] for key in stages_deploy_order if key in stages}

    for stage, stage_definition in sorted_stages.items():
        
        if stage.lower() in included_stages_list:        
            workspace_name = solution_name.format(stage=stage, environment=environment)
            workspace = fabfunc.get_workspace_by_name(fabric_token, workspace_name)
            workspace_id = workspace.get("id")

            datapipeline_items = fabfunc.list_items(fabric_token, workspace_id, "DataPipeline")

            update_pipeline = next((nb for nb in datapipeline_items if nb['displayName'] == "Utils_Update_DataPipeline_Metadata"), None)

            if update_pipeline is not None:
                payload = {
                "executionData": {
                        "parameters": {
                        "DataPipelineList": dplist,
                        "DataPipelineStartsWith": dpstartswith
                        }
                    }
                }

                fabfunc.run_fabric_job(fabric_token, workspace_id, update_pipeline.get("id"), "Pipeline", payload, True)

                miscfunc.print_info(f"Update of Data Pipelines in workspace {workspace_name} completed!", True)
            else:
                miscfunc.print_error(f"Utility pipeline for updating Data Pipeline metadata is not present in the workspace. Utility pipline must be present in the same workspace.", True)