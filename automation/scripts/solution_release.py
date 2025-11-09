#---------------------------------------------------------
# Default values
#---------------------------------------------------------
default_solution_path = ""
default_item_types_in_scope = "Notebook,DataPipeline"
default_stages_in_scope = "ingest,prepare,orchestrate"
default_environment = "tst"

#---------------------------------------------------------
# Main script
#---------------------------------------------------------
import os, sys, argparse, json
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items, change_log_level
from datetime import datetime

# Uncomment to enable debug logging
#change_log_level("DEBUG")

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
parser.add_argument("--item_types", required=False, default=default_item_types_in_scope, help="Comma seperated list of item types in scope. Must match Fabric ItemTypes exactly.")
parser.add_argument("--solution_path", required=False, default=default_solution_path, help="Path the the solution repository where items are stored.")

args = parser.parse_args()
fabric_token = args.fabric_token
environment = args.env
included_stages_list = [stage.strip().lower() for stage in args.stages.split(",")]
item_type_list = args.item_types.split(",")
solution_path = args.solution_path

is_devops_run = True if os.getenv("SYSTEM_TEAMFOUNDATIONCOLLECTIONURI") else False

# Load JSON files and merge
env_file_path = os.path.join(os.path.dirname(__file__), f'../environments/fabric_solution_{environment}.json')

if not os.path.exists(env_file_path):
    raise ValueError(f"Environment file was not found. {env_file_path}")

with open(env_file_path, 'r') as file:
    env_definition = json.load(file)

    miscfunc.print_header(f"Releasing to {environment} environment") if not is_devops_run else None

    if env_definition:
        solution_name = env_definition.get("name")
        stages = env_definition.get("parameters").get("stages")

        # Define the order of layers to be deployed. Ingest should be after prepare to handle notebook dependencies correctly.
        # Then sort the layer dictionary based on the predefined layer order
        stages_deploy_order = ["Core", "Store", "Prepare", "Ingest", "Model", "Semantic", "Orchestrate"]
        sorted_stages = {key: stages[key] for key in stages_deploy_order if key in stages}

        combined_environment_parameter = {}

        for stage, stage_definition in sorted_stages.items():
            
            if stage.lower() in included_stages_list:        
                workspace_name = solution_name.format(stage=stage, environment=environment)

                print(f"fetching workspace: {workspace_name}")
                workspace = fabfunc.get_workspace_by_name(fabric_token, workspace_name)
                workspace_id = workspace.get("id")

                miscfunc.print_info(f"Releasing {stage} to {environment} in workspace {workspace_name}!", True) if not is_devops_run else None

                repo_dir = os.path.join(solution_path, stage.lower())

                # Seperate the item types based on the identity type and deploy to the target workspace
                token_credential = authfunc.StaticTokenCredential(fabric_token)
                    
                target_workspace = FabricWorkspace(
                    workspace_id=workspace_id,
                    environment=environment,
                    repository_directory=repo_dir,
                    item_type_in_scope=item_type_list,
                    token_credential=token_credential,
                )

                combined_environment_parameter = {**target_workspace.environment_parameter, **combined_environment_parameter}
                target_workspace.environment_parameter = combined_environment_parameter

                # Publish all identity supported items from the repository to the target workspace
                publish_all_items(target_workspace)

                # Support deployment to multiple layers in the same environment by adding the guid mappings to the environment parameter dictionary
                if combined_environment_parameter:
                    for item_name in target_workspace.repository_items.values():
                        for item_details in item_name.values():
                            combined_environment_parameter["find_replace"].append({
                                "find_value": item_details.logical_id,
                                "replace_value": {environment: item_details.guid}
                            })
                            #combined_environment_parameter["find_replace"][item_details.logical_id] = {environment: item_details.guid}

                # Unpublish all items that are not in the repository but are in the target workspace
                unpublish_all_orphan_items(target_workspace)

                miscfunc.print_info(f"Release to workspace {workspace_name} completed! Environment: {environment}, stage: {stage} ", True)
    else:
        miscfunc.print_error(f"No environment definition found for environment {environment}! Release of {environment} has been skipped.", True)