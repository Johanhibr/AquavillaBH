import os, sys, argparse, shutil, subprocess, json, time
from datetime import datetime

os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
sys.path.append(os.getcwd())

import modules.auth_functions as authfunc, modules.fabric_functions as fabfunc, modules.misc_functions as miscfunc

start_time = datetime.now()
default_output_path = os.path.join(os.path.dirname(__file__), f'../parameters/env/')

parser = argparse.ArgumentParser(description="Fabric solution setup arguments")
parser.add_argument("--environments", required=False, default="dev,tst,prod", help="Comma seperated list of environments to setup.")
parser.add_argument("--access_token", required=False, default=os.getenv("access_token"), help="Azure access token for accessing the key vault of the environment.")
parser.add_argument("--output_path", required=False, default=default_output_path, help="Path to where the environment file should be saved.")
       
args = parser.parse_args()
environments = args.environments.split(",")
access_token = args.access_token
output_path = args.output_path

if access_token is None:
    print("Access token not set. Logging in to execute using Azure key vault using local user.")
    credential = authfunc.create_credentials_from_user()
    access_token = credential.get_token("https://api.fabric.microsoft.com/.default").token

workspaces = fabfunc.list_workspaces(access_token)

for environment in environments:
    setup_file_path = os.path.join(os.path.dirname(__file__), f'../environments/fabric_solution_{environment}.json')
    
    if os.path.exists(setup_file_path):
        print(f"Generating environment properties file for {environment}... " , end="")
        with open(setup_file_path, 'r') as file:
            data = json.load(file)

            fabric_solution_name = data.get("name")
            fabric_solution_stages = data.get("parameters").get("stages")

            for stage, stage_props in fabric_solution_stages.items():
                workspace_name = fabric_solution_name.format(stage=stage, environment=environment)
                workspace = next((ws for ws in workspaces if ws['displayName'] == workspace_name), None)
                
                if workspace:
                    workspace_id = workspace.get("id")
                    
                    # Update layer_definition
                    stage_props["workspace_id"] = workspace_id
                    stage_props["workspace_name"] = workspace_name

                    workspace_items = fabfunc.list_items(access_token, workspace_id, None)

                    if stage_props.get("items") is not None:
                        for item_type, items in stage_props.get("items").items():
                            for item in items:
                                connection_name = None
                                if isinstance(item, str):
                                    item_name = item
                                    connection_name = None
                                elif isinstance(item, dict):
                                    item_name = item["item_name"]
                                    if item.get("connection_name", None) is not None:
                                        connection_name = item.get("connection_name").format(stage=stage, environment=environment)


                                ws_item = next((item for item in workspace_items if item['displayName'] == item_name and item["type"] == item_type), None)
                                item["id"] = ws_item.get("id")
                                
                                if connection_name is not None:
                                    connection_result = fabfunc.get_connection(access_token, connection_name)
                                    if connection_result is not None:
                                        item["pbi_connection_name"] = connection_name
                                        item["pbi_connection_id"] = connection_result.get("id")
                                        item["pbi_connection_gatewayid"] = connection_result.get("gatewayId")

                                if item_type == "Lakehouse":          
                                    sqlendpoint_result = fabfunc.get_lakehouse_sqlendpoint(access_token, workspace_id, ws_item.get("id"))
                                    if sqlendpoint_result is not None:
                                        item["sql_endpoint_id"] = sqlendpoint_result.get("properties", {}).get("sqlEndpointProperties", {}).get("id", {})
                                        item["sql_endpoint_connectionstring"] = sqlendpoint_result.get("properties", {}).get("sqlEndpointProperties", {}).get("connectionString", {})
                                
                                elif item_type == "SQLDatabase":
                                    sqldb_result = fabfunc.get_sqldatabase(access_token, workspace_id, ws_item.get("id"))
                                    if sqldb_result is not None:
                                        item["sql_database_fqdn"] = sqldb_result.get("properties", {}).get("serverFqdn", {})
                                        item["sql_database_name"] = sqldb_result.get("properties", {}).get("databaseName", {})
  
        # Save environment properties to json file
        os.makedirs(output_path, exist_ok=True)
        miscfunc.save_json_to_file(data, f'{output_path}/env_data_{environment}.json')
    
        print("Done!")

duration = datetime.now() - start_time
print(f"Script duration: {duration}")
