import json, os, ast, sys, argparse
from datetime import datetime

os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
sys.path.append(os.getcwd())

import modules.auth_functions as authfunc, modules.azure_functions as azfunc, modules.fabric_functions as fabfunc

start_time = datetime.now()

parser = argparse.ArgumentParser(description="Fabric solution setup arguments")
parser.add_argument("--environments", required=False, default="dev", help="Comma seperated list of environments to setup.")
parser.add_argument("--access_token", required=False, default=os.getenv("access_token"), help="Azure access token for accessing the key vault of the environment.")

args = parser.parse_args()
environments = args.environments.split(",")
access_token = args.access_token

if access_token is None:
    print("Access token not set. Logging in to execute using Azure key vault using local user.")
    credential = authfunc.create_credentials_from_user()
    access_token = credential.get_token("https://vault.azure.net/.default").token

for environment in environments:
    setup_file_path = os.path.join(os.path.dirname(__file__), f'../environments/fabric_solution_{environment}.json')
    if os.path.exists(setup_file_path):
        with open(setup_file_path, 'r') as file:
            data = json.load(file)

            fabric_solution_name = data.get("name")
            fabric_solution_stages = data.get("parameters").get("stages")
            key_vault_name = data.get("key_vault_name")
            
            tenant_id = azfunc.get_keyvault_secret(access_token, key_vault_name, "TenantId")
            app_id = azfunc.get_keyvault_secret(access_token, key_vault_name, "Fabric-SPN-ClientID")
            app_secret = azfunc.get_keyvault_secret(access_token, key_vault_name, "Fabric-SPN-Secret")

            fabric_access_token = authfunc.get_access_token(tenant_id, app_id, app_secret, 'https://api.fabric.microsoft.com')

            print(f"############################## Clean up {environment} environment ##############################")
            for stage, stage_props in fabric_solution_stages.items():
                workspace_name = fabric_solution_name.format(stage=stage, environment=environment)
                workspace = fabfunc.get_workspace_by_name(fabric_access_token, workspace_name)
                if not workspace is None:
                    fabfunc.delete_workspace(fabric_access_token, workspace.get('id'), workspace_name)
                else:
                    print(f"Workspace {workspace_name} not found. Continuing...")

                if stage_props.get("items"):
                    for item_type, items in stage_props.get("items").items():
                        for item in items:    
                            if item.get("connection_name") and item_type in {"Lakehouse", "SQLDatabase"}:
                                connection_name = item.get("connection_name").format(stage=stage, environment=environment)  
                                connection = fabfunc.get_connection(fabric_access_token, connection_name)
                                if connection:
                                    fabfunc.delete_connection(fabric_access_token, connection.get("id"))
                
        print("")
    else:
        print(f"Environment file not found: {setup_file_path}")

duration = datetime.now() - start_time
print(f"Script duration: {duration}")