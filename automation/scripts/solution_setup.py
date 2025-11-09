import json, os, sys, argparse, time
from datetime import datetime

os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
sys.path.append(os.getcwd())

import modules.auth_functions as authfunc, modules.azure_functions as azfunc, modules.fabric_functions as fabfunc, modules.misc_functions as miscfunc

start_time = datetime.now()

parser = argparse.ArgumentParser(description="Fabric solution setup arguments")
parser.add_argument("--environments", required=False, default="dev", help="Comma seperated list of environments to setup.")
parser.add_argument("--access_token", required=False, default=os.getenv("access_token"), help="Azure access token for accessing the key vault of the environment.")
parser.add_argument("--output_folder", required=False, default=None, help="Folder path for saving environment specifications.")

args = parser.parse_args()
environments = args.environments.split(",")
access_token = args.access_token
output_folder = args.output_folder
fabric_upn_token = None
management_upn_token = None

if access_token is None:
    print("Access token not set. Logging in to execute using Azure key vault using local user.")
    credential = authfunc.create_credentials_from_user()
    access_token = credential.get_token("https://vault.azure.net/.default").token
    fabric_upn_token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    management_upn_token = credential.get_token("https://management.core.windows.net/.default").token

all_environments = {}

for environment in environments:
    setup_file_path = os.path.join(os.path.dirname(__file__), f'../environments/fabric_solution_{environment}.json')
    if os.path.exists(setup_file_path):
        with open(setup_file_path, 'r') as file:
            print(f"############################## Setting up {environment} environment ##############################")

            data = json.load(file)

            fabric_solution_name = data.get("name")
            fabric_solution_stages = data.get("parameters").get("stages")
            key_vault_name = data.get("key_vault_name")

            tenant_id = azfunc.get_keyvault_secret(access_token, key_vault_name, "TenantId")
            app_id = azfunc.get_keyvault_secret(access_token, key_vault_name, "Fabric-SPN-ClientID")
            app_secret = azfunc.get_keyvault_secret(access_token, key_vault_name, "Fabric-SPN-Secret")
            default_capacityid = azfunc.get_keyvault_secret(access_token, key_vault_name, "Fabric-CapacityID")
            fabric_access_token = authfunc.get_access_token(tenant_id, app_id, app_secret, 'https://api.fabric.microsoft.com')

            for stage, stage_props in fabric_solution_stages.items():
                workspace_name = fabric_solution_name.format(stage=stage, environment=environment)
                workspace = fabfunc.create_workspace(fabric_access_token, workspace_name, "Workspace automatically created from init_fabric.py script.")
                workspace_id = workspace.get("id")

                # Update layer_definition
                stage_props["workspace_id"] = workspace_id
                stage_props["workspace_name"] = workspace_name

                azfunc.set_keyvault_secret(access_token, key_vault_name, f"Fabric-WorkspaceID-{stage}", workspace_id)
                azfunc.set_keyvault_secret(access_token, key_vault_name, f"Fabric-WorkspaceName-{stage}", workspace_name)
                print(f"Added workspace id and name to key vault {key_vault_name} for stage {stage}.")

                capacity_id = stage_props.get("capacity_id",
                    data.get("parameters", {}).get("generic", {}).get("capacity_id")
                )

                if capacity_id is None:
                    capacity_id = default_capacityid

                if workspace.get("capacityId") is not None:
                    print(f"Workspace {workspace_name} is already assigned to capacity {workspace.get("capacityId")}")

                if workspace.get("capacityId") != capacity_id:
                    fabfunc.assign_workspace_to_capacity(fabric_access_token, workspace_id, capacity_id)

                permissions = stage_props.get("permissions",
                    data.get("parameters", {}).get("generic", {}).get("permissions")
                )

                if permissions is not None:
                    for permission, permission_definitions in permissions.items():
                        for definition in permission_definitions:
                            fabfunc.add_workspace_user(fabric_access_token, workspace_id, permission, definition.get("type"), definition.get("id") )

                if stage_props.get("items") is not None:
                    for item_type, items in stage_props.get("items").items():
                        if item_type == "Lakehouse":
                            item_array = []
                            for fabric_item in items:
                                connection_name = None
                                if isinstance(fabric_item, str):
                                    item_name = fabric_item
                                    connection_name = None
                                elif isinstance(fabric_item, dict):
                                    item_name = fabric_item["item_name"]
                                    if fabric_item.get("connection_name", None) is not None:
                                        connection_name = fabric_item.get("connection_name").format(stage=stage, environment=environment)

                                lh_result = fabfunc.create_item(fabric_access_token, workspace_id, item_name, "Lakehouse", None)

                                counter = 0
                                sql_endpoint_connectionstring = None

                                fabric_item["id"] = lh_result.get("id")

                                while sql_endpoint_connectionstring is None:
                                    lh_details = fabfunc.get_lakehouse(fabric_access_token, workspace_id, lh_result.get("id"))
                                    properties = lh_details.get("properties", {})
                                    sql_endpoint_properties = properties.get("sqlEndpointProperties")

                                    # Extract SQL endpoint details
                                    if sql_endpoint_properties is not None:
                                        sql_endpoint_id = sql_endpoint_properties.get("id")
                                        sql_endpoint_connectionstring = sql_endpoint_properties.get("connectionString")

                                    if sql_endpoint_connectionstring is None:
                                        if counter == 0: 
                                            print(f"- Provisioning SQL endpoint for lakehouse {item_name} ({lh_result.get('id')})...", end='')
                                        else:
                                            print(".", end='', flush=True)
                                        
                                        counter += 1
                                        time.sleep(2)
                                    elif sql_endpoint_connectionstring is not None and counter > 0:
                                        print("Done!")

                                lakehouse_item = {
                                    "lakehouse_id": lh_result.get("id"),
                                    "workspace_id": workspace_id,
                                    "lakehouse_name": item_name,
                                    "sql_endpoint_id": sql_endpoint_id,
                                    "sql_endpoint_connectionstring": sql_endpoint_connectionstring
                                }

                                fabric_item["sql_endpoint_id"] = sql_endpoint_id
                                fabric_item["sql_endpoint_connectionstring"] = sql_endpoint_connectionstring
                                
                                
                                if connection_name is not None:
                                    connection = fabfunc.create_sql_connection(
                                        fabric_access_token, 
                                        connection_name, 
                                        sql_endpoint_connectionstring, 
                                        item_name,
                                        tenant_id,
                                        app_id,
                                        app_secret)
                                    
                                    lakehouse_item["pbi_connection_name"] = connection_name
                                    lakehouse_item["pbi_connection_id"] = connection.get("id")

                                    if permissions is not None:
                                        for role, users in permissions.items():
                                            for user in users:
                                                assigned_role = "Owner" if role == "Admin" else "User"
                                                
                                                role_assignment = {
                                                    "principal": {
                                                        "id": user.get("id"),
                                                        "type": user.get("type")
                                                    },
                                                    "role": assigned_role
                                                }

                                                fabfunc.add_connection_roleassignment(fabric_access_token, connection.get("id"), role_assignment, True)

                                item_array.append(lakehouse_item)

                            azfunc.set_keyvault_secret(access_token, key_vault_name, f"Fabric-Lakehouses-{stage}", json.dumps(item_array, indent=4))
                            print(f"Added lakehouse definiton to key vault {key_vault_name} as secret Fabric-Lakehouses-{stage}.")
                        elif item_type == "SQLDatabase":
                            for fabric_item in items:
                                
                                if isinstance(fabric_item, str):
                                    item_name = fabric_item
                                    connection_name = None
                                elif isinstance(fabric_item, dict):
                                    item_name = fabric_item["item_name"]
                                    connection_name = fabric_item.get("connection_name").format(stage=stage, environment=environment)

                                sql_result = fabfunc.create_item(fabric_access_token, workspace_id, item_name, item_type, None)
                                if sql_result is None:
                                    items = fabfunc.list_items(fabric_access_token, workspace_id, item_type)
                                    sql_result = next((lh for lh in items if lh['displayName'] == item_name), None)

                                sql_properties = fabfunc.get_sqldatabase(fabric_access_token, workspace_id, sql_result.get("id"))
                                azfunc.set_keyvault_secret(access_token, key_vault_name, f"Fabric-SqlDatabaseFqdn-{item_name}", sql_properties['properties']['serverFqdn'])
                                azfunc.set_keyvault_secret(access_token, key_vault_name, f"Fabric-SqlDatabaseCatalog-{item_name}", sql_properties['properties']['databaseName'])
                                azfunc.set_keyvault_secret(access_token, key_vault_name, f"Fabric-SqlDatabaseConnectionString-{item_name}", f"Server={sql_properties['properties']['serverFqdn']};Authentication=Active Directory Service Principal;Encrypt=True;Database={sql_properties['properties']['databaseName']};User Id={app_id};Password={app_secret}")
                                print(f"Added Fabric Database properties as secrets (Fabric-SqlDatabase***) to key vault {key_vault_name}.")

                                if connection_name is not None:
                                    connection = fabfunc.create_sql_connection(
                                        fabric_access_token, 
                                        connection_name, 
                                        sql_properties['properties']['serverFqdn'], 
                                        sql_properties['properties']['databaseName'],
                                        tenant_id,
                                        app_id,
                                        app_secret)
                                    
                                    fabric_item["sql_database_fqdn"] = sql_properties['properties']['serverFqdn']
                                    fabric_item["sql_database_name"] = sql_properties['properties']['databaseName']

                                    azfunc.set_keyvault_secret(access_token, key_vault_name, f"Fabric-SqlDatabasePbiConnectionId-{item_name}", connection.get("id"))
                                    azfunc.set_keyvault_secret(access_token, key_vault_name, f"Fabric-SqlDatabasePbiConnectionName-{item_name}", connection_name)
                                
                                    if permissions is not None:
                                        for role, users in permissions.items():
                                            for user in users:
                                                assigned_role = "Owner" if role == "Admin" else "User"
                                                
                                                role_assignment = {
                                                    "principal": {
                                                        "id": user.get("id"),
                                                        "type": user.get("type")
                                                    },
                                                    "role": assigned_role
                                                }
                                                fabfunc.add_connection_roleassignment(fabric_access_token, connection.get("id"), role_assignment, True)
                        else:
                            for fabric_item in items:
                                lh_result = fabfunc.create_item(fabric_access_token, workspace_id, item_name, item_type, None)

            
                if stage_props.get("private_endpoints") is not None:
                    for private_endpoint in stage_props.get("private_endpoints"):
                        mpe = fabfunc.create_workspace_managed_private_endpoint(fabric_access_token, workspace_id, private_endpoint.get("name"), private_endpoint.get("id"))
                        if(private_endpoint.get("auto_approve")):
                            if mpe.get("connectionState", {}).get("status") != "Approved":
                                management_access_token = authfunc.get_access_token(tenant_id, app_id, app_secret, 'https://management.core.windows.net')
                                connection_name = f"{workspace_id}.{private_endpoint.get("name")}-conn"
                                azfunc.approve_private_endpoint(management_access_token, private_endpoint.get("id"), connection_name)
                            else:
                                print("Private endpoint connection already approved.")    

                git_props = stage_props.get("git_integration")
                if git_props is not None and fabric_upn_token is not None:
                    print (f"Setting up Git integration for workspace {workspace_name}")

                    connect_response = fabfunc.connect_workspace_to_git(
                        fabric_upn_token, 
                        workspace_id, 
                        git_props.get("DevOpsOrgName"),
                        git_props.get("DevOpsProjectName"), 
                        git_props.get("DevOpsRepoName"), 
                        git_props.get("DevOpsDefaultBranch"), 
                        git_props.get("DevOpsFabricSolutionFolder"))
                    
                    if connect_response is not None:
                        init_response = fabfunc.initialize_workspace_git_connection(fabric_upn_token, workspace.get('id'))
                        if init_response and init_response.get("requiredAction") != "None" and init_response.get("remoteCommitHash"):
                            fabfunc.update_workspace_from_git(fabric_upn_token, workspace.get('id'), init_response["remoteCommitHash"])
            print ("")

    #Generate data file
    if output_folder:
        output_folder
        miscfunc.save_json_to_file(data, f'{output_folder}/env_data_{environment}.json')

duration = datetime.now() - start_time
print(f"Script duration: {duration}")