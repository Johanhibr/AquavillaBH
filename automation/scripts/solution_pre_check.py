import json, os, sys, argparse, uuid

os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
sys.path.append(os.getcwd())

import modules.auth_functions as authfunc, modules.azure_functions as azfunc, modules.fabric_functions as fabfunc, modules.misc_functions as misc

total_errors = 0
error_count = 0
warning_count = 0
variables = {}
random_guid = str(uuid.uuid4())
test_workspace_name = 'x_precheck_ws_{guid}'

parser = argparse.ArgumentParser(description="Fabric solution setup arguments")
parser.add_argument("--environments", required=False, default="dev", help="Comma seperated list of environments to setup.")
parser.add_argument("--access_token", required=False, default=os.getenv("access_token"), help="Azure access token for accessing the key vault of the environment.")

args = parser.parse_args()
environments = args.environments.split(",")
access_token = args.access_token

required_secrets = ["TenantID", "Fabric-SPN-ClientID", "Fabric-SPN-Secret"]

def print_validation_result(error_count):
    if error_count == 0:
        misc.print_success(f"\u2714 Prerequisite check for {environment} was successful!\n", True)
    else:
        misc.print_error(f"\u2716 Prerequisite check for {environment} failed with {error_count} error(s)!\n", True)


if access_token is None:
    misc.print_error(f"Access Token is not provided as argument!")
    error_count += 1

if not environments:
    misc.print_error(f"No environments provided as argument!")
    error_count += 1

if error_count == 0:    
    for environment in environments:
        error_count = 0
        setup_file_path = os.path.join(os.path.dirname(__file__), f'../environments/fabric_solution_{environment}.json')
        if os.path.exists(setup_file_path):
            with open(setup_file_path, 'r') as file:
                print(f"#################### Checking prerequisites for setup of {environment} environment ####################")

                data = json.load(file)

                fabric_solution_name = data.get("name")
                fabric_solution_stages = data.get("parameters").get("stages", None)
                key_vault_name = data.get("key_vault_name")

                # Checking Stages
                print(f"→ Verifying stages in environment definition...", end="")
                if fabric_solution_stages and isinstance(fabric_solution_stages, dict) and bool(fabric_solution_stages):
                    misc.print_success(" OK!")
                else:
                    misc.print_warning(f" Missing or empty!")
                    warning_count += 1

                # Checking Key Vault
                print(f"→ Verifying Key Vault permissions...")
                kv_error = False
                try:
                    print(f"   * Creating Key Vault secret... ", end="")
                    azfunc.set_keyvault_secret(access_token, key_vault_name, f"00-precheck-{random_guid}", "0123")
                    misc.print_success("OK!")
                except:
                    misc.print_error(f"Failed!")
                    error_count += 1
                    kv_error = True
                
                if kv_error is False:
                    print(f"   * Deleting Key Vault secret... ", end="")
                    try:
                        if azfunc.delete_keyvault_secret(access_token, key_vault_name, f"00-precheck-{random_guid}", True) is False:
                            misc.print_warning(f" Failed! Ensure that the Identity has permissions to delete secrets. Secret 00-precheck-{random_guid} should be deleted manually.")
                        else:
                            misc.print_success("OK!")
                    except:
                        misc.print_warning(f"Failed! Ensure that the Identity has permissions to delete secrets. Secret 00-precheck-{random_guid} should be deleted manually.")

                if kv_error is True:
                    misc.print_error(f"\nFAILED! Verify that {key_vault_name} exists and the Service Principal is Key Vault Secret Officer.")
                    print_validation_result(error_count)
                    total_errors += error_count
                    continue
                else:
                    print(f"→ Verifying required Key Vault secrets:")
                    
                    for secret in required_secrets:
                        print(f"  * {secret}... ", end="")
                        secret_value = azfunc.get_keyvault_secret(access_token, key_vault_name, secret)
                        if secret_value:
                            misc.print_success("OK!")
                            variables[secret] = secret_value
                        else:
                            misc.print_error(f"NOT FOUND!")
                            error_count += 1
                            kv_error = True

                try:
                    print(f"→ Verify Microsoft Entra ID token generation based on Key Vault secrets... ", end="")
                    fabric_access_token = authfunc.get_access_token(variables["TenantID"], variables["Fabric-SPN-ClientID"],variables["Fabric-SPN-Secret"], 'https://api.fabric.microsoft.com')
                    misc.print_success(f"OK!")
                except:
                    misc.print_error("FAILED! Please verify the values for secrets: {",".join(required_secrets)}.")
                    error_count += 1

                print(f"→ Verifying workspace creation... ", end="")
                
                workspace_name = test_workspace_name.format(guid=random_guid)
                workspace = fabfunc.create_workspace(fabric_access_token, workspace_name, "Workspace automatically created as prerequisite check.", False)
                workspace_id = workspace.get("id", None)

                if workspace_id is not None:
                    misc.print_success("OK!")
                else:
                    misc.print_error("FAILED! Not able to create workspaces. Please verify Fabric tenant settings and SPN permissions!")
                    error_count += 1

                print(f"→ Verifying Fabric Capacity settings:")
                print(f"  * Capacity ID defined in Key Vault...", end="")
                capacity_ids = []
                has_generic_capacity = False
                
                capacity_id = azfunc.get_keyvault_secret(access_token, key_vault_name, "Fabric-CapacityID")
                if capacity_id is not None:
                    misc.print_success(" OK!")
                    has_generic_capacity = True
                    capacity_ids.append(capacity_id)
                else:
                    misc.print_warning(" NOT DEFINED!")

                print(f"  * Capacity ID defined in as generic property in environment file... ", end="")
                capacity_id = data.get("parameters", {}).get("generic", {}).get("capacity_id")
                if capacity_id:
                    misc.print_success("OK!")
                    has_generic_capacity = True
                    capacity_ids.append(capacity_id)
                else:
                    misc.print_warning("NOT DEFINED!")

                print(f"  * Capacity ID defined in stage definitions:")
                stage_capacity_cnt = 0
                for stage, stage_props in fabric_solution_stages.items(): 
                    print(f"    - {stage}... ", end="")
                    capacity_id = stage_props.get("capacity_id")
                    if capacity_id is None:
                        misc.print_warning(f"NOT DEFINED!")
                        stage_capacity_cnt += 1
                    else:
                        misc.print_success("OK!")
                        capacity_ids.append(capacity_id)

                print(f"  * Capacity ID defined for all stages... ", end="")
                if len(fabric_solution_stages) != stage_capacity_cnt and has_generic_capacity is False:
                    misc.print_error("NOT DEFINED! Not able to define Fabric Capacity for all stages.")
                    error_count += 1
                else:
                    misc.print_success("OK! Fabric Capacity defined for all stages!")

                print(f"→ Verifying workspace assignment for utilized Fabric Capacities:")      
                for capacity in list(set(capacity_ids)):
                    print(f"  * {capacity}... ", end="")
                    if fabfunc.assign_workspace_to_capacity(fabric_access_token, workspace_id, capacity, False) is True:
                        misc.print_success("OK!")
                    else:
                        misc.print_error("FAILED!")
                        error_count += 1
                
                print(f"→ Verifying permissions... ", end="")
                if data.get("parameters", {}).get("generic", {}).get("permissions"):
                    misc.print_success("OK! Set as generic property in environment file!")
                else:
                    print("No generic permissions specified. Verifying stages:")
                    for stage, stage_props in fabric_solution_stages.items(): 
                        print(f"* {stage}... ", end="")
                        permissions = stage_props.get("permissions",
                            data.get("parameters", {}).get("generic", {}).get("permissions")
                        )

                        if permissions is None:
                            misc.print_error(f"NOT DEFINED!")
                            error_count += 1
                        else:
                            misc.print_success("OK!")
             
                print(f"→ Verifying item definitions of stages:")
                for stage, stage_props in fabric_solution_stages.items(): 
                    print(f"  * {stage}... ", end="")
                    if stage_props.get("items") is None:
                        misc.print_warning("NOT DEFINED")
                    else:
                        misc.print_success("DEFINED!")

                print(f"→ Verifying item creation... ", end="")
                if fabfunc.create_item(fabric_access_token, workspace_id, "x_precheck_item", "Notebook", None, False, False) is None:
                    misc.print_error("FAILED!")
                    error_count += 1
                else:
                    misc.print_success("OK!")
                                        
                if workspace_id is not None:
                    print(f"→ Verifying workspace deletion... ", end="")
                    try:
                        workspace = fabfunc.delete_workspace(fabric_access_token, workspace_id, workspace_name, False)
                        misc.print_success("OK!")
                    except:
                        misc.print_error("FAILED!")
                        error_count += 1

                print_validation_result(error_count)
        
            total_errors += error_count
        else:
            misc.print_error(f"\u2716 ERROR! Environment file fabric_solution_{environment}.json does not exist!\n", True)
            error_count += 1

if total_errors != 0:
    if os.getenv("SYSTEM_TEAMFOUNDATIONCOLLECTIONURI"):
        print(f"##vso[task.logissue type=error;]Prerequisite check failed. {total_errors} error(s) occured!")
        print("##vso[task.complete result=Failed;]Text")
        sys.exit(1)