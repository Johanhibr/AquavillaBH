import os, sys, argparse, subprocess, json, shutil
from datetime import datetime
from pathlib import Path

dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
os.chdir(dir)
sys.path.append(os.getcwd())

import modules.auth_functions as authfunc, modules.azure_functions as azfunc, modules.fabric_functions as fabfunc

start_time = datetime.now()

parser = argparse.ArgumentParser(description="Fabric solution setup arguments")
parser.add_argument("--source_dir", required=True, help="Source directory containing Fabric Data pipelines.")
parser.add_argument("--environment_filepath", required=True, help="Path to environment file containing properties for the target environment.")
parser.add_argument("--key_vault", required=False, help="Name of the Azure Key Vault resources holding environment specific variables.")
parser.add_argument("--tabulareditor_dir", required=False, default=None, help="Path to where the Tabular Editor executable is located.")
parser.add_argument("--script_filepath", required=False, default=None, help="Path to where the Tabular Editor executable is located.")
parser.add_argument("--access_token", required=False, default=os.getenv("access_token"), help="Azure access token for accessing the key vault of the environment.")


args = parser.parse_args()
access_token = args.access_token
environment_filepath = Path(args.environment_filepath)
source_directory = Path(args.source_dir)
script_filepath = Path(args.script_filepath)
key_vault_name = args.key_vault
tabulareditor_directory = Path(args.tabulareditor_dir)

fabric_token = None
gateway_id = "00000000-0000-0000-0000-000000000000"

if not os.getenv("access_token"):
    print("Access token not set. Logging in to execute using Azure key vault using local user.")
    credential = authfunc.create_credentials_from_user()
    access_token = credential.get_token("https://vault.azure.net/.default").token

if not key_vault_name is None: 
    tenant_id = azfunc.get_keyvault_secret(access_token, key_vault_name, "TenantId")
    app_id = azfunc.get_keyvault_secret(access_token, key_vault_name, "Fabric-SPN-ClientID")
    app_secret = azfunc.get_keyvault_secret(access_token, key_vault_name, "Fabric-SPN-Secret")

    fabric_token = authfunc.get_access_token(tenant_id, app_id, app_secret, 'https://api.fabric.microsoft.com')
        
with open(environment_filepath, "r") as f:
    config = json.load(f)

semantic_workspace = config["parameters"]["stages"]["Semantic"]
target_workspace_name = semantic_workspace.get("workspace_name")
target_workspace_id = semantic_workspace.get("workspace_id")

curated_lakehouse = next((lh for lh in (config.get("parameters", {}).get("stages", {}).get("Store", {}).get("items", {}).get("Lakehouse") or []) if lh.get("item_name") == "Curated"), None)


xmla_endpoint = f"powerbi://api.powerbi.com/v1.0/myorg/{target_workspace_name}"

print(f"Looping through Semantic models in folder {source_directory}")

# Loop through Semantic model builds and release to environment
semantic_models_path = Path(source_directory)
if os.path.exists(semantic_models_path):
    for model_file in semantic_models_path.rglob("Model.bim"):
        semantic_model_name = model_file.parent.name
        semantic_model_path = semantic_models_path / semantic_model_name / "Model.bim"

        analysis_connection_string = (
            f"Provider=MSOLAP;Data Source={xmla_endpoint};User ID=app:{app_id}@{tenant_id};Password={app_secret};"
            "Persist Security Info=True;Impersonation Level=Impersonate"
        )

        print(f"Checking if model path {semantic_model_path} exists.")
        if semantic_model_path.exists():
            semantic_model_name_converted = semantic_model_name.title()
            print(f"Deploying semantic model: {semantic_model_name_converted} ....")

            new_script_path = os.path.join(semantic_models_path / semantic_model_name, "update_datasource.cs")

            destination_dir = os.path.dirname(new_script_path)
            os.makedirs(destination_dir, exist_ok=True)

            shutil.copyfile(script_filepath, new_script_path)
            
            with open(new_script_path, 'r') as file:
                content = file.read()
            
            # Update script file
            if curated_lakehouse:
                #print(f"Set lakehouse metadata from: \n{curated_lakehouse}")
                content = content.replace("@sqlEndpointConnectionString", curated_lakehouse.get("sql_endpoint_connectionstring")) 
                content = content.replace("@sqlEndpointId", curated_lakehouse.get("sql_endpoint_id")) 
                content = content.replace("@lakehouseName", curated_lakehouse.get("item_name"))
                curated_connection_id = curated_lakehouse.get("pbi_connection_id")
            else:
                print("Curated lakehouse not found.")

            with open(new_script_path, 'w') as file:
                file.write(content)
            
            # Prepare the arguments for TabularEditor.exe
            te_exec = os.path.join(tabulareditor_directory, "TabularEditor.exe")
            args = [
                te_exec,
                str(semantic_model_path)
            ]

            if script_filepath is not None:
                args += ["-S", new_script_path]

            args += ["-D", analysis_connection_string, semantic_model_name_converted]
            args += ["-O", "-R", "-V", "-E", "-P" ,"-S"]

            # Run Tabular Editor
            try:
                result = subprocess.run(args, check=True, shell=True)
            except subprocess.CalledProcessError as e:
                print(f"Error occurred while deploying model {semantic_model_name_converted}. Error: {e}")
                print('out: ', result.stdout)
                print('err: ', result.stderr)
                raise

            # Set owner to Service Principal and bind semantic model to cloud connection
            if curated_connection_id:
                target_items = fabfunc.list_items(fabric_token,target_workspace_id,"SemanticModel")
                target_dataset = next((dataset for dataset in target_items if dataset['displayName'] == semantic_model_name_converted), None)
                target_dataset_id = target_dataset.get("id")
                print(f"Set owner of semantic model (ID: {target_dataset_id}) to the executing user and bind it to data source {curated_connection_id}")
                fabfunc.take_over_dataset(fabric_token, target_workspace_id, target_dataset_id)
                fabfunc.bind_dataset_datasource(fabric_token, target_workspace_id, target_dataset_id, gateway_id, curated_connection_id)
else:
    print("No semantic models available. Skipping deployment!")
duration = datetime.now() - start_time
print(f"Script duration: {duration}")