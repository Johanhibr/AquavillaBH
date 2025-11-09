import requests, json, uuid
import time 

fabric_baseurl = "https://api.fabric.microsoft.com/v1"
powerbi_baseurl = "https://api.powerbi.com/v1.0/myorg"

def is_guid(value: str) -> bool:
    try:
        uuid_obj = uuid.UUID(value)
        return str(uuid_obj) == value.lower()
    except (ValueError, AttributeError, TypeError):
        return False

def get_workspace_by_name(access_token, workspace_name):
    """
    Retrieves a workspace by its name from Microsoft Power BI.

    Args:
        headers (dict): A dictionary containing the authorization header, including an OAuth 2.0 bearer token.
        workspace_name (str): The name of the workspace to retrieve.

    Returns:
        dict or None: A dictionary containing the details of the first workspace found with the specified name. 
                      Returns None if no workspace with the given name is found or if an error occurs.
    """

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(f"{powerbi_baseurl}/groups?$filter=name eq '{workspace_name}'", headers=headers)
        response.raise_for_status()
        workspaces = response.json().get("value", [])
        if workspaces:
            return workspaces[0]
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching workspace by name: {e}")
        return None
    

def create_workspace(access_token, workspace_name, workspace_description, print_output:bool = True):
    """
    Creates a new workspace in Microsoft Fabric if a workspace with the specified name does not already exist.

    Args:
        access_token (str): OAuth 2.0 bearer token for authenticating the API request.
        workspace_name (str): The name of the workspace to create.
        workspace_description (str): A description for the new workspace.

    Returns:
        dict or None: A dictionary containing the workspace details if the workspace is successfully created. 
                      Returns None if the workspace already exists or if an error occurs during creation.
    """
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    workspace = get_workspace_by_name(access_token, workspace_name)

    if workspace is None:
        body = {
            "displayName": workspace_name,
            "description": workspace_description
        }
        try:
            response = requests.post(f"{fabric_baseurl}/workspaces", headers=headers, json=body)
            response.raise_for_status()
            if print_output:
                print(f"Workspace {workspace_name} (id: {response.json()['id']}) was succesfully created.")
            return response.json()
        except requests.exceptions.RequestException as e:
            if print_output:
                print(f"Error creating workspace: {e}")
            return None
    else:
        if print_output: 
            print(f"[warning]Workspace {workspace_name} already exists.")
        return workspace
    

def assign_workspace_to_capacity(access_token, workspace_id, capacity_id, print_output:bool = True):
    """
    Assigns a specified workspace to a given capacity in Microsoft Fabric.

    Args:
        access_token (str): The OAuth 2.0 access token for authenticating the API request.
        workspace_id (str): The unique identifier of the workspace to assign to the capacity.
        capacity_id (str): The unique identifier of the capacity to which the workspace will be assigned.

    Returns:
        Boolean: Returns True if success and False if failed!
    """

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    body = {
        "capacityId": capacity_id
    }

    try:
        response = requests.post(f"{fabric_baseurl}/workspaces/{workspace_id}/assignToCapacity", headers=headers, json=body)
        response.raise_for_status()
        if print_output:
            print(f"Workspace {workspace_id} assigned to capacity {capacity_id}.")
        return True
    except requests.exceptions.RequestException as e:
        if print_output:
            print(f"Could not assign workspace to capacity. Error: {e}")
        return False


def delete_workspace(access_token, workspace_id, workspace_name = "", print_output:bool = True):
    """
    Removes a specified workspace from Microsoft Fabric

    Args:
        access_token (str): OAuth 2.0 bearer token used to authenticate the API request.
        workspace_id (str): The unique identifier of the workspace to be removed.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    url = f"{fabric_baseurl}/workspaces/{workspace_id}"

    try:
        response = requests.delete(url, headers=headers)
        response.raise_for_status() 
        if print_output:
            print(f"Workspace {workspace_name} with id {workspace_id} was succesfully removed.")
    except requests.exceptions.RequestException as e:
        error_details = response.json().get("error", {}).get("message", str(e))
        if print_output:
            print(f"Failed to remove workspace ({workspace_id}): {error_details}")


def add_workspace_user(access_token, workspace_id, access_role, identity_type, identity_identifier):
    """
    Assigns a specified access role to a user or group within a Power BI workspace.

    Args:
        access_token (str): The OAuth access token used to authorize the API request.
        workspace_id (str): The unique identifier of the Power BI workspace to which the role assignment applies.
        access_role (str): The role to assign to the user or group. Acceptable values are "Admin", "Contributor", "Member", "Viewer", or "None".
        identity_type (str): The type of identity being assigned. Acceptable values are "User" (for email-based identification) 
            and "Group" or "App" for Entra ID groups or service principals.
        identity_identifier (str): The identifier for the identity. For "User", this should be an email address; 
            for other types, this should be a unique identifier.
    """

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    if is_guid(identity_identifier):
        body = {
            "principal": {
                "id": identity_identifier,
                "type": identity_type
            },
            "role": access_role
        }

        try:
            response = requests.post(f"{fabric_baseurl}/workspaces/{workspace_id}/roleAssignments", headers=headers, json=body)
            response.raise_for_status()
            print(f"Workspace role assignment updated for workspace {workspace_id}. Added {identity_type} {identity_identifier} as {access_role}.")
        except requests.exceptions.HTTPError as http_err:
            error_response = response.json()
            error_code = error_response.get("errorCode")
            if error_code == "PrincipalAlreadyHasWorkspaceRolePermissions":
                print(f"The identity {identity_identifier} already exists in the workspace.")
            else:
                print(f"Request failed! {error_response.get("message")}")
    else:
        body = {
            "identifier": identity_identifier,
            "groupUserAccessRight": access_role,
            "principalType": identity_type
        }

        try:
            response = requests.post(f"{powerbi_baseurl}/groups/{workspace_id}/users", headers=headers, json=body)
            response.raise_for_status()
            print(f"Workspace role assignment updated for workspace {workspace_id}. Added {identity_type} {identity_identifier} as {access_role}.")
        except requests.exceptions.RequestException as e:
            if response is not None:
                error_code = response.json().get("error", {}).get("code")
                if error_code == "AddingAlreadyExistsGroupUserNotSupportedError":
                    print(f"The identity {identity_identifier} already exists in the workspace.")
                else:
                    error_message = response.json().get("error", {}).get("message", str(e))
                    print(f"Request failed: {error_message}")
            else:
                print(f"Request failed: {e}")


def connect_workspace_to_git(access_token, workspace_id, repo_org_name, repo_project_name, repo_name, repo_branch_name, repo_directory):
    """
    Connects a workspace to a Git repository in Azure DevOps.

    Args:
        access_token (str): Access token for authentication.
        workspace_id (str): Unique identifier of the workspace to connect.
        repo_org_name (str): Name of the Azure DevOps organization.
        repo_project_name (str): Name of the project within the Azure DevOps organization.
        repo_name (str): Name of the Git repository.
        repo_branch_name (str): Branch name in the repository to connect to.
        repo_directory (str): Directory within the repository to connect the workspace to.

    Returns:
        str: "OK" if the workspace is successfully connected to the Git repository.
        None: If the connection fails or if the workspace is already connected to the Git repository.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    body = {
        "gitProviderDetails": {
            "organizationName": repo_org_name,
            "projectName": repo_project_name,
            "gitProviderType": "AzureDevOps",
            "repositoryName": repo_name,
            "branchName": repo_branch_name,
            "directoryName": repo_directory
        }
    }

    url = f"{fabric_baseurl}/workspaces/{workspace_id}/git/connect"

    try:
        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()
        print(f"* Workspace {workspace_id} connected to GIT repository {repo_name} using directory {repo_directory}.")
        return "OK"
    except requests.exceptions.RequestException as e:
        if response.status_code == 409:
            print(f"* The workspace {workspace_id} is already connected to the Git repository.")
        else:
            print(f"* Could not connect workspace to Git. Error: {e}")
        return None


def update_workspace_from_git(access_token, workspace_id, remote_commit_hash):
    """
    Updates a workspace from a connected Git repository in Azure DevOps.

    Args:
        access_token (str): Access token for authentication.
        workspace_id (str): Unique identifier of the workspace to update.
        remote_commit_hash (str): The commit hash from the Git repository to update the workspace to.

    Returns:
        dict: JSON response from the successful update operation.
        None: If the update fails due to an error or an unresolved dependency.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    body = {
        "remoteCommitHash": remote_commit_hash,
        "conflictResolution": {
            "conflictResolutionType": "Workspace",
            "conflictResolutionPolicy": "PreferWorkspace"
        },
        "options": {
            "allowOverrideItems": True
        }
    }

    url = f"{fabric_baseurl}/workspaces/{workspace_id}/git/updateFromGit"

    try:
        print(f"* Updating workspace {workspace_id} from connected GIT repository...", end="")
        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()
        
        operation_id = response.headers.get('x-ms-operation-id')
        if operation_id is None:
            return response.json()
        else:
            get_operation_state_url = f"{fabric_baseurl}/operations/{operation_id}"

            # Poll the operation status until it's done
            while True:
                operation_state_response = requests.get(get_operation_state_url, headers=headers)
                operation_state = operation_state_response.json()
                status = operation_state.get("status")

                if status in ["NotStarted", "Running"]:
                    print(".", end="", flush=True)
                    time.sleep(5)
                elif status == "Succeeded":
                    print(" Done!")
                    return response.json()
                else:
                    print(" Failed!")
                    print("* Update workspace from git failed. Check dependencies etc. and resolve issues inside the repository before re-initializing.")
                    return None

    except requests.exceptions.RequestException as e:
        print(f" Failed! Check dependencies etc. and resolve issues before re-initializing.")


def initialize_workspace_git_connection(access_token, workspace_id):
    """
    Initializes a Git connection for a workspace in Azure DevOps.

    Args:
        access_token (str): Access token for authentication.
        workspace_id (str): Unique identifier of the workspace to initialize the Git connection.

    Returns:
        dict: JSON response from the successful initialization operation.
        None: If the initialization fails due to a conflict or other error.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    url = f"{fabric_baseurl}/workspaces/{workspace_id}/git/initializeConnection"

    try:
        print(f"* Initializing git connection for workspace (id: {workspace_id}) ...", end="")
        body = {
            "initializationStrategy":"PreferRemote"
        }
        
        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()

        operation_id = response.headers.get('x-ms-operation-id')
        if (response.status_code == 409):
            print(" Conflict - Git connection already initialized.")
            return None
        if operation_id is None:
            print(" Done!")
            return response.json()
        else:
            get_operation_state_url = f"{fabric_baseurl}/operations/{operation_id}"

            # Poll the operation status until it's done
            while True:
                operation_state_response = requests.get(get_operation_state_url, headers=headers)
                operation_state = operation_state_response.json()
                
                status = operation_state.get("Status")
                
                if status in ["NotStarted", "Running"]:
                    print(".", end="", flush=True)
                    time.sleep(5)
                elif status == "Succeeded":
                    print(" Done!")
                    return response.json()
                else:
                    print(" Failed!")
                    return response.json()
            
    except requests.exceptions.RequestException as e:
        print(e)
        print(f"* Failed to initialize git connection. Check dependencies and resolve issues inside the repository before re-initializing.")
        return None


def get_workspace_git_status(access_token, workspace_id):
    """
    Retrieves the current Git connection status for a specified workspace.

    Args:
        access_token (str): Access token for authentication.
        workspace_id (str): Unique identifier of the workspace for which to retrieve Git status.

    Returns:
        dict: JSON response with Git status information if the request is successful.
        None: If the request fails or encounters an error.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    url = f"{fabric_baseurl}/workspaces/{workspace_id}/git/status"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        error_details = response.json().get("error", {}).get("message", str(e))
        print(f"* Failed to get workspace Git status: {error_details}")
        return None


def get_private_endpoint_resource_type(private_link_resource_id):
    """
    Determines the resource type associated with a given private link resource ID.

    Args:
        private_link_resource_id (str): The Azure Resource ID for the private link resource.

    Returns:
        str or None: The resource type associated with the private link resource ID. Possible values include:
            - "vault" for Key Vault
            - "sqlServer" for SQL Server
            - "blob" for Blob storage
            - "databricks_ui_api" for Databricks
            - "SQL" for DocumentDB
            - "cluster" for Kusto clusters
            - "Sql" for Synapse workspaces
            - "sites" for Web Apps
            - "namespace" for Event Hubs
            - "iotHub" for IoT Hubs
            - "account" for Purview accounts
            - "amlworkspace" for Machine Learning workspaces
            - None if the resource type cannot be determined.
    """
    match private_link_resource_id:
        case _ if "Microsoft.KeyVault" in private_link_resource_id:
            return "vault"
        case _ if "Microsoft.Sql" in private_link_resource_id:
            return "sqlServer"
        case _ if "Microsoft.Storage/storageAccounts" in private_link_resource_id:
            return "blob"
        case _ if "Microsoft.Databricks" in private_link_resource_id:
            return "databricks_ui_api"
        case _ if "Microsoft.DocumentDB" in private_link_resource_id:
            return "SQL"
        case _ if "Microsoft.Kusto/clusters" in private_link_resource_id:
            return "cluster"
        case _ if "Microsoft.Synapse/workspaces" in private_link_resource_id:
            return "Sql"
        case _ if "Microsoft.Web/sites" in private_link_resource_id:
            return "sites"
        case _ if "Microsoft.EventHub/namespaces" in private_link_resource_id:
            return "namespace"
        case _ if "Microsoft.Devices/IotHubs" in private_link_resource_id:
            return "iotHub"
        case _ if "Microsoft.Purview/accounts" in private_link_resource_id:
            return "account"
        case _ if "Microsoft.MachineLearningServices/workspaces" in private_link_resource_id:
            return "amlworkspace"
        case _:
            return None


def list_managed_private_endpoionts(access_token, workspace_id, continuation_token = None):
    """
    Lists managed privated endpoints in a Microsoft Fabric workspace, supporting pagination through continuation tokens.

    Args:
        access_token (str): The OAuth 2.0 access token for authenticating the API request.
        workspace_id (str): The unique identifier of the workspace from which to items managed private endpoints.
        continuation_token (str, optional): A token used for paginated results. Default is None.

    Returns:
        list: A list of dictionaries representing the managed private endpoints in the workspace.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    items = []
    while True:
        # Construct the request URL
        request_url = f"{fabric_baseurl}/workspaces/{workspace_id}/managedPrivateEndpoints"
        if continuation_token is not None:
            request_url += f"&continuationToken={continuation_token}"

        response = requests.get(request_url, headers=headers)
        
        if response.status_code == 200:
            response_data = response.json()
            
            items.extend(response_data['value'])

            continuation_token = response_data.get('continuationToken')

            if continuation_token is None:
                break
        else:
            print(f"Error: {response.status_code}")
            print(response)
            break

    return items


def create_workspace_managed_private_endpoint(access_token, workspace_id, endpoint_name, private_link_resource_id):
    """
    Creates a managed private endpoint within a Microsoft Fabric workspace and monitors its provisioning state until completion.

    Args:
        access_token (str): The OAuth2 bearer token used for authorization in API requests.
        workspace_id (str): The unique identifier of the workspace where the private endpoint will be created.
        endpoint_name (str): The name to assign to the managed private endpoint.
        private_link_resource_id (str): The resource ID of the target private link that the managed endpoint will connect to.

    Returns:
        dict: JSON response of the final private endpoint status if successful, or error details if provisioning fails.
        None: If the resource type cannot be identified based on the private link resource ID.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    workspace_endpoints = list_managed_private_endpoionts(access_token, workspace_id, continuation_token = None)
    endpoint = any(item.get("targetPrivateLinkResourceId") == private_link_resource_id for item in workspace_endpoints)
    
    if endpoint:
        print("Managed private endpoint for the specified resources id already exists.")
        return endpoint
    else:
        url = f"{fabric_baseurl}/workspaces/{workspace_id}/managedPrivateEndpoints"

        resource_type = get_private_endpoint_resource_type(private_link_resource_id)

        if resource_type:
            request_message = f"Automated request for Fabric managed private endpoint {endpoint_name}"
        
            body = {
                "name": endpoint_name,
                "targetPrivateLinkResourceId": private_link_resource_id,
                "targetSubresourceType": resource_type,
                "requestMessage": request_message
                }
            
            try:
                create_response = requests.post(url, headers=headers, json=body)
                create_response.raise_for_status()
                
                private_endpoint_id = create_response.json().get("id")
                print(f"Private endpoint {endpoint_name} is being provisioned in workspace {workspace_id}", end="")

                endpoint_url = f"{fabric_baseurl}/workspaces/{workspace_id}/managedPrivateEndpoints/{private_endpoint_id}"
                
                # Poll the private endpoint status until it's no longer in a creating/provisioning state
                while True:
                    pe_response = requests.get(endpoint_url, headers=headers)
                    pe_response.raise_for_status()
                    private_endpoint = pe_response.json()
                    
                    if private_endpoint and private_endpoint.get('provisioningState') in ["Updating", "Provisioning", "Deleting"]:
                        print(".", end="", flush=True)
                        time.sleep(10)
                    else:
                        break

                if private_endpoint and private_endpoint.get('provisioningState') == "Succeeded":
                    print(" Done!")
                    return pe_response.json()
                else:
                    print("Error creating private endpoint!")
                    return pe_response.json()

            except requests.exceptions.RequestException as e:
                print(f"Could not create private endpoint {endpoint_name} in workspace {workspace_id}. {e}")
                return create_response.json()
        else:
            print("Resource type not identified based on private endpoint resource id. Skipping creation of workspace managed private endpoint.")
            return None


def update_notebook_lakehouse_definition(notebook_definition, target_lakehouses):
    """
    Updates the lakehouse definition in a notebook's metadata section based on the provided target lakehouses.

    The function scans the notebook's definition for an existing lakehouse definition in the metadata section 
    (denoted by "# META") and updates it with information from the provided target lakehouses. The updated 
    metadata will include the lakehouse ID, workspace ID, and known lakehouses, if a matching lakehouse is found 
    in the target lakehouses list.

    Args:
        notebook_definition (str): The current definition of the notebook as a string, including metadata.
        target_lakehouses (list): A list of dictionaries, where each dictionary represents a lakehouse, 
                                   containing keys such as 'lakehouse_name', 'lakehouse_id', 'workspace_id', 
                                   and 'known_lakehouses'.

    Returns:
        str: The updated notebook definition, with the modified lakehouse metadata section. If no lakehouse 
             definition is found in the notebook, the original definition is returned unchanged.

    Example:
        notebook_definition = "
        # META   "dependencies": {
        # META   "lakehouse": {
        # META   "default_lakehouse_name": "Landing"
        # META   }
        # META   }
        "

        target_lakehouses = [
            {"lakehouse_name": "Landing", "lakehouse_id": "aaaaaaaa-0000-1111-2222-bbbbbbbbbbbb", "workspace_id": "cccccccc-0000-1111-2222-dddddddddddd"},
            {"lakehouse_name": "Base", "lakehouse_id": "bbbbbbbb-1111-2222-3333-cccccccccccc", "workspace_id": "eeeeeeee-0000-1111-2222-eeeeeeeeeeee", "known_lakehouses": ["Curated"]}
        ]

        updated_definition = update_notebook_lakehouse_definition(notebook_definition, target_lakehouses)
        print(updated_definition)
    
    In the above example, the notebook's metadata section for "Landing" will be updated with the matching 
    lakehouse ID, workspace ID, and known lakehouses.
    """
    has_lakehouse_definition = notebook_definition.find('# META   "dependencies": {')

    if has_lakehouse_definition > -1:
        meta_end = notebook_definition.find("# META }", has_lakehouse_definition) + 10
        meta_start = notebook_definition.rfind("# META {", 0, meta_end)
        
        lakehouse_definition_raw = notebook_definition[meta_start:meta_end]
        lakehouse_definition = lakehouse_definition_raw.replace("# META", "").strip()

        json_object = json.loads(lakehouse_definition)
        
        matching_object = next(
            (lakehouse for lakehouse in target_lakehouses 
             if lakehouse.get("lakehouse_name") == json_object["dependencies"]["lakehouse"]["default_lakehouse_name"]),
            None
        )

        if matching_object:
            json_object["dependencies"]["lakehouse"]["default_lakehouse"] = matching_object.get("lakehouse_id")
            json_object["dependencies"]["lakehouse"]["default_lakehouse_workspace_id"] = matching_object.get("workspace_id")
            json_object["dependencies"]["lakehouse"]["known_lakehouses"] = matching_object.get("known_lakehouses", [])

        updated_json_string = json.dumps(json_object, indent=4)

        meta_updated_json = "\n".join([f"# META {line}" for line in updated_json_string.splitlines()])

        new_notebook_definition = (
            notebook_definition[:meta_start] +
            meta_updated_json +
            "\n\n"+
            notebook_definition[meta_end:]
        )
        return new_notebook_definition

    return notebook_definition

    
def list_items(access_token, workspace_id, item_type, continuation_token = None):
    """
    Lists items of a specific type in a Microsoft Fabric workspace, supporting pagination through continuation tokens.

    Args:
        access_token (str): The OAuth 2.0 access token for authenticating the API request.
        workspace_id (str): The unique identifier of the workspace from which to list items.
        item_type (str): The type of items to list (e.g., "DataPipeline", "Notebook").
        continuation_token (str, optional): A token used for paginated results. Default is None.

    Returns:
        list: A list of dictionaries representing the items in the workspace.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    items = []
    while True:
        # Construct the request URL
        request_url = f"{fabric_baseurl}/workspaces/{workspace_id}/items?dt=1"
        if item_type:
            request_url += f"&type={item_type}"

        if continuation_token:
            request_url += f"&continuationToken={continuation_token}"

        response = requests.get(request_url, headers=headers)
        
        if response.status_code == 200:
            response_data = response.json()
            
            items.extend(response_data['value'])

            continuation_token = response_data.get('continuationToken')

            if continuation_token is None:
                break
        else:
            print(f"Error: {response.status_code}")
            print(response)
            break

    return items


def get_operation_result(access_token, operation_id):
    """
    Retrieves the result of a Microsoft Fabric operation using its operation ID.

    Args:
        access_token (str): The OAuth 2.0 access token for authenticating the API request.
        operation_id (str): The unique identifier of the operation whose result is to be fetched.

    Returns:
        dict or None:
            - A dictionary containing the operation result if the request is successful.
            - None if the request fails or encounters an error.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    url = f"{fabric_baseurl}/operations/{operation_id}/result"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        error_details = response.json().get("error", {}).get("message", str(e))
        print(f"* Failed to get operation status: {error_details}")
        return None


def create_item(access_token, workspace_id, item_name, item_type, definition_base64, print_progress = False, print_output = True):
    """
    Creates a new item in a specified Microsoft Fabric workspace.

    Args:
        access_token (str): OAuth 2.0 bearer token used to authenticate the API request.
        workspace_id (str): The unique identifier of the workspace where the item will be created.
        item_name (str): The name of the item to be created in the workspace.
        item_type (str): The type of the item, such as "Lakehouse", "DataPipeline" or "Notebook".
        definition_base64 (str or None): A base64-encoded string containing the item's definition, if required. 
            If None, the definition is not included in the request.
        print_progress (bool, optional): A flag to print progress during the create operation. Default is `False`.
        print_output (bool, optional): A flag to indicate if function show print any output at all (progress or standard). Default is `True`.

    Returns:
        dict: A dictionary containing the JSON response from the API, which includes details of the newly created item.
    """

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    body = {
        "displayName": item_name,
        "type": item_type,
    }
    
    if definition_base64 is not None:
        item_path = ""
        if item_type == "DataPipeline":
            item_path = "pipeline-content.json"
        elif item_type == "Notebook":
            item_path = "notebook-content.py"

        body["definition"] = {
            "parts": [
                {
                    "path": item_path,
                    "payload": definition_base64,
                    "payloadType": "InlineBase64"
                }
            ]
        }
    
    if print_progress and print_output: print(f"Creating {item_type} {item_name}...", end='')

    try:
        response = requests.post(f"{fabric_baseurl}/workspaces/{workspace_id}/items", headers=headers, json=body)
        response.raise_for_status()
        operation_id = response.headers.get('x-ms-operation-id')

        if operation_id is None:
            if print_output:
                print("Done!" if print_progress else f"{item_type} {item_name} was successfully created.")
            return response.json()
        else:
            get_operation_state_url = f"{fabric_baseurl}/operations/{operation_id}"
            while True:
                operation_state_response = requests.get(get_operation_state_url, headers=headers)
                operation_state = operation_state_response.json()
                status = operation_state.get("status")
                if status in ["NotStarted", "Running"]:
                    if print_progress and print_output: print(".", end="", flush=True)
                    time.sleep(2)
                elif status == "Succeeded":
                    if print_output:
                        print("Done!" if print_progress else f"{item_type} {item_name} was successfully created.")
                    get_operation_result_url = f"{fabric_baseurl}/operations/{operation_id}/result"
                    operation_result_response = requests.get(get_operation_result_url, headers=headers)
                    return operation_result_response.json()
                else:
                    if print_output:
                        print(f" Failed (operationsid: {operation_id})!" if print_progress else f"{item_type} {item_name} could not be created (operationsid: {operation_id}).")
                    return response.json()
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 400:
            error_response = response.json()
            if error_response.get("errorCode") == "ItemDisplayNameAlreadyInUse":
                if print_output:
                    print(f"{item_type} {item_name} already in use.")
                items = list_items(access_token, workspace_id, item_type)
                return next((lh for lh in items if lh['displayName'] == item_name), None)
        return None
    

def update_item_definition(access_token, workspace_id, item_id, item_name, item_type, definition_base64, print_progress = False):
    """
    Updates the definition of an existing item in a Microsoft Fabric workspace.

    Args:
        access_token (str): The OAuth 2.0 access token for authenticating the API request.
        workspace_id (str): The unique identifier of the workspace containing the item.
        item_id (str): The unique identifier of the item to update.
        item_name (str): The name of the item to be updated, used for progress reporting.
        item_type (str): The type of the item (e.g., "DataPipeline", "Notebook").
        definition_base64 (str or None): A base64-encoded string containing the item's updated definition.
            If `None`, the update will not proceed, and `None` will be returned.
        print_progress (bool, optional): A flag to print progress during the update operation. Default is `False`.

    Returns:
        dict or None: A dictionary containing the JSON response from the API with details of the updated item if successful, 
        or None if the definition is missing.
    """

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    if definition_base64 is not None:
        item_path = ""
        if item_type == "DataPipeline":
            item_path = "pipeline-content.json"
        elif item_type == "Notebook":
            item_path = "notebook-content.py"

        body = {
            "definition": {
                "parts": [
                    {
                        "path": item_path,
                        "payload": definition_base64,
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }

        if print_progress: print(f"Updating {item_type} definition for {item_name}...", end='')
    
        response = requests.post(f"{fabric_baseurl}/workspaces/{workspace_id}/items/{item_id}/updateDefinition", headers=headers, json=body)
        response.raise_for_status()
        operation_id = response.headers.get('x-ms-operation-id')

        if operation_id is None:
            print("Done!" if print_progress else f"{item_type} definition for {item_name} was successfully updated.")
            return response.json()
        else:
            get_operation_state_url = f"{fabric_baseurl}/operations/{operation_id}"
            while True:
                operation_state_response = requests.get(get_operation_state_url, headers=headers)
                operation_state = operation_state_response.json()
                status = operation_state.get("status")
                
                if status in ["NotStarted", "Running"]:
                    if print_progress: print(".", end="", flush=True)
                    time.sleep(2)
                elif status == "Succeeded":
                    print("Done!" if print_progress else f"{item_type} definition for {item_name} was successfully updated.")
                    return response.json()
                else:
                    print(f" Failed (operationsid: {operation_id})!" if print_progress else f"{item_type} definition for {item_name} could not be updated (operationsid: {operation_id}).")
                    return response.json()
    else:
        print(f"{item_type} definition for {item_name} (id:{item_id}) could not be updated. Definition is missing!")
        return None


def get_lakehouse(access_token, workspace_id, lakehouse_id):
    """
    Fetches details of a specific lakehouse from a Microsoft Fabric workspace.

    Args:
        access_token (str): The OAuth2 access token for authentication.
        workspace_id (str): The unique identifier of the Microsoft Fabric workspace.
        lakehouse_id (str): The unique identifier of the lakehouse within the workspace.

    Returns:
        dict or None: A dictionary containing the lakehouse details if the request is successful, 
                      or None if the request fails or encounters an error.

    Raises:
        requests.exceptions.RequestException: If an error occurs during the API request.

    Example:
        lakehouse_details = get_lakehouse("your_access_token", "workspace123", "lakehouse456")
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    url = f"{fabric_baseurl}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        error_details = response.json().get("error", {}).get("message", str(e))
        print(f"* Failed getting lakehouse details: {error_details}")
        return None
    
def get_sqldatabase(access_token, workspace_id, database_id):
    """
    Fetches details of a specific SQL Database from a Microsoft Fabric workspace.

    Args:
        access_token (str): The OAuth2 access token for authentication.
        workspace_id (str): The unique identifier of the Microsoft Fabric workspace.
        database_id (str): The unique identifier of the SQL Database within the workspace.

    Returns:
        dict or None: A dictionary containing the lakehouse details if the request is successful, 
                      or None if the request fails or encounters an error.

    Raises:
        requests.exceptions.RequestException: If an error occurs during the API request.

    Example:
        database_details = get_sqldatabase("your_access_token", "workspace123", "database456")
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    url = f"{fabric_baseurl}/workspaces/{workspace_id}/SqlDatabases/{database_id}"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        error_details = response.json().get("error", {}).get("message", str(e))
        print(f"* Failed getting sql database details: {error_details}")
        return None


def create_sql_connection(access_token, connection_name, server_fqdn, database_name, tenant_id, username, password):
    """
    Creates a new connection in Microsoft Fabric/Power BI

    Args:
        access_token (str): OAuth 2.0 bearer token for authenticating the API request.
        connection_name (str): The name of the connection to create.
        server_fqdn (str): Fully qualified domain name/server address.
        database_name (str): Fully qualified domain name/server address.
        tenant_id (str): Tenant.
        username (str): Username.
        password (str): Password of user.

    Returns:
        dict or None: A dictionary containing the workspace details if the workspace is successfully created. 
                      Returns None if the workspace already exists or if an error occurs during creation.
    """
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    body = {
        "connectivityType": "ShareableCloud",
        "displayName":f"{connection_name}",
        "connectionDetails": {
            "type": "SQL",
            "creationMethod": "SQL",
            "parameters": [
            {
                "dataType": "Text",
                "name": "server",
                "value": f"{server_fqdn}"
            },
            {
                "dataType": "Text",
                "name": "database",
                "value": f"{database_name}"
            }
            ]
        },
        "privacyLevel": "Organizational",
        "credentialDetails":{
            "singleSignOnType":"None",
            "connectionEncryption": "Encrypted",
            "skipTestConnection": False,
            "credentials": {
                "credentialType": "ServicePrincipal",
                "servicePrincipalClientId": f"{username}",
                "servicePrincipalSecret": f"{password}",
                "tenantId": f"{tenant_id}"
            }
        }
    }
        
    try:
        print(f"Creating connection {connection_name}...", end="")
        response = requests.post(f"https://api.fabric.microsoft.com/v1/connections", headers=headers, json=body)
        response.raise_for_status()
        print(" Done!")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        error_response = response.json()
        error_code = error_response.get("errorCode")

        if error_code == "DuplicateConnectionName":
            print(" Already exists...", end="", flush=True)

            connection = get_connection(access_token, connection_name)
            
            if connection:
                connection_details = connection.get("connectionDetails")                
                old_connection_path = connection_details.get("path")
                new_connection_path = f"{server_fqdn};{database_name}"
        
                if old_connection_path == new_connection_path:
                    print(" Connection details match. Updating credentials...", end="", flush=True)
                    connection = update_connection_credentials(access_token, connection['id'], tenant_id, username, password)
                    print(" Done!")
                    return connection
                else:
                    print(" Connection details does not match. Delete and recreate.")
                    roleassignments = get_connection_roleassignments(access_token, connection['id'])
                    delete_connection(access_token, connection['id'])
                    new_connection = create_sql_connection(access_token, connection_name, server_fqdn, database_name, tenant_id, username, password)

                    #Re-apply existing role assignments for the connection
                    if roleassignments:
                        for roleassignment in roleassignments:
                            add_connection_roleassignment(access_token, new_connection.get("id"), roleassignment)
             
                    return new_connection
            else:
                print("Existing connection cannot be fetched. Possible reason: Identity is not user of the connection. Skipping connection creation.")
        else:
            print(f'Failed! {error_response.get("message")}')
            return None

def get_item(access_token, workspace_id, item_name):
    """
    Get a Fabric item by its name from a specific workspace

    Args:
        access_token (str): OAuth 2.0 bearer token for authenticating the API request.
        workspace_id (str): The ID of the workspace.
        item_name (str): The name of the item

    Returns:
        dict or None: A dictionary containing the item details
                      Returns None if the item does not exist.
    """
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.get("https://api.fabric.microsoft.com/v1/connections", headers=headers)
    data = response.json()
    connection = next((item for item in data["value"] if item["displayName"] == connection_name),None)
    return connection


def get_connection(access_token, connection_name):
    """
    Get connection in Microsoft Fabric/Power BI by name

    Args:
        access_token (str): OAuth 2.0 bearer token for authenticating the API request.
        connection_name (str): The name of the connection to get.

    Returns:
        dict or None: A dictionary containing the connection details
                      Returns None if the connection does not exist.
    """
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.get("https://api.fabric.microsoft.com/v1/connections", headers=headers)
    data = response.json()
    connection = next((item for item in data["value"] if item["displayName"] == connection_name),None)
    return connection

def get_connection_roleassignments(access_token, connection_id):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.get(f"https://api.fabric.microsoft.com/v1/connections/{connection_id}/roleAssignments", headers=headers)
    response.raise_for_status()

    return response.json()


def add_connection_roleassignment(access_token, connection_id, roleassignment, print_output: bool = True):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(f"https://api.fabric.microsoft.com/v1/connections/{connection_id}/roleAssignments", headers=headers, json=roleassignment)
        response.raise_for_status()
        print(f"Role assignment for connection ({connection_id}) successfully updated!") if print_output else None
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        error_response = response.json()
        error_code = error_response.get("errorCode")

        if error_code == "ConnectionRoleAssignmentAlreadyExists":
            print(f"Role assignment for connection ({connection_id}) already exists!") if print_output else None
            return roleassignment
        else:
            print(f"Role assignment for connection ({connection_id}) could not be created. Error code: {error_code}!") if print_output else None


def delete_connection(access_token, connection_id, print_output: bool = True):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.delete(f"https://api.fabric.microsoft.com/v1/connections/{connection_id}", headers=headers)
    response.raise_for_status()
    
    if print_output is True:
        print(f"Connection with id {connection_id} successfully deleted!")


def update_connection_credentials(access_token, connection_id, tenant_id, username, password):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    body = {
        "connectivityType": "ShareableCloud",
        "credentialDetails":{
            "singleSignOnType":"None",
            "connectionEncryption": "Encrypted",
            "skipTestConnection": False,
            "credentials": {
                "credentialType": "ServicePrincipal",
                "servicePrincipalClientId": f"{username}",
                "servicePrincipalSecret": f"{password}",
                "tenantId": f"{tenant_id}"
            }
        }
    }
    
    response = requests.patch(f"https://api.fabric.microsoft.com/v1/connections/{connection_id}", headers=headers, json=body)
    response.raise_for_status()

    return response.json()


def run_fabric_job(access_token, workspace_id, item_id, job_type, payload, print_output = False):
    """
    Run a Fabric job on demand.

    Args:
        access_token (str): The OAuth access token for authentication.
        workspace_id (str): The unique identifier of the workspace.
        item_id (str): The unique identifier of the item to run.
        job_type (str): The job type.
        payload (dict): Payload to pass as json to the job scheduler endpoint
        print_output (bool, optional): If True, prints status messages. Defaults to False.

    Returns:
        dict or None: The JSON response from the API if successful, otherwise None.

    Raises:
        requests.exceptions.RequestException: If an error occurs during the request.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    print(f"Executing job type {job_type}, item id: {item_id} in workspace {workspace_id}...", end="") if print_output == True else None
    response = requests.post(f"{fabric_baseurl}/workspaces/{workspace_id}/items/{item_id}/jobs/instances?jobType={job_type}", headers=headers, json = payload)
    response.raise_for_status()

    if response.status_code == 202:
        location = response.headers.get("Location")
        time.sleep(5) #Sleep for a couple of seconds to ensure the job has been submitted...

        while True:
            response = requests.get(location, headers=headers)
            response.raise_for_status()
            response_data = response.json()
            if response_data.get('status') in ["InProgress", "NotStarted"]:
                print(".", end="", flush=True) if print_output == True else None
                time.sleep(5)
            else:
                print(" Done!") if print_output == True else None
                return response_data
    else:
        print(f" Failed!") if print_output == True else None
        return None
    

def make_request(method, url, headers=None, json=None, params=None, data=None, max_retries=5, timeout=10, print_progress=False):
    retry_count = 0

    while retry_count < max_retries:
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=json,
            params=params,
            data=data,
            timeout=timeout
        )

        if response.status_code == 429:  # Too Many Requests
            retry_after = int(response.headers.get("Retry-After", 5)) 
            if print_progress: print("...", end="", flush=True)
            time.sleep(retry_after)
            retry_count += 1
            continue 

        response.raise_for_status()
        return response

    raise Exception("Max retries reached. Request failed.")  # Raise exception if max retries exceeded


def update_workspace_spark_settings(access_token, workspace_id, settings_definition, print_output = False):
    """
    Updates the Spark settings of a specified Microsoft Fabric workspace.

    Args:
        access_token (str): The OAuth access token for authentication.
        workspace_id (str): The unique identifier of the workspace.
        settings_definition (dict): The Spark settings to be applied to the workspace.
        print_output (bool, optional): If True, prints status messages. Defaults to False.

    Returns:
        dict or None: The JSON response from the API if successful, otherwise None.

    Raises:
        requests.exceptions.RequestException: If an error occurs during the request.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    try:
        print(f"Updating workspace spark settings... ", end="") if print_output == True else None
        response = requests.patch(f"{fabric_baseurl}/workspaces/{workspace_id}/spark/settings", headers=headers, json = settings_definition)
        response.raise_for_status()
        print("Done!") if print_output == True else None
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed! Error: {e}") if print_output == True else None
        print(response.json())
        return None
    

def set_connection(access_token, connection_id, server, database, tenant_id, username, password):
    """
    Updates the credentials for a specified data source in a Power BI gateway cluster.

    This function sends a PATCH request to update the data source credentials, using a service principal's 
    client ID, secret, and tenant ID. It authenticates the request with an access token and updates the 
    credential information for the specified cluster and data source.

    Args:
        access_token (str): The OAuth 2.0 access token for authenticating the API request.
        cluster_id (str): The ID of the gateway cluster containing the data source.
        datasource_id (str): The ID of the data source for which the credentials are being updated.
        tenant_id (str): The Azure AD tenant ID associated with the service principal.
        username (str): The service principal's client ID.
        password (str): The service principal's client secret.

    Returns:
        dict or None: A JSON response from the Power BI API containing the updated data source credentials
                      if successful, or `None` if an error occurs during the request.

    Raises:
        requests.exceptions.RequestException: If an error occurs while sending the HTTP request or if
                                              the API response indicates a failure.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    credential_value = json.dumps({
        "credentialData": [
            {"name": "servicePrincipalClientId", "value": username},
            {"name": "servicePrincipalSecret", "value": password},
            {"name": "tenantId", "value": tenant_id}
        ]
    })

    body = {
        "connectivityType": "ShareableCloud"
    }
    response = requests.patch(f"{fabric_baseurl}/connections/{connection_id}", headers=headers, json=body)
    response.raise_for_status()
    print(response.json())


def list_workspaces(access_token, continuation_token = None):
    """
    Lists workspaces with the executor has access to, supporting pagination through continuation tokens.

    Args:
        access_token (str): The OAuth 2.0 access token for authenticating the API request.
        continuation_token (str, optional): A token used for paginated results. Default is None.

    Returns:
        list: A list of dictionaries representing the wworkspace.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    workspaces = []
    while True:
        # Construct the request URL
        request_url = f"{fabric_baseurl}/workspaces?dt=1"
        
        if continuation_token:
            request_url += f"&continuationToken={continuation_token}"

        response = requests.get(request_url, headers=headers)
        
        if response.status_code == 200:
            response_data = response.json()
            
            workspaces.extend(response_data['value'])

            continuation_token = response_data.get('continuationToken')

            if continuation_token is None:
                break
        else:
            print(f"Error: {response.status_code}")
            print(response)
            break

    return workspaces

def get_lakehouse_sqlendpoint(access_token, workspace_id, lakehouse_id):
    """
    Retrieves the SQL endpoint connection string for a specified lakehouse in a Fabric workspace.

    This function repeatedly checks the provisioning status of the SQL endpoint for the specified lakehouse 
    in the Fabric workspace. It waits until the SQL endpoint is successfully provisioned or until it fails. 
    The function returns the lakehouse details, including the connection string if the provisioning is successful.

    Args:
        access_token (str): The OAuth 2.0 access token for authenticating the API request.
        workspace_id (str): The ID of the Fabric workspace containing the lakehouse.
        lakehouse_id (str): The ID of the lakehouse for which the SQL endpoint is being provisioned.

    Returns:
        dict: The details of the lakehouse, including the SQL endpoint connection string if successful,
              or the lakehouse details with an error message if the provisioning fails.

    Raises:
        requests.exceptions.RequestException: If an error occurs while fetching lakehouse details.
    """
    sql_endpoint_connectionstring = None
    
    while sql_endpoint_connectionstring is None:
        lh_details = get_lakehouse(access_token, workspace_id, lakehouse_id)
        properties = lh_details.get("properties", {})
        sql_endpoint_properties = properties.get("sqlEndpointProperties")
        
        # Extract SQL endpoint details
        if sql_endpoint_properties is not None:
            if (sql_endpoint_properties.get("provisioningStatus") == "InProgress"):
                time.sleep(2)
            elif (sql_endpoint_properties.get("provisioningStatus") == "Failed"):
                return lh_details
            else:
                sql_endpoint_connectionstring = sql_endpoint_properties.get("connectionString")
                return lh_details
        else:
            time.sleep(2)


def take_over_dataset(access_token, workspace_id, dataset_id):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/Default.TakeOver", headers=headers)
        response.raise_for_status()
        print(f"Semantic model {dataset_id} in workspace {workspace_id} taken over by SPN.")
    except requests.exceptions.RequestException as e:
        print(f"Could not take over dataset. Error: {e}")


def bind_dataset_datasource(access_token, workspace_id, dataset_id, gateway_id, datasource_id):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "gatewayObjectId": gateway_id,
        "datasourceObjectIds": [
            datasource_id
        ]
    }

    try:
        response = requests.post(f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/Default.BindToGateway", json=payload, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        error_details = response.json().get("error", {}).get("message", str(e))
        print(f"* Failed getting lakehouse details: {error_details}")
