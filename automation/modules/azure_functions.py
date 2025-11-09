import requests, time

def get_private_endpoint_api_version(private_link_resource_id):
    """
    Determines the appropriate API version for a given private link resource in Azure based on its resource type.

    Args:
        private_link_resource_id (str): The Azure Resource ID of the private link resource for which the API version is required.

    Returns:
        str: The API version string corresponding to the specified private link resource type. Returns None if the resource type
             is not recognized.

    Resource Type - API Version Mapping:
        - Microsoft.KeyVault: "2022-07-01"
        - Microsoft.Sql: "2021-11-01"
        - Microsoft.Storage/storageAccounts: "2018-02-01"
        - Microsoft.Databricks: "2024-05-01"
        - Microsoft.DocumentDB: "2024-05-01"
        - Microsoft.Kusto/clusters: "2023-08-15"
        - Microsoft.Synapse/workspaces: "2021-06-01"
        - Microsoft.Web/sites: "2024-04-01"
        - Microsoft.EventHub/namespaces: "2024-01-01"
        - Microsoft.Devices/IotHubs: "2023-06-30"
        - Microsoft.Purview/accounts: "2021-12-01"
        - Microsoft.MachineLearningServices/workspaces: "2024-04-01"
    """
    match private_link_resource_id:
        case _ if "Microsoft.KeyVault" in private_link_resource_id:
            api_version = "2022-07-01"
        case _ if "Microsoft.Sql" in private_link_resource_id:
            api_version = "2021-11-01"
        case _ if "Microsoft.Storage/storageAccounts" in private_link_resource_id:
            api_version = "2018-02-01"
        case _ if "Microsoft.Databricks" in private_link_resource_id:
            api_version = "2024-05-01"
        case _ if "Microsoft.DocumentDB" in private_link_resource_id:
            api_version = "2024-05-01"
        case _ if "Microsoft.Kusto/clusters" in private_link_resource_id:
            api_version = "2023-08-15"
        case _ if "Microsoft.Synapse/workspaces" in private_link_resource_id:
            api_version = "2021-06-01"
        case _ if "Microsoft.Web/sites" in private_link_resource_id:
            api_version = "2024-04-01"
        case _ if "Microsoft.EventHub/namespaces" in private_link_resource_id:
            api_version = "2024-01-01"
        case _ if "Microsoft.Devices/IotHubs" in private_link_resource_id:
            api_version = "2023-06-30"
        case _ if "Microsoft.Purview/accounts" in private_link_resource_id:
            api_version = "2021-12-01"
        case _ if "Microsoft.MachineLearningServices/workspaces" in private_link_resource_id:
            api_version = "2024-04-01"
        case _:
            api_version = None
    
    return api_version

def list_private_endpoints(access_token, private_link_resource_id):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    api_version = get_private_endpoint_api_version(private_link_resource_id)

    url = f"https://management.azure.com{private_link_resource_id}/privateEndpointConnections?api-version={api_version}"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error if the request fails
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Could not get private endpoints: {e}")
        return None


def get_private_endpoint_by_name(access_token, private_link_resource_id, private_endpoint_connection_name):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    response = list_private_endpoints(access_token, private_link_resource_id)
    result = next((item for item in response["value"] if private_endpoint_connection_name in item["name"]), None)
    return result

def approve_private_endpoint(access_token, private_link_resource_id, private_endpoint_connection_name):
    """
    Approves a private endpoint connection within Azure, updating its status to "Approved" through an automated API request.
    
    Args:
        access_token (str): The OAuth 2.0 bearer token used for authorization in API requests.
        private_link_resource_id (str): The Azure Resource ID of the target private link resource associated with the private endpoint connection.
        private_endpoint_connection_name (str): The name of the specific private endpoint connection to approve.
    
    Returns:
        dict: JSON response containing details of the approved private endpoint connection if the request is successful.
        None: If the approval request fails.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    api_version = get_private_endpoint_api_version(private_link_resource_id)

    url = f"https://management.azure.com{private_link_resource_id}/privateEndpointConnections/{private_endpoint_connection_name}?api-version={api_version}"

    body = {
        "properties": {
            "privateLinkServiceConnectionState": {
                "status": "Approved",
                "description": "Approved by automated flow."
            }
        }
    }

    try:
        response = requests.put(url, headers=headers, json=body)
        response.raise_for_status()  # Raise an error if the request fails
        print("Private endpoint connection was automatically approved.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Could not update private endpoint: {e}")
        return None
    

def get_keyvault_secret(access_token, key_vault_name, secret_name):
    """
    Retrieves a secret value from an Azure Key Vault.

    Args:
        access_token (str): The OAuth 2.0 access token for authenticating the API request.
        key_vault_name (str): The name of the Azure Key Vault.
        secret_name (str): The name of the secret to retrieve.

    Returns:
        str or None: The value of the secret if the request is successful, or None if the request fails.

    Example:
        secret_value = get_keyvault_secret("your_access_token", "myKeyVault", "mySecret")
    """
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    key_vault_url = f"https://{key_vault_name}.vault.azure.net/secrets/{secret_name}?api-version=7.4"

    response = requests.get(key_vault_url, headers=headers)

    if response.status_code == 200:
        return response.json()["value"]
    else:
        return None


def set_keyvault_secret(access_token, key_vault_name, secret_name, secret_value):
    """
    Sets a secret value in an Azure Key Vault.

    Args:
        access_token (str): The OAuth 2.0 access token for authenticating the API request.
        key_vault_name (str): The name of the Azure Key Vault.
        secret_name (str): The name of the secret to set.
        secret_value (str): The value to store for the secret.

    Returns:
        bool: True if the secret was successfully set, False otherwise.

    Example:
        success = set_keyvault_secret("your_access_token", "myKeyVault", "mySecret", "mySecretValue")
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    key_vault_url = f"https://{key_vault_name}.vault.azure.net/secrets/{secret_name}?api-version=7.4"

    data = {
        "value": secret_value
    }
    response = requests.put(key_vault_url, headers=headers, json=data)
    
    if response.status_code == 200:
        return True
    else:
        return False
    

def delete_keyvault_secret(access_token, key_vault_name, secret_name, purge: bool = True):
    """
    Deletes a secret in an Azure Key Vault.

    Args:
        access_token (str): The OAuth 2.0 access token for authenticating the API request.
        key_vault_name (str): The name of the Azure Key Vault.
        secret_name (str): The name of the secret to delete.
        purge (bool): Indicates if the secrets should be purged to only soft-deleted.

    Returns:
        bool: True if the secret was successfully deleted, False otherwise.

    Example:
        success = delete_keyvault_secret("your_access_token", "myKeyVault", "mySecret", True)
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    key_vault_url = f"https://{key_vault_name}.vault.azure.net/secrets/{secret_name}?api-version=7.4"

    response = requests.delete(key_vault_url, headers=headers)

    if response.status_code == 200:
        if purge is True:
            time.sleep(3)
            return purge_keyvault_secret(access_token, key_vault_name, secret_name)
        else:
            return True
    else:
        return False


def purge_keyvault_secret(access_token, key_vault_name, secret_name, purge: bool = True):
    """
    Purges a secret in an Azure Key Vault.

    Args:
        access_token (str): The OAuth 2.0 access token for authenticating the API request.
        key_vault_name (str): The name of the Azure Key Vault.
        secret_name (str): The name of the secret to purge

    Returns:
        bool: True if the secret was successfully purged, False otherwise.

    Example:
        success = purge_keyvault_secret("your_access_token", "myKeyVault", "mySecret")
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    key_vault_url = f"https://{key_vault_name}.vault.azure.net/deletedsecrets/{secret_name}?api-version=7.4"

    counter = 1
    while True:
        response = requests.delete(key_vault_url, headers=headers)

        if response.status_code == 204:
            result = True
            break
        elif counter == 10:
            result = False
            break
        elif 'error' in response.json() and response.json()['error'].get('innererror', {}).get('code') == 'ObjectIsBeingDeleted':
            time.sleep(2)
            counter +=1
        else:
            result = False
            break
    
    return result