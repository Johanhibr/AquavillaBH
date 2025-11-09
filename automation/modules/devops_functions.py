import requests
import json

def create_branch(access_token, devops_org_name, devops_project_name, devops_repo_name, devops_base_branch_name, devops_new_branch_name):
    """
    Creates a new branch in an Azure DevOps repository based on an existing branch.

    Args:
        access_token (str): Personal Access Token (PAT) for Azure DevOps authentication.
        devops_org_name (str): Name of the Azure DevOps organization.
        devops_project_name (str): Name of the Azure DevOps project.
        devops_repo_name (str): Name of the repository where the branch will be created.
        devops_base_branch_name (str): Name of the base branch to create the new branch from.
        devops_new_branch_name (str): Name of the new branch to be created.

    Returns:
        dict: JSON response from the Azure DevOps API containing details of the newly created branch.
        None: If there is an error during the branch creation process.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    devops_org_url = f"https://dev.azure.com/{devops_org_name}/"

    try:
        base_branch_url = f"{devops_org_url}{devops_project_name}/_apis/git/repositories/{devops_repo_name}/refs?filter=heads/{devops_base_branch_name}&api-version=7.0"
        base_branch_response = requests.get(base_branch_url, headers=headers)
        base_branch_response.raise_for_status()
        base_branch_data = base_branch_response.json()
        
        base_object_id = base_branch_data['value'][0]['objectId']

        new_branch_url = f"{devops_org_url}{devops_project_name}/_apis/git/repositories/{devops_repo_name}/refs?api-version=7.0"
        body = [
            {
                "name": f"refs/heads/{devops_new_branch_name}",
                "newObjectId": base_object_id,
                "oldObjectId": "0000000000000000000000000000000000000000"
            }
        ]
                   
        new_branch_response = requests.post(new_branch_url, headers=headers, json=body)
        new_branch_response.raise_for_status()
        
        print(f"New branch {devops_new_branch_name} was created based on {devops_base_branch_name}.")
        return new_branch_response.json()
    except requests.exceptions.RequestException as e:
        error_message = e.response.json().get("message", str(e))
        print(f"Error creating new branch {devops_new_branch_name} based on {devops_base_branch_name}. {error_message}")
        return None


def delete_branch(access_token, devops_org_name, devops_project_name, devops_repo_name, devops_branch_name):
    """
    Deletes a branch in an Azure DevOps repository.

    Args:
        access_token (str): Personal Access Token (PAT) for Azure DevOps authentication.
        devops_org_name (str): Name of the Azure DevOps organization.
        devops_project_name (str): Name of the Azure DevOps project.
        devops_repo_name (str): Name of the repository where the branch is located.
        devops_branch_name (str): Name of the branch to delete.

    Returns:
        dict: JSON response from the Azure DevOps API containing details of the updated (deleted) branch.
        None: If there is an error during the branch deletion process or the branch does not exists or is already deleted.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    devops_org_url = f"https://dev.azure.com/{devops_org_name}/"

    try:
        branch_url = f"{devops_org_url}{devops_project_name}/_apis/git/repositories/{devops_repo_name}/refs?filter=heads/{devops_branch_name}&api-version=7.0"
        branch_response = requests.get(branch_url, headers=headers)
        branch_response.raise_for_status()
        branch_data = branch_response.json()
        
        if not branch_data is None and branch_data.get("count",0) > 0 :
            object_id = branch_data['value'][0]['objectId']

            delete_branch_url = f"{devops_org_url}{devops_project_name}/_apis/git/repositories/{devops_repo_name}/refs?api-version=7.0"
            body = [
                {
                    "name": f"refs/heads/{devops_branch_name}",
                    "oldObjectId": object_id,
                    "newObjectId": "0000000000000000000000000000000000000000",
                    
                }
            ]
                    
            delete_branch_response = requests.post(delete_branch_url, headers=headers, json=body)
            delete_branch_response.raise_for_status()
            
            print(f"Branch {devops_branch_name} was deleted.")
            return delete_branch_response.json()
        else:
            return None
    except requests.exceptions.RequestException as e:
        error_message = e.response.json().get("message", str(e))
        print(f"Error deleting branch {devops_branch_name}. {error_message}")
        return None


def get_pull_request(access_token, devops_org_name, devops_project_name, devops_repo_name, devops_commit_id):
    """
    Retrieves pull request details associated with a specific commit in an Azure DevOps repository.

    Args:
        access_token (str): Personal Access Token (PAT) for Azure DevOps authentication.
        devops_org_name (str): Name of the Azure DevOps organization.
        devops_project_name (str): Name of the Azure DevOps project.
        devops_repo_name (str): Name of the repository to search for pull requests.
        devops_commit_id (str): Commit ID for which the pull request is queried.

    Returns:
        dict: JSON response from the Azure DevOps API containing pull request details.
        None: If there is an error during the query process.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    devops_org_url = f"https://dev.azure.com/{devops_org_name}/"
    query_url = f"{devops_org_url}{devops_project_name}/_apis/git/repositories/{devops_repo_name}/pullrequestquery?api-version=7.0"
    #print (f"query_url: {query_url}")
    # Create body
    body = {
        "queries": [
            {
                "items": [devops_commit_id],
                "type": "lastMergeCommit"
            }
        ]
    }
    #print (f"body: {body}")
    try:
        response = requests.post(query_url, headers=headers, json=body)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error querying pull request for commit {devops_commit_id}: {str(e)}")
        return None
