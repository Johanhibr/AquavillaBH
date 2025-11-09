# Local Feature Creation Script (Alternative Option)

This guide outlines an alternative approach for feature development using a local **Python script**. This script automates both feature setup and cleanup once development is completed and merged into the main branch.

This is an alternative to using the two Azure DevOps pipelines **"Fabric - Feature Creation"** and **"Fabric - Feature Cleanup"**.

## Overview

The script relies on a JSON configuration file (`fabric_setup.json`) to define:

- Fabric capacity settings
- Workspace creation details
- Permissions
- Managed private endpoints
- Git integration settings

This file is shared among all developers and should not be modified without notifying the team. The file is located in the `automation\cicd\locale` folder.

### Sample Configuration (`fabric_setup.json`)

```json
{
    "feature_name": "_{feature_name} - {stage}",
    "capacity_id": "ABCD1234-6789-6789-6789-ABCD1234ABCD",
    "spn_managed_identity": "xyz9x9x9-xyz9-xyz9-xyz9-xyz9xyz9",
    "key_vault_name": "KV-xxxyyy-dev",
    "git_integration": { 
        "DevOpsOrgName": "XXX",
        "DevOpsProjectName": "YYY",
        "DevOpsRepoName": "Fabric",
        "DevOpsDefaultBranch": "main"
    },
    "stages": {
        "Prepare": { 
            "git_folder": "solution/prepare",
            "private_endpoints": [
                {
                    "name": "mpe-sql-xxx-dev",
                    "auto_approve": true,
                    "id": "/subscriptions/xyz9xyz9-xyz9-xyz9-xyz9-xyz9xyz99/resourceGroups/RG-XXX-dev/providers/Microsoft.Sql/servers/sql-xxx-dev"
                },
                {
                    "name": "mpe-kv-xxx-dev",
                    "auto_approve": true,
                    "id": "/subscriptions/xyz9xyz9-xyz9-xyz9-xyz9-xyz9xyz99/resourceGroups/RG-XXX-dev/providers/Microsoft.KeyVault/vaults/KV-xxx-dev"
                }
            ]
        },
        "Ingest": { "git_folder": "solution/ingest" }
    }
}
```

## Creating a Feature

Run **`locale\maintain_feature.py`** with `action="create"` and specify `feature_name`.

If `mpe_spn_approval=True`, the script retrieves **Service Principal** credentials from **Azure Key Vault** and executes managed private endpoint approvals.

> **Note:** Only enable SPN approval when running the script from a **jump server** (requires network access to Azure Key Vault behind a firewall). Ensure that the identity executing the approval has **Contributor** permissions on relevant Azure resources (e.g., Key Vault, Azure SQL DB).

### Naming Convention

The feature name should follow the format: **`feature/[Initials]/[FeatureName]`**  
**Example:** `feature/abc/NewDimension`

### Prerequisites

Ensure you have Python 3.x installed with essential libraries (`requests` and `azure-identity`) required for interacting with REST APIs and authenticating to Azure.

You can install the required Python libraries by using the command:

```sh
pip install requests azure-identity --user
```

### Script Execution

```python
# Feature branch settings
action = "create"  # Options: create / delete
feature_name = "feature/[Initials]/[FeatureName]"
mpe_spn_approval = False  # Set to True if SPN authentication is required
```

### Result

Running this script will create:

- `_abc/NewDimension - Ingest`
- `_abc/NewDimension - Prepare`

If the feature does **not** require work on Ingest or Prepare, you can manually create the feature branch in **Azure DevOps** under **Repos → Branches** and proceed with development using **Tabular Editor** or **Visual Studio Code**.

## Manual Cleanup of a Feature

Once the feature is merged into `main`, manually clean up workspaces by running `maintain_feature.py` with `action="delete"`.

### Script Execution

```python
# Feature branch settings
action = "delete"  # Options: create / delete
feature_name = "feature/[Initials]/[FeatureName]"
```

### Result

The script will delete the specified workspaces:

- `_abc/NewDimension - Ingest`
- `_abc/NewDimension - Prepare`
