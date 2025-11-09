import os, json
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

yaml = YAML()
yaml.indent(mapping=4, sequence=4, offset=2)

cdefault = '\033[0m'
cdefault_bold = '\033[1m'
cred = '\033[91m'
cred_bold = '\033[1;91m'
cyellow = '\033[33m'
cyellow_bold = '\033[1;33m'
cgreen = '\033[32m'
cgreen_bold = '\033[1;32m'
cblue_bold = '\033[1;34m'

def print_error(value, bold:bool = False):
    if bold:
        print(f"{cred_bold}{value}{cdefault}")
    else:
        print(f"{cred}{value}{cdefault}")


def print_warning(value, bold:bool = False):
    if bold:
        print(f"{cyellow_bold}{value}{cdefault}")
    else:
        print(f"{cyellow}{value}{cdefault}")


def print_success(value, bold:bool = False):
    if bold:
        print(f"{cgreen_bold}{value}{cdefault}")
    else:
        print(f"{cgreen}{value}{cdefault}")

def print_info(value:str = "", bold:bool = False, end:str = "\n"):
    if bold:
        print(f"{cdefault_bold}{value}{cdefault}", end=end)
    else:
        print(f"{value}", end=end)
        
def print_header(value):
    print("")
    print(f"{cblue_bold}#################################################################################################################################{cdefault}")
    print(f"{cblue_bold}# {value.center(125)} #{cdefault}")
    print(f"{cblue_bold}#################################################################################################################################{cdefault}")
    
def manage_find_replace(
    yml_path: str,
    action: str,
    find_value: str,
    replace_value: dict = None,
    comment: str = None,
    print_operations: bool = False
):

    if not os.path.isfile(yml_path):
        with open(yml_path, "w") as f:
            f.write("find_replace:\n")

    with open(yml_path, "r") as f:
        data = yaml.load(f)

    if 'find_replace' not in data or not isinstance(data['find_replace'], list):
        data['find_replace'] = CommentedSeq()

    entries: CommentedSeq = data['find_replace']
    entries[:] = [entry for entry in entries if entry is not None]

    def find_index(value):
        for idx, entry in enumerate(entries):
            if entry and entry.get('find_value') == value:
                return idx
        return None

    idx = find_index(find_value)

    if action == 'delete':
        if idx is not None:
            del entries[idx]
            print(f"✅ Deleted entry with find_value: {find_value}") if print_operations is True else None
        else:
            print(f"⚠️ No entry found to delete for find_value: {find_value}") if print_operations is True else None
    elif action == 'upsert':
        new_entry = CommentedMap()
        new_entry['find_value'] = find_value
        if comment:
            new_entry.yaml_add_eol_comment(comment, key='find_value')
        rv_map = CommentedMap()
        if replace_value:
            for k, v in replace_value.items():
                rv_map[k] = v
        new_entry['replace_value'] = rv_map

        if idx is not None:
            entries[idx] = new_entry
            print(f"🔁 Updated existing entry for find_value: {find_value}") if print_operations is True else None
        else:
            entries.append(new_entry)
            print(f"➕ Added new entry for find_value: {find_value}") if print_operations is True else None
    else:
        raise ValueError("Action must be 'upsert' or 'delete'")

    with open(yml_path, "w") as f:
        yaml.dump(data, f)

def find_item(data, stage_name, item_type, item_name):
    stage = data["parameters"]["stages"].get(stage_name)
    if not stage:
        return None
    
    items = stage.get("items", {}).get(item_type, [])
    for item in items:
        if item.get("item_name") == item_name:
            return item
    
    return None  # Not found

def build_parameter_yml(yaml_file, all_environments):
    """
    Creates and returns a dictionary structure for a YAML file.

    Args:
        all_environments (dict): Dict of all environments and their properties to be used for deriving the parameter yml file.

    Returns:
        dict: The parameter dictionary with predefined sections ('find_replace' and 'spark_pool') and their mapped items
    """
    
    # Use dev as primary
    primary_env = "dev"

    item_props_in_scope = {
        "id": {"comment": "Item Guids"},
        "pbi_connection_id": {"comment": "Connection Guids"},
        "sql_endpoint_id": {"comment": "SQL Endpoint Guids"},
        "sql_database_fqdn": {"comment": "Fabric SQL Database addresses"},
        "sql_database_name": {"comment": "Fabric SQL Database names"}    
    }

    primary_layers = all_environments.get(primary_env).get("parameters").get("stages")
 
    for stage_name, stage_data in primary_layers.items(): 
        primary_id = stage_data["workspace_id"]
        
        # Map workspaces across environments
        replace_value = {}
        for env, env_data in all_environments.items():
            if env == primary_env or not env_data:
                continue  # Skip primary environment
            
            env_stage = env_data.get("parameters").get("stages").get(stage_name)
            
            if env_stage:
                replace_value[env] = env_stage["workspace_id"]
                
        manage_find_replace(
            yml_path = yaml_file,
            action = "upsert",
            find_value = primary_id,
            replace_value = replace_value,
            comment = f"Workspace - {stage_name}"
        )

        print(f"Added replacement value for Workspace - {stage_name}")
    
        if "items" in stage_data:
            for item_type, items in stage_data["items"].items():
                for item in items:
                    item_name = item.get("item_name")

                    for item_prop_name, item_props in item_props_in_scope.items():
                        replace_value = {}
                        item.get(item_prop_name)
                        for env, env_data in all_environments.items():
                            if env == primary_env or not env_data:
                                continue # Skip primary environment
                            
                            env_item = find_item(env_data, stage_name, item_type, item_name)
                            
                            if (env_item):
                                if env_item.get(item_prop_name):
                                    replace_value[env] = env_item.get(item_prop_name)

                        if replace_value:
                            manage_find_replace(
                                yml_path = yaml_file,
                                action = "upsert",
                                find_value = item.get(item_prop_name),
                                replace_value = replace_value,
                                comment = f"{item_type}: {item_name} - {item_props.get('comment')}"
                            )    

                            print(f"Added replacement value for {item_type}: {item_name} - {item_props.get('comment')}")

    print(f"Parameter file succesfully created in path {yaml_file}")


def save_json_to_file(data, filepath):
    """
    Save JSON data to a file with indentation and sorted keys.
    
    Args:
        data (dict): The JSON data to write.
        filepath (str): The path to the output file.
    """
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, sort_keys=True, ensure_ascii=False)


def read_json_from_file(filepath):
    """
    Read JSON data from a file.
    
    Args:
        filepath (str): The path to the JSON file to read.
        
    Returns:
        dict: The parsed JSON data.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)
    

def load_environments_as_dict(env_file_map):
    """
    Load multiple JSON files into a dictionary keyed by environment name.
    
    Args:
        env_file_map (dict): A mapping of environment name to file path.
    
    Returns:
        dict: A dictionary where each key is the environment name and value is its JSON data.
    """
    all_environments = {}

    for env_name, file_path in env_file_map.items():
        with open(file_path, 'r', encoding='utf-8') as f:
            all_environments[env_name] = json.load(f)

    return all_environments
