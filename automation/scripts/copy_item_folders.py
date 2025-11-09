import os, argparse, shutil
from datetime import datetime
from pathlib import Path

start_time = datetime.now()
source_dir = f"{os.getenv('BUILD_SOURCEDIRECTORY')}\\solution"
target_dir = f"{os.getenv('BUILD_ARTIFACTSTAGINGDIRECTORY')}"
            
parser = argparse.ArgumentParser(description="Copy Fabric item folders")
parser.add_argument("--source_dir", required=False, default=source_dir, help="Source directory containing Fabric item folders.")
parser.add_argument("--target_dir", required=False, default=source_dir, help="Source directory containing Fabric item folders.")
parser.add_argument("--stages", required=True, help="Commaseperated list of stages. Ie. ingest, prepare etc.")
parser.add_argument("--item_types", required=True, help="Commaseperated list of types. Ie. DataPipeline, Notebook etc.")
parser.add_argument("--parameter_file", required=False, default=None, help="fabric-cicd paramenter file to add to each layer folder.")

args = parser.parse_args()
source_dir = Path(args.source_dir)
target_dir = args.target_dir
item_types = args.item_types.split(",")
stages = args.stages.split(",")
parameter_file = args.parameter_file

def matches_pattern(name: str, patterns: list[str]) -> bool:
    return any(pattern in name for pattern in patterns)

def copy_folders(source_dir: str, target_dir: str, stages: list[str], item_types: list[str]):
    source_path = Path(source_dir).resolve()
    target_path = Path(target_dir).resolve()
    parameter_file_path = Path(parameter_file).resolve()

    for stage in stages:
        stage_path = source_path / stage
        if not stage_path.exists() or not stage_path.is_dir():
            continue
        
        stage_has_files = False
        for item_folder in stage_path.iterdir():
            if item_folder.is_dir() and matches_pattern(item_folder.name, item_types):
                relative_path = item_folder.relative_to(source_path)
                dest_folder = target_path / relative_path

                print(f"Copying: {item_folder} -> {dest_folder}")
                stage_has_files = True

                os.makedirs(dest_folder.parent, exist_ok=True)
                shutil.copytree(item_folder, dest_folder, dirs_exist_ok=True)
        
        # if stage_has_files == True:
        #     dest_parameter_file = target_path / stage / parameter_file_path.name
        #     shutil.copy(parameter_file, dest_parameter_file)

copy_folders(source_dir, target_dir, stages, item_types)

duration = datetime.now() - start_time
print(f"Script duration: {duration}")