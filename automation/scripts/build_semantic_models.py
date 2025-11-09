import os, sys, argparse, shutil, subprocess, json
from datetime import datetime
from pathlib import Path

os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
sys.path.append(os.getcwd())

start_time = datetime.now()

source_dir = f"{os.getenv('BUILD_SOURCEDIRECTORY')}\\solution\\semantic\\semantic-models"
target_dir = f"{os.getenv('BUILD_SOURCEDIRECTORY')}\\solution\\semantic\\semantic-models\\build"
tabulareditor_dir = f"{os.getenv('BUILD_SOURCEDIRECTORY')}\\solution\\semantic\\semantic-models"

parser = argparse.ArgumentParser(description="Semantic model build script arguments")
parser.add_argument("--source_dir", required=False, default=source_dir, help="Source directory containing semantic models.")
parser.add_argument("--target_dir", required=False, default=target_dir, help="Target directory holding the build of semantic models.")
parser.add_argument("--tabulareditor_dir", required=False, default=None, help="Directory where Tabular Editor 2.x executable file is stored.")

args = parser.parse_args()
source_directory = Path(args.source_dir)
target_directory = Path(args.target_dir)
tabulareditor_directory = Path(args.tabulareditor_dir)

print("Building Semantic models")

print(os.path.exists(target_directory))
if os.path.exists(target_directory):
    shutil.rmtree(target_directory)

if source_directory:
    if os.path.exists(source_directory):
        with os.scandir(source_directory) as models:
            if models:
                for model in models:
                    if model.is_dir():
                        model_name = model.name
                        print(f"Building model {model_name}...")

                        destination = os.path.join(target_directory, model_name)

                        te_exec = os.path.join(tabulareditor_directory, "TabularEditor.exe")
                        file_path = os.path.join(model.path, "definition/database.tmdl")
                        model_path = os.path.join(destination, "model.bim")

                        destination_dir = os.path.dirname(model_path)
                        os.makedirs(destination_dir, exist_ok=True)

                        subprocess.run([
                            te_exec, file_path, 
                            "-B", model_path
                            ], 
                            check=True)

                        print (" Done!")
            else:
                print(f"No models found in source directory {source_directory.resolve()}.")
    else:
        print(f"No model directory found. Skipping build.")
else:
    print("Source directory is not set.")

duration = datetime.now() - start_time
print(f"Script duration: {duration}")