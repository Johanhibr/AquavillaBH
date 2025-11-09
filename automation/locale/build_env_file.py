import subprocess, os, sys
from datetime import datetime

environments = "dev,tst,prd" # Environment shortnames corresponding to the postfix on the fabric_solution environment files.
env_folder_path = os.path.join(os.path.dirname(__file__), f'../environments/')

#region Setup script
start_time = datetime.now() 

os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
sys.path.append(os.getcwd())

import modules.auth_functions as authfunc

credential = authfunc.create_credentials_from_user()
access_token = credential.get_token("https://api.fabric.microsoft.com/.default").token

init_command = [
    # "python",
    sys.executable,
    "-X", "frozen_modules=off",
    f"scripts/build_env_file.py",
    "--environments", environments,
    "--access_token", access_token,
    "--output_path", env_folder_path
]
result = subprocess.run(init_command)

if result.returncode == 0:
    print(f"Environment finished successfully.\n")
else:
    print(f"Environment exited with error. Error code {result.returncode}.\n")
    
duration = datetime.now() - start_time
print(f"Overall script duration: {duration}")
#endregion