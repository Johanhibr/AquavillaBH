import subprocess, os, sys
from datetime import datetime

# Setop options
action = "setup" # Options: setup / cleanup
environments = "dev,tst,prd" # Environment shortnames corresponding to the postfix on the fabric_solution environment files. 

#region Setup script
start_time = datetime.now()

os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
sys.path.append(os.getcwd())

import modules.auth_functions as authfunc

credential = authfunc.create_credentials_from_user()
access_token = credential.get_token("https://vault.azure.net/.default").token

init_command = [
    "python", 
    "-X", "frozen_modules=off",
    f"scripts/solution_pre_check.py", 
    "--environments", environments, 
    "--access_token", access_token
]

result = subprocess.run(init_command, check=True)
        
duration = datetime.now() - start_time
print(f"Script duration: {duration}")
#endregion