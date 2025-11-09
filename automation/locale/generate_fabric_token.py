print_token = False;

import os, sys, pyperclip
os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
sys.path.append(os.getcwd())

import modules.auth_functions as authfunc

credential = authfunc.create_credentials_from_user()
fabric_upn_token = credential.get_token("https://api.fabric.microsoft.com/.default").token

if print_token is True:
    print("####################################### Microsoft Entra access token #######################################")
    print(fabric_upn_token)
    print("##############################################################################################################")
else:
    pyperclip.copy(fabric_upn_token)
    print("Microsoft Entra access token was copied to the clipboard!")