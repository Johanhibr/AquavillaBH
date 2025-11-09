from azure.identity import InteractiveBrowserCredential
from azure.core.credentials import AccessToken, TokenCredential
import requests, time, json, os, jwt

def get_credentials_from_file(file_name):
    """
    Loads credentials from a specified JSON file located in the same directory as the script.

    Args:
        file_name (str): The name of the JSON file containing the credentials.

    Returns:
        dict: A dictionary containing the credentials as key-value pairs.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, file_name)

    with open(file_path, "r") as file:
        return json.load(file)


def create_credentials_from_user():
    """
    Creates a ResourceManagementClient instance for Azure resources using 
    InteractiveBrowserCredential for authentication.
    """
    return InteractiveBrowserCredential()


def get_access_token_from_credentials(credential : InteractiveBrowserCredential, resource):
    """
    Creates a ResourceManagementClient instance for Azure resources using 
    InteractiveBrowserCredential for authentication.

    Args:
        credential (InteractiveBrowserCredential): The credential object for the authenticated user.
        resource (str): The URI of the resource for which the access token is requested, e.g., "https://management.azure.com/" for Azure Management APIs.

    Returns:
        ResourceManagementClient: A client instance used to authenticate and manage Azure resources.
    """
    return credential.get_token(resource).token


def get_access_token(tenant_id, client_id, client_secret, resource):
    """
    Obtains an OAuth 2.0 access token for authenticating with Azure, Power BI, or Fabric services.

    Args:
        tenant_id (str): The Azure AD tenant ID where the application is registered.
        client_id (str): The client (application) ID of the registered Azure AD application.
        client_secret (str): The client secret of the registered Azure AD application.
        resource (str): The URI of the resource for which the access token is requested, e.g., "https://management.azure.com/" for Azure Management APIs.

    Returns:
        str: The access token string used to authenticate requests to the specified resource.
    """
    request_access_token_uri = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': resource
    }

    try:
        response = requests.post(request_access_token_uri,data=payload,headers={'Content-Type': 'application/x-www-form-urlencoded'})
        response.raise_for_status()
        return response.json().get('access_token')

    except requests.exceptions.HTTPError as e:
        # HTTP error from Azure AD token endpoint — print rich details
        r = e.response or response  # fall back to response if available
        print(f"HTTP {r.status_code} {r.reason} for {r.url}")
        try:
            err = r.json()
            print("error:", err.get("error"))
            print("error_description:", err.get("error_description"))
            print("error_codes:", err.get("error_codes"))
            print("correlation_id:", err.get("correlation_id"))
            print("trace_id:", err.get("trace_id"))
            print("timestamp:", err.get("timestamp"))
        except ValueError:
            # Not JSON
            print("Body:", r.text)
        wa = r.headers.get("WWW-Authenticate")
        if wa:
            print("WWW-Authenticate:", wa)
        return None

    except requests.exceptions.RequestException as e:
        # Network/transport issues, DNS, timeouts, etc.
        print(f"Error obtaining access token (network/transport): {e}")
        return None

def is_service_principal(token):
    """
    Determines whether the given token belongs to a service principal.

    This function decodes the JWT token without verifying the signature 
    and checks the "idtyp" (identity type) claim. If the claim is "User", 
    the token represents a user; otherwise, it is assumed to be a service principal.

    Args:
        token (str): The JWT token to inspect.

    Returns:
        bool: True if the token belongs to a service principal, False if it belongs to a user.
    """
    decoded = jwt.decode(token, options={"verify_signature": False})
    
    if decoded.get("idtyp", "") == "User":
        return False
    else:
        return True
    

class StaticTokenCredential(TokenCredential):
    def __init__(self, access_token: str, expires_on: int = None):
        self.aad_token = access_token
        self.aad_token_expiration = expires_on or int(time.time()) + 3600

    def get_token(self, *scopes) -> AccessToken:
        return AccessToken(self.aad_token, self.aad_token_expiration)