import hvac
from credential_providers.base_credential_provider import BaseCredentialProvider

class VaultCredentialProvider(BaseCredentialProvider):
    def __init__(self, url: str, token: str, secret_path: str):
        self.url = url
        self.token = token
        self.secret_path = secret_path

    def get_credentials(self) -> dict:
        client = hvac.Client(url=self.url, token=self.token)
        response = client.secrets.kv.v2.read_secret_version(path=self.secret_path)
        data = response["data"]["data"]

        return {
            "username": data["username"],
            "password": data["password"]
        }