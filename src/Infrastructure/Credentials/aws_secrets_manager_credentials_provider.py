import json
import boto3
from credential_providers.base_credential_provider import BaseCredentialProvider

class AWSSecretsManagerCredentialProvider(BaseCredentialProvider):
    def __init__(self, secret_name: str, region_name: str):
        self.secret_name = secret_name
        self.region_name = region_name

    def get_credentials(self) -> dict:
        client = boto3.client("secretsmanager", region_name=self.region_name)
        response = client.get_secret_value(SecretId=self.secret_name)
        secret = json.loads(response["SecretString"])

        return {
            "username": secret["username"],
            "password": secret["password"]
        }