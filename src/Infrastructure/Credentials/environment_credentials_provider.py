import os
from credential_providers.base_credential_provider import BaseCredentialProvider

class EnvironmentCredentialProvider(BaseCredentialProvider):
    def __init__(self, username_env_var: str, password_env_var: str):
        self.username_env_var = username_env_var
        self.password_env_var = password_env_var

    def get_credentials(self) -> dict:
        username = os.getenv(self.username_env_var)
        password = os.getenv(self.password_env_var)

        if not username or not password:
            raise ValueError("Database credentials not found in environment variables")

        return {
            "username": username,
            "password": password
        }