from pyspark.sql import DataFrame

from Infrastructure.base.base_feature_store import BaseFeatureStore
from Infrastructure.base.base_credentials_provider import BaseCredentialProvider


class PostgreSQLFeatureStore(BaseFeatureStore):
    def __init__(
        self,
        host: str,
        port: int,
        database_name: str,
        credential_provider: BaseCredentialProvider,
    ):
        self.host = host
        self.port = port
        self.database_name = database_name
        self.credential_provider = credential_provider

    def _jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database_name}"

    def _connection_properties(self) -> dict:
        credentials = self.credential_provider.get_credentials()

        return {
            "user": credentials.username,
            "password": credentials.password,
            "driver": "org.postgresql.Driver",
        }

    def publish(self, df: DataFrame, table_name: str) -> None:
        (
            df.write
            .mode("overwrite")
            .jdbc(
                url=self._jdbc_url(),
                table=table_name,
                properties=self._connection_properties(),
            )
        )