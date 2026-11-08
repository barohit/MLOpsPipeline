from pyspark.sql import SparkSession, DataFrame
from src.Infrastructure.Interfaces.base_data_source import BaseDataSource
from src.Infrastructure.Interfaces.base_credentials_provider import BaseCredentialProvider

class PostgreSQLDataSource(BaseDataSource):
    def __init__(
        self,
        host: str,
        port: int,
        database_name: str,
        credential_provider: BaseCredentialProvider
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
            "user": credentials["username"],
            "password": credentials["password"],
            "driver": "org.postgresql.Driver"
        }

    def read_table(self, spark: SparkSession, table_name: str) -> DataFrame:
        return spark.read.jdbc(
            url=self._jdbc_url(),
            table=table_name,
            properties=self._connection_properties()
        )
