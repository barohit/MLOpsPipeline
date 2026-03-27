from pyspark.sql import SparkSession, DataFrame

from data_sources.base_data_source import BaseDataSource
from credential_providers.base_database_credential_provider import BaseDatabaseCredentialProvider


class SnowflakeDataSource(BaseDataSource):
    def __init__(
        self,
        account: str,
        database_name: str,
        schema_name: str,
        warehouse: str,
        credential_provider: BaseDatabaseCredentialProvider,
        role: str | None = None,
    ):
        self.account = account
        self.database_name = database_name
        self.schema_name = schema_name
        self.warehouse = warehouse
        self.credential_provider = credential_provider
        self.role = role

    def _get_auth(self):
        creds = self.credential_provider.get_credentials()
        return creds.username, creds.password

    def _options(self):
        username, password = self._get_auth()

        options = {
            "sfURL": f"{self.account}.snowflakecomputing.com",
            "sfUser": username,
            "sfPassword": password,
            "sfDatabase": self.database_name,
            "sfSchema": self.schema_name,
            "sfWarehouse": self.warehouse,
        }

        if self.role:
            options["sfRole"] = self.role

        return options

    def read_table(self, spark: SparkSession, table_name: str) -> DataFrame:
        return (
            spark.read
            .format("snowflake")
            .options(**self._options())
            .option("dbtable", table_name)
            .load()
        )

    def read_query(self, spark: SparkSession, query: str) -> DataFrame:
        return (
            spark.read
            .format("snowflake")
            .options(**self._options())
            .option("query", query)
            .load()
        )