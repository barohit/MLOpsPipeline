from pyspark.sql import DataFrame

from Infrastructure.base.base_feature_store import BaseFeatureStore
from Infrastructure.base.base_credentials_provider import BaseCredentialProvider


class SnowflakeFeatureStore(BaseFeatureStore):
    def __init__(
        self,
        account: str,
        database_name: str,
        schema_name: str,
        warehouse: str,
        credential_provider: BaseCredentialProvider,
        role: str | None = None,
    ):
        self.account = account
        self.database_name = database_name
        self.schema_name = schema_name
        self.warehouse = warehouse
        self.credential_provider = credential_provider
        self.role = role

    def _options(self) -> dict[str, str]:
        credentials = self.credential_provider.get_credentials()

        options = {
            "sfURL": f"{self.account}.snowflakecomputing.com",
            "sfUser": credentials.username,
            "sfPassword": credentials.password,
            "sfDatabase": self.database_name,
            "sfSchema": self.schema_name,
            "sfWarehouse": self.warehouse,
        }

        if self.role:
            options["sfRole"] = self.role

        return options

    def publish(self, df: DataFrame, table_name: str) -> None:
        (
            df.write
            .format("snowflake")
            .options(**self._options())
            .option("dbtable", table_name)
            .mode("overwrite")
            .save()
        )