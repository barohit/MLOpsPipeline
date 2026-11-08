from pyspark.sql import DataFrame

from src.Infrastructure.Interfaces.base_feature_store import BaseFeatureStore


class InMemoryFeatureStore(BaseFeatureStore):
    """Publishes into a dict of Spark DataFrames. Share the dict with InMemoryDataSource
    and later feature processes can read what earlier ones published."""

    def __init__(self, tables: dict[str, DataFrame]):
        self.tables = tables

    def publish(self, df: DataFrame, table_name: str) -> None:
        self.tables[table_name] = df
