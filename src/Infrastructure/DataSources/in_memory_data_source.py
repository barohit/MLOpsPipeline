from pyspark.sql import DataFrame, SparkSession

from src.Infrastructure.Interfaces.base_data_source import BaseDataSource


class InMemoryDataSource(BaseDataSource):
    """Reads tables from a dict of Spark DataFrames. Used to run the pipeline without a database."""

    def __init__(self, tables: dict[str, DataFrame]):
        self.tables = tables

    def read_table(self, spark: SparkSession, table_name: str) -> DataFrame:
        if table_name not in self.tables:
            raise KeyError(f"Table '{table_name}' not found. Available: {sorted(self.tables)}")
        return self.tables[table_name]
