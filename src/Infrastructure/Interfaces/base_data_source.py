from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame

class BaseDataSource(ABC):
    @abstractmethod
    def read_table(self, spark: SparkSession, table_name: str) -> DataFrame:
        pass