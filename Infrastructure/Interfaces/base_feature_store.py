from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BaseFeatureStore(ABC):
    @abstractmethod
    def publish(self, df: DataFrame, table_name: str) -> None:
        pass