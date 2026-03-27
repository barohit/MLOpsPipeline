from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BaseJob(ABC):
    @abstractmethod
    def process_data(self, df: DataFrame) -> DataFrame:
        pass