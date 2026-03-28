from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class BaseJob(ABC):
    @abstractmethod
    def process_data(self, dataframes: dict[str, DataFrame]) -> DataFrame:
        pass