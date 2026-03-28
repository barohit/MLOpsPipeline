from pyspark.sql import SparkSession, DataFrame

from Infrastructure.base.base_data_source import BaseDataSource
from Infrastructure.base.base_feature_store import BaseFeatureStore
from FeatureEngineering.base.base_job import BaseJob


class BaseFeatureProcess:
    def __init__(
        self,
        spark: SparkSession,
        data_source: BaseDataSource,
        job: BaseJob,
        feature_store: BaseFeatureStore,
    ):
        self.spark = spark
        self.data_source = data_source
        self.job = job
        self.feature_store = feature_store

    def run(self, source_tables: list[str], output_table: str) -> None:
        dataframes: dict[str, DataFrame] = {
            table_name: self.data_source.read_table(self.spark, table_name)
            for table_name in source_tables
        }

        feature_df = self.job.process_data(dataframes)
        self.feature_store.publish(feature_df, output_table)