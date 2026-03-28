from pyspark.sql import DataFrame
from pyspark.sql.functions import count, col

from FeatureEngineering.base.base_job import BaseJob


class ConferenceTeamCountJob(BaseJob):
    def process_data(self, dataframes: dict[str, DataFrame]) -> DataFrame:
        return (
            df = dataframes["teams"]
            df.groupBy("conference_id")
            .agg(count("id").alias("team_count"))
            .select(
                col("conference_id"),
                col("team_count")
            )
        )