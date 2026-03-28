from pyspark.sql import DataFrame
from pyspark.sql.functions import count, col

from FeatureEngineering.base.base_job import BaseJob


class TeamPlayerCountJob(BaseJob):
    def process_data(self, dataframes: dict[str, DataFrame]) -> DataFrame:
        df = dataframes["players"]
        return (
            df.groupBy("team_id")
            .agg(count("id").alias("player_count"))
            .select(
                col("team_id"),
                col("player_count")
            )
        )