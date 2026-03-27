from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

from FeatureEngineering.base.base_job import BaseJob


class TeamWinPercentageJob(BaseJob):
    def process_data(self, df: DataFrame) -> DataFrame:
        total_games = col("regular_season_wins") + col("regular_season_losses")

        return (
            df.select(
                col("id").alias("team_id"),
                col("team_name"),
                col("conference_id"),
                col("regular_season_wins"),
                col("regular_season_losses")
            )
            .withColumn(
                "win_percentage",
                when(total_games != 0, col("regular_season_wins") / total_games).otherwise(None)
            )
        )