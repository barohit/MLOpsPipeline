from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, when

from FeatureEngineering.base.base_job import BaseJob


class ConferenceWinRateJob(BaseJob):
    def process_data(self, df: DataFrame) -> DataFrame:
        aggregated_df = (
            df.groupBy("conference_id")
            .agg(
                spark_sum("regular_season_wins").alias("total_wins"),
                spark_sum("regular_season_losses").alias("total_losses")
            )
        )

        total_games = col("total_wins") + col("total_losses")

        return (
            aggregated_df.withColumn(
                "conference_win_percentage",
                when(total_games != 0, col("total_wins") / total_games).otherwise(None)
            )
        )