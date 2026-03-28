from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, sum as spark_sum, when

from FeatureEngineering.base.base_job import BaseJob


class TeamRosterCompositionJob(BaseJob):
    def process_data(self, dataframes: dict[str, DataFrame]) -> DataFrame:
        df = dataframes["players"]

        return (
            df.groupBy("team_id")
            .agg(
                count("id").alias("player_count"),
                avg("height_inches").alias("avg_height_inches"),
                spark_sum(when(col("position") == "G", 1).otherwise(0)).alias("guard_count"),
                spark_sum(when(col("position") == "F", 1).otherwise(0)).alias("forward_count"),
                spark_sum(when(col("position") == "C", 1).otherwise(0)).alias("center_count"),
                spark_sum(when(col("class_year") == "FR", 1).otherwise(0)).alias("freshman_count"),
                spark_sum(when(col("class_year") == "SO", 1).otherwise(0)).alias("sophomore_count"),
                spark_sum(when(col("class_year") == "JR", 1).otherwise(0)).alias("junior_count"),
                spark_sum(when(col("class_year") == "SR", 1).otherwise(0)).alias("senior_count"),
                spark_sum(when(col("class_year") == "GR", 1).otherwise(0)).alias("grad_count")
            )
            .withColumn(
                "upperclassman_ratio",
                when(
                    col("player_count") > 0,
                    (col("junior_count") + col("senior_count") + col("grad_count")) / col("player_count")
                )
            )
            .withColumn(
                "experience_score",
                when(
                    col("player_count") > 0,
                    (
                        col("freshman_count") * 1
                        + col("sophomore_count") * 2
                        + col("junior_count") * 3
                        + col("senior_count") * 4
                        + col("grad_count") * 5
                    ) / col("player_count")
                )
            )
            .withColumn(
                "guard_ratio",
                when(col("player_count") > 0, col("guard_count") / col("player_count"))
            )
            .withColumn(
                "forward_ratio",
                when(col("player_count") > 0, col("forward_count") / col("player_count"))
            )
            .withColumn(
                "center_ratio",
                when(col("player_count") > 0, col("center_count") / col("player_count"))
            )
        )