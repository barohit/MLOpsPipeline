from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, sum as spark_sum, when

from FeatureEngineering.base.base_job import BaseJob


class TeamAggregatedStatsJob(BaseJob):
    def process_data(self, dataframes: dict[str, DataFrame]) -> DataFrame:
        df = dataframes["player_derived_metrics_features"]

        aggregated_df = (
            df.groupBy("team_id")
            .agg(
                spark_sum("minutes_played").alias("team_total_minutes_played"),
                spark_sum("total_points").alias("team_total_points"),
                spark_sum("two_point_attempts").alias("team_total_two_point_attempts"),
                spark_sum("two_point_made").alias("team_total_two_point_made"),
                spark_sum("three_point_attempts").alias("team_total_three_point_attempts"),
                spark_sum("three_point_made").alias("team_total_three_point_made"),
                spark_sum("free_throw_attempts").alias("team_total_free_throw_attempts"),
                spark_sum("free_throw_made").alias("team_total_free_throw_made"),
                spark_sum("total_assists").alias("team_total_assists"),
                spark_sum("offensive_rebounds").alias("team_total_offensive_rebounds"),
                spark_sum("defensive_rebounds").alias("team_total_defensive_rebounds"),
                spark_sum("total_rebounds").alias("team_total_rebounds"),
                spark_sum("steals").alias("team_total_steals"),
                spark_sum("blocks").alias("team_total_blocks"),
                spark_sum("total_turnovers").alias("team_total_turnovers"),
                spark_sum("total_defensive_fouls").alias("team_total_defensive_fouls"),
                spark_sum("total_offensive_fouls").alias("team_total_offensive_fouls"),
                spark_sum("non_offensive_foul_turnovers").alias("team_non_offensive_foul_turnovers"),
                spark_sum("triple_double_candidate_flag").alias("team_triple_double_candidate_count"),
                spark_sum("high_impact_player_flag").alias("team_high_impact_player_count"),
                avg("two_point_percentage").alias("avg_two_point_percentage"),
                avg("three_point_percentage").alias("avg_three_point_percentage"),
                avg("free_throw_percentage").alias("avg_free_throw_percentage"),
                avg("field_goal_percentage").alias("avg_field_goal_percentage"),
                avg("assist_turnover_ratio").alias("avg_assist_turnover_ratio"),
                avg("three_point_attempt_rate").alias("avg_three_point_attempt_rate"),
                avg("points_per_minute").alias("avg_points_per_minute"),
                avg("rebounds_per_minute").alias("avg_rebounds_per_minute"),
                avg("assists_per_minute").alias("avg_assists_per_minute"),
                avg("steals_per_minute").alias("avg_steals_per_minute"),
                avg("blocks_per_minute").alias("avg_blocks_per_minute"),
                avg("fouls_per_minute").alias("avg_fouls_per_minute")
            )
        )

        return (
            aggregated_df
            .withColumn(
                "team_two_point_percentage",
                when(
                    col("team_total_two_point_attempts") > 0,
                    col("team_total_two_point_made") / col("team_total_two_point_attempts")
                )
            )
            .withColumn(
                "team_three_point_percentage",
                when(
                    col("team_total_three_point_attempts") > 0,
                    col("team_total_three_point_made") / col("team_total_three_point_attempts")
                )
            )
            .withColumn(
                "team_free_throw_percentage",
                when(
                    col("team_total_free_throw_attempts") > 0,
                    col("team_total_free_throw_made") / col("team_total_free_throw_attempts")
                )
            )
            .withColumn(
                "team_field_goal_percentage",
                when(
                    (col("team_total_two_point_attempts") + col("team_total_three_point_attempts")) > 0,
                    (col("team_total_two_point_made") + col("team_total_three_point_made"))
                    / (col("team_total_two_point_attempts") + col("team_total_three_point_attempts"))
                )
            )
            .withColumn(
                "team_assist_turnover_ratio",
                when(
                    col("team_total_turnovers") > 0,
                    col("team_total_assists") / col("team_total_turnovers")
                )
            )
            .withColumn(
                "team_offensive_rebound_ratio",
                when(
                    col("team_total_rebounds") > 0,
                    col("team_total_offensive_rebounds") / col("team_total_rebounds")
                )
            )
            .withColumn(
                "team_defensive_rebound_ratio",
                when(
                    col("team_total_rebounds") > 0,
                    col("team_total_defensive_rebounds") / col("team_total_rebounds")
                )
            )
            .withColumn(
                "team_points_per_minute",
                when(
                    col("team_total_minutes_played") > 0,
                    col("team_total_points") / col("team_total_minutes_played")
                )
            )
            .withColumn(
                "team_steals_per_minute",
                when(
                    col("team_total_minutes_played") > 0,
                    col("team_total_steals") / col("team_total_minutes_played")
                )
            )
            .withColumn(
                "team_blocks_per_minute",
                when(
                    col("team_total_minutes_played") > 0,
                    col("team_total_blocks") / col("team_total_minutes_played")
                )
            )
            .withColumn(
                "team_fouls_per_minute",
                when(
                    col("team_total_minutes_played") > 0,
                    (col("team_total_defensive_fouls") + col("team_total_offensive_fouls"))
                    / col("team_total_minutes_played")
                )
            )
            .withColumn(
                "team_three_point_attempt_rate",
                when(
                    (col("team_total_two_point_attempts") + col("team_total_three_point_attempts")) > 0,
                    col("team_total_three_point_attempts")
                    / (col("team_total_two_point_attempts") + col("team_total_three_point_attempts"))
                )
            )
        )