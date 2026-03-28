from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, greatest, least, floor

from FeatureEngineering.base.base_job import BaseJob


class PlayerDerivedMetricsJob(BaseJob):
    def process_data(self, dataframes: dict[str, DataFrame]) -> DataFrame:
        players_df = dataframes["players"]
        stats_df = dataframes["player_season_stats"]

        df = (
            players_df.join(
                stats_df,
                players_df["id"] == stats_df["player_id"],
                "inner"
            )
            .select(
                players_df["id"].alias("player_id"),
                players_df["player_name"],
                players_df["team_id"],
                players_df["position"],
                players_df["class_year"],
                players_df["height_inches"],
                stats_df["season_year"],
                stats_df["minutes_played"],
                stats_df["two_point_attempts"],
                stats_df["two_point_made"],
                stats_df["three_point_attempts"],
                stats_df["three_point_made"],
                stats_df["free_throw_attempts"],
                stats_df["free_throw_made"],
                stats_df["total_assists"],
                stats_df["offensive_rebounds"],
                stats_df["defensive_rebounds"],
                stats_df["steals"],
                stats_df["blocks"],
                stats_df["total_defensive_fouls"],
                stats_df["total_offensive_fouls"],
                stats_df["total_turnovers"]
            )
        )

        total_points = (
            col("two_point_made") * lit(2)
            + col("three_point_made") * lit(3)
            + col("free_throw_made")
        )

        total_rebounds = col("offensive_rebounds") + col("defensive_rebounds")

        total_field_goal_attempts = col("two_point_attempts") + col("three_point_attempts")
        total_field_goals_made = col("two_point_made") + col("three_point_made")

        estimated_games_played = greatest(lit(1), floor(col("minutes_played") / lit(25)))

        return (
            df.withColumn("total_points", total_points)
            .withColumn("total_rebounds", total_rebounds)
            .withColumn("total_field_goal_attempts", total_field_goal_attempts)
            .withColumn("total_field_goals_made", total_field_goals_made)
            .withColumn("estimated_games_played", estimated_games_played)
            .withColumn(
                "two_point_percentage",
                when(col("two_point_attempts") > 0, col("two_point_made") / col("two_point_attempts"))
            )
            .withColumn(
                "three_point_percentage",
                when(col("three_point_attempts") > 0, col("three_point_made") / col("three_point_attempts"))
            )
            .withColumn(
                "free_throw_percentage",
                when(col("free_throw_attempts") > 0, col("free_throw_made") / col("free_throw_attempts"))
            )
            .withColumn(
                "field_goal_percentage",
                when(col("total_field_goal_attempts") > 0, col("total_field_goals_made") / col("total_field_goal_attempts"))
            )
            .withColumn(
                "assist_turnover_ratio",
                when(col("total_turnovers") > 0, col("total_assists") / col("total_turnovers"))
            )
            .withColumn(
                "non_offensive_foul_turnovers",
                greatest(lit(0), col("total_turnovers") - col("total_offensive_fouls"))
            )
            .withColumn(
                "three_point_attempt_rate",
                when(
                    total_field_goal_attempts > 0,
                    col("three_point_attempts") / total_field_goal_attempts
                )
            )
            .withColumn(
                "points_per_minute",
                when(col("minutes_played") > 0, col("total_points") / col("minutes_played"))
            )
            .withColumn(
                "rebounds_per_minute",
                when(col("minutes_played") > 0, col("total_rebounds") / col("minutes_played"))
            )
            .withColumn(
                "assists_per_minute",
                when(col("minutes_played") > 0, col("total_assists") / col("minutes_played"))
            )
            .withColumn(
                "steals_per_minute",
                when(col("minutes_played") > 0, col("steals") / col("minutes_played"))
            )
            .withColumn(
                "blocks_per_minute",
                when(col("minutes_played") > 0, col("blocks") / col("minutes_played"))
            )
            .withColumn(
                "fouls_per_minute",
                when(
                    col("minutes_played") > 0,
                    (col("total_defensive_fouls") + col("total_offensive_fouls")) / col("minutes_played")
                )
            )
            .withColumn(
                "points_per_estimated_game",
                col("total_points") / col("estimated_games_played")
            )
            .withColumn(
                "rebounds_per_estimated_game",
                col("total_rebounds") / col("estimated_games_played")
            )
            .withColumn(
                "assists_per_estimated_game",
                col("total_assists") / col("estimated_games_played")
            )
            .withColumn(
                "triple_double_candidate_flag",
                when(
                    (col("points_per_estimated_game") >= 10)
                    & (col("rebounds_per_estimated_game") >= 10)
                    & (col("assists_per_estimated_game") >= 10),
                    lit(1)
                ).otherwise(lit(0))
            )
            .withColumn(
                "high_impact_player_flag",
                when(
                    (col("points_per_minute") > 0.45)
                    & (col("rebounds_per_minute") > 0.18)
                    & (col("assists_per_minute") > 0.12),
                    lit(1)
                ).otherwise(lit(0))
            )
        )