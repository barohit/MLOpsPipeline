from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from FeatureEngineering.base.base_job import BaseJob


class TeamTrainingDatasetJob(BaseJob):
    def process_data(self, dataframes: dict[str, DataFrame]) -> DataFrame:
        aggregated_df = dataframes["team_aggregated_stats_features"]
        roster_df = dataframes["team_roster_composition_features"]
        teams_df = dataframes["teams"]

        df = (
            aggregated_df.join(roster_df, "team_id", "inner")
            .join(
                teams_df.select(
                    col("id").alias("team_id"),
                    col("team_name"),
                    col("conference_id"),
                    col("regular_season_wins")
                ),
                "team_id",
                "inner"
            )
        )

        return df.select(
            col("team_id"),
            col("team_name"),
            col("conference_id"),

            col("player_count"),
            col("avg_height_inches"),
            col("guard_count"),
            col("forward_count"),
            col("center_count"),
            col("freshman_count"),
            col("sophomore_count"),
            col("junior_count"),
            col("senior_count"),
            col("grad_count"),
            col("upperclassman_ratio"),
            col("experience_score"),
            col("guard_ratio"),
            col("forward_ratio"),
            col("center_ratio"),

            col("team_total_points"),
            col("team_total_assists"),
            col("team_total_rebounds"),
            col("team_total_offensive_rebounds"),
            col("team_total_defensive_rebounds"),
            col("team_total_turnovers"),
            col("team_total_steals"),
            col("team_total_blocks"),
            col("team_total_defensive_fouls"),
            col("team_total_offensive_fouls"),
            col("team_non_offensive_foul_turnovers"),

            col("team_triple_double_candidate_count"),
            col("team_high_impact_player_count"),

            col("team_two_point_percentage"),
            col("team_three_point_percentage"),
            col("team_free_throw_percentage"),
            col("team_field_goal_percentage"),
            col("team_assist_turnover_ratio"),
            col("team_offensive_rebound_ratio"),
            col("team_defensive_rebound_ratio"),
            col("team_points_per_minute"),
            col("team_steals_per_minute"),
            col("team_blocks_per_minute"),
            col("team_fouls_per_minute"),
            col("team_three_point_attempt_rate"),

            col("avg_two_point_percentage"),
            col("avg_three_point_percentage"),
            col("avg_free_throw_percentage"),
            col("avg_field_goal_percentage"),
            col("avg_assist_turnover_ratio"),
            col("avg_three_point_attempt_rate"),
            col("avg_points_per_minute"),
            col("avg_rebounds_per_minute"),
            col("avg_assists_per_minute"),
            col("avg_steals_per_minute"),
            col("avg_blocks_per_minute"),
            col("avg_fouls_per_minute"),

            col("regular_season_wins").alias("target_regular_season_wins")
        )