"""What the model is trained on, in one place.

Training and serving both import from here. If someone adds, removes or reorders a
feature, it changes for both sides at once instead of drifting apart quietly.
"""

MODEL_NAME = "team_regular_season_win_count_linear_regression"
FEATURE_TABLE_NAME = "team_training_dataset_features"
TARGET_COLUMN = "target_regular_season_wins"

# The order matters: the model is fitted on these columns in this order.
FEATURE_COLUMNS = [
    "team_points_per_minute",
    "team_two_point_percentage",
    "team_three_point_percentage",
    "team_free_throw_percentage",
    "team_assist_turnover_ratio",
    "team_non_offensive_foul_turnovers",
    "team_total_rebounds",
    "team_offensive_rebound_ratio",
    "team_steals_per_minute",
    "team_blocks_per_minute",
    "team_three_point_attempt_rate",
    "player_count",
    "avg_height_inches",
    "upperclassman_ratio",
    "experience_score",
    "team_high_impact_player_count",
]
