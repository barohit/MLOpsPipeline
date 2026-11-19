"""Synthetic source tables and the feature pipeline, shared by integration tests."""
import random

from src.FeatureEngineering.FeatureProcesses.player_derived_metrics_feature_process import (
    PlayerDerivedMetricsFeatureProcess,
)
from src.FeatureEngineering.FeatureProcesses.team_aggregated_stats_feature_process import (
    TeamAggregatedStatsFeatureProcess,
)
from src.FeatureEngineering.FeatureProcesses.team_roster_composition_feature_process import (
    TeamRosterCompositionFeatureProcess,
)
from src.FeatureEngineering.FeatureProcesses.team_training_dataset_feature_process import (
    TeamTrainingDatasetFeatureProcess,
)
from src.FeatureEngineering.FeatureStores.in_memory_feature_store import InMemoryFeatureStore
from src.Infrastructure.DataSources.in_memory_data_source import InMemoryDataSource


def seed_source_tables(spark, team_count=20, players_per_team=5, seed=7):
    rng = random.Random(seed)
    teams, players, stats = [], [], []
    player_id = 1

    for team_id in range(1, team_count + 1):
        wins = rng.randint(10, 30)
        teams.append((team_id, f"Team {team_id}", 1 + team_id % 3, f"{wins}-{34 - wins}", wins, 34 - wins))

        for _ in range(players_per_team):
            players.append((
                player_id, f"Player {player_id}", team_id,
                rng.choice(["G", "F", "C"]), rng.choice(["FR", "SO", "JR", "SR", "GR"]),
                rng.randint(70, 84), "Somewhere",
            ))
            minutes = rng.randint(250, 1100)
            two_att, three_att, ft_att = rng.randint(80, 300), rng.randint(20, 200), rng.randint(25, 140)
            stats.append((
                player_id, player_id, 2026, minutes,
                two_att, int(two_att * rng.uniform(0.42, 0.62)),
                three_att, int(three_att * rng.uniform(0.28, 0.46)),
                ft_att, int(ft_att * rng.uniform(0.62, 0.87)),
                rng.randint(10, 120), rng.randint(5, 60), rng.randint(20, 150),
                rng.randint(5, 40), rng.randint(2, 30), rng.randint(10, 60), rng.randint(2, 15), rng.randint(15, 90),
            ))
            player_id += 1

    return {
        "teams": spark.createDataFrame(
            teams,
            ["id", "team_name", "conference_id", "regular_season_record", "regular_season_wins", "regular_season_losses"],
        ),
        "players": spark.createDataFrame(
            players,
            ["id", "player_name", "team_id", "position", "class_year", "height_inches", "hometown"],
        ),
        "player_season_stats": spark.createDataFrame(
            stats,
            [
                "id", "player_id", "season_year", "minutes_played",
                "two_point_attempts", "two_point_made", "three_point_attempts", "three_point_made",
                "free_throw_attempts", "free_throw_made", "total_assists", "offensive_rebounds",
                "defensive_rebounds", "steals", "blocks", "total_defensive_fouls",
                "total_offensive_fouls", "total_turnovers",
            ],
        ),
    }


def run_feature_pipeline(spark, tables):
    """Runs the four feature processes in dependency order. Returns (data_source, tables)."""
    data_source = InMemoryDataSource(tables)
    feature_store = InMemoryFeatureStore(tables)
    for process in (
        PlayerDerivedMetricsFeatureProcess,
        TeamRosterCompositionFeatureProcess,
        TeamAggregatedStatsFeatureProcess,
        TeamTrainingDatasetFeatureProcess,
    ):
        process(spark, data_source, feature_store).run()
    return data_source, tables
