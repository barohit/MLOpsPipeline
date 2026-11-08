"""Runs the whole pipeline in-process, with no database and no cluster:

    source tables -> four feature processes -> training dataset -> training
    -> local model registry -> inference service -> /predict and /metrics

Postgres/Snowflake, MLflow and Kubernetes are the parts this does NOT cover.
"""
import json
import random
import re

import pytest
from fastapi.testclient import TestClient

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
from src.MetricsAndMonitoring.team_regular_reason_win_percentage_prometheus_grafana_monitoring_service import (
    PrometheusGrafanaMonitoringService,
)
from src.ModelTrainingAndDeployment.InferenceServices import (
    team_regular_season_win_percentage_linear_regression_prediction_model_inference_service as service,
)
from src.ModelTrainingAndDeployment.ModelRegistries.LocalFileModelRegistry import LocalFileModelRegistry
from src.ModelTrainingAndDeployment.TrainingProcesses.team_regular_season_win_percentage_linear_regression_model_training_process import (
    TeamRegularSeasonWinCountLinearRegressionModelTrainer,
)
from src.ModelTrainingAndDeployment.feature_schema import (
    FEATURE_COLUMNS,
    MODEL_NAME,
    TARGET_COLUMN,
)

TEAM_COUNT = 20
PLAYERS_PER_TEAM = 5


def seed_source_tables(spark):
    rng = random.Random(7)
    teams, players, stats = [], [], []
    player_id = 1

    for team_id in range(1, TEAM_COUNT + 1):
        wins = rng.randint(10, 30)
        teams.append((team_id, f"Team {team_id}", 1 + team_id % 3, f"{wins}-{34 - wins}", wins, 34 - wins))

        for _ in range(PLAYERS_PER_TEAM):
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


@pytest.fixture(scope="module")
def trained(spark, tmp_path_factory):
    tables = seed_source_tables(spark)
    data_source = InMemoryDataSource(tables)
    feature_store = InMemoryFeatureStore(tables)

    # Same order as the dependencies between them
    for process in (
        PlayerDerivedMetricsFeatureProcess,
        TeamRosterCompositionFeatureProcess,
        TeamAggregatedStatsFeatureProcess,
        TeamTrainingDatasetFeatureProcess,
    ):
        process(spark, data_source, feature_store).run()

    registry = LocalFileModelRegistry(str(tmp_path_factory.mktemp("registry")))
    result = TeamRegularSeasonWinCountLinearRegressionModelTrainer(spark, data_source, registry).run()
    return {"tables": tables, "result": result}


def test_training_dataset_has_every_feature_and_the_target(trained):
    columns = set(trained["tables"]["team_training_dataset_features"].columns)
    assert set(FEATURE_COLUMNS) <= columns
    assert TARGET_COLUMN in columns
    assert trained["tables"]["team_training_dataset_features"].count() == TEAM_COUNT


def test_training_registers_a_model_with_metrics_and_feature_list(trained):
    result = trained["result"]
    assert set(result["metrics"]) == {"mae", "rmse", "r2"}
    assert result["metadata"]["feature_columns"] == FEATURE_COLUMNS

    with open(f"{result['model_uri']}/metadata.json", encoding="utf-8") as f:
        saved = json.load(f)
    assert saved["feature_columns"] == FEATURE_COLUMNS
    assert saved["model_name"] == MODEL_NAME
    assert saved["version"] == 1


def test_service_predicts_and_exports_the_metrics_the_dashboard_uses(trained, monkeypatch):
    monkeypatch.setenv("MODEL_URI", trained["result"]["model_uri"])
    row = trained["tables"]["team_training_dataset_features"].collect()[0].asDict()
    payload = {column: float(row[column]) for column in FEATURE_COLUMNS}

    with TestClient(service.app) as client:
        assert client.get("/health").json()["model_loaded"] is True

        response = client.post("/predict", json=payload)
        assert response.status_code == 200
        assert isinstance(response.json()["predicted_regular_season_wins"], float)

        # a request with a missing field is rejected, not guessed at
        payload_without_field = {k: v for k, v in payload.items() if k != "player_count"}
        assert client.post("/predict", json=payload_without_field).status_code == 422

        metrics_text = client.get("/metrics").text

    dashboard = PrometheusGrafanaMonitoringService().build_grafana_dashboard_configmap(MODEL_NAME, "default")
    dashboard_json = next(iter(dashboard["data"].values()))
    for metric_name in set(re.findall(r"team_win_prediction_\w+", dashboard_json)):
        assert metric_name in metrics_text, f"dashboard queries {metric_name} but the service doesn't export it"


def test_service_refuses_a_model_trained_on_different_features(trained, monkeypatch, tmp_path):
    model_dir = tmp_path / "tampered"
    model_dir.mkdir()
    source = trained["result"]["model_uri"]
    (model_dir / "model.pkl").write_bytes(open(f"{source}/model.pkl", "rb").read())
    metadata = json.load(open(f"{source}/metadata.json", encoding="utf-8"))
    metadata["feature_columns"] = list(reversed(metadata["feature_columns"]))
    (model_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")

    monkeypatch.setenv("MODEL_URI", str(model_dir))
    with pytest.raises(RuntimeError, match="Training/serving mismatch"):
        with TestClient(service.app):
            pass
