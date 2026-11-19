import json
import pickle

import pytest
from sklearn.linear_model import LinearRegression

from src.MetricsAndMonitoring.Drift.prediction_log import (
    JsonlPredictionLogger,
    NullPredictionLogger,
    build_prediction_logger,
    read_predictions,
)
from src.ModelTrainingAndDeployment.ModelRegistries.model_loader import load_model, parse_mlflow_uri


def test_parse_mlflow_uris():
    assert parse_mlflow_uri("models:/m/3") == ("m", "3", None)
    assert parse_mlflow_uri("models:/m@champion") == ("m", None, "champion")
    with pytest.raises(ValueError):
        parse_mlflow_uri("models:/only-a-name")


def test_load_local_directory_returns_model_metadata_and_version(tmp_path):
    model = LinearRegression().fit([[1.0], [2.0]], [1.0, 2.0])
    (tmp_path / "model.pkl").write_bytes(pickle.dumps(model))
    (tmp_path / "metadata.json").write_text(json.dumps({"version": 4, "feature_columns": ["x"]}))

    loaded = load_model(str(tmp_path))
    assert loaded.version == "4"
    assert loaded.metadata["feature_columns"] == ["x"]


def test_load_missing_directory_fails_clearly(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_model(str(tmp_path / "nope"))


def test_logger_is_off_unless_configured(monkeypatch):
    monkeypatch.delenv("PREDICTION_LOG_PATH", raising=False)
    assert isinstance(build_prediction_logger(), NullPredictionLogger)


def test_logged_predictions_can_be_read_back_and_filtered_by_version(tmp_path):
    path = tmp_path / "logs" / "predictions.jsonl"
    logger = JsonlPredictionLogger(str(path))
    logger.log({"request_id": "a", "timestamp": "2026-11-01T00:00:00", "model_version": "1", "prediction": 10.0})
    logger.log({"request_id": "b", "timestamp": "2026-11-02T00:00:00", "model_version": "2", "prediction": 11.0})

    with open(path, "a", encoding="utf-8") as f:
        f.write("an unrelated log line\n")

    assert [r["request_id"] for r in read_predictions(str(tmp_path / "logs"))] == ["a", "b"]
    assert [r["request_id"] for r in read_predictions(str(path), model_version="2")] == ["b"]
    assert [r["request_id"] for r in read_predictions(str(path), since="2026-11-02")] == ["b"]
