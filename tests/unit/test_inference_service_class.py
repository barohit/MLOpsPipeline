import json
import pickle

import pytest
from sklearn.linear_model import LinearRegression

from src.Infrastructure.Interfaces.base_inference_service import BaseInferenceService
from src.ModelTrainingAndDeployment.InferenceServices.team_regular_season_win_percentage_linear_regression_prediction_model_inference_service import (
    TeamWinCountInferenceService,
)
from src.ModelTrainingAndDeployment.feature_schema import FEATURE_COLUMNS


def _save_model(directory, columns):
    rows = [[float(i + j) for j in range(len(FEATURE_COLUMNS))] for i in range(5)]
    model = LinearRegression().fit(rows, [1.0, 2.0, 3.0, 4.0, 5.0])
    (directory / "model.pkl").write_bytes(pickle.dumps(model))
    (directory / "metadata.json").write_text(json.dumps({"feature_columns": columns}), encoding="utf-8")


def test_base_class_cannot_be_used_directly():
    with pytest.raises(TypeError):
        BaseInferenceService("/some/path")


def test_service_knows_its_location_inputs_and_output(tmp_path):
    service = TeamWinCountInferenceService(str(tmp_path))
    assert service.model_uri == str(tmp_path)
    assert service.input_columns == FEATURE_COLUMNS
    assert service.output_name == "predicted_regular_season_wins"
    assert service.is_loaded is False


def test_load_then_predict_returns_the_named_output(tmp_path):
    _save_model(tmp_path, FEATURE_COLUMNS)
    service = TeamWinCountInferenceService(str(tmp_path))
    service.load()

    result = service.predict({column: 1.0 for column in FEATURE_COLUMNS})

    assert service.is_loaded is True
    assert list(result) == ["predicted_regular_season_wins"]
    assert isinstance(result["predicted_regular_season_wins"], float)


def test_load_refuses_a_model_with_different_features(tmp_path):
    _save_model(tmp_path, list(reversed(FEATURE_COLUMNS)))
    with pytest.raises(RuntimeError, match="Training/serving mismatch"):
        TeamWinCountInferenceService(str(tmp_path)).load()


def test_load_without_a_model_uri_fails_clearly():
    with pytest.raises(RuntimeError, match="MODEL_URI"):
        TeamWinCountInferenceService(None).load()
