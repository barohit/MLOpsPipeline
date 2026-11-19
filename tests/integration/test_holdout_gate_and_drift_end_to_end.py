"""Holdout -> training -> evaluation gate -> serving with prediction logs -> drift check.

Everything runs in-process: the feature pipeline on local Spark, a local file registry,
the FastAPI app through TestClient, and the drift CLI on the log file it wrote.
"""
import json
import pickle

import pytest
from fastapi.testclient import TestClient

from src.MetricsAndMonitoring.Drift import run_drift_check
from src.MetricsAndMonitoring.Drift.data_drift import ALERT
from src.ModelTrainingAndDeployment.Evaluation import run_gate
from src.ModelTrainingAndDeployment.Evaluation.create_holdout import create_holdout
from src.ModelTrainingAndDeployment.Evaluation.holdout import load_holdout
from src.ModelTrainingAndDeployment.InferenceServices import (
    team_regular_season_win_percentage_linear_regression_prediction_model_inference_service as service,
)
from src.ModelTrainingAndDeployment.ModelRegistries.LocalFileModelRegistry import LocalFileModelRegistry
from src.ModelTrainingAndDeployment.TrainingProcesses.team_regular_season_win_percentage_linear_regression_model_training_process import (
    TeamRegularSeasonWinCountLinearRegressionModelTrainer,
)
from src.ModelTrainingAndDeployment.feature_schema import FEATURE_COLUMNS, FEATURE_TABLE_NAME, TARGET_COLUMN
from tests.pipeline_fixtures import run_feature_pipeline, seed_source_tables

TEAM_COUNT = 80


@pytest.fixture(scope="module")
def pipeline(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("lifecycle")
    data_source, tables = run_feature_pipeline(spark, seed_source_tables(spark, team_count=TEAM_COUNT, seed=11))
    training_pdf = tables[FEATURE_TABLE_NAME].toPandas()

    create_holdout(training_pdf, str(root / "holdouts"), "v1", fraction=0.25)
    holdout_df, manifest = load_holdout(str(root / "holdouts" / "v1"))

    registry = LocalFileModelRegistry(str(root / "registry"))

    def train(with_holdout=True):
        return TeamRegularSeasonWinCountLinearRegressionModelTrainer(
            spark, data_source, registry,
            holdout_df=holdout_df if with_holdout else None,
            holdout_manifest=manifest if with_holdout else None,
        ).run()

    return {
        "root": root, "training_pdf": training_pdf, "holdout_df": holdout_df, "manifest": manifest,
        "v1": train(), "v2": train(), "no_holdout": train(with_holdout=False),
    }


def test_training_excludes_the_holdout_and_records_versioning_metadata(pipeline):
    metadata = pipeline["v1"]["metadata"]
    assert metadata["excluded_holdout_rows"] == len(pipeline["holdout_df"]) > 0
    assert metadata["holdout_sha256"] == pipeline["manifest"]["sha256"]
    assert set(metadata["drift_baseline"]["features"]) == set(FEATURE_COLUMNS)
    assert "rmse" in metadata["holdout_metrics"]
    # Same data in, same fingerprint out: the data version is reproducible
    assert metadata["training_data_sha256"] == pipeline["v2"]["metadata"]["training_data_sha256"]


def _gate(pipeline, candidate_uri, production_uri=None, *extra):
    args = ["--holdout-dir", str(pipeline["root"] / "holdouts" / "v1"), "--candidate-uri", candidate_uri,
            "--report", str(pipeline["root"] / "gate.json"), *extra]
    if production_uri:
        args += ["--production-uri", production_uri]
    return run_gate.main(args)


def test_gate_passes_the_first_model_and_an_equal_retrain(pipeline):
    missing = str(pipeline["root"] / "registry" / "does-not-exist")
    assert _gate(pipeline, pipeline["v1"]["model_uri"], missing, "--allow-missing-production") == 0
    assert _gate(pipeline, pipeline["v2"]["model_uri"], pipeline["v1"]["model_uri"]) == 0

    report = json.loads((pipeline["root"] / "gate.json").read_text())
    assert report["passed"] is True
    assert report["holdout_version"] == "v1"


def test_gate_rejects_a_model_that_was_not_trained_against_this_holdout(pipeline):
    assert _gate(pipeline, pipeline["no_holdout"]["model_uri"], pipeline["v1"]["model_uri"]) == 1
    reasons = json.loads((pipeline["root"] / "gate.json").read_text())["reasons"]
    assert any("not trained against this holdout" in reason for reason in reasons)


class _Terrible:
    def predict(self, X):
        return [1000.0] * len(X)


def test_gate_rejects_a_worse_model(pipeline):
    bad_dir = pipeline["root"] / "bad"
    bad_dir.mkdir()
    (bad_dir / "model.pkl").write_bytes(pickle.dumps(_Terrible()))
    (bad_dir / "metadata.json").write_text(json.dumps(pipeline["v1"]["metadata"]))
    assert _gate(pipeline, str(bad_dir), pipeline["v1"]["model_uri"]) == 1


def _serve_and_log(pipeline, monkeypatch, log_path, rows, scale=1.0, prefix="r"):
    monkeypatch.setenv("MODEL_URI", pipeline["v1"]["model_uri"])
    monkeypatch.setenv("PREDICTION_LOG_PATH", str(log_path))
    request_ids = []
    with TestClient(service.app) as client:
        assert client.get("/ready").status_code == 200
        for index, row in enumerate(rows):
            payload = {column: float(row[column]) * scale for column in FEATURE_COLUMNS}
            response = client.post("/predict", json=payload, headers={"X-Request-ID": f"{prefix}{index}"})
            assert response.status_code == 200
            body = response.json()
            assert body["model_version"] == "1"
            request_ids.append(body["request_id"])
        metrics_text = client.get("/metrics").text
    return request_ids, metrics_text


def _drift(pipeline, log_path, labels_path=None, *extra):
    args = ["--baseline-file", str(pipeline["v1"]["model_uri"]) + "/metadata.json",
            "--predictions", str(log_path), "--min-samples", "20", "--min-labeled", "20",
            "--report", str(pipeline["root"] / "drift.json"), *extra]
    if labels_path:
        args += ["--labels", str(labels_path)]
    assert run_drift_check.main(args) == 0
    return json.loads((pipeline["root"] / "drift.json").read_text())


def test_served_predictions_are_logged_with_ids_and_versioned_metrics(pipeline, monkeypatch, tmp_path):
    rows = pipeline["training_pdf"].to_dict("records")[:5]
    request_ids, metrics_text = _serve_and_log(pipeline, monkeypatch, tmp_path / "p.jsonl", rows)

    logged = [json.loads(line) for line in (tmp_path / "p.jsonl").read_text().splitlines()]
    assert [record["request_id"] for record in logged] == request_ids == [f"r{i}" for i in range(5)]
    assert set(logged[0]["features"]) == set(FEATURE_COLUMNS)
    assert 'team_win_prediction_value_count{model_version="1"}' in metrics_text
    assert 'team_win_model_info{model_name=' in metrics_text


def test_drift_check_alerts_on_shifted_inputs_but_not_on_training_like_inputs(pipeline, monkeypatch, tmp_path):
    rows = pipeline["training_pdf"].to_dict("records")

    _serve_and_log(pipeline, monkeypatch, tmp_path / "normal.jsonl", rows)
    normal = _drift(pipeline, tmp_path / "normal.jsonl")
    assert normal["prediction_count"] == len(rows)
    assert normal["data_drift"]["status"] != ALERT

    _serve_and_log(pipeline, monkeypatch, tmp_path / "shifted.jsonl", rows, scale=3.0)
    shifted = _drift(pipeline, tmp_path / "shifted.jsonl")
    assert shifted["data_drift"]["status"] == ALERT
    assert len(shifted["data_drift"]["drifted_features"]) > 0


def test_concept_drift_alerts_when_real_outcomes_stop_matching(pipeline, monkeypatch, tmp_path):
    rows = pipeline["training_pdf"].to_dict("records")
    log_path = tmp_path / "c.jsonl"
    request_ids, _ = _serve_and_log(pipeline, monkeypatch, log_path, rows, prefix="c")

    predictions = {json.loads(line)["request_id"]: json.loads(line)["prediction"]
                   for line in log_path.read_text().splitlines()}

    # The world changed: true outcomes now sit 15 wins away from what the model predicts
    labels_path = tmp_path / "labels.jsonl"
    labels_path.write_text("\n".join(
        json.dumps({"request_id": rid, "actual": predictions[rid] + 15.0}) for rid in request_ids
    ))

    report = _drift(pipeline, log_path, labels_path)
    assert report["concept_drift"]["labeled_count"] == len(rows)
    assert report["concept_drift"]["status"] == ALERT
    assert "relationship changed" in report["diagnosis"] or "accuracy dropped" in report["diagnosis"]


def test_drift_metrics_are_pushed_for_prometheus(pipeline, monkeypatch, tmp_path):
    import prometheus_client

    pushed = {}
    monkeypatch.setattr(prometheus_client, "push_to_gateway",
                        lambda gateway, job, registry, grouping_key: pushed.update(
                            gateway=gateway, job=job, grouping_key=grouping_key,
                            names={m.name for m in registry.collect()}))

    rows = pipeline["training_pdf"].to_dict("records")
    _serve_and_log(pipeline, monkeypatch, tmp_path / "m.jsonl", rows)
    _drift(pipeline, tmp_path / "m.jsonl", None, "--pushgateway", "pushgateway:9091")

    assert pushed["grouping_key"] == {"model_version": "1"}
    assert {"model_feature_psi", "model_drift_status", "model_prediction_psi"} <= pushed["names"]
