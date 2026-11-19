import numpy as np
import pandas as pd

from src.MetricsAndMonitoring.Drift.baseline import build_baseline
from src.MetricsAndMonitoring.Drift.concept_drift import ConceptDriftDetector, diagnose, join_predictions_with_labels
from src.MetricsAndMonitoring.Drift.data_drift import (
    ALERT,
    INSUFFICIENT,
    OK,
    DataDriftDetector,
    population_stability_index,
)

RNG = np.random.default_rng(0)


def _baseline():
    train = pd.DataFrame({"a": RNG.normal(0, 1, 2000), "b": RNG.uniform(0, 10, 2000)})
    return build_baseline(train, ["a", "b"], training_predictions=RNG.normal(20, 3, 2000), bins=10)


def test_psi_is_zero_for_identical_distributions_and_large_for_shifted_ones():
    assert population_stability_index([0.25] * 4, [0.25] * 4) == 0.0
    assert population_stability_index([0.25] * 4, [0.7, 0.1, 0.1, 0.1]) > 0.25


def test_same_distribution_is_ok_and_a_shifted_feature_alerts():
    detector = DataDriftDetector(_baseline(), min_samples=100)

    same = detector.check({"a": RNG.normal(0, 1, 1000), "b": RNG.uniform(0, 10, 1000)})
    assert same["status"] == OK
    assert same["drifted_features"] == []

    shifted = detector.check({"a": RNG.normal(1.5, 1, 1000), "b": RNG.uniform(0, 10, 1000)})
    assert shifted["features"]["a"]["status"] == ALERT
    assert shifted["features"]["b"]["status"] == OK
    assert shifted["status"] == ALERT
    assert shifted["drifted_features"] == ["a"]
    assert shifted["features"]["a"]["mean_shift_in_std"] > 1


def test_too_few_samples_is_reported_as_insufficient_not_ok():
    report = DataDriftDetector(_baseline(), min_samples=100).check({"a": [0.1] * 10, "b": [5.0] * 10})
    assert report["status"] == INSUFFICIENT


def test_prediction_drift_is_checked_when_predictions_are_given():
    report = DataDriftDetector(_baseline(), min_samples=100).check(
        {"a": RNG.normal(0, 1, 500), "b": RNG.uniform(0, 10, 500)},
        live_predictions=RNG.normal(28, 3, 500),
    )
    assert report["prediction"]["status"] == ALERT


def _records(n, error):
    predictions = [{"request_id": f"r{i}", "timestamp": f"2026-11-{1 + i % 28:02d}T00:00:{i % 60:02d}",
                    "prediction": 20.0 + error} for i in range(n)]
    labels = [{"request_id": f"r{i}", "actual": 20.0} for i in range(n)]
    return predictions, labels


def test_join_only_keeps_predictions_with_known_outcomes():
    predictions, labels = _records(10, 1.0)
    assert len(join_predictions_with_labels(predictions, labels[:4])) == 4


def test_concept_drift_ok_when_live_error_matches_holdout_and_alert_when_much_worse():
    detector = ConceptDriftDetector(baseline_rmse=2.0, min_labeled=20)

    ok = detector.check(*_records(50, 2.0))
    assert ok["status"] == OK
    assert round(ok["rmse_ratio"], 6) == 1.0

    drifted = detector.check(*_records(50, 3.0))
    assert drifted["status"] == ALERT
    assert drifted["rmse_ratio"] == 1.5


def test_concept_drift_needs_enough_labels():
    assert ConceptDriftDetector(baseline_rmse=2.0, min_labeled=30).check(*_records(5, 9.0))["status"] == INSUFFICIENT


def test_diagnosis_distinguishes_the_four_cases():
    assert "relationship changed" in diagnose(OK, ALERT)
    assert "accuracy is holding" in diagnose(ALERT, OK)
    assert "confirm the upstream data" in diagnose(ALERT, ALERT)
    assert diagnose(OK, OK) == "No drift detected."
