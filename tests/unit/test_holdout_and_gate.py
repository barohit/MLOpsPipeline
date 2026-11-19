import pandas as pd
import pytest
from sklearn.linear_model import LinearRegression

from src.ModelTrainingAndDeployment.Evaluation.evaluation import EvaluationGate, evaluate
from src.ModelTrainingAndDeployment.Evaluation.holdout import (
    HoldoutIntegrityError,
    freeze_holdout,
    load_holdout,
    split_holdout,
)


def _frame(n=200):
    return pd.DataFrame({
        "team_id": range(1, n + 1),
        "conference_id": [1 + i % 3 for i in range(n)],
        "x": [float(i) for i in range(n)],
        "y": [2.0 * i + 1.0 for i in range(n)],
    })


def test_split_is_deterministic_and_disjoint():
    df = _frame()
    train_a, holdout_a = split_holdout(df, "team_id", fraction=0.2, salt="v1")
    train_b, holdout_b = split_holdout(df.sample(frac=1, random_state=3), "team_id", fraction=0.2, salt="v1")

    assert set(holdout_a.team_id) == set(holdout_b.team_id)  # row order doesn't matter
    assert not set(holdout_a.team_id) & set(train_a.team_id)
    assert 20 < len(holdout_a) < 60  # roughly 20%


def test_a_key_keeps_its_side_when_new_rows_arrive():
    small, bigger = _frame(100), _frame(300)
    _, holdout_small = split_holdout(small, "team_id", salt="v1")
    _, holdout_big = split_holdout(bigger, "team_id", salt="v1")
    assert set(holdout_small.team_id) <= set(holdout_big.team_id)


def test_freeze_then_load_round_trips_and_detects_tampering(tmp_path):
    _, holdout = split_holdout(_frame(), "team_id", salt="v1")
    manifest = freeze_holdout(holdout, str(tmp_path), "v1", "team_id", ["x"], "y", "v1", 0.2,
                              extra_columns=["conference_id"])

    loaded, loaded_manifest = load_holdout(str(tmp_path / "v1"))
    assert loaded_manifest["sha256"] == manifest["sha256"]
    assert len(loaded) == manifest["row_count"]
    assert "conference_id" in loaded.columns

    csv_path = tmp_path / "v1" / "holdout.csv"
    csv_path.write_text(csv_path.read_text() + "999,1,1.0,3.0\n")
    with pytest.raises(HoldoutIntegrityError):
        load_holdout(str(tmp_path / "v1"))


def test_a_holdout_version_cannot_be_overwritten(tmp_path):
    _, holdout = split_holdout(_frame(), "team_id", salt="v1")
    freeze_holdout(holdout, str(tmp_path), "v1", "team_id", ["x"], "y", "v1", 0.2)
    with pytest.raises(FileExistsError):
        freeze_holdout(holdout, str(tmp_path), "v1", "team_id", ["x"], "y", "v1", 0.2)


class _Offset:
    """A model that predicts y + offset, optionally worse on one conference."""

    def __init__(self, offset=0.0, bad_conference=None, bad_offset=0.0, frame=None):
        self.offset, self.bad_conference, self.bad_offset, self.frame = offset, bad_conference, bad_offset, frame

    def predict(self, X):
        rows = self.frame.loc[X.index]
        extra = (rows.conference_id == self.bad_conference) * self.bad_offset
        return (rows.y + self.offset + extra).to_numpy()


def test_gate_passes_an_equal_or_better_candidate_and_fails_a_worse_one():
    df = _frame(60)
    production = evaluate(_Offset(1.0, frame=df), df, ["x"], "y", "conference_id")
    better = evaluate(_Offset(0.5, frame=df), df, ["x"], "y", "conference_id")
    worse = evaluate(_Offset(2.0, frame=df), df, ["x"], "y", "conference_id")

    gate = EvaluationGate()
    assert gate.check(better, production).passed
    result = gate.check(worse, production)
    assert not result.passed
    assert "Overall RMSE" in result.reasons[0]


def test_gate_catches_one_segment_getting_worse_even_when_the_average_improves():
    df = _frame(60)
    production = evaluate(_Offset(1.0, frame=df), df, ["x"], "y", "conference_id")
    # Much better on average, but conference 2 is badly worse
    candidate = evaluate(_Offset(0.1, bad_conference=2, bad_offset=1.6, frame=df), df, ["x"], "y", "conference_id")

    assert candidate["rmse"] < production["rmse"]
    result = EvaluationGate(segment_tolerance=0.15).check(candidate, production)
    assert not result.passed
    assert any("Segment 2" in reason for reason in result.reasons)


def test_first_model_is_gated_on_an_absolute_limit():
    df = _frame(40)
    model = LinearRegression().fit(df[["x"]], df["y"])
    candidate = evaluate(model, df, ["x"], "y")
    assert EvaluationGate(max_rmse_without_baseline=1.0).check(candidate, None).passed
    assert not EvaluationGate(max_rmse_without_baseline=-1.0).check(candidate, None).passed
