"""Concept drift: for the same kind of input, has the right answer changed?

This can only be measured once real outcomes arrive. Every prediction is logged with
a request_id; when the true outcome is known (here: a team's actual regular-season
wins), it is recorded against that request_id. We join the two and compare live RMSE
to the RMSE the model scored on the frozen holdout at training time.
"""
from dataclasses import dataclass

import numpy as np

from src.MetricsAndMonitoring.Drift.data_drift import ALERT, INSUFFICIENT, OK, WARNING


def join_predictions_with_labels(predictions: list[dict], labels: list[dict]) -> list[dict]:
    actual_by_request = {label["request_id"]: float(label["actual"]) for label in labels if "actual" in label}
    joined = []
    for record in predictions:
        actual = actual_by_request.get(record.get("request_id"))
        if actual is not None:
            joined.append({**record, "actual": actual})
    return sorted(joined, key=lambda record: record.get("timestamp", ""))


def _rmse(rows: list[dict]) -> float:
    errors = np.array([row["prediction"] - row["actual"] for row in rows], dtype=float)
    return float(np.sqrt(np.mean(errors ** 2)))


@dataclass
class ConceptDriftDetector:
    baseline_rmse: float
    warning_ratio: float = 1.10
    alert_ratio: float = 1.25
    min_labeled: int = 30
    window_size: int | None = None

    def _status(self, ratio: float, count: int) -> str:
        if count < self.min_labeled:
            return INSUFFICIENT
        if ratio >= self.alert_ratio:
            return ALERT
        if ratio >= self.warning_ratio:
            return WARNING
        return OK

    def check(self, predictions: list[dict], labels: list[dict]) -> dict:
        joined = join_predictions_with_labels(predictions, labels)

        if not joined:
            return {"status": INSUFFICIENT, "labeled_count": 0, "baseline_rmse": self.baseline_rmse}

        windows = []
        if self.window_size:
            for start in range(0, len(joined), self.window_size):
                chunk = joined[start:start + self.window_size]
                windows.append({
                    "start": chunk[0].get("timestamp"),
                    "end": chunk[-1].get("timestamp"),
                    "count": len(chunk),
                    "rmse": _rmse(chunk),
                })

        # Judge on the most recent full window if there is one, otherwise on everything
        recent = joined
        if windows and windows[-1]["count"] >= self.min_labeled:
            recent = joined[-windows[-1]["count"]:]

        live_rmse = _rmse(recent)
        ratio = live_rmse / self.baseline_rmse if self.baseline_rmse else float("inf")

        return {
            "status": self._status(ratio, len(recent)),
            "labeled_count": len(joined),
            "live_rmse": live_rmse,
            "baseline_rmse": self.baseline_rmse,
            "rmse_ratio": ratio,
            "windows": windows,
        }


def diagnose(data_status: str, concept_status: str) -> str:
    """What the combination of the two checks usually means, and the next step."""
    data_drifted = data_status in (WARNING, ALERT)
    concept_drifted = concept_status in (WARNING, ALERT)

    if data_drifted and concept_drifted:
        return ("Inputs shifted and accuracy dropped. First confirm the upstream data is correct "
                "(a broken feature job looks like drift), then retrain on recent data and gate it.")
    if data_drifted:
        return ("Inputs shifted but accuracy is holding so far. Check the source of the shift "
                "(pipeline change, new population) and keep watching labeled accuracy.")
    if concept_drifted:
        return ("Accuracy dropped while inputs look the same: the relationship changed. "
                "Retrain on recent labeled data and run it through the evaluation gate.")
    if INSUFFICIENT in (data_status, concept_status):
        return "Not enough live data or labels yet to judge one or both checks."
    return "No drift detected."
