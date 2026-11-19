"""Data drift: do live inputs (and the model's outputs) still look like training?

Population Stability Index per feature, on the training baseline's bins:

    PSI = sum over bins of (live% - train%) * ln(live% / train%)

Common rule of thumb: < 0.1 stable, 0.1 to 0.25 worth watching, > 0.25 a real shift.
This needs no labels, so it works from the first request. Prediction drift is the
same check applied to the model's outputs, an early hint that something changed.
"""
from dataclasses import dataclass

import numpy as np

from src.MetricsAndMonitoring.Drift.baseline import bin_proportions

OK, WARNING, ALERT, INSUFFICIENT = "ok", "warning", "alert", "insufficient_data"
SEVERITY = {INSUFFICIENT: 0, OK: 1, WARNING: 2, ALERT: 3}


def population_stability_index(expected, actual, epsilon: float = 1e-4) -> float:
    expected = np.clip(np.asarray(expected, dtype=float), epsilon, None)
    actual = np.clip(np.asarray(actual, dtype=float), epsilon, None)
    return float(np.sum((actual - expected) * np.log(actual / expected)))


def worst_status(statuses) -> str:
    statuses = list(statuses)
    if not statuses:
        return INSUFFICIENT
    return max(statuses, key=lambda status: SEVERITY[status])


@dataclass
class DataDriftDetector:
    baseline: dict
    psi_warning: float = 0.1
    psi_alert: float = 0.25
    min_samples: int = 50

    def _status(self, psi: float, count: int) -> str:
        if count < self.min_samples:
            return INSUFFICIENT
        if psi >= self.psi_alert:
            return ALERT
        if psi >= self.psi_warning:
            return WARNING
        return OK

    def _compare(self, summary: dict, live_values) -> dict:
        live = np.asarray(live_values, dtype=float)
        live = live[~np.isnan(live)]
        psi = population_stability_index(summary["proportions"], bin_proportions(live, summary["edges"]))

        live_mean = float(live.mean()) if len(live) else None
        shift = None
        if live_mean is not None and summary.get("std"):
            shift = (live_mean - summary["mean"]) / summary["std"]

        return {
            "psi": psi,
            "status": self._status(psi, len(live)),
            "live_count": int(len(live)),
            "live_mean": live_mean,
            "baseline_mean": summary.get("mean"),
            "mean_shift_in_std": shift,
        }

    def check(self, live_features: dict[str, list], live_predictions=None) -> dict:
        features = {}
        for column, summary in self.baseline["features"].items():
            if column not in live_features:
                features[column] = {"psi": None, "status": INSUFFICIENT, "live_count": 0}
                continue
            features[column] = self._compare(summary, live_features[column])

        report = {
            "features": features,
            "status": worst_status(result["status"] for result in features.values()),
            "drifted_features": sorted(
                column for column, result in features.items() if result["status"] in (WARNING, ALERT)
            ),
        }

        if live_predictions is not None and "prediction" in self.baseline:
            report["prediction"] = self._compare(self.baseline["prediction"], live_predictions)

        return report
