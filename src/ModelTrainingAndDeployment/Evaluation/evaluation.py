"""Score a model on the frozen holdout, and decide whether a candidate may replace
the model in production.

The gate checks two things, because an average can hide a regression:
- overall RMSE must not be worse than production's by more than a tolerance
- no segment (e.g. a conference) may get worse by more than a larger tolerance
"""
from dataclasses import asdict, dataclass, field

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


def _rmse(y_true, y_pred) -> float:
    return float(mean_squared_error(y_true, y_pred) ** 0.5)


def evaluate(
    model,
    holdout_df: pd.DataFrame,
    feature_columns: list[str],
    target_column: str,
    segment_column: str | None = None,
) -> dict:
    X = holdout_df[feature_columns]
    y_true = holdout_df[target_column].to_numpy(dtype=float)
    y_pred = np.asarray(model.predict(X), dtype=float)

    result = {
        "row_count": int(len(holdout_df)),
        "rmse": _rmse(y_true, y_pred),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        # R^2 is undefined on fewer than two rows
        "r2": float(r2_score(y_true, y_pred)) if len(holdout_df) > 1 else None,
        "segments": {},
    }

    if segment_column and segment_column in holdout_df.columns:
        errors = pd.DataFrame({
            "segment": holdout_df[segment_column].to_numpy(),
            "y_true": y_true,
            "y_pred": y_pred,
        })
        for segment, group in errors.groupby("segment"):
            result["segments"][str(segment)] = {
                "row_count": int(len(group)),
                "rmse": _rmse(group["y_true"], group["y_pred"]),
            }

    return result


@dataclass
class GateResult:
    passed: bool
    reasons: list[str] = field(default_factory=list)
    candidate: dict = field(default_factory=dict)
    production: dict | None = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class EvaluationGate:
    # Candidate overall RMSE may be at most this fraction worse than production's
    rmse_tolerance: float = 0.0
    # Any single segment may be at most this fraction worse than production's
    segment_tolerance: float = 0.15
    # Segments with fewer rows than this are reported but not gated on (too noisy)
    min_segment_rows: int = 1
    # Used only when there is no production model yet
    max_rmse_without_baseline: float | None = None

    def check(self, candidate: dict, production: dict | None) -> GateResult:
        reasons = []

        if production is None:
            if self.max_rmse_without_baseline is not None and candidate["rmse"] > self.max_rmse_without_baseline:
                reasons.append(
                    f"No production model; candidate RMSE {candidate['rmse']:.4f} exceeds "
                    f"the absolute limit {self.max_rmse_without_baseline:.4f}"
                )
            return GateResult(passed=not reasons, reasons=reasons, candidate=candidate, production=None)

        if candidate["row_count"] != production["row_count"]:
            reasons.append("Candidate and production were not scored on the same holdout")

        allowed = production["rmse"] * (1 + self.rmse_tolerance)
        if candidate["rmse"] > allowed:
            reasons.append(
                f"Overall RMSE {candidate['rmse']:.4f} is worse than production "
                f"{production['rmse']:.4f} (allowed up to {allowed:.4f})"
            )

        for segment, prod_segment in production.get("segments", {}).items():
            cand_segment = candidate.get("segments", {}).get(segment)
            if cand_segment is None or prod_segment["row_count"] < self.min_segment_rows:
                continue

            segment_allowed = prod_segment["rmse"] * (1 + self.segment_tolerance)
            if cand_segment["rmse"] > segment_allowed:
                reasons.append(
                    f"Segment {segment}: RMSE {cand_segment['rmse']:.4f} vs production "
                    f"{prod_segment['rmse']:.4f} (allowed up to {segment_allowed:.4f})"
                )

        return GateResult(passed=not reasons, reasons=reasons, candidate=candidate, production=production)
