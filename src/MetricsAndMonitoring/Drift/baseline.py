"""The training-time snapshot that live traffic is compared against.

For each feature we store quantile bin edges and the share of training rows in each
bin, plus mean and standard deviation. Storing bins (not raw rows) keeps the baseline
small enough to live in the model's metadata, and it is all PSI needs.
"""
import numpy as np


def _quantile_edges(values: np.ndarray, bins: int) -> list[float]:
    """Interior cut points. Bin i holds values with edges[i-1] < v <= edges[i]."""
    if len(values) == 0:
        return []
    interior = np.quantile(values, np.linspace(0, 1, bins + 1)[1:-1])
    return [float(edge) for edge in np.unique(interior)]


def bin_proportions(values, edges: list[float]) -> np.ndarray:
    values = np.asarray(values, dtype=float)
    values = values[~np.isnan(values)]
    counts = np.bincount(np.searchsorted(edges, values, side="left"), minlength=len(edges) + 1)
    total = counts.sum()
    return counts / total if total else counts.astype(float)


def summarize(values, bins: int = 10) -> dict:
    values = np.asarray(values, dtype=float)
    values = values[~np.isnan(values)]
    edges = _quantile_edges(values, bins)
    return {
        "edges": edges,
        "proportions": [float(p) for p in bin_proportions(values, edges)],
        "mean": float(values.mean()) if len(values) else None,
        "std": float(values.std()) if len(values) else None,
        "count": int(len(values)),
    }


def build_baseline(training_features, feature_columns: list[str], training_predictions=None, bins: int = 10) -> dict:
    """training_features: a pandas DataFrame of the rows the model was fitted on."""
    baseline = {
        "bins": bins,
        "features": {column: summarize(training_features[column].to_numpy(), bins) for column in feature_columns},
    }
    if training_predictions is not None:
        baseline["prediction"] = summarize(training_predictions, bins)
    return baseline
