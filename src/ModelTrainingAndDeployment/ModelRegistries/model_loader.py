"""Load a registered model and its metadata from either registry.

Two URI shapes are supported, so the same code works locally and in a cluster:

    /path/to/registry/<model_name>/v3        a LocalFileModelRegistry version directory
    models:/<model_name>/3                   an MLflow registered model version
    models:/<model_name>@champion            an MLflow registered model alias

Whatever loads the model also gets its metadata (feature list, metrics, drift
baseline, training-data fingerprint), because serving and evaluation both need it.
"""
import json
import os
import pickle
from dataclasses import dataclass, field
from typing import Any


@dataclass
class LoadedModel:
    model: Any
    metadata: dict = field(default_factory=dict)
    uri: str = ""
    version: str = "unknown"


def is_mlflow_uri(uri: str) -> bool:
    return uri.startswith("models:/")


def parse_mlflow_uri(uri: str) -> tuple[str, str | None, str | None]:
    """Return (model_name, version, alias). Exactly one of version/alias is set."""
    if not is_mlflow_uri(uri):
        raise ValueError(f"Not an MLflow models:/ URI: {uri}")

    reference = uri[len("models:/"):]

    if "@" in reference:
        model_name, alias = reference.split("@", 1)
        if not model_name or not alias:
            raise ValueError(f"Malformed alias URI: {uri}")
        return model_name, None, alias

    parts = reference.split("/")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Expected models:/<name>/<version> or models:/<name>@<alias>, got {uri}")

    return parts[0], parts[1], None


def load_model(uri: str) -> LoadedModel:
    if not uri:
        raise ValueError("A model URI is required")

    if is_mlflow_uri(uri):
        return _load_from_mlflow(uri)

    return _load_from_directory(uri)


def _load_from_directory(directory: str) -> LoadedModel:
    model_file_path = os.path.join(directory, "model.pkl")
    if not os.path.isfile(model_file_path):
        raise FileNotFoundError(f"No model.pkl in {directory}")

    with open(model_file_path, "rb") as model_file:
        model = pickle.load(model_file)

    metadata = {}
    metadata_file_path = os.path.join(directory, "metadata.json")
    if os.path.isfile(metadata_file_path):
        with open(metadata_file_path, encoding="utf-8") as metadata_file:
            metadata = json.load(metadata_file)

    return LoadedModel(
        model=model,
        metadata=metadata,
        uri=directory,
        version=str(metadata.get("version", "unknown")),
    )


def _load_from_mlflow(uri: str) -> LoadedModel:
    # Imported lazily so the serving image only needs MLflow when it actually uses it
    import mlflow
    import mlflow.sklearn
    from mlflow.tracking import MlflowClient

    model_name, version, alias = parse_mlflow_uri(uri)
    client = MlflowClient()

    if alias is not None:
        model_version = client.get_model_version_by_alias(model_name, alias)
    else:
        model_version = client.get_model_version(model_name, version)

    # Resolve the alias to a concrete version so we log and report exactly what we serve
    resolved_uri = f"models:/{model_name}/{model_version.version}"
    model = mlflow.sklearn.load_model(resolved_uri)

    metadata = {}
    try:
        metadata = mlflow.artifacts.load_dict(f"runs:/{model_version.run_id}/metadata.json")
    except Exception:
        # Models registered before metadata.json was logged still load; they just lose
        # the feature-list check and the drift baseline.
        metadata = {}

    return LoadedModel(
        model=model,
        metadata=metadata,
        uri=resolved_uri,
        version=str(model_version.version),
    )
