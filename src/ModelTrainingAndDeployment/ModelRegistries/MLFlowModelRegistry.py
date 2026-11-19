import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

from src.Infrastructure.Interfaces.base_model_registry import BaseModelRegistry


# MLflow is an open source model registry. Versions are immutable; aliases such as
# "champion" (serving) and "challenger" (being rolled out) are movable pointers.
CHAMPION_ALIAS = "champion"
CHALLENGER_ALIAS = "challenger"
PREVIOUS_CHAMPION_ALIAS = "previous_champion"


class MLflowModelRegistry(BaseModelRegistry):
    def __init__(self, tracking_uri: str | None = None, experiment_name: str | None = None):
        self.tracking_uri = tracking_uri
        self.experiment_name = experiment_name

        if self.tracking_uri:
            mlflow.set_tracking_uri(self.tracking_uri)

        if self.experiment_name:
            mlflow.set_experiment(self.experiment_name)

    def register_model(self, model, model_name: str, metadata: dict) -> str:
        with mlflow.start_run() as run:
            metrics = metadata.get("metrics", {})
            feature_columns = metadata.get("feature_columns", [])
            coefficients = metadata.get("coefficients", {})

            for metric_name, metric_value in metrics.items():
                if isinstance(metric_value, (int, float)):
                    mlflow.log_metric(metric_name, metric_value)

            for metadata_key, metadata_value in metadata.items():
                if metadata_key in {"metrics", "coefficients", "feature_columns"}:
                    continue

                if isinstance(metadata_value, (str, int, float, bool)):
                    mlflow.log_param(metadata_key, metadata_value)

            if feature_columns:
                mlflow.log_param("feature_columns", ",".join(feature_columns))

            for coefficient_name, coefficient_value in coefficients.items():
                if isinstance(coefficient_value, (int, float)):
                    mlflow.log_metric(f"coef_{coefficient_name}", coefficient_value)

            # The full metadata (feature list, drift baseline, data fingerprint) travels
            # with the model so serving, evaluation and drift checks can all read it.
            mlflow.log_dict(
                {**metadata, "model_name": model_name, "run_id": run.info.run_id},
                "metadata.json",
            )

            model_info = mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="model"
            )

            registered_model = mlflow.register_model(
                model_uri=model_info.model_uri,
                name=model_name
            )

            return f"models:/{model_name}/{registered_model.version}"

    def set_alias(self, model_name: str, alias: str, version: str | int) -> None:
        MlflowClient().set_registered_model_alias(model_name, alias, str(version))

    def get_alias_version(self, model_name: str, alias: str) -> str | None:
        try:
            return str(MlflowClient().get_model_version_by_alias(model_name, alias).version)
        except Exception:
            return None

    def promote(self, model_name: str, version: str | int) -> str | None:
        """Point "champion" at a new version. Returns the version it pointed to before,
        which is what a rollback goes back to."""
        previous = self.get_alias_version(model_name, CHAMPION_ALIAS)
        if previous is not None and previous != str(version):
            self.set_alias(model_name, PREVIOUS_CHAMPION_ALIAS, previous)
        self.set_alias(model_name, CHAMPION_ALIAS, version)
        return previous

    def rollback(self, model_name: str) -> str:
        """Point "champion" back at the version it replaced."""
        previous = self.get_alias_version(model_name, PREVIOUS_CHAMPION_ALIAS)
        if previous is None:
            raise RuntimeError(f"No previous champion recorded for {model_name}")
        current = self.get_alias_version(model_name, CHAMPION_ALIAS)
        self.set_alias(model_name, CHAMPION_ALIAS, previous)
        if current is not None:
            self.set_alias(model_name, PREVIOUS_CHAMPION_ALIAS, current)
        return previous
