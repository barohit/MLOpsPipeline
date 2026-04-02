import mlflow
import mlflow.sklearn

from ModelTraining.base.base_model_registry import BaseModelRegistry


#Note: MLflow is an open source Machine Learning modle registry
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

            model_info = mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="model"
            )

            registered_model = mlflow.register_model(
                model_uri=model_info.model_uri,
                name=model_name
            )

            return f"models:/{model_name}/{registered_model.version}"