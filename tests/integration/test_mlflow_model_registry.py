import mlflow
import mlflow.sklearn
from sklearn.linear_model import LinearRegression

from src.ModelTrainingAndDeployment.ModelRegistries.MLFlowModelRegistry import MLflowModelRegistry


def test_registers_and_reloads_a_model_and_versions_increment(tmp_path, monkeypatch):
    # MLflow no longer allows the plain ./mlruns file store by default, so use SQLite.
    # chdir keeps the artifact folder MLflow creates inside the temp directory.
    monkeypatch.chdir(tmp_path)
    registry = MLflowModelRegistry(
        tracking_uri=f"sqlite:///{tmp_path}/mlflow.db",
        experiment_name="registry-test",
    )

    model = LinearRegression().fit([[1.0], [2.0], [3.0]], [2.0, 4.0, 6.0])
    metadata = {
        "metrics": {"mae": 0.1},
        "feature_columns": ["x"],
        "coefficients": {"x": 2.0},
        "model_type": "linear_regression",
    }

    first_uri = registry.register_model(model, "demo_model", metadata)
    second_uri = registry.register_model(model, "demo_model", metadata)

    assert first_uri == "models:/demo_model/1"
    assert second_uri == "models:/demo_model/2"
    assert round(float(mlflow.sklearn.load_model(first_uri).predict([[5.0]])[0]), 4) == 10.0
