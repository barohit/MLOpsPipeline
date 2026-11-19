"""The registry flow the cluster depends on: register, load by version and by alias,
promote, roll back. Real MLflow, SQLite backend, no server."""
from sklearn.linear_model import LinearRegression

from src.ModelTrainingAndDeployment.ModelRegistries.MLFlowModelRegistry import MLflowModelRegistry
from src.ModelTrainingAndDeployment.ModelRegistries.model_loader import load_model


def test_load_by_version_and_alias_then_promote_and_roll_back(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    registry = MLflowModelRegistry(tracking_uri=f"sqlite:///{tmp_path}/mlflow.db", experiment_name="aliases")

    metadata = {"feature_columns": ["x"], "metrics": {"rmse": 0.5}, "drift_baseline": {"bins": 2, "features": {}}}
    v1 = registry.register_model(LinearRegression().fit([[1.0], [2.0]], [2.0, 4.0]), "demo", metadata)
    v2 = registry.register_model(LinearRegression().fit([[1.0], [2.0]], [3.0, 6.0]), "demo", metadata)

    loaded = load_model(v2)
    assert loaded.version == "2"
    assert loaded.metadata["feature_columns"] == ["x"]
    assert "drift_baseline" in loaded.metadata

    assert registry.promote("demo", 1) is None
    assert load_model("models:/demo@champion").version == "1"

    assert registry.promote("demo", 2) == "1"
    champion = load_model("models:/demo@champion")
    assert champion.version == "2"
    assert champion.uri == "models:/demo/2"  # the alias is resolved to a concrete version
    assert round(float(champion.model.predict([[2.0]])[0]), 4) == 6.0

    assert registry.rollback("demo") == "1"
    assert load_model("models:/demo@champion").version == "1"
    assert v1 == "models:/demo/1"
