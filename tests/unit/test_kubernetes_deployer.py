import json

from src.MetricsAndMonitoring.team_regular_reason_win_percentage_prometheus_grafana_monitoring_service import (
    PrometheusGrafanaMonitoringService,
)
from src.ModelTrainingAndDeployment.InferenceServices.inference_service_spec import InferenceServiceSpec
from src.ModelTrainingAndDeployment.ModelDeployment.KuberentesDeployer import KubernetesModelDeployer


def _deploy(tmp_path, **kwargs):
    deployer = KubernetesModelDeployer(namespace="ml", manifests_directory=str(tmp_path), apply_manifests=False)
    spec = InferenceServiceSpec(image_uri="registry.example.com/win-model:1.0", environment={"LOG_LEVEL": "info"})
    return deployer.deploy(
        model_uri="/models/win/v3",
        model_name="team_win_model",
        inference_service=spec,
        monitoring_service=PrometheusGrafanaMonitoringService(),
        **kwargs,
    )


def _load(path):
    return json.loads(open(path, encoding="utf-8").read())


def test_deployment_manifest_has_probes_env_and_scrape_annotations(tmp_path):
    files = _deploy(tmp_path, replicas=2)
    deployment = _load(files["deployment_manifest"])

    assert deployment["metadata"]["namespace"] == "ml"
    assert deployment["spec"]["replicas"] == 2

    pod = deployment["spec"]["template"]
    assert pod["metadata"]["annotations"]["prometheus.io/scrape"] == "true"
    assert pod["metadata"]["annotations"]["prometheus.io/path"] == "/metrics"

    container = pod["spec"]["containers"][0]
    assert container["image"] == "registry.example.com/win-model:1.0"
    assert container["readinessProbe"]["httpGet"]["path"] == "/health"
    assert container["livenessProbe"]["httpGet"]["path"] == "/health"

    env = {item["name"]: item["value"] for item in container["env"]}
    assert env["MODEL_URI"] == "/models/win/v3"
    assert env["MODEL_NAME"] == "team_win_model"
    assert env["LOG_LEVEL"] == "info"


def test_service_manifest_points_at_the_container_port(tmp_path):
    files = _deploy(tmp_path)
    service = _load(files["service_manifest"])

    assert service["spec"]["ports"][0]["targetPort"] == 8000
    assert service["spec"]["selector"] == {"app": "team-win-model"}


def test_grafana_dashboard_configmap_is_written(tmp_path):
    files = _deploy(tmp_path)
    dashboard = _load(files["grafana_dashboard_manifest"])

    assert dashboard["kind"] == "ConfigMap"
    assert dashboard["metadata"]["labels"]["grafana_dashboard"] == "1"
