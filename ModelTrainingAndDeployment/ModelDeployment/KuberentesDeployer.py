import json
import subprocess
from pathlib import Path

from ModelTraining.base.base_model_deployer import BaseModelDeployer


class KubernetesModelDeployer(BaseModelDeployer):
    def __init__(
        self,
        namespace: str = "default",
        manifests_directory: str = "./k8s_manifests",
        apply_manifests: bool = False,
    ):
        self.namespace = namespace
        self.manifests_directory = Path(manifests_directory)
        self.apply_manifests = apply_manifests

    def deploy(
        self,
        model_uri: str,
        model_name: str,
        inference_service,
        monitoring_service=None,
        replicas: int = 1,
    ) -> dict[str, str]:
        deployment_name = self._sanitize_name(model_name)
        service_name = f"{deployment_name}-service"

        self.manifests_directory.mkdir(parents=True, exist_ok=True)

        deployment_manifest = self._build_deployment_manifest(
            deployment_name=deployment_name,
            model_name=model_name,
            model_uri=model_uri,
            inference_service=inference_service,
            monitoring_service=monitoring_service,
            replicas=replicas,
        )

        service_manifest = self._build_service_manifest(
            service_name=service_name,
            deployment_name=deployment_name,
            inference_service=inference_service,
        )

        deployment_file_path = self.manifests_directory / f"{deployment_name}_deployment.json"
        service_file_path = self.manifests_directory / f"{deployment_name}_service.json"

        with open(deployment_file_path, "w", encoding="utf-8") as deployment_file:
            json.dump(deployment_manifest, deployment_file, indent=4)

        with open(service_file_path, "w", encoding="utf-8") as service_file:
            json.dump(service_manifest, service_file, indent=4)

        output = {
            "deployment_manifest": str(deployment_file_path),
            "service_manifest": str(service_file_path),
        }

        if monitoring_service is not None:
            dashboard_manifest = monitoring_service.build_grafana_dashboard_configmap(
                model_name=model_name,
                namespace=self.namespace,
            )
            dashboard_file_path = self.manifests_directory / f"{deployment_name}_grafana_dashboard.json"

            with open(dashboard_file_path, "w", encoding="utf-8") as dashboard_file:
                json.dump(dashboard_manifest, dashboard_file, indent=4)

            output["grafana_dashboard_manifest"] = str(dashboard_file_path)

        if self.apply_manifests:
            self._apply_manifest(deployment_file_path)
            self._apply_manifest(service_file_path)

            if monitoring_service is not None:
                self._apply_manifest(Path(output["grafana_dashboard_manifest"]))

        return output

    def _build_deployment_manifest(
        self,
        deployment_name: str,
        model_name: str,
        model_uri: str,
        inference_service,
        monitoring_service,
        replicas: int,
    ) -> dict:
        pod_annotations = {}
        if monitoring_service is not None:
            pod_annotations.update(
                monitoring_service.get_pod_annotations(
                    metrics_path=inference_service.metrics_path,
                    metrics_port=inference_service.container_port,
                )
            )

        env = [
            {"name": "MODEL_NAME", "value": model_name},
            {"name": "MODEL_URI", "value": model_uri},
        ]

        for env_name, env_value in inference_service.get_environment_variables(model_name, model_uri).items():
            env.append({"name": env_name, "value": str(env_value)})

        return {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": deployment_name,
                "namespace": self.namespace,
                "labels": {"app": deployment_name},
            },
            "spec": {
                "replicas": replicas,
                "selector": {"matchLabels": {"app": deployment_name}},
                "template": {
                    "metadata": {
                        "labels": {"app": deployment_name},
                        "annotations": pod_annotations,
                    },
                    "spec": {
                        "containers": [
                            {
                                "name": deployment_name,
                                "image": inference_service.image_uri,
                                "ports": [{"containerPort": inference_service.container_port}],
                                "env": env,
                                "readinessProbe": {
                                    "httpGet": {
                                        "path": inference_service.health_path,
                                        "port": inference_service.container_port,
                                    },
                                    "initialDelaySeconds": 5,
                                    "periodSeconds": 10,
                                },
                                "livenessProbe": {
                                    "httpGet": {
                                        "path": inference_service.health_path,
                                        "port": inference_service.container_port,
                                    },
                                    "initialDelaySeconds": 10,
                                    "periodSeconds": 15,
                                },
                            }
                        ]
                    },
                },
            },
        }

    def _build_service_manifest(
        self,
        service_name: str,
        deployment_name: str,
        inference_service,
    ) -> dict:
        return {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": service_name,
                "namespace": self.namespace,
            },
            "spec": {
                "selector": {"app": deployment_name},
                "ports": [
                    {
                        "protocol": "TCP",
                        "port": 80,
                        "targetPort": inference_service.container_port,
                    }
                ],
                "type": "ClusterIP",
            },
        }

    def _apply_manifest(self, manifest_path: Path) -> None:
        subprocess.run(
            ["kubectl", "apply", "-f", str(manifest_path)],
            check=True,
        )

    def _sanitize_name(self, name: str) -> str:
        return name.lower().replace("_", "-")