from dataclasses import dataclass, field


@dataclass
class InferenceServiceSpec:
    """What the Kubernetes deployer needs to know about a serving container."""

    image_uri: str
    container_port: int = 8000
    health_path: str = "/health"
    metrics_path: str = "/metrics"
    environment: dict[str, str] = field(default_factory=dict)

    def get_environment_variables(self, model_name: str, model_uri: str) -> dict[str, str]:
        # MODEL_NAME and MODEL_URI are added by the deployer itself; this is for anything extra.
        return dict(self.environment)
