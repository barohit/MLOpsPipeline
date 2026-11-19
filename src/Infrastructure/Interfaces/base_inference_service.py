from abc import ABC, abstractmethod


class BaseInferenceService(ABC):
    """What every model-serving class must provide.

    It knows where the model is (model_uri), what goes in (input_columns),
    what comes out (output_name), how to load the model, and how to predict.
    The web layer (FastAPI) only talks to this interface, not to a specific model.
    """

    def __init__(self, model_uri: str):
        self.model_uri = model_uri
        self.model = None
        # Filled in by load(): which registered version is being served, and its metadata
        self.model_version = "unknown"
        self.metadata: dict = {}

    @property
    @abstractmethod
    def input_columns(self) -> list[str]:
        """The feature names the model expects, in order."""

    @property
    @abstractmethod
    def output_name(self) -> str:
        """The name of the value the model returns."""

    @abstractmethod
    def load(self) -> None:
        """Load the model from model_uri into memory. Called once at startup."""

    @abstractmethod
    def predict(self, features: dict[str, float]) -> dict[str, float]:
        """Take {feature_name: value} and return {output_name: prediction}."""

    @property
    def is_loaded(self) -> bool:
        return self.model is not None
