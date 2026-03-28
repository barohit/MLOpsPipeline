from abc import ABC, abstractmethod

class BaseModelRegistry(ABC):
    @abstractmethod
    def register_model(self, model, model_name: str, metadata: dict) -> str:
        pass