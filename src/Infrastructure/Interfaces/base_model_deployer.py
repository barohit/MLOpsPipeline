from abc import ABC, abstractmethod

class BaseModelDeployer(ABC):
    @abstractmethod
    def deploy(self, model_uri: str, model_name: str) -> None:
        pass