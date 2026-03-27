from abc import ABC, abstractmethod

class BaseCredentialProvider(ABC):
    @abstractmethod
    def get_credentials(self) -> dict:
        pass