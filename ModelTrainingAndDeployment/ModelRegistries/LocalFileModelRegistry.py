import json
import pickle
from datetime import datetime
from pathlib import Path

from ModelTraining.base.base_model_registry import BaseModelRegistry


class LocalFileModelRegistry(BaseModelRegistry):
    def __init__(self, base_directory: str):
        self.base_directory = Path(base_directory)

    def register_model(self, model, model_name: str, metadata: dict) -> str:
        model_root_directory = self.base_directory / model_name
        model_root_directory.mkdir(parents=True, exist_ok=True)

        version = self._get_next_version(model_root_directory)
        version_directory = model_root_directory / f"v{version}"
        version_directory.mkdir(parents=True, exist_ok=True)

        model_file_path = version_directory / "model.pkl"
        metadata_file_path = version_directory / "metadata.json"

        with open(model_file_path, "wb") as model_file:
            pickle.dump(model, model_file)

        metadata_to_write = {
            **metadata,
            "model_name": model_name,
            "version": version,
            "registered_at": datetime.utcnow().isoformat()
        }

        with open(metadata_file_path, "w", encoding="utf-8") as metadata_file:
            json.dump(metadata_to_write, metadata_file, indent=4)

        return str(version_directory)

    def _get_next_version(self, model_root_directory: Path) -> int:
        existing_versions = []

        for child in model_root_directory.iterdir():
            if child.is_dir() and child.name.startswith("v"):
                version_suffix = child.name[1:]

                if version_suffix.isdigit():
                    existing_versions.append(int(version_suffix))

        if not existing_versions:
            return 1

        return max(existing_versions) + 1