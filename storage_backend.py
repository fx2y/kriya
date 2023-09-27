import json
import os
from abc import ABC, abstractmethod


class StorageBackend(ABC):
    @abstractmethod
    def read_object(self, object_key: str) -> bytes:
        pass

    @abstractmethod
    def write_object(self, object_key: str, object_data: bytes) -> None:
        pass

    @abstractmethod
    def delete_object(self, object_key: str) -> None:
        pass

    @abstractmethod
    def object_exists(self, object_key: str) -> bool:
        pass

    @abstractmethod
    def read_metadata(self, object_key: str, metadata_key: str) -> str:
        pass


class DiskStorageBackend(StorageBackend):
    def __init__(self, base_path: str):
        self.base_path = base_path

    def read_object(self, object_key: str) -> bytes:
        with open(self._get_path(object_key), "rb") as f:
            return f.read()

    def write_object(self, object_key: str, object_data: bytes) -> None:
        with open(self._get_path(object_key), "wb") as f:
            f.write(object_data)

    def delete_object(self, object_key: str) -> None:
        path = self._get_path(object_key)
        if os.path.exists(path):
            os.remove(path)

    def object_exists(self, object_key: str) -> bool:
        return os.path.exists(self._get_path(object_key))

    def read_metadata(self, object_key: str, metadata_key: str) -> str:
        path = self._get_path(object_key) + ".metadata"
        if os.path.exists(path):
            with open(path, "r") as f:
                metadata = json.load(f)
                return metadata.get(metadata_key, "")
        else:
            return ""

    def _get_path(self, object_key: str) -> str:
        return os.path.join(self.base_path, object_key)


class StorageManager:
    def __init__(self, backend: str, base_path: str):
        if backend == "disk":
            self.storage_backend = DiskStorageBackend(base_path)
        else:
            raise ValueError("Unsupported backend")

    def read_object(self, object_key: str) -> bytes:
        return self.storage_backend.read_object(object_key)

    def write_object(self, object_key: str, object_data: bytes) -> None:
        self.storage_backend.write_object(object_key, object_data)

    def delete_object(self, object_key: str) -> None:
        self.storage_backend.delete_object(object_key)

    def object_exists(self, object_key: str) -> bool:
        return self.storage_backend.object_exists(object_key)
