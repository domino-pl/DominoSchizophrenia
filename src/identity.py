from __future__ import annotations

import uuid
from pathlib import Path

IDENTITY_FILE_NAME = "schizophrenia.identity"


class Identity:
    def __init__(self, identity_str: str) -> None:
        self.uuid = uuid.UUID(identity_str)

    def __eq__(self, other: object) -> bool:
        return self.uuid == other.__getattribute__("uuid")

    def get_file_name(self) -> str:
        return IDENTITY_FILE_NAME

    @staticmethod
    def get_real_path(storage_path: Path) -> Path:
        return storage_path / IDENTITY_FILE_NAME

    def _to_str(self) -> str:
        return str(self.uuid)

    @staticmethod
    def _from_str(identity_str: str) -> Identity:
        return Identity(identity_str)

    @staticmethod
    def new() -> Identity:
        return Identity(str(uuid.uuid4()))

    def to_db(self) -> str:
        return self._to_str()

    @staticmethod
    def from_db(identity_str: str) -> Identity:
        return Identity._from_str(identity_str)

    def save_to_storage_dir(self, storage_abs_path: Path) -> None:
        if not storage_abs_path.is_dir():
            raise FileNotFoundError(storage_abs_path)

        file_path = self.get_real_path(storage_abs_path)
        if file_path.exists():
            raise FileExistsError(file_path)

        with open(file_path, "w") as f:
            f.write(self._to_str())

    @staticmethod
    def read_from_storage_dir(storage_path: Path) -> Identity:
        file_path = Identity.get_real_path(storage_path)
        if not file_path.exists():
            raise FileNotFoundError(file_path)

        with open(file_path, "r") as f:
            return Identity._from_str(f.read().strip())
