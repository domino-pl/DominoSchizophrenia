# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Manage UUID-based storage identities and their on-disk representation."""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import override

IDENTITY_FILE_NAME = "schizophrenia.identity"


class Identity:
    """Represent a storage identity backed by a UUID."""

    def __init__(self, identity_str: str) -> None:
        """Initialize the identity from a valid UUID string."""
        self.uuid = uuid.UUID(identity_str)

    @override
    def __eq__(self, other: object) -> bool:
        """Compare UUIDs with another object exposing an `uuid` attribute.

        Returns:
            Whether the UUIDs are equal.

        """
        return self.uuid == other.__getattribute__("uuid")

    @classmethod
    def get_file_name(cls) -> str:
        """Return the filename used to persist storage identities.

        Returns:
            The identity filename shared by all storage directories.

        """
        return IDENTITY_FILE_NAME

    @staticmethod
    def get_real_path(storage_path: Path) -> Path:
        """Return the identity file path inside the storage directory.

        Args:
            storage_path: Directory containing the identity file.

        Returns:
            The storage path joined with the identity filename.

        """
        return storage_path / IDENTITY_FILE_NAME

    def to_str(self) -> str:
        """Return the UUID in its canonical string representation.

        Returns:
            The UUID formatted with hyphens.

        """
        return str(self.uuid)

    @staticmethod
    def _from_str(identity_str: str) -> Identity:
        """Parse an identity from a UUID string.

        Args:
            identity_str: UUID string to parse.

        Returns:
            An identity containing the parsed UUID.

        """
        return Identity(identity_str)

    @staticmethod
    def new() -> Identity:
        """Create an identity with a randomly generated version 4 UUID.

        Returns:
            A new storage identity.

        """
        return Identity(str(uuid.uuid4()))

    def to_db(self) -> str:
        """Return the UUID string to store in the database.

        Returns:
            The canonical UUID string.

        """
        return self.to_str()

    @staticmethod
    def from_db(identity_str: str) -> Identity:
        """Restore an identity from a UUID string stored in the database.

        Args:
            identity_str: UUID string read from the database.

        Returns:
            The restored storage identity.

        """
        return Identity._from_str(identity_str)

    def save_to_storage_dir(self, storage_abs_path: Path) -> None:
        """Write the identity to an existing storage directory.

        Args:
            storage_abs_path: Directory in which to create the identity file.

        Raises:
            FileNotFoundError: If the storage path is not an existing directory.
            FileExistsError: If an identity file already exists.

        """
        if not storage_abs_path.is_dir():
            raise FileNotFoundError(storage_abs_path)

        file_path = self.get_real_path(storage_abs_path)
        if file_path.exists():
            raise FileExistsError(file_path)

        Path(file_path).write_text(self.to_str(), encoding="utf-8")

    @staticmethod
    def read_from_storage_dir(storage_path: Path) -> Identity:
        """Read an identity from a storage directory.

        Args:
            storage_path: Directory containing the identity file.

        Returns:
            The identity parsed from the file after stripping whitespace.

        Raises:
            FileNotFoundError: If the identity file does not exist.

        """
        file_path = Identity.get_real_path(storage_path)
        if not file_path.exists():
            raise FileNotFoundError(file_path)

        file_content = Path(file_path).read_text(encoding="utf-8")
        return Identity._from_str(file_content.strip())
