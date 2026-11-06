# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Database schema and operations for configured storage locations."""

import logging
from pathlib import Path
from typing import NamedTuple, override

from src.db.base import SchizophreniaDBBase
from src.identity import Identity

log = logging.getLogger(__name__)


class Storage(NamedTuple):
    """A storage record with its filesystem path and decoded identity."""

    id: int
    name: str
    abs_path: Path
    identity: Identity


class SchizophreniaDBStorage(SchizophreniaDBBase):
    """Manage persistent storage configuration records.

    Part of the database inheritance chain.
    """

    @override
    def _init_db(self) -> None:
        """Create the storage table after initializing the base schema."""
        super()._init_db()

        sqls = [
            # Names, absolute paths, and identities each uniquely identify a storage.
            """
            CREATE TABLE IF NOT EXISTS storage
            (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                name     TEXT NOT NULL UNIQUE,
                abs_path TEXT NOT NULL UNIQUE,
                identity TEXT NOT NULL UNIQUE
            )
            """,
        ]
        self.execute_list(sqls)

    def select_storage(self, storage_name: str) -> Storage:
        """Fetch a storage by name and convert database values to domain types.

        Returns:
            The matching storage record with its path and identity decoded.

        """
        sql = "SELECT id, name, abs_path, identity FROM storage WHERE name = ?"
        cursor = self.conn.execute(sql, (storage_name,))
        row = cursor.fetchone()
        return Storage(
            id=int(row[0]),
            name=str(row[1]),
            abs_path=Path(row[2]),
            identity=Identity.from_db(row[3]),
        )

    def insert_storage(self, name: str, abs_path: Path, identity: Identity) -> Storage:
        """Persist a storage configuration and return the saved record.

        Returns:
            The saved storage record, including its database-generated ID.

        """
        sql = "INSERT INTO storage(name, abs_path, identity) VALUES (?, ?, ?)"
        # Serialize the path and identity into values supported by the database.
        self.conn.execute(sql, (name, str(abs_path), identity.to_db()))
        self.conn.commit()
        # Read back the generated ID along with the stored configuration.
        return self.select_storage(name)
