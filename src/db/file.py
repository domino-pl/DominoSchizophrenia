# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Database schema for file metadata and content identifiers."""

from typing import override

from src.db.storage import SchizophreniaDBStorage


class SchizophreniaDBFile(SchizophreniaDBStorage):
    """Store file locations, metadata, and shared content identifiers.

    Part of the database schema inheritance chain.
    """

    @override
    def _init_db(self) -> None:
        """Create file tables and indexes after initializing the base schema."""
        super()._init_db()

        sqls = [
            # Identify content by its size and both checksums.
            """
            CREATE TABLE IF NOT EXISTS uids
            (
                uid    INTEGER PRIMARY KEY AUTOINCREMENT,
                size   INTEGER NOT NULL,
                md5    TEXT    NOT NULL,
                sha256 TEXT    NOT NULL
            )
            """,
            # Allow only one UID to each combination of size and checksums.
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
                uids_uidx_size_md5_sha256 ON uids (size, md5, sha256)
            """,
            # Record each file's relative path and metadata within its storage.
            """
            CREATE TABLE IF NOT EXISTS file
            (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                storage_id INTEGER NOT NULL,
                rel_path   TEXT    NOT NULL,
                uid        INTEGER NOT NULL,
                size       INTEGER NOT NULL,
                mtime      TEXT    NOT NULL,
                md5        TEXT    NOT NULL,
                sha256     TEXT    NOT NULL
            )
            """,
            # A relative path must be unique within a storage.
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
                file_unique_idx_storage_path ON file (storage_id, rel_path)
            """,
            # Speed up lookups of files sharing the same content identifier.
            """
            CREATE INDEX IF NOT EXISTS idx_file_uid ON file(uid)
            """,
        ]
        self.execute_list(sqls)
