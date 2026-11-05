# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Provide shared SQLite connection management and database helpers."""

import logging
import sqlite3
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

log = logging.getLogger(__name__)


class MissingLastRowIdError(RuntimeError):
    """Indicate that a database operation did not provide an expected row ID."""


class SchizophreniaDBBase:
    """Manage a connection to the application database."""

    def __init__(self) -> None:
        """Open the application database and run the initialization hook."""
        self.conn = sqlite3.connect("schizophrenia.db")
        self._init_db()

    def close(self) -> None:
        """Close the database connection without committing pending changes."""
        self.conn.close()

    def _init_db(self) -> None:
        """Provide a hook for subclasses to initialize the database schema."""

    def commit(self) -> None:
        """Commit the current transaction and log the operation."""
        self.conn.commit()
        log.debug("Commiting changes.")

    @staticmethod
    def _assert_inserted(cursor: sqlite3.Cursor) -> int:
        """Return the last inserted row ID when one is available.

        Args:
            cursor: Cursor whose last inserted row ID should be checked.

        Returns:
            The row ID reported by the cursor.

        Raises:
            MissingLastRowIdError: If the cursor does not provide a row ID.

        """
        row_id = cursor.lastrowid
        if row_id is None:
            msg = "Expected an inserted row ID, but cursor.lastrowid is None"
            raise MissingLastRowIdError(msg)
        return row_id

    def execute_list(self, sqls: Iterable[str]) -> None:
        """Execute SQL statements in order without committing them.

        Args:
            sqls: SQL statements to execute on the database connection.

        """
        for sql in sqls:
            self.conn.execute(sql)
