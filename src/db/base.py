# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Provide shared SQLite connection management and database helpers."""

import logging
import sqlite3
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

log = logging.getLogger(__name__)


class InsertVerificationError(RuntimeError):
    """Indicate that an expected single-row insert could not be verified."""


class FetchEmptyError(LookupError):
    """Indicate that an expected database row was not found."""


class SchizophreniaDBBase:
    """Provide the foundation for successive database subclasses.

    Share connection management and SQL helpers across the inheritance chain.
    Subclasses extend the schema through _init_db(), calling super()._init_db()
    before adding their own tables and indexes.
    """

    def __init__(self) -> None:
        """Open the application database and run the initialization hook."""
        self.conn = sqlite3.connect("schizophrenia.db")
        try:
            self._init_db()
        except BaseException:
            self.conn.close()
            raise

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
        """Check a single-row INSERT and return its row ID.

        Use immediately after an INSERT into a rowid table, without RETURNING.
        Cursor metadata cannot distinguish INSERT from UPDATE or DELETE, so
        callers must ensure that the cursor belongs to an INSERT operation.

        Args:
            cursor: Cursor returned by the single-row INSERT.

        Returns:
            The row ID reported by the cursor.

        Raises:
            InsertVerificationError: If exactly one affected row and a row ID
                are not reported by the cursor.

        """
        row_id = cursor.lastrowid
        if cursor.rowcount != 1 or row_id is None:
            msg = (
                "Expected one inserted row with a row ID; "
                f"got rowcount={cursor.rowcount}, lastrowid={row_id}"
            )
            raise InsertVerificationError(msg)
        return row_id

    def execute_list(self, sqls: Iterable[str]) -> None:
        """Execute SQL statements in order without committing them.

        Args:
            sqls: SQL statements to execute on the database connection.

        """
        for sql in sqls:
            self.conn.execute(sql)
