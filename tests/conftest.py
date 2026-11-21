# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Provide an isolated database with the application schema."""

import sqlite3
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

from src.db import SchizophreniaDB

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def empty_db() -> Iterator[SchizophreniaDB]:
    """Open the application database on a private in-memory connection.

    Yields:
        An empty database with the production schema.

    """
    connection = sqlite3.connect(":memory:")
    with patch("src.db.base.sqlite3.connect", return_value=connection):
        db = SchizophreniaDB()
    try:
        yield db
    finally:
        db.close()
