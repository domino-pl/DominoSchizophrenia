# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Expose the SQLite database API for storage synchronization.

SchizophreniaDBBase manages connections. The storage, file, and scan layers
manage registered locations, file records, and scan results. SchizophreniaDBSync
uses the temporary tmp_sync_calc table to calculate synchronization actions.
"""

from src.db.sync import SchizophreniaDBSync


class SchizophreniaDB(SchizophreniaDBSync):  # ruff: ignore[non-empty-init-module]
    """Provide access to storage, file, scan, and synchronization operations."""
