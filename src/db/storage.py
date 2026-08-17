import logging
from pathlib import Path
from typing import NamedTuple

from src.db.base import SchizophreniaDBBase
from src.identity import Identity

log = logging.getLogger(__name__)


class Storage(NamedTuple):
    id: int
    name: str
    abs_path: Path
    identity: Identity


class SchizophreniaDBStorage(SchizophreniaDBBase):
    def _init_db(self):
        super()._init_db()

        sqls = [
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
        sql = "SELECT id, name, abs_path, identity FROM storage WHERE name = ?"
        cursor = self.conn.execute(sql, (storage_name,))
        row = cursor.fetchone()
        storage = Storage(
            id=int(row[0]),
            name=str(row[1]),
            abs_path=Path(row[2]),
            identity=Identity.from_db(row[3]),
        )
        # if not storage.abs_path.is_absolute():
        #     raise RuntimeError("Absolute path not allowed for storage configuration.")
        return storage

    def insert_storage(self, name: str, abs_path: Path, identity: Identity) -> Storage:
        sql = "INSERT INTO storage(name, abs_path, identity) VALUES (?, ?, ?)"
        self.conn.execute(sql, (name, str(abs_path), identity.to_db()))
        self.conn.commit()
        return self.select_storage(name)
