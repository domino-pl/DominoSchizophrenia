import logging
import sqlite3
from typing import Iterable

log = logging.getLogger(__name__)


class SchizophreniaDBBase:
    def __init__(self):
        self.conn = sqlite3.connect("schizophrenia.db")
        self._init_db()

    def close(self):
        self.conn.close()

    def _init_db(self):
        pass

    def commit(self) -> None:
        self.conn.commit()
        log.debug("Commiting changes.")

    def _assert_inserted(self, cursor: sqlite3.Cursor) -> int:
        row_id = cursor.lastrowid
        if row_id is None:
            raise RuntimeError("Insert failed")
        return row_id

    def execute_list(self, sqls: Iterable[str]) -> None:
        for sql in sqls:
            self.conn.execute(sql)
