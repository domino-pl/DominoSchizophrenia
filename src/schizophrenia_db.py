import logging
import sqlite3
from enum import IntEnum
from pathlib import Path
from typing import NamedTuple

from src.identity import Identity

log = logging.getLogger(__name__)


class ListItem(NamedTuple):
    rel_path: Path
    size: int
    mtime: str


class Storage(NamedTuple):
    id: int
    name: str
    abs_path: Path
    identity: Identity


class ScanStep(IntEnum):
    INSERT = 0
    LIST = 1
    MATCH = 2
    CHECKSUM = 3
    COMPARE = 4
    APPLY = 5
    ABORT = 255


class FileStatus(IntEnum):
    UNKNOWN = 0
    NEW = 1
    REMOVED = 2
    META_CHANGED = 3
    CONTENT_CHANGED = 4
    NOT_CHANGED = 5
    REMOVE = 6


class FileToChecksumCalculation(NamedTuple):
    item_id: int
    rel_path: Path


class Scan(NamedTuple):
    id: int
    storage_id: int
    finished_step: ScanStep


class CheckSums(NamedTuple):
    item_id: int
    md5: str
    sha256: str


class SchizophreniaDB:
    def __init__(self):
        self.conn = sqlite3.connect("schizophrenia.db")
        self._init_db()

    def close(self):
        self.conn.close()

    def _init_db(self):
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
            """
            CREATE TABLE IF NOT EXISTS file
            (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                storage_id INTEGER NOT NULL,
                rel_path   TEXT    NOT NULL,
                size       INTEGER NOT NULL,
                mtime      INTEGER NOT NULL,
                md5        TEXT    NOT NULL,
                sha256     TEXT    NOT NULL
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS file_unique_idx_storage_path ON file(storage_id, rel_path)
            """,
            """
            CREATE TABLE IF NOT EXISTS scan
            (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                storage_id    INTEGER NOT NULL,
                finished_step INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS scan_item
            (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id  INTEGER NOT NULL,
                file_id  INTEGER NULL,
                rel_path TEXT    NOT NULL,
                size     INTEGER NOT NULL,
                mtime    TEXT    NOT NULL,
                status   INTEGER NOT NULL,
                md5      TEXT    NULL,
                sha256   TEXT    NULL
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS scan_item_unique_idx_scan_path ON scan_item (scan_id, rel_path)
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS scan_item_unique_idx_scan_file_id ON scan_item (scan_id, file_id)
            """,
        ]
        for sql in sqls:
            self.conn.execute(sql)

    def commit(self) -> None:
        self.conn.commit()
        log.debug("Commiting changes.")

    def _assert_inserted(self, cursor: sqlite3.Cursor) -> int:
        row_id = cursor.lastrowid
        if row_id is None:
            raise RuntimeError("Insert failed")
        return row_id

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
        if not storage.abs_path.is_absolute():
            raise RuntimeError("Absolute path not allowed for storage configuration.")
        return storage

    def insert_storage(self, name: str, abs_path: Path, identity: Identity) -> Storage:
        sql = "INSERT INTO storage(name, abs_path, identity) VALUES (?, ?, ?)"
        self.conn.execute(sql, (name, str(abs_path), identity.to_db()))
        self.conn.commit()
        return self.select_storage(name)

    def select_scan(self, scan_id: int) -> Scan:
        sql = "SELECT id, storage_id, finished_step FROM scan WHERE id = ?"
        cursor = self.conn.execute(sql, (scan_id,))
        row = cursor.fetchone()
        return Scan(
            id=int(row[0]),
            storage_id=int(row[1]),
            finished_step=ScanStep(row[2]),
        )

    def select_last_storage_scan(self, storage_id: int) -> Scan | None:
        # TODO ORDER BY TIME
        sql = "SELECT id FROM scan WHERE storage_id = ? ORDER BY id DESC LIMIT 1"
        cursor = self.conn.execute(sql, (storage_id,))
        return self.select_scan(cursor.fetchone()[0]) if cursor.rowcount else None

    def insert_scan(self, storage_id: int) -> Scan:
        sql = "INSERT INTO scan(storage_id, finished_step) VALUES (?, ?)"
        cursor = self.conn.execute(sql, (storage_id, int(ScanStep.INSERT)))
        self.conn.commit()
        scan_id = self._assert_inserted(cursor)
        return self.select_scan(scan_id)

    def delete_all_storage_scan_items(self, storage_id: int) -> None:
        sql = "DELETE FROM scan_item WHERE scan_id IN (SELECT id FROM scan WHERE storage_id = ?)"
        cursor = self.conn.execute(sql, (storage_id,))
        log.debug(f"Removed {cursor.rowcount} old scan items.")

    def insert_scan_items(self, scan: Scan, list_items: list[ListItem]) -> None:
        sql = "INSERT INTO scan_item(scan_id, rel_path, size, mtime, status) VALUES (?, ?, ? ,?, ?)"
        self.conn.executemany(
            sql,
            [
                (
                    scan.id,
                    str(item.rel_path),
                    item.size,
                    item.mtime,
                    int(FileStatus.UNKNOWN),
                )
                for item in list_items
            ],
        )

    def match_scan_files(self, scan: Scan):
        log.debug("Match already known files.")
        sql = """
              UPDATE scan_item
              SET file_id=(SELECT file.id FROM file WHERE file.storage_id = ? AND file.rel_path = scan_item.rel_path)
              WHERE scan_item.scan_id = ?
              """
        self.conn.execute(sql, (scan.storage_id, scan.id))
        # TODO: Print count.

        log.debug("Mark new files.")
        sql = """
              UPDATE scan_item
              SET status=?
              WHERE scan_item.scan_id = ?
                AND file_id IS NULL
              """
        cursor = self.conn.execute(sql, (int(FileStatus.NEW), scan.id))
        log.debug(f"Marked {cursor.rowcount} new files.")

        log.debug("Looking for deleted files.")
        sql = """
              INSERT INTO scan_item(scan_id, file_id, rel_path, size, mtime, md5, sha256, status)
              SELECT ?,
                     id,
                     rel_path,
                     size,
                     mtime,
                     md5,
                     sha256,
                     ?
              FROM file
              WHERE storage_id = ?
                AND id NOT IN (SELECT file_id FROM scan_item WHERE scan_id = ? AND file_id IS NOT NULL)
              """
        cursor = self.conn.execute(
            sql, (scan.id, int(FileStatus.REMOVED), scan.storage_id, scan.id)
        )
        log.debug(f"Found {cursor.rowcount} deleted files.")

        log.debug("Mark not modified files.")
        sql = """
              UPDATE scan_item
              SET status=?
              WHERE scan_id = ?
                AND id IN (SELECT scan_item.id
                           FROM scan_item
                                    JOIN file ON file.id = scan_item.file_id
                           WHERE scan_item.scan_id = ?
                             AND scan_item.status = ?
                             AND scan_item.size = file.size
                             AND scan_item.mtime = file.mtime)
              """
        cursor = self.conn.execute(
            sql,
            (int(FileStatus.NOT_CHANGED), scan.id, scan.id, int(FileStatus.UNKNOWN)),
        )
        log.debug(f"Found {cursor.rowcount} not modified files.")

    def select_files_for_checksum_calculation(
        self, scan: Scan, limit: int = 1000
    ) -> list[FileToChecksumCalculation]:
        sql = """
              SELECT id, rel_path
              FROM scan_item
              WHERE scan_id = ?
                AND status IN (?,?)
                AND (md5 is NULL OR sha256 is NULL)
              LIMIT ?
              """
        cursor = self.conn.execute(
            sql, (scan.id, int(FileStatus.NEW), int(FileStatus.UNKNOWN), limit)
        )
        return [
            FileToChecksumCalculation(item_id=row[0], rel_path=row[1])
            for row in cursor.fetchall()
        ]

    def update_scan_items_sum(self, checksums: list[CheckSums]):
        sql = """
              UPDATE scan_item
              SET md5=?,
                  sha256=?
              WHERE id = ? \
              """
        self.conn.executemany(
            sql, [(item.md5, item.sha256, item.item_id) for item in checksums]
        )

    def update_scan_items_content_changed(self, scan: Scan):
        sql = """
              UPDATE scan_item
              SET status=?
              WHERE id IN (SELECT scan_item.id
                           FROM scan_item
                                    JOIN file ON file.id = scan_item.file_id
                           WHERE scan_item.scan_id = ?
                             AND scan_item.status = ?
                             AND (scan_item.size != file.size OR scan_item.md5 != file.md5 OR
                                  scan_item.sha256 != file.sha256))
              """
        self.conn.execute(
            sql, (int(FileStatus.CONTENT_CHANGED), scan.id, int(FileStatus.UNKNOWN))
        )

        sql = "UPDATE scan_item SET status=? WHERE scan_id = ? AND status = ?"
        self.conn.execute(
            sql, (int(FileStatus.META_CHANGED), scan.id, int(FileStatus.UNKNOWN))
        )

    def update_scan_step(self, scan_id: int, new_step: ScanStep) -> Scan:
        sql = "UPDATE scan SET finished_step=? WHERE id = ?"
        self.conn.execute(sql, (int(new_step), scan_id))
        return self.select_scan(scan_id)

