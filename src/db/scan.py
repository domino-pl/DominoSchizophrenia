import logging
from enum import IntEnum
from pathlib import PurePosixPath
from typing import NamedTuple, override

from src.db.file import SchizophreniaDBFile

log = logging.getLogger(__name__)


class ScanListItem(NamedTuple):
    rel_path: PurePosixPath
    size: int
    mtime: str


class ScanStep(IntEnum):
    INSERT = 0
    LIST = 1
    MATCH = 2
    CHECKSUM = 3
    COMPARE = 4
    ASSIGN_UID = 5
    APPLY = 6
    ABORT = 255


class ScanFileStatus(IntEnum):
    UNKNOWN = 0
    NEW = 1
    REMOVED = 2
    META_CHANGED = 3
    CONTENT_CHANGED = 4
    NOT_CHANGED = 5
    REMOVE = 6


class FileToChecksumCalculation(NamedTuple):
    item_id: int
    rel_path: PurePosixPath


class Scan(NamedTuple):
    id: int
    storage_id: int
    finished_step: ScanStep


class ScanItemCheckSums(NamedTuple):
    item_id: int
    md5: str
    sha256: str


class SchizophreniaDBScan(SchizophreniaDBFile):
    @override
    def _init_db(self):
        super()._init_db()

        sqls = [
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
                uid      INTEGER NULL,
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
        self.execute_list(sqls)

    def scan_select(self, scan_id: int) -> Scan:
        sql = "SELECT id, storage_id, finished_step FROM scan WHERE id = ?"
        cursor = self.conn.execute(sql, (scan_id,))
        row = cursor.fetchone()
        return Scan(
            id=int(row[0]),
            storage_id=int(row[1]),
            finished_step=ScanStep(row[2]),
        )

    def scan_select_last_for_storage(self, storage_id: int) -> Scan | None:
        # TODO ORDER BY TIME
        sql = "SELECT id FROM scan WHERE storage_id = ? ORDER BY id DESC LIMIT 1"
        cursor = self.conn.execute(sql, (storage_id,))
        row = cursor.fetchone()
        return self.scan_select(row[0]) if row else None

    def scan_insert(self, storage_id: int) -> Scan:
        sql = "INSERT INTO scan(storage_id, finished_step) VALUES (?, ?)"
        cursor = self.conn.execute(sql, (storage_id, int(ScanStep.INSERT)))
        self.conn.commit()
        scan_id = self._assert_inserted(cursor)
        return self.scan_select(scan_id)

    def scan_delete_all_storage_scan_items(self, storage_id: int) -> None:
        sql = "DELETE FROM scan_item WHERE scan_id IN (SELECT id FROM scan WHERE storage_id = ?)"
        cursor = self.conn.execute(sql, (storage_id,))
        log.debug(f"Removed {cursor.rowcount} old scan items.")

    def scan_insert_scan_items(
        self, scan: Scan, list_items: list[ScanListItem]
    ) -> None:
        sql = "INSERT INTO scan_item(scan_id, rel_path, size, mtime, status) VALUES (?, ?, ? ,?, ?)"
        self.conn.executemany(
            sql,
            [
                (
                    scan.id,
                    item.rel_path.as_posix(),
                    item.size,
                    item.mtime,
                    int(ScanFileStatus.UNKNOWN),
                )
                for item in list_items
            ],
        )

    def scan_match_files(self, scan: Scan):
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
        cursor = self.conn.execute(sql, (int(ScanFileStatus.NEW), scan.id))
        log.debug(f"Marked {cursor.rowcount} new files.")

        log.debug("Looking for deleted files.")
        sql = """
              INSERT INTO scan_item(scan_id, file_id, rel_path, uid, size, mtime, md5, sha256, status)
              SELECT ?,
                     id,
                     rel_path,
                     uid,
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
            sql, (scan.id, int(ScanFileStatus.REMOVED), scan.storage_id, scan.id)
        )
        log.debug(f"Found {cursor.rowcount} deleted files.")

        log.debug("Mark not modified files.")
        sql = """
              UPDATE scan_item
              SET status=?,
                  uid=file.uid
              FROM file
              WHERE scan_item.scan_id = ?
                AND scan_item.status = ?
                AND scan_item.file_id = file.id
                AND scan_item.size = file.size
                AND scan_item.mtime = file.mtime
              """
        cursor = self.conn.execute(
            sql,
            (int(ScanFileStatus.NOT_CHANGED), scan.id, int(ScanFileStatus.UNKNOWN)),
        )
        log.debug(f"Found {cursor.rowcount} not modified files.")

    def scan_select_files_for_checksum_calculation(
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
            sql, (scan.id, int(ScanFileStatus.NEW), int(ScanFileStatus.UNKNOWN), limit)
        )
        return [
            FileToChecksumCalculation(
                item_id=int(row[0]),
                rel_path=PurePosixPath(str(row[1])),
            )
            for row in cursor.fetchall()
        ]

    def scan_update_scan_items_sum(self, checksums: list[ScanItemCheckSums]):
        sql = """
              UPDATE scan_item
              SET md5=?,
                  sha256=?
              WHERE id = ? \
              """
        self.conn.executemany(
            sql, [(item.md5, item.sha256, item.item_id) for item in checksums]
        )

    def scan_update_scan_items_content_changed(self, scan: Scan):
        sql = """
              UPDATE scan_item
              SET status=?,
                  uid=file.uid
              FROM file
              WHERE scan_item.scan_id = ?
                AND scan_item.status = ?
                AND scan_item.file_id = file.id
                AND scan_item.size = file.size
                AND scan_item.md5 = file.md5
                AND scan_item.sha256 = file.sha256
              """
        self.conn.execute(
            sql,
            (int(ScanFileStatus.META_CHANGED), scan.id, int(ScanFileStatus.UNKNOWN)),
        )

        sql = "UPDATE scan_item SET status=? WHERE scan_id = ? AND status = ?"
        self.conn.execute(
            sql,
            (int(ScanFileStatus.CONTENT_CHANGED), scan.id, int(ScanFileStatus.UNKNOWN)),
        )

    def scan_assign_scan_new_uid(self, scan_id: int):
        sql = """
              INSERT OR IGNORE INTO uids(size, md5, sha256)
              SELECT size, md5, sha256
              FROM scan_item
              WHERE scan_id = ?
                AND uid IS NULL
              """
        self.conn.execute(sql, (scan_id,))

        sql = """
              UPDATE scan_item
              SET uid=uids.uid
              FROM uids
              WHERE scan_item.scan_id = ?
                AND scan_item.uid IS NULL
                AND scan_item.size = uids.size
                AND scan_item.md5 = uids.md5
                AND scan_item.sha256 = uids.sha256
              """
        self.conn.execute(sql, (scan_id,))

    def scan_update_step(self, scan_id: int, new_step: ScanStep) -> Scan:
        sql = "UPDATE scan SET finished_step=? WHERE id = ?"
        self.conn.execute(sql, (int(new_step), scan_id))
        return self.scan_select(scan_id)

    def scan_delete_file_records_scanned_as_deleted(self, scan_id: int) -> int:
        sql = "DELETE FROM file WHERE id IN(SELECT file_id FROM scan_item WHERE scan_id = ? AND status = ?)"

        log.debug(
            f"Removing file records of files scanned as deleted for scan {scan_id}."
        )
        cursor = self.conn.execute(sql, (scan_id, int(ScanFileStatus.REMOVED)))
        log.debug(
            f"Removed {cursor.rowcount} file records of files scanned as deleted."
        )
        return cursor.rowcount

    def scan_update_file_records_with_meta_changed(self, scan_id: int):
        sql = """UPDATE file
                 SET size=scan_item.size,
                     mtime=scan_item.mtime
                 FROM scan_item
                 WHERE file.id = scan_item.file_id
                   AND scan_item.scan_id = ?
                   AND scan_item.status = ?
              """

        log.debug(f"Updating file records witch changed meta for scan {scan_id}.")
        cursor = self.conn.execute(sql, (scan_id, int(ScanFileStatus.META_CHANGED)))
        log.debug(f"Updated {cursor.rowcount} file records wich changed meta.")
        return cursor.rowcount

    def scan_update_file_records_with_content_changed(self, scan_id: int):
        sql = """UPDATE file
                 SET uid=scan_item.uid,
                     size=scan_item.size,
                     mtime=scan_item.mtime,
                     md5=scan_item.md5,
                     sha256=scan_item.sha256
                 FROM scan_item
                 WHERE file.id = scan_item.file_id
                   AND scan_item.scan_id = ?
                   AND scan_item.status = ?
              """

        log.debug(f"Updating file records witch changed content for scan {scan_id}.")
        cursor = self.conn.execute(sql, (scan_id, int(ScanFileStatus.CONTENT_CHANGED)))
        log.debug(f"Updated {cursor.rowcount} file records wich content changed.")
        return cursor.rowcount

    def scan_insert_new_scanned_file_records(self, scan_id: int) -> None:
        sql = """
              INSERT INTO file(storage_id, rel_path, uid, size, mtime, md5, sha256)
              SELECT scan.storage_id,
                     scan_item.rel_path,
                     scan_item.uid,
                     scan_item.size,
                     scan_item.mtime,
                     scan_item.md5,
                     scan_item.sha256
              FROM scan
                       JOIN scan_item ON scan.id = scan_item.scan_id
              WHERE scan.id = ?
                AND scan_item.status = ?
              """

        log.debug(f"Inserting new file records for files scanned in scan {scan_id}.")
        cursor = self.conn.execute(sql, (scan_id, int(ScanFileStatus.NEW)))
        log.debug(f"Inserted {cursor.rowcount} new file records.")
