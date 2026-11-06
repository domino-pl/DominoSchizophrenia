# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Store scan progress, classify file changes, and apply scan results."""

import logging
from enum import IntEnum
from pathlib import PurePosixPath
from typing import NamedTuple, override

from src.db.base import FetchEmptyError
from src.db.file import SchizophreniaDBFile

log = logging.getLogger(__name__)


class ScanListItem(NamedTuple):
    """File path and metadata collected during storage listing."""

    rel_path: PurePosixPath
    size: int
    mtime: str


class ScanStep(IntEnum):
    """Persisted stages of a scan's progress."""

    INSERT = 0
    LIST = 1
    MATCH = 2
    CHECKSUM = 3
    COMPARE = 4
    ASSIGN_UID = 5
    APPLY = 6
    ABORT = 255


class ScanFileStatus(IntEnum):
    """File change classifications used during scanning."""

    UNKNOWN = 0
    NEW = 1
    REMOVED = 2
    META_CHANGED = 3
    CONTENT_CHANGED = 4
    NOT_CHANGED = 5
    REMOVE = 6


class FileToChecksumCalculation(NamedTuple):
    """Scan item identifier and path requiring checksum calculation."""

    item_id: int
    rel_path: PurePosixPath


class Scan(NamedTuple):
    """Scan identifier, target storage, and last completed stage."""

    id: int
    storage_id: int
    finished_step: ScanStep


class ScanItemCheckSums(NamedTuple):
    """Calculated MD5 and SHA-256 checksums for a scan item."""

    item_id: int
    md5: str
    sha256: str


class SchizophreniaDBScan(SchizophreniaDBFile):
    """Manage scan records as part of the database inheritance chain.

    Part of the database schema inheritance chain.
    """

    @override
    def _init_db(self) -> None:
        """Create scan tables and indexes after initializing the base schema."""
        super()._init_db()

        sqls = [
            # Keep scan progress separate from the individual file observations.
            """
            CREATE TABLE IF NOT EXISTS scan
            (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                storage_id    INTEGER NOT NULL,
                finished_step INTEGER NOT NULL
            )
            """,
            # File links, content identities, and checksums are filled in later stages.
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
            # A path can appear only once within a scan.
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
                scan_item_unique_idx_scan_path ON scan_item (scan_id, rel_path)
            """,
            # Match each stored file at most once per scan; NULL allows new files.
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
                scan_item_unique_idx_scan_file_id ON scan_item (scan_id, file_id)
            """,
        ]
        self.execute_list(sqls)

    def scan_select(self, scan_id: int) -> Scan:
        """Fetch a scan by its database identifier.

        Returns:
            The matching scan record.

        Raises:
            FetchEmptyError: If no scan with the given ID exists.

        """
        sql = "SELECT id, storage_id, finished_step FROM scan WHERE id = ?"
        cursor = self.conn.execute(sql, (scan_id,))
        row = cursor.fetchone()
        if row is None:
            raise FetchEmptyError
        return Scan(
            id=int(row[0]),
            storage_id=int(row[1]),
            finished_step=ScanStep(row[2]),
        )

    def scan_select_last_for_storage(self, storage_id: int) -> Scan | None:
        """Find the scan with the highest ID for a storage.

        Returns:
            The latest scan by ID, or `None` if the storage has no scans.

        """
        # TODO: ORDER BY TIME
        sql = "SELECT id FROM scan WHERE storage_id = ? ORDER BY id DESC LIMIT 1"
        cursor = self.conn.execute(sql, (storage_id,))
        row = cursor.fetchone()
        return self.scan_select(row[0]) if row else None

    def scan_insert(self, storage_id: int) -> Scan:
        """Create a scan at the initial stage.

        Commit the entire current transaction, including earlier pending changes.

        Returns:
            The newly created scan record.

        """
        sql = "INSERT INTO scan(storage_id, finished_step) VALUES (?, ?)"
        cursor = self.conn.execute(sql, (storage_id, int(ScanStep.INSERT)))
        self.conn.commit()
        scan_id = self._assert_inserted(cursor)
        return self.scan_select(scan_id)

    def scan_delete_all_storage_scan_items(self, storage_id: int) -> None:
        """Delete items from all scans belonging to the given storage."""
        sql = """
              DELETE FROM scan_item WHERE scan_id IN (
                  SELECT id FROM scan WHERE storage_id = ?
              )
        """
        cursor = self.conn.execute(sql, (storage_id,))
        log.debug("Removed %s old scan items.", cursor.rowcount)

    def scan_insert_scan_items(
        self,
        scan: Scan,
        list_items: list[ScanListItem],
    ) -> None:
        """Insert listed files into a scan with `UNKNOWN` status."""
        sql = """
              INSERT INTO scan_item
              (
                  scan_id, rel_path, size, mtime, status
              )
              VALUES (
                         ?, ?, ?, ?, ?
                     )
              """
        self.conn.executemany(
            sql,
            [
                (
                    scan.id,
                    # Store paths in a platform-independent format.
                    item.rel_path.as_posix(),
                    item.size,
                    item.mtime,
                    int(ScanFileStatus.UNKNOWN),
                )
                for item in list_items
            ],
        )

    def scan_match_files(self, scan: Scan) -> None:
        """Match paths to stored files and mark new, removed, and unchanged files."""
        log.debug("Match already known files.")
        # Match by location within the storage, independently of content changes.
        sql = """
              UPDATE scan_item
              SET file_id=
                      (SELECT file.id
                       FROM file
                       WHERE file.storage_id = ?
                         AND file.rel_path = scan_item.rel_path)
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
        log.debug("Marked %s new files.", cursor.rowcount)

        log.debug("Looking for deleted files.")
        # Add stored files absent from the listing so deletions can be applied later.
        # Exclude NULL links from NOT IN to preserve SQL's expected matching behavior.
        sql = """
              INSERT INTO scan_item
              (
                  scan_id,
                  file_id,
                  rel_path,
                  uid,
                  size,
                  mtime,
                  md5,
                  sha256,
                  status
              )
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
                AND id NOT IN (SELECT file_id
                               FROM scan_item
                               WHERE scan_id = ?
                                 AND file_id IS NOT NULL)
              """
        cursor = self.conn.execute(
            sql,
            (scan.id, int(ScanFileStatus.REMOVED), scan.storage_id, scan.id),
        )
        log.debug("Found %s deleted files.", cursor.rowcount)

        log.debug("Mark not modified files.")
        # Trust matching size and mtime here and reuse the existing content identity.
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
        log.debug("Found %s not modified files.", cursor.rowcount)

    def scan_select_files_for_checksum_calculation(
        self,
        scan: Scan,
        limit: int = 1000,
    ) -> list[FileToChecksumCalculation]:
        """Select new or unclassified files with missing checksums.

        Returns:
            Up to limit items requiring checksum calculation; order is unspecified.

        """
        # Select incomplete checksum pairs so interrupted calculations can resume.
        sql = """
              SELECT id, rel_path
              FROM scan_item
              WHERE scan_id = ?
                AND status IN (?,?)
                AND (md5 is NULL OR sha256 is NULL)
              LIMIT ?
              """
        cursor = self.conn.execute(
            sql,
            (scan.id, int(ScanFileStatus.NEW), int(ScanFileStatus.UNKNOWN), limit),
        )
        return [
            FileToChecksumCalculation(
                item_id=int(row[0]),
                rel_path=PurePosixPath(str(row[1])),
            )
            for row in cursor.fetchall()
        ]

    def scan_update_scan_items_sum(self, checksums: list[ScanItemCheckSums]) -> None:
        """Save calculated checksums for the supplied scan item IDs."""
        sql = """
              UPDATE scan_item
              SET md5=?,
                  sha256=?
              WHERE id = ?
              """
        self.conn.executemany(
            sql,
            [(item.md5, item.sha256, item.item_id) for item in checksums],
        )

    def scan_update_scan_items_content_changed(self, scan: Scan) -> None:
        """Classify unknown items as metadata-only or content changes.

        Reuse the stored UID when size and both checksums match the stored file.
        """
        # Matching content leaves only metadata changes to apply.
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

        # Remaining unknown items failed the content comparison above.
        sql = "UPDATE scan_item SET status=? WHERE scan_id = ? AND status = ?"
        self.conn.execute(
            sql,
            (int(ScanFileStatus.CONTENT_CHANGED), scan.id, int(ScanFileStatus.UNKNOWN)),
        )

    def scan_assign_scan_new_uid(self, scan_id: int) -> None:
        """Create missing content identities and assign UIDs by size and checksums."""
        # Reuse existing identities instead of duplicating the same content tuple.
        sql = """
              INSERT INTO uids(size, md5, sha256)
              SELECT size, md5, sha256
              FROM scan_item
              WHERE scan_id = ?
                AND uid IS NULL
              ON CONFLICT(size, md5, sha256) DO NOTHING
              """
        self.conn.execute(sql, (scan_id,))

        # Link items to either a newly inserted or a previously known identity.
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
        """Set the scan's completed stage.

        Returns:
            The scan record with its updated stage.

        """
        sql = "UPDATE scan SET finished_step=? WHERE id = ?"
        self.conn.execute(sql, (int(new_step), scan_id))
        return self.scan_select(scan_id)

    def scan_delete_file_records_scanned_as_deleted(self, scan_id: int) -> int:
        """Delete stored file records marked as removed by the scan.

        Returns:
            The number of deleted file records.

        """
        sql = """
              DELETE
              FROM file
              WHERE id IN
                    (SELECT file_id
                     FROM scan_item
                     WHERE scan_id = ?
                       AND status = ?)
              """

        log.debug(
            "Removing file records of files scanned as deleted for scan %s.",
            scan_id,
        )
        cursor = self.conn.execute(sql, (scan_id, int(ScanFileStatus.REMOVED)))
        log.debug(
            "Removed %s file records of files scanned as deleted.",
            cursor.rowcount,
        )
        return cursor.rowcount

    def scan_update_file_records_with_meta_changed(self, scan_id: int) -> int:
        """Apply metadata-only changes to stored file records.

        Returns:
            The number of updated file records.

        """
        # Preserve the UID and checksums because the file content is unchanged.
        sql = """UPDATE file
                 SET size=scan_item.size,
                     mtime=scan_item.mtime
                 FROM scan_item
                 WHERE file.id = scan_item.file_id
                   AND scan_item.scan_id = ?
                   AND scan_item.status = ?
              """

        log.debug("Updating file records witch changed meta for scan %s.", scan_id)
        cursor = self.conn.execute(sql, (scan_id, int(ScanFileStatus.META_CHANGED)))
        log.debug("Updated %s file records wich changed meta.", cursor.rowcount)
        return cursor.rowcount

    def scan_update_file_records_with_content_changed(self, scan_id: int) -> int:
        """Apply changed content identities, metadata, and checksums to stored files.

        Returns:
            The number of updated file records.

        """
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

        log.debug("Updating file records witch changed content for scan %s.", scan_id)
        cursor = self.conn.execute(sql, (scan_id, int(ScanFileStatus.CONTENT_CHANGED)))
        log.debug("Updated %s file records wich content changed.", cursor.rowcount)
        return cursor.rowcount

    def scan_insert_new_scanned_file_records(self, scan_id: int) -> None:
        """Copy files classified as new from the scan into the file table."""
        # Obtain the storage ID from the parent scan for every newly discovered file.
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

        log.debug("Inserting new file records for files scanned in scan %s.", scan_id)
        cursor = self.conn.execute(sql, (scan_id, int(ScanFileStatus.NEW)))
        log.debug("Inserted %s new file records.", cursor.rowcount)
