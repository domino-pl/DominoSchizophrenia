# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Build synchronization plans from stored file paths and content identities."""

import logging
from dataclasses import dataclass
from enum import IntEnum
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, NamedTuple

from src.db.scan import SchizophreniaDBScan

if TYPE_CHECKING:
    from collections.abc import Iterator

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SyncFileMeta:
    """Immutable file identifier, metadata, and checksums."""

    id: int
    mtime: str
    size: int
    md5: str
    sha256: str


@dataclass(frozen=True)
class SyncFileInfo(SyncFileMeta):
    """File metadata extended with content identity, path, and move target ID."""

    uid: int
    rel_path: PurePosixPath
    move_dst_fid: int | None = None


def make_file_info(  # ruff: ignore[too-many-arguments]
    *,
    file_id: int | None,
    uid: int | None,
    rel_path: str | None,
    mtime: str | None,
    size: int | None,
    md5: str | None,
    sha256: str | None,
    move_dst_fid: int | None = None,
) -> SyncFileInfo | None:
    """Convert nullable database fields into a file record.

    Returns:
        The file record, or `None` if `file_id` is `None`.

    Raises:
        ValueError: If an existing file lacks required metadata.

    """
    # An absent outer-join match produces NULL fields rather than a file record.
    if file_id is None:
        return None

    # Once the ID exists, all required metadata must be present as well.
    if (
        uid is None  # ruff: ignore[too-many-boolean-expressions]
        or rel_path is None
        or mtime is None
        or size is None
        or md5 is None
        or sha256 is None
    ):
        msg = f"Incomplete file information for ID {file_id}"
        raise ValueError(msg)

    return SyncFileInfo(
        id=file_id,
        mtime=mtime,
        size=size,
        md5=md5,
        sha256=sha256,
        uid=uid,
        rel_path=PurePosixPath(rel_path),
        move_dst_fid=move_dst_fid,
    )


class SyncFileStatus(IntEnum):
    """File presence and content differences between source and destination."""

    UNKNOWN = 0
    APPEARED = 1
    DISAPPEARED = 2
    NOT_CHANGED = 3
    CHANGED = 4


class SyncStrategy(IntEnum):
    """Actions used to make destination files match the source."""

    UNDETERMINED = 0
    DO_NOTHING = 1
    IN_COPY = 2
    OUT_COPY = 3
    MOVE_TARGET = 4
    DELETE = 5


class SyncResultRow(NamedTuple):
    """Planned action with source, destination, and optional local copy donor."""

    sync_status: SyncFileStatus
    sync_strategy: SyncStrategy

    src: SyncFileInfo | None
    dst: SyncFileInfo | None
    in_src: SyncFileInfo | None


class SchizophreniaDBSync(SchizophreniaDBScan):
    """Manage synchronization planning tables and operations.

    Part of the database schema inheritance chain.
    """

    def sync_clear(self) -> None:
        """Drop the previous synchronization view and planning table."""
        sqls = [
            # Drop the dependent view before its backing table.
            "DROP VIEW IF EXISTS v_tmp_sync_calc",
            "DROP TABLE IF EXISTS tmp_sync_calc",
        ]
        log.debug("Removing old sync table.")
        self.execute_list(sqls)
        log.info("Old sync tables removed.")

    def sync_create_table(self) -> None:
        """Create the synchronization planning table and lookup indexes."""
        sqls = [
            # Missing sides and unresolved strategies remain NULL during planning.
            """
            CREATE TABLE tmp_sync_calc
            (
                sync_status   INTEGER NOT NULL,
                sync_strategy INTEGER NULL,
                src_fid       INTEGER NULL,
                dst_fid       INTEGER NULL,
                src_uid       INTEGER NULL,
                dst_uid       INTEGER NULL,
                in_src_fid    INTEGER NULL,
                move_dst_fid  INTEGER NULL
            )
            """,
            # Support file matching and status-based updates of the plan.
            "CREATE INDEX index_sync_calc_a ON tmp_sync_calc (src_fid)",
            "CREATE INDEX index_sync_calc_b ON tmp_sync_calc (dst_fid)",
            "CREATE INDEX index_sync_calc_sync_status ON tmp_sync_calc (sync_status)",
        ]
        log.debug("Creating new sync table.")
        self.execute_list(sqls)
        log.info("New sync tables created.")

    def sync_fill_table(self, src_storage_id: int, dst_storage_id: int) -> int:
        """Populate the plan by matching source and destination relative paths.

        Returns:
            The cursor's row count, which may be `-1` for this CTE-based INSERT.

        """
        sql = """
              WITH src AS (SELECT id, rel_path, uid
                           FROM file
                           WHERE storage_id = ?),
                   dst AS (SELECT id, rel_path, uid
                           FROM file
                           WHERE storage_id = ?)
              INSERT
              INTO tmp_sync_calc(sync_status, src_fid, dst_fid, src_uid, dst_uid)
              SELECT 0, src.id, dst.id, src.uid, dst.uid
              FROM src
                       FULL OUTER JOIN dst ON dst.rel_path = src.rel_path;
              """

        log.debug(
            "Filling sync table for %s and %s storages.",
            src_storage_id,
            dst_storage_id,
        )
        cursor = self.conn.execute(sql, (src_storage_id, dst_storage_id))
        log.info("Sync table filled with %s rows.", cursor.rowcount)
        return cursor.rowcount

    def sync_mark_sync_state(self) -> None:
        """Classify `UNKNOWN` entries by file presence and content identity.

        Assign `DO_NOTHING` to unchanged files and `UNDETERMINED` to the rest.
        """
        # TODO: for
        # Classify absent files first; UID comparisons apply to the remaining pairs.
        sql = """
              UPDATE tmp_sync_calc
              SET sync_status   = ?,
                  sync_strategy = ?
              WHERE sync_status = ?
                AND dst_fid IS NULL
              """

        log.debug("Marking file sync state as %s.", SyncFileStatus.APPEARED)
        cursor = self.conn.execute(
            sql,
            (
                SyncFileStatus.APPEARED,
                SyncStrategy.UNDETERMINED,
                SyncFileStatus.UNKNOWN,
            ),
        )
        log.info("%s files marked as %s.", cursor.rowcount, SyncFileStatus.APPEARED)

        sql = """
              UPDATE tmp_sync_calc
              SET sync_status = ?,
                  sync_strategy = ?
              WHERE sync_status = ?
                AND src_fid IS NULL
              """
        log.debug("Marking file sync state as %s.", SyncFileStatus.DISAPPEARED)
        cursor = self.conn.execute(
            sql,
            (
                SyncFileStatus.DISAPPEARED,
                SyncStrategy.UNDETERMINED,
                SyncFileStatus.UNKNOWN,
            ),
        )
        log.info("%s files marked as %s.", cursor.rowcount, SyncFileStatus.DISAPPEARED)

        sql = """
              UPDATE tmp_sync_calc
              SET sync_status = ?,
                  sync_strategy = ?
              WHERE sync_status = ?
                AND src_uid != dst_uid
              """
        log.debug("Marking file sync state as %s.", SyncFileStatus.CHANGED)
        cursor = self.conn.execute(
            sql,
            (SyncFileStatus.CHANGED, SyncStrategy.UNDETERMINED, SyncFileStatus.UNKNOWN),
        )
        log.info("%s files marked as %s.", cursor.rowcount, SyncFileStatus.CHANGED)

        sql = """
              UPDATE tmp_sync_calc
              SET sync_status=?,
                  sync_strategy = ?
              WHERE sync_status = ?
                AND src_uid = dst_uid
              """
        log.debug("Marking file sync state as %s.", SyncFileStatus.NOT_CHANGED)
        cursor = self.conn.execute(
            sql,
            (
                SyncFileStatus.NOT_CHANGED,
                SyncStrategy.DO_NOTHING,
                SyncFileStatus.UNKNOWN,
            ),
        )
        log.info("%s files marked as %s.", cursor.rowcount, SyncFileStatus.NOT_CHANGED)

        # TODO: check 0 count of SyncFileStatus.UNKNOWN

    def sync_find_strategy(self) -> None:
        """Assign copy or delete strategies to unresolved entries.

        Prefer `IN_COPY` when matching content exists in the destination;
        otherwise use `OUT_COPY`. Assign `DELETE` to disappeared files.
        """
        # Reuse matching content already present anywhere in the destination.
        sql = """
              UPDATE tmp_sync_calc as copy_dst
              SET sync_strategy = ?,
                  in_src_fid    = copy_src.dst_fid FROM tmp_sync_calc AS copy_src
              WHERE copy_dst.sync_status IN (?, ?)
                AND copy_dst.sync_strategy = ?
                AND copy_dst.src_uid = copy_src.dst_uid
              """
        log.debug("Assigning strategy %s.", SyncStrategy.IN_COPY)
        cursor = self.conn.execute(
            sql,
            (
                SyncStrategy.IN_COPY,
                SyncFileStatus.APPEARED,
                SyncFileStatus.CHANGED,
                SyncStrategy.UNDETERMINED,
            ),
        )
        log.info(
            "Strategy %s assigned to %s files.",
            SyncStrategy.IN_COPY,
            cursor.rowcount,
        )

        # Files without a local content match must be copied from the source.
        sql = """
              UPDATE tmp_sync_calc
              SET sync_strategy = ?
              WHERE sync_status IN (?, ?)
                AND sync_strategy = ?;
              """
        log.debug("Assigning strategy %s.", SyncStrategy.OUT_COPY)
        cursor = self.conn.execute(
            sql,
            (
                SyncStrategy.OUT_COPY,
                SyncFileStatus.APPEARED,
                SyncFileStatus.CHANGED,
                SyncStrategy.UNDETERMINED,
            ),
        )
        log.info(
            "Strategy %s assigned to %s files.",
            SyncStrategy.OUT_COPY,
            cursor.rowcount,
        )

        # Destination-only paths are no longer needed in the synchronized storage.
        sql = """
              UPDATE tmp_sync_calc
              SET sync_strategy = ?
              WHERE sync_status = ?
                AND sync_strategy = ?;
              """
        log.debug("Assigning strategy %s.", SyncStrategy.DELETE)
        cursor = self.conn.execute(
            sql,
            (
                SyncStrategy.DELETE,
                SyncFileStatus.DISAPPEARED,
                SyncStrategy.UNDETERMINED,
            ),
        )
        log.info(
            "Strategy %s assigned to %s files.",
            SyncStrategy.DELETE,
            cursor.rowcount,
        )
        # TODO: assert 0 UNDETERMINED left.

    def sync_iter_solving_move_strategy(self) -> int:
        """Perform one pass replacing eligible local copies with moves.

        Pair copy targets with destination files being removed or replaced,
        recording `MOVE_TARGET` and the corresponding donor's target file ID.

        Returns:
            The number of planning rows updated, including both ends of each move.

        """
        # Select one target and one unclaimed donor per UID in each pass.
        # Freeze the pairs before updating either end. A row may be both a
        # receiver and a donor, so resolve its two roles independently.
        as_materialized = "AS MATERIALIZED"  # Trick PyCharm inspection
        sql = f"""
              WITH moves {as_materialized} (
                  SELECT max(move_dst.src_fid) AS src_fid,
                         min(move_src.dst_fid) AS move_in_src_fid
                  FROM tmp_sync_calc AS move_dst
                           JOIN tmp_sync_calc AS move_src
                                ON move_dst.src_uid = move_src.dst_uid
                  WHERE move_dst.sync_strategy = ?
                    AND move_src.sync_status IN (?, ?)
                    AND move_src.move_dst_fid IS NULL
                  GROUP BY move_dst.src_uid
              )
              UPDATE tmp_sync_calc
              SET sync_strategy = CASE
                      WHEN EXISTS (SELECT 1 FROM moves
                                   WHERE moves.src_fid = tmp_sync_calc.src_fid)
                          THEN ?
                      ELSE sync_strategy
                  END,
                  in_src_fid = coalesce(
                      (SELECT move_in_src_fid FROM moves
                       WHERE moves.src_fid = tmp_sync_calc.src_fid),
                      in_src_fid),
                  move_dst_fid = coalesce(
                      (SELECT src_fid FROM moves
                       WHERE moves.move_in_src_fid = tmp_sync_calc.dst_fid),
                      move_dst_fid)
              WHERE EXISTS (SELECT 1 FROM moves
                            WHERE moves.src_fid = tmp_sync_calc.src_fid
                               OR moves.move_in_src_fid = tmp_sync_calc.dst_fid)
              RETURNING rowid
              """  # ruff: ignore[hardcoded-sql-expression]

        log.debug("Iterating looking for move strategy solutions.")
        cursor = self.conn.execute(
            sql,
            (
                SyncStrategy.IN_COPY,
                SyncFileStatus.DISAPPEARED,
                SyncFileStatus.CHANGED,
                SyncStrategy.MOVE_TARGET,
            ),
        )
        # CTE-based UPDATEs report rowcount=-1; count returned rows instead.
        updated_rows = len(cursor.fetchall())
        log.info("Updated %s planning rows for moves.", updated_rows)
        return updated_rows

    def sync_iter_entries(self) -> Iterator[SyncResultRow]:  # ruff: ignore[too-many-locals]
        """Iterate over planned actions with their associated file records.

        Yields:
            A plan entry with `None` for any absent source, destination, or donor.

        """
        # Keep every plan entry even when some associated files are absent.
        query = """
                SELECT calc.sync_status,
                       calc.sync_strategy,

                       src.id,
                       dst.id,
                       in_src.id,
                       calc.move_dst_fid,

                       src.rel_path,
                       dst.rel_path,
                       in_src.rel_path,

                       src.uid,
                       dst.uid,
                       in_src.uid,

                       src.mtime,
                       dst.mtime,
                       in_src.mtime,

                       src.size,
                       dst.size,
                       in_src.size,

                       src.md5,
                       dst.md5,
                       in_src.md5,

                       src.sha256,
                       dst.sha256,
                       in_src.sha256

                FROM tmp_sync_calc AS calc
                         LEFT JOIN file AS src
                                   ON calc.src_fid = src.id
                         LEFT JOIN file AS dst
                                   ON calc.dst_fid = dst.id
                         LEFT JOIN file AS in_src
                                   ON calc.in_src_fid = in_src.id
                """

        cursor = self.conn.execute(query)

        for row in cursor:
            (
                sync_status,
                row_sync_strategy,
                src_id,
                dst_id,
                in_src_id,
                move_dst_fid,
                src_rel_path,
                dst_rel_path,
                in_src_rel_path,
                src_uid,
                dst_uid,
                in_src_uid,
                src_mtime,
                dst_mtime,
                in_src_mtime,
                src_size,
                dst_size,
                in_src_size,
                src_md5,
                dst_md5,
                in_src_md5,
                src_sha256,
                dst_sha256,
                in_src_sha256,
            ) = row

            yield SyncResultRow(
                sync_status=SyncFileStatus(sync_status),
                sync_strategy=SyncStrategy(row_sync_strategy),
                src=make_file_info(
                    file_id=src_id,
                    uid=src_uid,
                    rel_path=src_rel_path,
                    mtime=src_mtime,
                    size=src_size,
                    md5=src_md5,
                    sha256=src_sha256,
                    move_dst_fid=move_dst_fid,
                ),
                dst=make_file_info(
                    file_id=dst_id,
                    uid=dst_uid,
                    rel_path=dst_rel_path,
                    mtime=dst_mtime,
                    size=dst_size,
                    md5=dst_md5,
                    sha256=dst_sha256,
                    move_dst_fid=move_dst_fid,
                ),
                in_src=make_file_info(
                    file_id=in_src_id,
                    uid=in_src_uid,
                    rel_path=in_src_rel_path,
                    mtime=in_src_mtime,
                    size=in_src_size,
                    md5=in_src_md5,
                    sha256=in_src_sha256,
                    move_dst_fid=move_dst_fid,
                ),
            )
