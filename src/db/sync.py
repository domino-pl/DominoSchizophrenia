import logging
from collections.abc import Iterator
from dataclasses import dataclass
from enum import IntEnum
from pathlib import PurePosixPath
from typing import NamedTuple

from src.db.scan import SchizophreniaDBScan

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SyncFileMeta:
    id: int
    mtime: str
    size: int
    md5: str
    sha256: str


@dataclass(frozen=True)
class SyncFileInfo(SyncFileMeta):
    uid: int
    rel_path: PurePosixPath
    move_dst_fid: int | None = None


def make_file_info(
    file_id: int | None,
    uid: int | None,
    rel_path: str | None,
    mtime: str | None,
    size: int | None,
    md5: str | None,
    sha256: str | None,
    move_dst_fid: int | None = None,
) -> SyncFileInfo | None:
    if file_id is None:
        return None

    if (
        uid is None
        or rel_path is None
        or mtime is None
        or size is None
        or md5 is None
        or sha256 is None
    ):
        raise ValueError(f"Incomplete file information for ID {file_id}")

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
    UNKNOWN = 0
    APPEARED = 1
    DISAPPEARED = 2
    NOT_CHANGED = 3
    CHANGED = 4


class SyncStrategy(IntEnum):
    UNDETERMINED = 0
    DO_NOTHING = 1
    IN_COPY = 2
    OUT_COPY = 3
    MOVE_TARGET = 4
    DELETE = 5


class SyncResultRow(NamedTuple):
    sync_status: SyncFileStatus
    sync_strategy: SyncStrategy

    src: SyncFileInfo | None
    dst: SyncFileInfo | None
    in_src: SyncFileInfo | None


class SchizophreniaDBSync(SchizophreniaDBScan):
    def sync_clear(self):
        sqls = [
            "DROP VIEW IF EXISTS v_tmp_sync_calc",
            "DROP TABLE IF EXISTS tmp_sync_calc",
        ]
        log.debug("Removing old sync table.")
        self.execute_list(sqls)
        log.info("Old sync tables removed.")

    def sync_create_table(self) -> None:
        sqls = [
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
            "CREATE INDEX index_sync_calc_a ON tmp_sync_calc (src_fid)",
            "CREATE INDEX index_sync_calc_b ON tmp_sync_calc (dst_fid)",
            "CREATE INDEX index_sync_calc_sync_status ON tmp_sync_calc (sync_status)",
        ]
        log.debug("Creating new sync table.")
        self.execute_list(sqls)
        log.info("New sync tables created.")

    def sync_fill_table(self, src_storage_id: int, dst_storage_id: int) -> int:
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
            f"Filling sync table for {src_storage_id} and {dst_storage_id} storages."
        )
        cursor = self.conn.execute(sql, (src_storage_id, dst_storage_id))
        log.info(f"Sync table filled with {cursor.rowcount} rows.")
        return cursor.rowcount

    def sync_mark_sync_state(self) -> None:
        # TODO: for
        sql = """
              UPDATE tmp_sync_calc
              SET sync_status   = ?,
                  sync_strategy = ?
              WHERE sync_status = ?
                AND dst_fid IS NULL
              """

        log.debug(f"Marking file sync state as {SyncFileStatus.APPEARED}.")
        cursor = self.conn.execute(
            sql,
            (
                SyncFileStatus.APPEARED,
                SyncStrategy.UNDETERMINED,
                SyncFileStatus.UNKNOWN,
            ),
        )
        log.info(f"{cursor.rowcount} files marked as {SyncFileStatus.APPEARED}.")

        sql = """
              UPDATE tmp_sync_calc
              SET sync_status = ?,
                  sync_strategy = ?
              WHERE sync_status = ?
                AND src_fid IS NULL
              """
        log.debug(f"Marking file sync state as {SyncFileStatus.DISAPPEARED}.")
        cursor = self.conn.execute(
            sql,
            (
                SyncFileStatus.DISAPPEARED,
                SyncStrategy.UNDETERMINED,
                SyncFileStatus.UNKNOWN,
            ),
        )
        log.info(f"{cursor.rowcount} files marked as {SyncFileStatus.DISAPPEARED}.")

        sql = """
              UPDATE tmp_sync_calc
              SET sync_status = ?,
                  sync_strategy = ?
              WHERE sync_status = ?
                AND src_uid != dst_uid
              """
        log.debug(f"Marking file sync state as {SyncFileStatus.CHANGED}.")
        cursor = self.conn.execute(
            sql,
            (SyncFileStatus.CHANGED, SyncStrategy.UNDETERMINED, SyncFileStatus.UNKNOWN),
        )
        log.info(f"{cursor.rowcount} files marked as {SyncFileStatus.CHANGED}.")

        sql = """
              UPDATE tmp_sync_calc
              SET sync_status=?,
                  sync_strategy = ?
              WHERE sync_status = ?
                AND src_uid = dst_uid
              """
        log.debug(f"Marking file sync state as {SyncFileStatus.NOT_CHANGED}.")
        cursor = self.conn.execute(
            sql,
            (
                SyncFileStatus.NOT_CHANGED,
                SyncStrategy.DO_NOTHING,
                SyncFileStatus.UNKNOWN,
            ),
        )
        log.info(f"{cursor.rowcount} files marked as {SyncFileStatus.NOT_CHANGED}.")

        # TODO: check 0 count of SyncFileStatus.UNKNOWN

    def sync_find_strategy(self) -> None:
        sql = """
              UPDATE tmp_sync_calc as copy_dst
              SET sync_strategy = ?,
                  in_src_fid    = copy_src.dst_fid FROM tmp_sync_calc AS copy_src
              WHERE copy_dst.sync_status IN (?, ?)
                AND copy_dst.sync_strategy = ?
                AND copy_dst.src_uid = copy_src.dst_uid
              """
        log.debug(f"Assigning strategy {SyncStrategy.IN_COPY}.")
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
            f"Strategy {SyncStrategy.IN_COPY} assigned to {cursor.rowcount} files."
        )

        sql = """
              UPDATE tmp_sync_calc
              SET sync_strategy = ?
              WHERE sync_status IN (?, ?)
                AND sync_strategy = ?;
              """
        log.debug(f"Assigning strategy {SyncStrategy.OUT_COPY}.")
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
            f"Strategy {SyncStrategy.OUT_COPY} assigned to {cursor.rowcount} files."
        )

        sql = """
              UPDATE tmp_sync_calc
              SET sync_strategy = ?
              WHERE sync_status = ?
                AND sync_strategy = ?;
              """
        log.debug(f"Assigning strategy {SyncStrategy.DELETE}.")
        cursor = self.conn.execute(
            sql,
            (
                SyncStrategy.DELETE,
                SyncFileStatus.DISAPPEARED,
                SyncStrategy.UNDETERMINED,
            ),
        )
        log.info(f"Strategy {SyncStrategy.DELETE} assigned to {cursor.rowcount} files.")
        # TODO: assert 0 UNDETERMINED left.

    def sync_iter_solving_move_strategy(self) -> int:
        sql = """
              UPDATE tmp_sync_calc
              SET sync_strategy = (CASE
                                       WHEN tmp_sync_calc.src_fid = sub.src_fid
                                           THEN ?
                                       ELSE tmp_sync_calc.sync_strategy
                  END),
                  in_src_fid    = (CASE
                                       WHEN tmp_sync_calc.src_fid = sub.src_fid
                                           THEN sub.move_in_src_fid
                                       ELSE tmp_sync_calc.in_src_fid
                      END),
                  move_dst_fid  = (CASE
                                       WHEN tmp_sync_calc.dst_fid = sub.move_in_src_fid
                                           THEN sub.src_fid
                                       ELSE tmp_sync_calc.move_dst_fid
                      END
                      ) FROM (
                  SELECT max (move_dst.src_fid) AS src_fid, min (move_src.dst_fid) AS move_in_src_fid
                  FROM tmp_sync_calc AS move_dst
                  JOIN tmp_sync_calc AS move_src ON move_dst.src_uid = move_src.dst_uid
                  WHERE move_dst.sync_strategy = ?
                  AND move_src.sync_status IN (?, ?)
                  AND move_src.move_dst_fid IS NULL
                  GROUP BY move_dst.src_uid
                  ) AS sub
              WHERE tmp_sync_calc.src_fid = sub.src_fid
                 OR tmp_sync_calc.dst_fid = sub.move_in_src_fid
              """

        log.debug("Iterating looking for move strategy solutions.")
        cursor = self.conn.execute(
            sql,
            (
                SyncStrategy.MOVE_TARGET,
                SyncStrategy.IN_COPY,
                SyncFileStatus.DISAPPEARED,
                SyncFileStatus.CHANGED,
            ),
        )
        log.info(f"Found in_move solutions for {cursor.rowcount / 2} files.")
        return cursor.rowcount

    def sync_iter_entries(self) -> Iterator[SyncResultRow]:
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
                    src_id,
                    src_uid,
                    src_rel_path,
                    src_mtime,
                    src_size,
                    src_md5,
                    src_sha256,
                    move_dst_fid,
                ),
                dst=make_file_info(
                    dst_id,
                    dst_uid,
                    dst_rel_path,
                    dst_mtime,
                    dst_size,
                    dst_md5,
                    dst_sha256,
                    move_dst_fid,
                ),
                in_src=make_file_info(
                    in_src_id,
                    in_src_uid,
                    in_src_rel_path,
                    in_src_mtime,
                    in_src_size,
                    in_src_md5,
                    in_src_sha256,
                    move_dst_fid,
                ),
            )
