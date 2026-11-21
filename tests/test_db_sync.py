# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Verify how the SQLite planner classifies and assigns file operations."""

from operator import itemgetter
from typing import TYPE_CHECKING

import pytest

from src.db.sync import SyncFileStatus, SyncPlanningError, SyncStrategy

if TYPE_CHECKING:
    from src.db.sync import SchizophreniaDBSync


@pytest.fixture
def db(empty_db: SchizophreniaDBSync) -> SchizophreniaDBSync:
    """Populate the shared-donor scenario and prepare its initial sync plan.

    Returns:
        A database with source a:X, b:X, c:Y and destination a:Y, b:Y, c:X.

    """
    empty_db.conn.executemany(
        """
        INSERT INTO file(storage_id, rel_path, uid, size, mtime, md5, sha256)
        VALUES (?, ?, ?, 1, '2026-11-08', ?, ?)
        """,
        [
            (storage_id, path, uid, str(uid), str(uid))
            for storage_id, contents in [
                (1, (("a", 1), ("b", 1), ("c", 2))),
                (2, (("a", 2), ("b", 2), ("c", 1))),
            ]
            for path, uid in contents
        ],
    )
    empty_db.sync_create_table()
    empty_db.sync_fill_table(1, 2)
    empty_db.sync_mark_sync_state()
    empty_db.sync_find_strategy()
    return empty_db


@pytest.mark.parametrize("passes", [1, 2, 3])
def test_assigned_actions_and_file_ids(db: SchizophreniaDBSync, passes: int) -> None:
    """Check each file's action and IDs after planning and subsequent passes."""
    for _ in range(passes):
        db.sync_iter_solving_move_strategy()

    actions = [
        (
            row.sync_strategy,
            row.src.id if row.src is not None else None,
            row.dst.id if row.dst is not None else None,
            row.in_src.id if row.in_src is not None else None,
            row.dst.move_dst_fid if row.dst is not None else None,
        )
        for row in db.sync_iter_entries()
    ]
    # Columns: action, source ID, destination ID, donor ID, donor's move target ID.
    # a copies X from c and donates Y to c; b receives X moved from c.
    # c receives Y moved from a and donates X to b, so each donor is used once.
    assert sorted(actions, key=itemgetter(1)) == [
        (SyncStrategy.IN_COPY, 1, 4, 6, 3),
        (SyncStrategy.MOVE_TARGET, 2, 5, 6, None),
        (SyncStrategy.MOVE_TARGET, 3, 6, 4, 2),
    ]


def test_planner_classifies_paths_and_assigns_strategies(
    empty_db: SchizophreniaDBSync,
) -> None:
    """Choose a strategy for new, removed, changed, and unchanged paths."""
    empty_db.conn.executemany(
        """
        INSERT INTO file(storage_id, rel_path, uid, size, mtime, md5, sha256)
        VALUES (?, ?, ?, 1, '2026-11-08', 'md5', 'sha256')
        """,
        [
            (1, "new", 1),
            (1, "changed", 2),
            (1, "unchanged", 3),
            (2, "unchanged", 3),
            (2, "changed", 4),
            (2, "removed", 5),
        ],
    )
    empty_db.sync_create_table()
    empty_db.sync_fill_table(1, 2)
    empty_db.sync_mark_sync_state()
    empty_db.sync_find_strategy()

    observed = {}
    for row in empty_db.sync_iter_entries():
        file_info = row.src if row.src is not None else row.dst
        assert file_info is not None
        observed[file_info.rel_path.as_posix()] = (row.sync_status, row.sync_strategy)
    assert observed == {
        "new": (SyncFileStatus.APPEARED, SyncStrategy.OUT_COPY),
        "removed": (SyncFileStatus.DISAPPEARED, SyncStrategy.DELETE),
        "changed": (SyncFileStatus.CHANGED, SyncStrategy.OUT_COPY),
        "unchanged": (SyncFileStatus.NOT_CHANGED, SyncStrategy.DO_NOTHING),
    }


def test_planner_rejects_unknown_status(empty_db: SchizophreniaDBSync) -> None:
    """Reject a pair whose missing source UID prevents classification."""
    empty_db.sync_create_table()
    empty_db.conn.execute(
        "INSERT INTO tmp_sync_calc (sync_status, src_fid, dst_fid, dst_uid) "
        "VALUES (?, 1, 2, 2)",
        (SyncFileStatus.UNKNOWN,),
    )

    with pytest.raises(SyncPlanningError, match="could not classify 1"):
        empty_db.sync_mark_sync_state()


def test_planner_rejects_unassigned_strategy(empty_db: SchizophreniaDBSync) -> None:
    """Reject an entry whose strategy was never assigned."""
    empty_db.sync_create_table()
    empty_db.conn.execute(
        "INSERT INTO tmp_sync_calc (sync_status, sync_strategy) VALUES (?, ?)",
        (SyncFileStatus.NOT_CHANGED, SyncStrategy.UNDETERMINED),
    )

    with pytest.raises(SyncPlanningError, match="could not assign a strategy to 1"):
        empty_db.sync_find_strategy()
