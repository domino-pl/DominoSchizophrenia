# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Verify the scan lifecycle against real SQLite tables and temporary files."""

import errno
import os
import sqlite3
from itertools import count
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from src.db.base import FetchEmptyError
from src.db.scan import ScanStep
from src.identity import IDENTITY_FILE_NAME, Identity
from src.scantask import (
    InvalidScanStateError,
    ScanFileChangedError,
    ScanTask,
    UnsupportedFileTypeError,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from src.db import SchizophreniaDB
    from src.db.scan import FileToChecksumCalculation, Scan, ScanItemCheckSums


@pytest.fixture
def scan_task(empty_db: SchizophreniaDB, tmp_path: Path) -> ScanTask:
    """Register a temporary directory and write its identity marker.

    Returns:
        The scan task for the registered directory.

    """
    identity = Identity.new()
    identity.save_to_storage_dir(tmp_path)
    empty_db.insert_storage("test", tmp_path, identity)
    return ScanTask(empty_db, "test")


def test_storage_metadata_round_trip(empty_db: SchizophreniaDB, tmp_path: Path) -> None:
    """Restore the typed path and identity for the registered storage."""
    identity = Identity.new()
    storage = empty_db.insert_storage("demo", tmp_path, identity)

    assert empty_db.select_storage("demo") == storage
    assert storage.abs_path == tmp_path
    assert storage.identity == identity
    assert storage.name == "demo"
    with pytest.raises(FetchEmptyError):
        empty_db.select_storage("missing")


def test_scan_apply_reuses_uid_for_identical_content(scan_task: ScanTask) -> None:
    """Apply two equal files and exclude the root identity marker."""
    root = scan_task.storage.abs_path
    (root / "a.txt").write_bytes(b"abc")
    (root / "b.txt").write_bytes(b"abc")

    scan_task.start()
    conn = scan_task.db.conn
    assert conn.execute("SELECT COUNT(*) FROM file").fetchone() == (0,)
    scan_task.apply()

    rows = conn.execute("SELECT rel_path, uid FROM file ORDER BY rel_path").fetchall()
    assert [path for path, _ in rows] == ["a.txt", "b.txt"]
    assert len({uid for _, uid in rows}) == 1
    steps = conn.execute("SELECT finished_step FROM scan").fetchall()
    assert steps == [(ScanStep.APPLY,)]


def test_rescan_changes_only_the_selected_storage(
    empty_db: SchizophreniaDB, tmp_path: Path,
) -> None:
    """Keep equal relative paths independent when applying a mixed rescan."""
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    for root in (first_root, second_root):
        root.mkdir()
        identity = Identity.new()
        identity.save_to_storage_dir(root)
        empty_db.insert_storage(root.name, root, identity)
        (root / "shared.txt").write_text(root.name, encoding="utf-8")
        (root / "removed.txt").write_text(root.name, encoding="utf-8")

    first = ScanTask(empty_db, first_root.name)
    second = ScanTask(empty_db, second_root.name)
    for task in (first, second):
        task.start()
        task.apply()

    query = "SELECT storage_id, rel_path, uid, md5, sha256 FROM file"
    before = {
        (storage_id, path): (uid, md5, sha256)
        for storage_id, path, uid, md5, sha256 in empty_db.conn.execute(query)
    }
    first_shared = (first.storage.id, "shared.txt")
    second_shared = (second.storage.id, "shared.txt")
    second_removed = (second.storage.id, "removed.txt")
    assert before[first_shared][0] != before[second_shared][0]

    (first_root / "removed.txt").unlink()
    (first_root / "added.txt").write_text("new data", encoding="utf-8")
    first.start()
    first.apply()

    after = {
        (storage_id, path): (uid, md5, sha256)
        for storage_id, path, uid, md5, sha256 in empty_db.conn.execute(query)
    }
    assert after.keys() == {
        first_shared, (first.storage.id, "added.txt"), second_shared, second_removed,
    }
    for unchanged in (first_shared, second_shared, second_removed):
        assert after[unchanged] == before[unchanged]


def test_rescan_preserves_content_identity_after_rename(scan_task: ScanTask) -> None:
    """Retain the content UID when a file moves to a different relative path."""
    file_path = scan_task.storage.abs_path / "old.txt"
    file_path.write_bytes(b"same contents")
    scan_task.start()
    scan_task.apply()
    conn = scan_task.db.conn
    query = "SELECT rel_path, uid FROM file"
    before = conn.execute(query).fetchone()
    assert before is not None
    assert before[0] == "old.txt"

    nested = file_path.parent / "nested"
    nested.mkdir()
    file_path.rename(nested / "new.txt")
    scan_task.start()
    scan_task.apply()

    assert conn.execute(query).fetchall() == [("nested/new.txt", before[1])]


def test_scan_rejects_symlink_to_file_outside_storage(
    scan_task: ScanTask, tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Reject a symlink before external content can be indexed."""
    storage_root = scan_task.storage.abs_path
    target = tmp_path_factory.mktemp("outside-storage") / "outside.txt"
    target.write_bytes(b"external contents")
    symlink = storage_root / "link.txt"
    try:
        symlink.symlink_to(target)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"Cannot create a symlink: {error}")

    with pytest.raises(UnsupportedFileTypeError) as exc:
        scan_task.start()

    assert exc.value.file_path == symlink
    with pytest.raises(InvalidScanStateError):
        scan_task.apply()
    assert scan_task.db.conn.execute("SELECT rel_path FROM file").fetchall() == []


def test_nested_identity_filename_is_scanned(scan_task: ScanTask) -> None:
    """Treat the identity filename as ordinary data outside the storage root."""
    nested = scan_task.storage.abs_path / "nested"
    nested.mkdir()
    (nested / IDENTITY_FILE_NAME).write_text("ordinary data", encoding="utf-8")

    scan_task.start()
    scan_task.apply()

    rows = scan_task.db.conn.execute("SELECT rel_path FROM file").fetchall()
    assert rows == [(f"nested/{IDENTITY_FILE_NAME}",)]


@pytest.mark.parametrize("change_content", [False, True], ids=["mtime-only", "content"])
def test_rescan_updates_metadata_and_content_identity(
    scan_task: ScanTask, *, change_content: bool,
) -> None:
    """Preserve the UID for identical content and replace it for changed bytes."""
    file_path = scan_task.storage.abs_path / "file.txt"
    file_path.write_bytes(b"abc")
    scan_task.start()
    scan_task.apply()
    conn = scan_task.db.conn
    select_file = "SELECT uid, mtime, md5, sha256 FROM file"
    before = conn.execute(select_file).fetchone()
    assert before is not None
    before_uid, before_mtime, before_md5, before_sha256 = before
    if change_content:
        file_path.write_bytes(b"xyz")
    original_stat = file_path.stat()
    os.utime(
        file_path,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns + 3_000_000_000),
    )

    scan_task.start()
    scan_task.apply()

    after = conn.execute(select_file).fetchone()
    assert after is not None
    uid, mtime, md5, sha256 = after
    assert mtime != before_mtime
    if change_content:
        assert uid != before_uid
        assert md5 != before_md5
        assert sha256 != before_sha256
    else:
        assert (uid, md5, sha256) == (before_uid, before_md5, before_sha256)


@pytest.mark.parametrize(
    ("initial_file", "remove_file", "expected_paths"),
    [
        pytest.param(False, False, [], id="empty"),
        pytest.param(True, False, ["file.txt"], id="unchanged"),
        pytest.param(True, True, [], id="removals-only"),
    ],
)
def test_scan_without_checksums_can_be_applied(
    scan_task: ScanTask,
    expected_paths: list[str],
    *,
    initial_file: bool,
    remove_file: bool,
) -> None:
    """Apply a scan even when it has no new checksums to calculate."""
    file_path = scan_task.storage.abs_path / "file.txt"
    if initial_file:
        file_path.write_bytes(b"abc")
        scan_task.start()
        scan_task.apply()
    if remove_file:
        file_path.unlink()

    scan_task.start()
    scan_task.apply()

    rows = scan_task.db.conn.execute("SELECT rel_path FROM file ORDER BY rel_path")
    assert [path for (path,) in rows] == expected_paths


def test_resume_after_partial_checksum_batch(
    scan_task: ScanTask, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finish the pending checksums after interruption."""
    root = scan_task.storage.abs_path
    (root / "a.txt").write_bytes(b"abc")
    (root / "b.txt").write_bytes(b"xyz")
    db = scan_task.db
    select_pending = db.scan_select_files_for_checksum_calculation
    selections = count()

    def interrupted_selection(scan: Scan) -> list[FileToChecksumCalculation]:
        if next(selections) == 0:
            return select_pending(scan, limit=1)
        message = "Interrupted after the first checksum batch"
        raise InterruptedError(message)

    with monkeypatch.context() as patches:
        patches.setattr(
            db, "scan_select_files_for_checksum_calculation", interrupted_selection,
        )
        with pytest.raises(InterruptedError, match="first checksum batch"):
            scan_task.start()

    checkpoint = db.scan_select_last_for_storage(scan_task.storage.id)
    assert checkpoint is not None
    assert checkpoint.finished_step == ScanStep.MATCH
    assert len(select_pending(checkpoint)) == 1
    assert not db.conn.in_transaction
    scan_task.resume()
    scan_task.apply()

    rows = db.conn.execute("SELECT rel_path, md5, sha256 FROM file ORDER BY rel_path")
    checksums = {path: (md5, sha256) for path, md5, sha256 in rows}
    assert checksums.keys() == {"a.txt", "b.txt"}
    assert all(md5 and sha256 for md5, sha256 in checksums.values())
    assert checksums["a.txt"] != checksums["b.txt"]


_DUPLICATE_CHECKSUM_WRITE = "Checksum IDs were written more than once"


@pytest.mark.parametrize(
    "updated_content", [b"longer content", b"xyz"], ids=["size", "mtime"],
)
def test_resume_rejects_file_changed_since_listing(
    scan_task: ScanTask, monkeypatch: pytest.MonkeyPatch, updated_content: bytes,
) -> None:
    """Keep the index unchanged when a pending file has changed."""
    file_path = scan_task.storage.abs_path / "file.txt"
    file_path.write_bytes(b"abc")
    db = scan_task.db

    def interrupt_before_checksums(_scan: Scan) -> list[FileToChecksumCalculation]:
        message = "Interrupted before calculating checksums"
        raise InterruptedError(message)

    with monkeypatch.context() as patches:
        patches.setattr(
            db,
            "scan_select_files_for_checksum_calculation",
            interrupt_before_checksums,
        )
        with pytest.raises(InterruptedError, match="before calculating checksums"):
            scan_task.start()

    checkpoint = db.scan_select_last_for_storage(scan_task.storage.id)
    assert checkpoint is not None
    assert checkpoint.finished_step == ScanStep.MATCH

    original = file_path.stat()
    file_path.write_bytes(updated_content)
    mtime_ns = original.st_mtime_ns
    if len(updated_content) == original.st_size:
        mtime_ns -= 10_000_000_000
    os.utime(file_path, ns=(original.st_atime_ns, mtime_ns))
    with pytest.raises(ScanFileChangedError, match="File changed since it was listed"):
        scan_task.resume()
    with pytest.raises(InvalidScanStateError):
        scan_task.apply()
    assert db.conn.execute("SELECT rel_path FROM file").fetchall() == []


def test_failed_uid_assignment_rolls_back_new_identities(scan_task: ScanTask) -> None:
    """Roll back newly created UIDs when linking them to scan items fails."""
    (scan_task.storage.abs_path / "new.txt").write_bytes(b"new content")
    db = scan_task.db
    db.conn.execute("""
        CREATE TEMP TRIGGER reject_uid_assignment
        BEFORE UPDATE OF uid ON scan_item
        WHEN NEW.uid IS NOT NULL
        BEGIN
            SELECT RAISE(ABORT, 'Simulated UID assignment failure');
        END
    """)

    with pytest.raises(sqlite3.IntegrityError, match="Simulated UID assignment"):
        scan_task.start()

    checkpoint = db.scan_select_last_for_storage(scan_task.storage.id)
    assert checkpoint is not None
    assert checkpoint.finished_step == ScanStep.COMPARE
    assert db.conn.execute("SELECT uid FROM uids").fetchall() == []
    assert not db.conn.in_transaction

    db.conn.execute("DROP TRIGGER reject_uid_assignment")
    scan_task.resume()
    scan_task.apply()
    assert db.conn.execute("SELECT uid FROM file").fetchall() == (
        db.conn.execute("SELECT uid FROM uids").fetchall()
    )
    assert db.conn.execute("SELECT rel_path FROM file").fetchall() == [("new.txt",)]


def test_failed_apply_preserves_previous_index(scan_task: ScanTask) -> None:
    """Roll back a failed apply and retry the same prepared scan."""
    root = scan_task.storage.abs_path
    removed = root / "removed.txt"
    removed.write_bytes(b"old content")
    scan_task.start()
    scan_task.apply()

    removed.unlink()
    (root / "added.txt").write_bytes(b"new content")
    scan_task.start()
    db = scan_task.db
    db.conn.execute("""
        CREATE TEMP TRIGGER reject_new_file
        BEFORE INSERT ON file
        WHEN NEW.rel_path = 'added.txt'
        BEGIN
            SELECT RAISE(ABORT, 'Simulated file insert failure');
        END
    """)

    with pytest.raises(sqlite3.IntegrityError, match="Simulated file insert failure"):
        scan_task.apply()

    checkpoint = db.scan_select_last_for_storage(scan_task.storage.id)
    assert checkpoint is not None
    assert checkpoint.finished_step == ScanStep.ASSIGN_UID
    rows = db.conn.execute("SELECT rel_path FROM file").fetchall()
    assert rows == [("removed.txt",)]
    assert not db.conn.in_transaction

    db.conn.execute("DROP TRIGGER reject_new_file")
    scan_task.apply()

    applied = db.scan_select_last_for_storage(scan_task.storage.id)
    assert applied is not None
    assert applied.id == checkpoint.id
    assert applied.finished_step == ScanStep.APPLY
    assert db.conn.execute("SELECT rel_path FROM file").fetchall() == [("added.txt",)]
    assert not db.conn.in_transaction


def test_unreadable_subdirectory_does_not_erase_known_file(
    scan_task: ScanTask, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep a known file indexed when directory traversal fails temporarily."""
    root = scan_task.storage.abs_path
    nested = root / "nested"
    nested.mkdir()
    known_file = nested / "known.txt"
    known_file.write_bytes(b"existing content")
    scan_task.start()
    scan_task.apply()

    conn = scan_task.db.conn
    query = "SELECT rel_path FROM file"
    expected_rows = [("nested/known.txt",)]
    assert conn.execute(query).fetchall() == expected_rows
    (nested / "new.txt").write_bytes(b"new content")
    read_error = PermissionError(errno.EACCES, "Simulated directory failure", nested)
    scandir = os.scandir

    def reject_nested_scan(path: str | Path) -> Iterator[os.DirEntry[str]]:
        if Path(path) == nested:
            raise read_error
        return scandir(path)

    with monkeypatch.context() as patches:
        patches.setattr(os, "scandir", reject_nested_scan)
        with pytest.raises(PermissionError, match="Simulated directory failure") as exc:
            scan_task.start()
        assert exc.value is read_error

    with pytest.raises(InvalidScanStateError):
        scan_task.apply()
    assert known_file.is_file()
    assert conn.execute(query).fetchall() == expected_rows

    scan_task.resume()
    scan_task.apply()
    assert set(conn.execute(query)) == {("nested/known.txt",), ("nested/new.txt",)}


def test_checksum_batches_do_not_rewrite_previous_sums(
    scan_task: ScanTask, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persist each checksum once when the queue spans multiple batches."""
    root = scan_task.storage.abs_path
    (root / "a.txt").write_bytes(b"abc")
    (root / "b.txt").write_bytes(b"xyz")
    db = scan_task.db
    select_pending = db.scan_select_files_for_checksum_calculation
    write_sums = db.scan_update_scan_items_sum
    written_ids: list[int] = []

    def select_one(scan: Scan) -> list[FileToChecksumCalculation]:
        return select_pending(scan, limit=1)

    def record_write(checksums: list[ScanItemCheckSums]) -> None:
        written_ids.extend(item.item_id for item in checksums)
        write_sums(checksums)

    monkeypatch.setattr(db, "scan_select_files_for_checksum_calculation", select_one)
    monkeypatch.setattr(db, "scan_update_scan_items_sum", record_write)
    scan_task.start()
    scan_task.apply()

    scan_items = db.conn.execute("SELECT id FROM scan_item")
    expected_ids = {item_id for (item_id,) in scan_items}
    assert set(written_ids) == expected_ids
    assert len(written_ids) == len(expected_ids), _DUPLICATE_CHECKSUM_WRITE
