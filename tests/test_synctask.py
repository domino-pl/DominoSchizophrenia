# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Verify synchronization plans and the execution of their actions."""

import csv
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath
from shutil import copystat
from threading import Barrier, Event
from typing import TYPE_CHECKING, cast
from unittest.mock import patch

import pytest

from src.db.sync import SyncFileInfo
from src.identity import Identity
from src.scantask import ScanTask
from src.synctask import (
    SyncActionOutCopy2Via,
    SyncExecutionError,
    SyncFSActionInCopy2LWD,
    SyncFSActionPriority,
    SyncFSActionRemoveDeleted,
    SyncFSBaseAction,
    SyncPlanError,
    SyncStorageType,
    SyncTask,
    SyncValidationError,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Future
    from typing import IO, BinaryIO

    from src.db import SchizophreniaDB


@pytest.mark.parametrize("has_destination", [False, True], ids=["remove", "copy"])
def test_csv_round_trip_preserves_action_fields(*, has_destination: bool) -> None:
    """Preserve the concrete action type and CSV-sensitive path components."""
    file_info = SyncFileInfo(
        id=2,
        uid=3,
        rel_path=PurePosixPath('data/a,"b".txt'),
        mtime="2026-11-18T00:00:00.000000+00:00",
        size=10,
        md5="md5",
        sha256="sha256",
        move_dst_fid=8,
    )
    action = (
        SyncFSActionInCopy2LWD(file_info, PurePosixPath('stage/c,"d".txt'))
        if has_destination
        else SyncFSActionRemoveDeleted(file_info)
    )
    expected_operation_id = 7
    row = action.dump(expected_operation_id)
    fields = next(csv.reader([row]))
    assert len(fields) == SyncFSBaseAction.ACTION_LINE_COLUMNS
    assert not fields[-1]

    operation_id, restored = SyncFSBaseAction.load_str(row)

    assert operation_id == expected_operation_id
    assert type(restored) is type(action)
    assert restored.file_info == file_info
    assert restored.src_storage_type is action.src_storage_type
    assert restored.dst_storage_type is action.dst_storage_type
    assert restored.dst_rel_path == action.dst_rel_path


@pytest.fixture
def sync_task(empty_db: SchizophreniaDB, tmp_path: Path) -> SyncTask:
    """Register independent source and destination directories.

    Returns:
        A task with its plan and staging directories in the temporary root.

    """
    for name in ("src", "dst", "via", "lwd"):
        (tmp_path / name).mkdir()
    for name in ("src", "dst"):
        root = tmp_path / name
        identity = Identity.new()
        identity.save_to_storage_dir(root)
        empty_db.insert_storage(name, root, identity)
        (root / "common.txt").write_bytes(b"common content")
    return SyncTask(
        empty_db, "src", "dst", tmp_path / "plan.csv",
        via=tmp_path / "via", lwd=tmp_path / "lwd",
    )


def _prepare_plan(task: SyncTask) -> None:
    for name in ("src", "dst"):
        scan = ScanTask(task.db, name)
        scan.start()
        scan.apply()
    task.prepare()


@pytest.fixture
def prepared_plan(sync_task: SyncTask) -> SyncTask:
    """Scan both storages and prepare a plan for a new source file.

    Returns:
        A task with a plan ready to initialize its staging directories.

    """
    (sync_task.src_storage.abs_path / "new.txt").write_bytes(b"new contents")
    _prepare_plan(sync_task)
    return sync_task


def test_load_plan_rejects_malformed_csv(sync_task: SyncTask) -> None:
    """Keep the CSV exception as the cause of the plan error."""
    sync_task.sync_plan_file.write_text('"unclosed\n', encoding="utf-8")

    with pytest.raises(SyncPlanError, match="Invalid storage CSV at line 1") as caught:
        sync_task.load_sync_plan()

    assert isinstance(caught.value.__cause__, csv.Error)


def test_load_plan_rejects_duplicate_action_id(prepared_plan: SyncTask) -> None:
    """Reject a plan that assigns the same operation ID twice."""
    _, actions = prepared_plan.load_sync_plan()
    plan_path = prepared_plan.sync_plan_file
    plan = plan_path.read_text(encoding="utf-8")
    duplicate = f"{actions[1].dump(1)}\n{prepared_plan.ACTION_LOG_SEPARATOR}"
    plan_path.write_text(
        plan.replace(prepared_plan.ACTION_LOG_SEPARATOR, duplicate, 1),
        encoding="utf-8",
    )

    with pytest.raises(SyncPlanError, match="Duplicate action number: 1"):
        prepared_plan.load_sync_plan()


def test_load_plan_rejects_mismatched_storage_identity(prepared_plan: SyncTask) -> None:
    """Reject a storage whose marker no longer identifies the planned location."""
    src = prepared_plan.src_storage.abs_path
    Identity.get_real_path(src).write_text(Identity.new().to_str(), encoding="utf-8")

    with pytest.raises(SyncPlanError, match="Identity mismatch for src storage"):
        prepared_plan.load_sync_plan()


def _completed_actions(task: SyncTask) -> list[str]:
    plan = task.sync_plan_file.read_text(encoding="utf-8")
    return plan.split(task.ACTION_LOG_SEPARATOR, 1)[1].splitlines()[1:]


@pytest.mark.parametrize(
    ("entry", "message"),
    [
        pytest.param("99,done", "Unknown action number 99", id="unknown-operation"),
        pytest.param("1,pending", "Invalid action status", id="invalid-status"),
        pytest.param(None, "separator in sync plan", id="missing-separator"),
    ],
)
def test_run_via_rejects_invalid_action_log_before_transfer(
    prepared_plan: SyncTask, entry: str | None, message: str,
) -> None:
    """Validate the full action log before writing any transferred file."""
    prepared_plan.init_via()
    path = prepared_plan.sync_plan_file
    plan = path.read_text(encoding="utf-8")
    if entry is None:
        plan = plan.removesuffix(f"{prepared_plan.ACTION_LOG_SEPARATOR}\n")
    else:
        plan += f"{entry}\n"
    path.write_text(plan, encoding="utf-8")

    with pytest.raises(SyncPlanError, match=message):
        prepared_plan.run_via()

    via = prepared_plan.via
    assert via is not None
    assert sorted(item.name for item in via.iterdir()) == [Identity.get_file_name()]
    assert path.read_text(encoding="utf-8") == plan


@pytest.mark.parametrize("show_progress", [False, True])
@pytest.mark.parametrize("parallel_checksums", [False, True])
def test_run_sync_copies_new_file_and_skips_completed_actions(
    prepared_plan: SyncTask,
    capsys: pytest.CaptureFixture[str],
    *,
    show_progress: bool,
    parallel_checksums: bool,
) -> None:
    """Copy through transfer and local staging and replay without new work."""
    prepared_plan.show_progress = show_progress
    if not parallel_checksums:
        prepared_plan.parallel_checksums = False
    prepared_plan.init_via()
    prepared_plan.init_lwd()
    with patch.object(
        ThreadPoolExecutor,
        "submit",
        autospec=True,
        side_effect=ThreadPoolExecutor.submit,
    ) as submit:
        prepared_plan.run_via()
    assert submit.call_count == (2 if parallel_checksums else 0)

    destination = prepared_plan.dst_storage.abs_path / "new.txt"
    assert not destination.exists()
    assert _completed_actions(prepared_plan) == ["1,done"]

    prepared_plan.run_lwd()
    assert destination.read_bytes() == b"new contents"
    assert _completed_actions(prepared_plan) == ["1,done", "2,done", "3,done"]
    expected_progress = [
        "[1/1 - 12 B / 12 B (100.0%)]: new.txt - 12 B / 12 B (100.0%)",
        "[1/2 - 12 B / 24 B (50.0%)]: new.txt - 12 B / 12 B (100.0%)",
        "[2/2 - 24 B / 24 B (100.0%)]: new.txt - 12 B / 12 B (100.0%)",
    ]
    assert capsys.readouterr().err.splitlines() == (
        expected_progress if show_progress else []
    )

    # Re-running an action would try to overwrite a destination or use a moved file.
    prepared_plan.run_via()
    prepared_plan.run_lwd()
    assert destination.read_bytes() == b"new contents"
    assert _completed_actions(prepared_plan) == ["1,done", "2,done", "3,done"]
    assert not capsys.readouterr().err


def test_sync_writes_next_block_while_hash_updates_are_pending(
    sync_task: SyncTask, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove that copying writes the next block before either hash task returns."""
    first_block = b"a" * (1024 * 1024)
    second_block = b"b"
    content = first_block + second_block
    source = sync_task.src_storage.abs_path / "new.txt"
    source.write_bytes(content)
    _prepare_plan(sync_task)
    sync_task.init_via()
    via = sync_task.via
    assert via is not None
    destination = via / source.name

    hashing_started = Barrier(3, timeout=5)
    second_block_written = Event()
    real_submit = ThreadPoolExecutor.submit
    real_open = Path.open

    def submit_hash(
        executor: ThreadPoolExecutor, update: Callable[[bytes], None], chunk: bytes,
    ) -> Future[None]:
        def update_hash() -> None:
            if chunk == first_block:
                hashing_started.wait()
                assert second_block_written.wait(timeout=5)
            update(chunk)

        return real_submit(executor, update_hash)

    def open_file(
        path: Path,
        mode: str = "r",
        *,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> IO[str] | IO[bytes]:
        if path != destination or mode != "wb":
            return cast(
                "IO[str] | IO[bytes]",
                real_open(
                    path, mode, encoding=encoding, errors=errors, newline=newline,
                ),
            )

        file: BinaryIO = real_open(path, "wb")
        real_write = file.write

        def write(chunk: bytes) -> int:
            if chunk != second_block:
                return real_write(chunk)
            hashing_started.wait()
            written = real_write(chunk)
            second_block_written.set()
            return written

        monkeypatch.setattr(file, "write", write)
        return file

    with monkeypatch.context() as patches:
        patches.setattr(ThreadPoolExecutor, "submit", submit_hash)
        patches.setattr(Path, "open", open_file)
        sync_task.run_via()

    assert second_block_written.is_set()
    assert destination.read_bytes() == content
    assert _completed_actions(sync_task) == ["1,done"]


def test_failed_action_is_not_marked_done(prepared_plan: SyncTask) -> None:
    """Leave a failed transfer pending so the next run can retry it."""
    prepared_plan.init_via()
    with (
        patch.object(
            SyncActionOutCopy2Via, "run", side_effect=SyncExecutionError("copy failed"),
        ),
        pytest.raises(SyncExecutionError, match="copy failed"),
    ):
        prepared_plan.run_via()

    assert _completed_actions(prepared_plan) == []
    prepared_plan.run_via()
    assert _completed_actions(prepared_plan) == ["1,done"]


def test_run_via_rejects_changed_source_mtime(prepared_plan: SyncTask) -> None:
    """Reject a source modified since planning before copying it."""
    prepared_plan.init_via()
    source = prepared_plan.src_storage.abs_path / "new.txt"
    original = source.stat()
    changed_mtime = int(original.st_mtime) - 120
    os.utime(source, (original.st_atime, changed_mtime))

    with pytest.raises(SyncValidationError, match="Source modification time changed"):
        prepared_plan.run_via()

    via = prepared_plan.via
    assert via is not None
    assert not (via / source.name).exists()
    assert _completed_actions(prepared_plan) == []


def test_run_via_retries_after_checksum_mismatch(prepared_plan: SyncTask) -> None:
    """Transfer corrected content after an earlier checksum mismatch."""
    prepared_plan.init_via()
    source = prepared_plan.src_storage.abs_path / "new.txt"
    via = prepared_plan.via
    assert via is not None
    staged_file = via / source.name
    expected_bytes = source.read_bytes()
    source_stat = source.stat()
    source_times = (source_stat.st_atime_ns, source_stat.st_mtime_ns)
    # Keep size and mtime stable so the transfer must detect the changed bytes.
    source.write_bytes(b"bad contents")
    os.utime(source, ns=source_times)

    with pytest.raises(SyncExecutionError, match="MD5 error"):
        prepared_plan.run_via()

    assert _completed_actions(prepared_plan) == []
    assert not staged_file.exists()
    source.write_bytes(expected_bytes)
    os.utime(source, ns=source_times)

    prepared_plan.run_via()
    assert staged_file.read_bytes() == expected_bytes
    assert _completed_actions(prepared_plan) == ["1,done"]


def test_run_via_rejects_existing_copy_with_wrong_content(
    prepared_plan: SyncTask,
) -> None:
    """Keep a conflicting file even when its size and timestamp match the plan."""
    prepared_plan.init_via()
    via = prepared_plan.via
    assert via is not None
    staged_file = via / "new.txt"
    staged_file.write_bytes(b"bad contents")
    copystat(prepared_plan.src_storage.abs_path / staged_file.name, staged_file)

    with pytest.raises(FileExistsError) as error:
        prepared_plan.run_via()

    assert error.value.args == (staged_file,)
    assert staged_file.read_bytes() == b"bad contents"
    assert _completed_actions(prepared_plan) == []


def test_sync_removes_file_present_only_at_destination(
    sync_task: SyncTask, capsys: pytest.CaptureFixture[str],
) -> None:
    """Remove a destination file that has no source counterpart."""
    destination = sync_task.dst_storage.abs_path / "obsolete.txt"
    destination.write_bytes(b"old contents")
    sync_task.show_progress = True
    _prepare_plan(sync_task)
    sync_task.init_via()
    sync_task.init_lwd()
    sync_task.run_via()
    sync_task.run_lwd()

    assert not destination.exists()
    deleted_staging = sync_task.storage_type_to_path(SyncStorageType.LWD_DELETED)
    assert deleted_staging is not None
    assert not (deleted_staging / destination.name).exists()
    assert _completed_actions(sync_task) == ["1,done", "2,done"]
    assert capsys.readouterr().err.splitlines() == [
        "[1/2 - 12 B / 24 B (50.0%)]: obsolete.txt - 12 B / 12 B (100.0%)",
        "[2/2 - 24 B / 24 B (100.0%)]: obsolete.txt - 12 B / 12 B (100.0%)",
    ]


def test_sync_reuses_destination_only_file_as_move_donor(sync_task: SyncTask) -> None:
    """Move reusable content to its new destination path instead of deleting it."""
    donor = sync_task.dst_storage.abs_path / "old-name.txt"
    target = sync_task.dst_storage.abs_path / "new-name.txt"
    source = sync_task.src_storage.abs_path / target.name
    payload = b"reusable contents"
    donor.write_bytes(payload)
    source.write_bytes(payload)
    copystat(donor, source)

    _prepare_plan(sync_task)
    sync_task.init_via()
    sync_task.init_lwd()
    sync_task.run_via()
    assert _completed_actions(sync_task) == []
    sync_task.run_lwd()

    assert not donor.exists()
    assert target.read_bytes() == payload
    assert _completed_actions(sync_task) == ["1,done", "2,done"]


def test_sync_copies_from_unchanged_destination_donor(sync_task: SyncTask) -> None:
    """Reuse an unchanged destination file and preserve it at its original path."""
    donor = sync_task.dst_storage.abs_path / "common.txt"
    source = sync_task.src_storage.abs_path / "copy.txt"
    target = sync_task.dst_storage.abs_path / source.name
    payload = donor.read_bytes()
    source.write_bytes(payload)
    copystat(donor, source)

    _prepare_plan(sync_task)
    _, actions = sync_task.load_sync_plan()
    assert [action.PRIORITY for action in actions.values()] == [
        SyncFSActionPriority.IN_CP2_LWD,
        SyncFSActionPriority.IN_MV2_DST,
    ]

    sync_task.init_lwd()
    sync_task.run_lwd()

    assert donor.read_bytes() == payload
    assert target.read_bytes() == payload


@pytest.mark.parametrize(
    "reuse_old_version", [False, True], ids=["discard-old", "move-old"],
)
def test_sync_replaces_changed_destination_file(
    sync_task: SyncTask, *, reuse_old_version: bool,
) -> None:
    """Replace the destination file and move its old contents if needed."""
    src = sync_task.src_storage.abs_path / "changed.txt"
    destination = sync_task.dst_storage.abs_path / "changed.txt"
    src.write_bytes(b"new file contents")
    destination.write_bytes(b"old contents")
    moved_destination = destination.with_name("moved.txt")
    if reuse_old_version:
        moved_source = src.with_name(moved_destination.name)
        moved_source.write_bytes(destination.read_bytes())
        copystat(destination, moved_source)

    _prepare_plan(sync_task)
    _, actions = sync_task.load_sync_plan()
    priorities = {action.PRIORITY for action in actions.values()}
    assert (SyncFSActionPriority.CHANGED_MV2_LWD in priorities) is not reuse_old_version
    assert (SyncFSActionPriority.CHANGED_RM in priorities) is not reuse_old_version
    assert (SyncFSActionPriority.MOVED_MV2_DST in priorities) is reuse_old_version

    sync_task.init_via()
    sync_task.init_lwd()
    sync_task.run_via()

    sync_task.run_lwd()
    assert destination.read_bytes() == b"new file contents"
    if reuse_old_version:
        assert moved_destination.read_bytes() == b"old contents"
    changed_staging = sync_task.storage_type_to_path(SyncStorageType.LWD_CHANGED)
    assert changed_staging is not None
    assert not (changed_staging / destination.name).exists()


def test_sync_swaps_destination_files_without_via(sync_task: SyncTask) -> None:
    """Stage both local donors before installing either at its final path."""
    src = sync_task.src_storage.abs_path
    dst = sync_task.dst_storage.abs_path
    left = dst / "left.txt"
    right = dst / "right.txt"
    left.write_bytes(b"left contents")
    right.write_bytes(b"right contents")
    (src / left.name).write_bytes(right.read_bytes())
    (src / right.name).write_bytes(left.read_bytes())
    copystat(right, src / left.name)
    copystat(left, src / right.name)

    _prepare_plan(sync_task)
    sync_task.init_lwd()
    sync_task.run_lwd()

    assert left.read_bytes() == b"right contents"
    assert right.read_bytes() == b"left contents"
    assert _completed_actions(sync_task) == ["1,done", "2,done", "3,done", "4,done"]


@pytest.mark.parametrize("move_donor", [False, True], ids=["copy-donor", "move-donor"])
def test_sync_reuses_local_content_with_different_mtime(
    sync_task: SyncTask, *, move_donor: bool,
) -> None:
    """Reuse matching content and install it with the source file's timestamp."""
    src = sync_task.src_storage.abs_path
    dst = sync_task.dst_storage.abs_path
    donor = dst / ("obsolete.txt" if move_donor else "common.txt")
    if move_donor:
        donor.write_bytes(b"reusable contents")
    target_source = src / "target.txt"
    target_source.write_bytes(donor.read_bytes())
    donor_mtime = donor.stat().st_mtime_ns
    source_mtime = int(donor.stat().st_mtime) - 120
    os.utime(target_source, (source_mtime, source_mtime))

    _prepare_plan(sync_task)
    sync_task.init_lwd()
    sync_task.run_lwd()
    target = dst / target_source.name
    assert target.read_bytes() == target_source.read_bytes()
    assert target.stat().st_mtime_ns == target_source.stat().st_mtime_ns
    if not move_donor:
        assert donor.stat().st_mtime_ns == donor_mtime


def test_sync_replaces_file_with_directory(sync_task: SyncTask) -> None:
    """Clear an obsolete file before creating a directory at the same path."""
    src = sync_task.src_storage.abs_path / "entry"
    dst = sync_task.dst_storage.abs_path / "entry"
    src.mkdir()
    (src / "child.txt").write_bytes(b"new contents")
    dst.write_bytes(b"old contents")

    _prepare_plan(sync_task)
    sync_task.init_via()
    sync_task.init_lwd()
    sync_task.run_via()
    sync_task.run_lwd()

    assert (dst / "child.txt").read_bytes() == b"new contents"
