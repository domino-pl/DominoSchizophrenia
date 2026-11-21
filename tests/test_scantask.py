# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Verify scan state handling and file listing."""

import errno
import stat
from contextlib import nullcontext
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from src import scantask
from src.db import SchizophreniaDB
from src.db.scan import Scan, ScanStep
from src.scantask import InvalidScanStateError, ScanTask, UnsupportedFileTypeError

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.mark.parametrize(
    ("attributes", "expected"),
    [
        (stat.FILE_ATTRIBUTE_SYSTEM, True),
        (stat.FILE_ATTRIBUTE_HIDDEN, False),
    ],
)
def test_is_system_directory_on_windows(
    monkeypatch: pytest.MonkeyPatch,
    attributes: int,
    *,
    expected: bool,
) -> None:
    """Read the SYSTEM flag only on Windows and without following symlinks."""
    monkeypatch.setattr(scantask, "sys", SimpleNamespace(platform="win32"))
    directory = Mock(spec=Path)
    directory.name = "$RECYCLE.BIN"
    directory.stat.return_value = SimpleNamespace(st_file_attributes=attributes)

    check_directory = getattr(ScanTask, "_is_system_directory", None)
    assert check_directory is not None
    assert check_directory(directory) is expected
    directory.stat.assert_called_once_with(follow_symlinks=False)


@pytest.mark.parametrize(
    ("name", "result", "expected"),
    [
        ("$RECYCLE.BIN", b"\x00\x00\x00\x04", True),
        ("System Volume Information", b"\x00\x00\x00\x02", False),
        ("$RECYCLE.BIN", b"\x04", False),
        ("$RECYCLE.BIN", OSError(errno.ENODATA, "No attribute"), False),
        ("$RECYCLE.BIN", OSError(errno.ENOTSUP, "Unsupported"), False),
        ("ordinary", b"\x00\x00\x00\x04", False),
    ],
)
def test_is_system_directory_on_linux(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    result: bytes | OSError,
    *,
    expected: bool,
) -> None:
    """Decode supported NTFS attributes and skip the lookup for ordinary names."""
    monkeypatch.setattr(scantask, "sys", SimpleNamespace(platform="linux"))
    getxattr = Mock(side_effect=[result])
    monkeypatch.setattr(scantask, "os", SimpleNamespace(getxattr=getxattr))
    directory = Path(name)
    check_directory = getattr(ScanTask, "_is_system_directory", None)
    assert check_directory is not None

    assert check_directory(directory) is expected
    if name == "ordinary":
        getxattr.assert_not_called()
    else:
        getxattr.assert_called_once_with(
            directory,
            "system.ntfs_attrib_be",
            follow_symlinks=False,
        )


def test_linux_system_directory_propagates_io_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Propagate unexpected NTFS attribute read failures."""
    monkeypatch.setattr(scantask, "sys", SimpleNamespace(platform="linux"))
    error = OSError(errno.EIO, "I/O error")
    monkeypatch.setattr(
        scantask,
        "os",
        SimpleNamespace(getxattr=Mock(side_effect=error)),
    )
    check_directory = getattr(ScanTask, "_is_system_directory", None)
    assert check_directory is not None

    with pytest.raises(OSError, match="I/O error") as caught:
        check_directory(Path("$RECYCLE.BIN"))
    assert caught.value is error


def test_list_skips_adjacent_system_directories(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Prune both adjacent SYSTEM directories before walking their contents."""
    protected_names = ["$RECYCLE.BIN", "System Volume Information"]
    remaining_dir = tmp_path / "user" / "$RECYCLE.BIN"
    remaining_dir.mkdir(parents=True)
    (remaining_dir / "file.txt").touch()
    for name in protected_names:
        directory = tmp_path / name
        directory.mkdir()
        (directory / "file.txt").touch()

    original_walk = Path.walk

    def walk(
        path: Path,
        **_options: object,
    ) -> Iterator[tuple[Path, list[str], list[str]]]:
        dirnames = [*protected_names, "user"]
        yield path, dirnames, []
        for dirname in dirnames:
            yield from original_walk(path / dirname)

    def getxattr(
        directory_path: Path, _attribute: str, *, follow_symlinks: bool,
    ) -> bytes:
        assert not follow_symlinks
        attributes = (
            stat.FILE_ATTRIBUTE_SYSTEM if directory_path.parent == tmp_path else 0
        )
        return attributes.to_bytes(4, "big")

    monkeypatch.setattr(scantask, "sys", SimpleNamespace(platform="linux"))
    monkeypatch.setattr(scantask, "os", SimpleNamespace(getxattr=getxattr))
    db = Mock(spec=SchizophreniaDB)
    db.write_transaction.side_effect = nullcontext
    db.select_storage.return_value.abs_path = tmp_path
    db.scan_select_last_for_storage.return_value = Scan(
        id=1,
        storage_id=1,
        finished_step=ScanStep.INSERT,
    )
    task = ScanTask(db, "test")
    for stage in ("_match", "_collect_sums", "_compare", "_assign_uid"):
        monkeypatch.setattr(task, stage, Mock())

    with monkeypatch.context() as patches:
        patches.setattr(Path, "walk", walk)
        task.resume()

    db.scan_insert_scan_items.assert_called_once()
    assert [item.rel_path for item in db.scan_insert_scan_items.call_args.args[1]] == [
        PurePosixPath("user/$RECYCLE.BIN/file.txt"),
    ]


def test_unsupported_file_type_error() -> None:
    """Expose the rejected path and a diagnostic as a filesystem error."""
    file_path = Path("storage") / "unsupported"
    error = UnsupportedFileTypeError(file_path)

    assert isinstance(error, OSError)
    assert error.file_path == file_path
    assert str(error) == (
        f"Unsupported file type: {file_path}; expected a regular file without symlinks."
    )


@pytest.mark.parametrize(
    ("actual_step", "expected_steps", "message"),
    [
        (None, (ScanStep.INSERT,), "Invalid scan state: expected INSERT; got no scan."),
        (
            ScanStep.CHECKSUM,
            (ScanStep.COMPARE,),
            "Invalid scan state: expected COMPARE; got CHECKSUM.",
        ),
        (
            ScanStep.INSERT,
            (ScanStep.APPLY, ScanStep.ABORT),
            "Invalid scan state: expected APPLY or ABORT; got INSERT.",
        ),
    ],
)
def test_invalid_scan_state_message(
    actual_step: ScanStep | None,
    expected_steps: tuple[ScanStep, ...],
    message: str,
) -> None:
    """Expose actual and expected stages while preserving RuntimeError ancestry."""
    error = InvalidScanStateError(
        actual_step=actual_step,
        expected_steps=expected_steps,
    )
    assert isinstance(error, RuntimeError)
    assert str(error) == message
    assert error.actual_step == actual_step
    assert error.expected_steps == expected_steps


@pytest.mark.parametrize(
    ("method_name", "expected_steps"),
    [
        ("_list", (ScanStep.INSERT,)),
        ("_match", (ScanStep.LIST,)),
        ("_collect_sums", (ScanStep.MATCH,)),
        ("_compare", (ScanStep.CHECKSUM,)),
        ("_assign_uid", (ScanStep.COMPARE,)),
        ("apply", (ScanStep.ASSIGN_UID,)),
        ("_insert_new_scan", (ScanStep.APPLY, ScanStep.ABORT)),
    ],
)
@pytest.mark.parametrize("missing_scan", [False, True])
def test_invalid_stage_prevents_mutations(
    method_name: str,
    expected_steps: tuple[ScanStep, ...],
    *,
    missing_scan: bool,
) -> None:
    """Reject missing or incompatible stages before any writes or commits."""
    db = Mock(spec=SchizophreniaDB)
    actual_step = next(step for step in ScanStep if step not in expected_steps)
    db.scan_select_last_for_storage.return_value = (
        None if missing_scan else Scan(id=1, storage_id=1, finished_step=actual_step)
    )
    task = ScanTask(db, "test")
    db.reset_mock()

    if method_name == "_insert_new_scan" and missing_scan:
        db.write_transaction.side_effect = nullcontext
        getattr(task, method_name)()
        db.write_transaction.assert_called_once_with()
        db.scan_insert.assert_called_once()
        return

    with pytest.raises(InvalidScanStateError) as caught:
        getattr(task, method_name)()

    assert caught.value.actual_step == (None if missing_scan else actual_step)
    assert caught.value.expected_steps == expected_steps
    assert db.method_calls == [
        ("scan_select_last_for_storage", (task.storage.id,), {}),
    ]
