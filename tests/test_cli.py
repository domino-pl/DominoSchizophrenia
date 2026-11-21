# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Verify storage registration, argument dispatch, and CLI database cleanup."""

import sqlite3
import sys
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

import schizophrenia
from schizophrenia import ScanAction, SyncAction
from src.db.base import FetchEmptyError
from src.db.scan import ScanStep
from src.identity import Identity
from src.scantask import InvalidScanStateError

if TYPE_CHECKING:
    from pathlib import Path

    from src.db import SchizophreniaDB


def test_add_storage_registers_the_identity_written_to_disk(
    empty_db: SchizophreniaDB, tmp_path: Path,
) -> None:
    """Persist the same identity in SQLite and the registered directory."""
    schizophrenia.add_storage(empty_db, "test", tmp_path)

    registered = empty_db.select_storage("test")
    assert registered.abs_path == tmp_path
    assert registered.identity == Identity.read_from_storage_dir(tmp_path)


def test_duplicate_storage_name_leaves_no_identity_marker(
    empty_db: SchizophreniaDB, tmp_path: Path,
) -> None:
    """Reject a duplicate name without leaving an unregistered identity file."""
    original_path = tmp_path / "original"
    original_path.mkdir()
    schizophrenia.add_storage(empty_db, "test", original_path)
    original = empty_db.select_storage("test")
    duplicate_path = tmp_path / "duplicate"
    duplicate_path.mkdir()

    with pytest.raises(sqlite3.IntegrityError, match=r"UNIQUE.*storage\.name"):
        schizophrenia.add_storage(empty_db, "test", duplicate_path)

    assert empty_db.select_storage("test") == original
    marker = Identity.get_real_path(duplicate_path)
    assert not marker.exists(), "Duplicate registration left a marker"


def test_existing_marker_rolls_back_storage_registration(
    empty_db: SchizophreniaDB, tmp_path: Path,
) -> None:
    """Keep the existing marker and roll back the new registration."""
    existing = Identity.new()
    existing.save_to_storage_dir(tmp_path)

    with pytest.raises(FileExistsError):
        schizophrenia.add_storage(empty_db, "test", tmp_path)

    assert Identity.read_from_storage_dir(tmp_path) == existing
    with pytest.raises(FetchEmptyError):
        empty_db.select_storage("test")


def test_failed_marker_write_rolls_back_storage_registration(
    empty_db: SchizophreniaDB, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Roll back the inserted row when the marker write fails."""
    marker = Identity.get_real_path(tmp_path)

    def fail_during_write(_identity: Identity) -> str:
        assert empty_db.select_storage("test").abs_path == tmp_path
        assert marker.is_file()
        msg = "marker write failed"
        raise OSError(msg)

    monkeypatch.setattr(Identity, "to_db", lambda identity: str(identity.uuid))
    monkeypatch.setattr(Identity, "to_str", fail_during_write)

    with pytest.raises(OSError, match="marker write failed"):
        schizophrenia.add_storage(empty_db, "test", tmp_path)

    with pytest.raises(FetchEmptyError):
        empty_db.select_storage("test")


@pytest.mark.parametrize(
    ("arguments", "action"),
    [
        (["scan", "resume", "test"], ScanAction.RESUME),
        (
            ["sync", "prepare", "src", "dst", "plan.csv"],
            SyncAction.PREPARE,
        ),
    ],
)
def test_cli_dispatches_typed_actions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    arguments: list[str],
    action: ScanAction | SyncAction,
) -> None:
    """Pass the parsed enum to the command and close the database."""
    monkeypatch.chdir(tmp_path)
    command = arguments[0]
    db_factory = Mock()
    handler = Mock()
    monkeypatch.setattr(sys, "argv", ["schizophrenia.py", *arguments])
    monkeypatch.setattr(schizophrenia, "SchizophreniaDB", db_factory)
    monkeypatch.setattr(schizophrenia, command, handler)

    assert schizophrenia.main() == 0

    handler.assert_called_once()
    if isinstance(action, ScanAction):
        assert handler.call_args.args[2] is action
    else:
        assert handler.call_args.args[4] is action
        assert handler.call_args.args[3] == tmp_path / "plan.csv"
    db_factory.return_value.close.assert_called_once_with()


def test_cli_makes_optional_sync_paths_absolute(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Forward the explicitly supplied staging paths as absolute paths."""
    monkeypatch.chdir(tmp_path)
    db_factory = Mock()
    handler = Mock()
    arguments = [
        "schizophrenia.py", "sync", "prepare", "src", "dst", "plan.csv",
        "--via", "via dir", "--lwd", "work dir", "--progress",
    ]
    monkeypatch.setattr(sys, "argv", arguments)
    monkeypatch.setattr(schizophrenia, "SchizophreniaDB", db_factory)
    monkeypatch.setattr(schizophrenia, "sync", handler)

    assert schizophrenia.main() == 0

    handler.assert_called_once_with(
        db_factory.return_value, "src", "dst", tmp_path / "plan.csv",
        SyncAction.PREPARE, tmp_path / "via dir", tmp_path / "work dir",
        progress=True,
    )


@pytest.mark.parametrize(
    ("command", "operands"),
    [("scan", ["test"]), ("sync", ["src", "dst", "plan.csv"])],
)
def test_cli_rejects_unknown_action_before_opening_db(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    operands: list[str],
) -> None:
    """Reject bad action names during argument parsing."""
    db_factory = Mock()
    monkeypatch.setattr(
        sys, "argv", ["schizophrenia.py", command, "unknown", *operands],
    )
    monkeypatch.setattr(schizophrenia, "SchizophreniaDB", db_factory)

    with pytest.raises(SystemExit) as caught:
        schizophrenia.main()

    argparse_error_status = 2
    assert caught.value.code == argparse_error_status
    db_factory.assert_not_called()


def test_cli_closes_database_when_handler_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Close the database even when the command raises."""
    db_factory = Mock()
    monkeypatch.setattr(sys, "argv", ["schizophrenia.py", "scan", "start", "test"])
    monkeypatch.setattr(schizophrenia, "SchizophreniaDB", db_factory)
    monkeypatch.setattr(
        schizophrenia, "scan", Mock(side_effect=RuntimeError("scan failed")),
    )

    with pytest.raises(RuntimeError, match="scan failed"):
        schizophrenia.main()

    db_factory.return_value.close.assert_called_once_with()


def test_cli_registers_and_applies_scan_across_invocations(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """Use a persisted registration and prepared scan in later commands."""
    monkeypatch.chdir(tmp_path)
    storage = tmp_path / "my storage"
    storage.mkdir()
    file_path = storage / "notes.txt"
    file_path.write_bytes(b"notes")

    for arguments in (
        ["add_storage", "test", storage.name],
        ["-p", "scan", "start", "test"],
        ["scan", "apply", "test"],
    ):
        monkeypatch.setattr(sys, "argv", ["schizophrenia.py", *arguments])
        assert schizophrenia.main() == 0

    assert capsys.readouterr().err.splitlines() == [
        "[1/1 - 5 B / 5 B (100.0%)]: notes.txt - 5 B / 5 B (100.0%)",
    ]

    db = schizophrenia.SchizophreniaDB()
    try:
        registered = db.select_storage("test")
        assert registered.abs_path == storage
        assert registered.identity == Identity.read_from_storage_dir(storage)
        scan = db.scan_select_last_for_storage(registered.id)
        assert scan is not None
        assert scan.finished_step is ScanStep.APPLY
        files = db.conn.execute(
            "SELECT rel_path, size FROM file WHERE storage_id = ?", (registered.id,),
        )
        assert files.fetchall() == [(file_path.name, file_path.stat().st_size)]
    finally:
        db.close()


def test_scan_abort_allows_starting_a_new_scan(
    empty_db: SchizophreniaDB, tmp_path: Path,
) -> None:
    """Abort a prepared scan and permit a fresh scan for the same storage."""
    schizophrenia.add_storage(empty_db, "test", tmp_path)
    (tmp_path / "pending.txt").write_bytes(b"pending content")
    storage_id = empty_db.select_storage("test").id
    schizophrenia.scan(empty_db, "test", ScanAction.START)
    prepared = empty_db.scan_select_last_for_storage(storage_id)
    assert prepared is not None
    assert prepared.finished_step is ScanStep.ASSIGN_UID

    schizophrenia.scan(empty_db, "test", ScanAction.ABORT)

    aborted = empty_db.scan_select_last_for_storage(storage_id)
    assert aborted is not None
    assert aborted.id == prepared.id
    assert aborted.finished_step is ScanStep.ABORT
    assert empty_db.conn.execute("SELECT id FROM file").fetchall() == []
    with pytest.raises(InvalidScanStateError, match="got ABORT"):
        schizophrenia.scan(empty_db, "test", ScanAction.RESUME)
    with pytest.raises(InvalidScanStateError, match="got ABORT"):
        schizophrenia.scan(empty_db, "test", ScanAction.APPLY)

    schizophrenia.scan(empty_db, "test", ScanAction.START)
    restarted = empty_db.scan_select_last_for_storage(storage_id)
    assert restarted is not None
    assert restarted.id != aborted.id
    assert restarted.finished_step is ScanStep.ASSIGN_UID
