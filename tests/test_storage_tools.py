# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Verify identity persistence and file utilities."""

import errno
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from pathlib import PureWindowsPath
from shutil import SameFileError
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from src.db.base import FetchEmptyError
from src.identity import Identity
from src.tools import CopyResult, copy_with_hashes, format_mtime, format_size

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from src.db import SchizophreniaDB


@pytest.mark.parametrize("field", ["abs_path", "identity"])
def test_storage_rejects_duplicate_path_or_identity(
    empty_db: SchizophreniaDB, tmp_path: Path, field: str,
) -> None:
    """Reject a new name that reuses an existing path or identity."""
    original = empty_db.insert_storage(
        "original", tmp_path / "original", Identity.new(),
    )
    empty_db.commit()
    path = original.abs_path if field == "abs_path" else tmp_path / "second"
    identity = original.identity if field == "identity" else Identity.new()

    with pytest.raises(sqlite3.IntegrityError, match=rf"UNIQUE.*storage\.{field}"):
        empty_db.insert_storage("second", path, identity)

    assert empty_db.select_storage("original") == original
    with pytest.raises(FetchEmptyError):
        empty_db.select_storage("second")


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        (PureWindowsPath(r"C:\data\storage"), "C:/data/storage"),
        (PureWindowsPath(r"\\server\share\storage"), "//server/share/storage"),
    ],
)
def test_storage_persists_windows_paths_with_posix_separators(
    empty_db: SchizophreniaDB, path: PureWindowsPath, expected: str,
) -> None:
    """Store drives and network shares with forward slashes on every platform."""
    empty_db.insert_storage("test", path, Identity.new())
    assert empty_db.conn.execute("SELECT abs_path FROM storage").fetchall() == [
        (expected,),
    ]


def test_identity_round_trip(tmp_path: Path) -> None:
    """Read the same UUID from its database value and the storage file."""
    identity = Identity("12345678-1234-5678-9abc-def012345678")
    identity.save_to_storage_dir(tmp_path)

    assert (tmp_path / "schizophrenia.identity").read_text(encoding="utf-8") == (
        identity.to_str()
    )
    assert Identity.read_from_storage_dir(tmp_path) == identity
    assert Identity.from_db(identity.to_db()) == identity


def test_identity_reads_marker_with_surrounding_whitespace(tmp_path: Path) -> None:
    """Accept a valid marker with spaces and a trailing newline."""
    identity = Identity.new()
    marker = Identity.get_real_path(tmp_path)
    marker.write_text(f"  {identity.to_str()}\n", encoding="utf-8")

    assert Identity.read_from_storage_dir(tmp_path) == identity


def test_identity_rejects_invalid_uuid() -> None:
    """Reject invalid identity data when decoding the database value."""
    with pytest.raises(ValueError, match="badly formed hexadecimal UUID string"):
        Identity.from_db("invalid")


def test_save_identity_does_not_overwrite(tmp_path: Path) -> None:
    """Keep the original identity when a second write is attempted."""
    original = Identity("12345678-1234-5678-9abc-def012345678")
    original.save_to_storage_dir(tmp_path)
    replacement = Identity("87654321-1234-5678-9abc-def012345678")

    with pytest.raises(FileExistsError):
        replacement.save_to_storage_dir(tmp_path)

    assert Identity.read_from_storage_dir(tmp_path) == original


def test_identity_can_be_saved_again_after_partial_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep the marker path available when a write fails partway through."""
    identity = Identity.new()
    marker = Identity.get_real_path(tmp_path)
    real_open = type(marker).open
    write_error = OSError(errno.ENOSPC, "Simulated full storage", str(marker))

    @contextmanager
    def open_with_failing_write(
        path: Path, mode: str, *, encoding: str,
    ) -> Generator[Mock]:
        with real_open(path, mode, encoding=encoding) as file:
            def write_prefix(text: str) -> int:
                file.write(text[:8])
                raise write_error

            yield Mock(write=Mock(side_effect=write_prefix))

    with monkeypatch.context() as patches:
        patches.setattr(type(marker), "open", open_with_failing_write)
        with pytest.raises(OSError, match="Simulated full storage") as error:
            identity.save_to_storage_dir(tmp_path)
        assert error.value is write_error

    assert not marker.exists()
    identity.save_to_storage_dir(tmp_path)
    assert Identity.read_from_storage_dir(tmp_path) == identity


@pytest.mark.parametrize(
    ("size", "expected"), [(0, "0 B"), (1023, "1023 B"), (1024, "1.00 KiB")],
)
def test_format_size_at_binary_boundary(size: int, expected: str) -> None:
    """Switch from bytes to KiB at 1024 bytes."""
    assert format_size(size) == expected


def test_format_mtime_truncates_to_utc_microseconds() -> None:
    """Keep microseconds and discard the remaining nanoseconds."""
    file_stat = Mock(st_mtime_ns=1_234_567_899)
    assert format_mtime(file_stat) == "1970-01-01T00:00:01.234567+00:00"


@pytest.mark.parametrize("parallel", [False, True], ids=["sequential", "parallel"])
def test_copy_with_hashes_preserves_content_and_mtime(
    tmp_path: Path, *, parallel: bool,
) -> None:
    """Copy over multiple chunks and preserve the source timestamp."""
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.write_bytes(b"abc")
    destination.write_bytes(b"older contents")
    timestamp = 1_700_000_000
    os.utime(source, (timestamp, timestamp))

    written_chunks: list[int] = []
    pool = ThreadPoolExecutor(max_workers=2) if parallel else nullcontext()
    with pool as executor:
        result = copy_with_hashes(
            source, destination, chunk_size=2, preserve_mtime=True,
            on_chunk=written_chunks.append,
            executor=executor,
        )
    assert written_chunks == [2, 1]

    assert result == CopyResult(
        size=3,
        md5="900150983cd24fb0d6963f7d28e17f72",
        sha256="ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    )
    assert destination.read_bytes() == b"abc"
    assert destination.stat().st_mtime_ns == source.stat().st_mtime_ns


def test_copy_to_same_path_preserves_source(tmp_path: Path) -> None:
    """Reject copying a file onto itself before truncating it."""
    source = tmp_path / "source"
    source.write_bytes(b"abc")

    with pytest.raises(SameFileError):
        copy_with_hashes(source, source)

    assert source.read_bytes() == b"abc"


def test_copy_to_hard_link_preserves_source(tmp_path: Path) -> None:
    """Reject a distinct path that points to the same underlying file."""
    source = tmp_path / "source"
    alias = tmp_path / "alias"
    source.write_bytes(b"abc")
    try:
        alias.hardlink_to(source)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"Cannot create a hard link: {error}")

    with pytest.raises(SameFileError):
        copy_with_hashes(source, alias)

    assert source.read_bytes() == b"abc"


def test_copy_empty_file_returns_empty_digests(tmp_path: Path) -> None:
    """Create an empty copy and return the digests of zero bytes."""
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.touch()

    result = copy_with_hashes(source, destination)

    assert destination.read_bytes() == b""
    assert result == CopyResult(
        size=0,
        md5="d41d8cd98f00b204e9800998ecf8427e",
        sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    )


@pytest.mark.parametrize("chunk_size", [0, -1], ids=["zero", "negative"])
def test_copy_rejects_nonpositive_chunks_without_overwriting(
    tmp_path: Path, chunk_size: int,
) -> None:
    """Reject an invalid chunk size before touching an existing destination."""
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.write_bytes(b"new contents")
    destination.write_bytes(b"existing contents")

    with pytest.raises(ValueError, match="chunk_size must be positive"):
        copy_with_hashes(source, destination, chunk_size=chunk_size)

    assert destination.read_bytes() == b"existing contents"
