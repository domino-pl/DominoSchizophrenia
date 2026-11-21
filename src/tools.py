# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Common utils."""

import os
import sys
from datetime import UTC, datetime, timedelta
from functools import partial
from hashlib import md5, sha256
from os import stat_result
from shutil import SameFileError
from time import monotonic
from typing import TYPE_CHECKING, NamedTuple, override

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from concurrent.futures import ThreadPoolExecutor
    from pathlib import Path
    from typing import BinaryIO


def raise_walk_error(error: OSError) -> None:
    """Propagate a directory read error to stop walking the tree."""
    raise error


def format_size(size_bytes: int) -> str:
    """Format a byte count using binary units from B through PiB.

    Args:
        size_bytes: Size in bytes to format.

    Returns:
        The size with its unit, using whole numbers for bytes and two decimal
        places for larger units. Values beyond PiB remain expressed in PiB.

    Raises:
        AssertionError: If unit selection unexpectedly finishes without returning.

    """
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")

    size = float(size_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:  # ruff: ignore[magic-value-comparison]
            return f"{int(size)} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024

    msg = "Unit selection unexpectedly finished without returning"
    raise AssertionError(msg)


class Progress:
    """Accept progress events without displaying them."""

    def start_file(self, name: str, size: int) -> None:
        """Ignore a file starting."""

    def advance(self, size: int) -> None:
        """Ignore processed bytes."""

    def finish_file(self) -> None:
        """Ignore a completed file."""


NO_PROGRESS = Progress()


class FileProgress(Progress):
    """Report cumulative and per-file byte counts to standard error."""

    UPDATE_INTERVAL = 0.2

    def __init__(self, file_count: int, total_size: int) -> None:
        """Initialize a new run from the files that remain to be processed."""
        self.file_count = file_count
        self.total_size = total_size
        self.total_done = 0
        self.file_number = 0
        self.file_size = 0
        self.file_done = 0
        self.file_name = ""
        self.last_update = 0.0

    @override
    def start_file(self, name: str, size: int) -> None:
        """Begin reporting a file using its planned size."""
        self.file_number += 1
        self.file_name = name
        self.file_size = size
        self.file_done = 0
        self.last_update = monotonic()

    @override
    def advance(self, size: int) -> None:
        """Add processed bytes and refresh the display periodically."""
        self.file_done += size
        self.total_done += size
        if (
            self.file_done < self.file_size
            and monotonic() - self.last_update >= self.UPDATE_INTERVAL
        ):
            self._report()

    @override
    def finish_file(self) -> None:
        """Report the final byte counts after processing a file."""
        self._report()

    def _report(self) -> None:
        total_percent = self.total_done / self.total_size if self.total_size else 1
        file_percent = self.file_done / self.file_size if self.file_size else 1
        sys.stderr.write(
            f"[{self.file_number}/{self.file_count} - "
            f"{format_size(self.total_done)} / {format_size(self.total_size)} "
            f"({total_percent:.1%})]: {self.file_name} - "
            f"{format_size(self.file_done)} / {format_size(self.file_size)} "
            f"({file_percent:.1%})\n",
        )
        sys.stderr.flush()
        self.last_update = monotonic()


def format_mtime(stat: stat_result) -> str:
    """Format a file's modification time as an ISO 8601 timestamp in UTC.

    Args:
        stat: File metadata containing the modification time in nanoseconds.

    Returns:
        A timestamp truncated to microseconds with the ``+00:00`` UTC offset.

    """
    seconds, nanoseconds = divmod(stat.st_mtime_ns, 1_000_000_000)
    timestamp = datetime.fromtimestamp(seconds, tz=UTC) + timedelta(
        microseconds=nanoseconds // 1000,
    )
    return timestamp.isoformat(timespec="microseconds")


class FileMetadataChangedError(OSError):
    """Indicate that a file no longer has its expected metadata."""

    def __init__(self, file_path: Path) -> None:
        """Identify the file whose metadata changed."""
        super().__init__(f"File metadata changed: {file_path}")


def file_checksums(
    path: Path,
    *,
    expected_metadata: tuple[int, str],
    chunk_size: int,
    progress: Progress,
    executor: ThreadPoolExecutor | None = None,
) -> tuple[str, str]:
    """Read a file once, optionally hashing it in parallel.

    Args:
        path: File to read.
        expected_metadata: Listed size in bytes and ISO-formatted mtime.
        chunk_size: Maximum number of bytes read per block.
        progress: Reporter for hashed bytes.
        executor: Pool with two workers for parallel hashing, if enabled.

    Returns:
        The hexadecimal MD5 and SHA-256 digests.

    Raises:
        FileMetadataChangedError: If the file differs from the expected metadata
            or changes during reading.
        ValueError: If chunk_size is not positive.

    """
    if chunk_size <= 0:
        msg = "chunk_size must be positive"
        raise ValueError(msg)

    with path.open("rb") as file:
        before = os.fstat(file.fileno())
        if (before.st_size, format_mtime(before)) != expected_metadata:
            raise FileMetadataChangedError(path)

        chunks = iter(partial(file.read, chunk_size), b"")
        result = _hash_chunks(chunks, progress.advance, executor)
        after = os.fstat(file.fileno())

    if after.st_size != before.st_size or after.st_mtime_ns != before.st_mtime_ns:
        raise FileMetadataChangedError(path)
    return result.md5, result.sha256


def _hash_chunks(
    chunks: Iterator[bytes],
    on_chunk: Callable[[int], None],
    executor: ThreadPoolExecutor | None,
) -> CopyResult:
    """Hash sequentially or dispatch to the supplied workers.

    Returns:
        The processed byte count and hexadecimal MD5 and SHA-256 digests.

    """
    if executor is not None:
        return _hash_chunks_parallel(chunks, on_chunk, executor)

    md5_hash = md5()  # ruff: ignore[hashlib-insecure-hash-function]
    sha256_hash = sha256()
    size = 0
    for chunk in chunks:
        md5_hash.update(chunk)
        sha256_hash.update(chunk)
        size += len(chunk)
        on_chunk(len(chunk))
    return CopyResult(size, md5_hash.hexdigest(), sha256_hash.hexdigest())


def _hash_chunks_parallel(
    chunks: Iterator[bytes],
    on_chunk: Callable[[int], None],
    executor: ThreadPoolExecutor,
) -> CopyResult:
    """Read one block ahead while two workers hash the current block.

    Returns:
        The processed byte count and hexadecimal MD5 and SHA-256 digests.

    """
    md5_hash = md5()  # ruff: ignore[hashlib-insecure-hash-function]
    sha256_hash = sha256()
    size = 0
    chunk = next(chunks, b"")
    while chunk:
        md5_update = executor.submit(md5_hash.update, chunk)
        sha256_update = executor.submit(sha256_hash.update, chunk)
        next_chunk = next(chunks, b"")
        md5_update.result()
        sha256_update.result()
        size += len(chunk)
        on_chunk(len(chunk))
        chunk = next_chunk
    return CopyResult(size, md5_hash.hexdigest(), sha256_hash.hexdigest())


class CopyResult(NamedTuple):
    """Store the copied byte count and hexadecimal MD5 and SHA-256 digests."""

    size: int
    md5: str
    sha256: str


def _write_chunks(chunks: Iterator[bytes], destination: BinaryIO) -> Iterator[bytes]:
    """Write blocks to the destination.

    Yields:
        Each written block, ready to be hashed.

    """
    for chunk in chunks:
        destination.write(chunk)
        yield chunk


def copy_with_hashes(  # ruff: ignore[too-many-arguments]
    source: Path,
    destination: Path,
    *,
    chunk_size: int = 1024 * 1024,
    preserve_mtime: bool = False,
    on_chunk: Callable[[int], None] = NO_PROGRESS.advance,
    executor: ThreadPoolExecutor | None = None,
) -> CopyResult:
    """Copy a file while computing its size and MD5 and SHA-256 digests.

    Args:
        source: Path to the file to read.
        destination: Path to the output file, created or overwritten.
        chunk_size: Number of bytes to read at a time.
        preserve_mtime: Whether to retain the source's modification time on
            the destination, preserving the destination's access time.
        on_chunk: Callback receiving the number of bytes written per chunk.
        executor: Pool with two workers to hash while reading and writing.

    Returns:
        The copied byte count and hexadecimal MD5 and SHA-256 digests.

    Raises:
        SameFileError: If source and destination refer to the same file.
        ValueError: If chunk_size is not positive.

    """
    if chunk_size <= 0:
        msg = "chunk_size must be positive"
        raise ValueError(msg)

    if destination.exists() and source.samefile(destination):
        msg = "Source and destination refer to the same file"
        raise SameFileError(msg)

    with source.open("rb") as src, destination.open("wb") as dst:
        source_stat = os.fstat(src.fileno())
        chunks = iter(partial(src.read, chunk_size), b"")
        result = _hash_chunks(_write_chunks(chunks, dst), on_chunk, executor)

    if preserve_mtime:
        destination_stat = destination.stat()

        os.utime(
            destination,
            ns=(
                destination_stat.st_atime_ns,
                source_stat.st_mtime_ns,
            ),
        )

    return result
