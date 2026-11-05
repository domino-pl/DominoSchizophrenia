# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Common utils."""

import os
from datetime import UTC, datetime
from hashlib import md5, sha256
from os import stat_result
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from pathlib import Path


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


def format_mtime(stat: stat_result) -> str:
    """Format a file's modification time as an ISO 8601 timestamp in UTC.

    Args:
        stat: File metadata containing the modification time in nanoseconds.

    Returns:
        A timestamp with microsecond precision and the ``+00:00`` UTC offset.

    """
    return datetime.fromtimestamp(stat.st_mtime_ns / 1_000_000_000, tz=UTC).isoformat(
        timespec="microseconds",
    )


class CopyResult(NamedTuple):
    """Store the copied byte count and hexadecimal MD5 and SHA-256 digests."""

    size: int
    md5: str
    sha256: str


def copy_with_hashes(
    source: Path,
    destination: Path,
    *,
    chunk_size: int = 1024 * 1024,
    preserve_mtime: bool = False,
) -> CopyResult:
    """Copy a file while computing its size and MD5 and SHA-256 digests.

    Args:
        source: Path to the file to read.
        destination: Path to the output file, created or overwritten.
        chunk_size: Number of bytes to read at a time.
        preserve_mtime: Whether to retain the source's modification time on
            the destination, preserving the destination's access time.

    Returns:
        The copied byte count and hexadecimal MD5 and SHA-256 digests.

    """
    md5_hash = md5()  # ruff: ignore[hashlib-insecure-hash-function]
    sha256_hash = sha256()
    copied_size = 0

    with source.open("rb") as src, destination.open("wb") as dst:
        source_stat = os.fstat(src.fileno())

        while chunk := src.read(chunk_size):
            md5_hash.update(chunk)
            sha256_hash.update(chunk)
            dst.write(chunk)
            copied_size += len(chunk)

    if preserve_mtime:
        destination_stat = destination.stat()

        os.utime(
            destination,
            ns=(
                destination_stat.st_atime_ns,
                source_stat.st_mtime_ns,
            ),
        )

    return CopyResult(
        size=copied_size,
        md5=md5_hash.hexdigest(),
        sha256=sha256_hash.hexdigest(),
    )
