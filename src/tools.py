import os
from datetime import UTC, datetime
from hashlib import md5, sha256
from os import stat_result
from pathlib import Path
from typing import NamedTuple


def format_size(size_bytes: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")

    size = float(size_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{int(size)} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024

    raise AssertionError("unreachable")


def format_mtime(stat: stat_result) -> str:
    return datetime.fromtimestamp(stat.st_mtime_ns / 1_000_000_000, tz=UTC).isoformat(
        timespec="microseconds"
    )


class CopyResult(NamedTuple):
    size: int
    md5: str
    sha256: str


def copy_with_hashes(
    source: Path,
    destination: Path,
    chunk_size: int = 1024 * 1024,
    preserve_mtime: bool = False,
) -> CopyResult:
    md5_hash = md5()
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
