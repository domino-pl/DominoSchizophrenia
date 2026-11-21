#!/bin/python
# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Command-line entry point for storage registration, scanning, and synchronization."""

import logging
import sys
from argparse import ArgumentParser
from enum import StrEnum
from pathlib import Path

from src.db import SchizophreniaDB
from src.identity import Identity
from src.scantask import ScanTask
from src.synctask import SyncTask

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

log.setLevel(logging.DEBUG)


class ScanAction(StrEnum):
    """Select an operation on a storage scan."""

    START = "start"
    RESUME = "resume"
    ABORT = "abort"
    APPLY = "apply"


class SyncAction(StrEnum):
    """Select a stage of storage synchronization."""

    PREPARE = "prepare"
    INIT_VIA = "init_via"
    RUN_VIA = "run_via"
    INIT_LWD = "init_lwd"
    RUN_LWD = "run_lwd"


class RelativeStoragePathError(RuntimeError):
    """Report a storage path that is not absolute."""

    def __init__(self, path: Path) -> None:
        """Include the rejected path in the error message."""
        super().__init__(f"Not absolute storage path: {path}.")


class StoragePathNotDirectoryError(NotADirectoryError):
    """Report a storage path that does not identify a directory."""

    def __init__(self, path: Path) -> None:
        """Include the rejected path in the error message."""
        super().__init__(f"Path {path} is not a directory.")


def add_storage(db: SchizophreniaDB, name: str, storage_abs_path: Path) -> None:
    """Write a storage identity and register the storage in the database.

    Raises:
        RelativeStoragePathError: If the storage path is relative.
        StoragePathNotDirectoryError: If the path does not identify a directory.

    """
    if not storage_abs_path.is_absolute():
        raise RelativeStoragePathError(storage_abs_path)

    if not storage_abs_path.is_dir():
        raise StoragePathNotDirectoryError(storage_abs_path)

    identity = Identity.new()
    with db.write_transaction():
        db.insert_storage(name, storage_abs_path, identity)
        identity.save_to_storage_dir(storage_abs_path)
    log.info("Added storage %s with absolute path %s.", name, storage_abs_path)


def scan(db: SchizophreniaDB, storage_name: str, action: ScanAction) -> None:
    """Run the requested scan operation for a registered storage."""
    scan_task = ScanTask(db, storage_name)

    match action:
        case ScanAction.START:
            scan_task.start()
        case ScanAction.RESUME:
            scan_task.resume()
        # TODO: ABORT
        # TODO: SHOW
        case ScanAction.APPLY:
            scan_task.apply()


def sync(  # ruff: ignore[too-many-arguments, too-many-positional-arguments]
    db: SchizophreniaDB,
    src_storage_name: str,
    dst_storage_name: str,
    syncplan_file: Path,
    action: SyncAction,
    via: Path | None = None,
    lwd: Path | None = None,
) -> None:
    """Run the requested synchronization stage for two registered storages."""
    sync_task = SyncTask(
        db, src_storage_name, dst_storage_name, syncplan_file, via, lwd,
    )

    match action:
        case SyncAction.PREPARE:
            sync_task.prepare()
        case SyncAction.INIT_VIA:
            sync_task.init_via()
        case SyncAction.INIT_LWD:
            sync_task.init_lwd()
        case SyncAction.RUN_VIA:
            sync_task.run_via()
        case SyncAction.RUN_LWD:
            sync_task.run_lwd()


def main() -> int:
    """Parse the command line and dispatch the requested operation.

    Returns:
        Zero after the command completes.

    """
    parser = ArgumentParser()
    commands = parser.add_subparsers(
        dest="command",
        required=True,
    )

    add_storage_parser = commands.add_parser("add_storage")
    add_storage_parser.add_argument("storage_name", type=str)
    add_storage_parser.add_argument("path", type=Path)

    scan_parser = commands.add_parser("scan")
    scan_parser.add_argument("action", type=ScanAction, choices=ScanAction)
    scan_parser.add_argument("storage_name", type=str)

    sync_parser = commands.add_parser("sync")
    sync_parser.add_argument("action", type=SyncAction, choices=SyncAction)
    sync_parser.add_argument("src_storage_name", type=str)
    sync_parser.add_argument("dst_storage_name", type=str)
    sync_parser.add_argument("syncplan_file", type=Path)
    sync_parser.add_argument("--via", type=Path)
    sync_parser.add_argument("--lwd", type=Path)

    args = parser.parse_args()
    action = getattr(args, "action", None)
    db = SchizophreniaDB()

    try:
        if args.command == "add_storage":
            add_storage(db, args.storage_name, args.path.absolute())
        elif args.command == "scan" and isinstance(action, ScanAction):
            scan(db, args.storage_name, action)
        elif args.command == "sync" and isinstance(action, SyncAction):
            sync(
                db,
                args.src_storage_name,
                args.dst_storage_name,
                args.syncplan_file.absolute(),
                action,
                args.via.absolute() if args.via else None,
                args.lwd.absolute() if args.lwd else None,
            )

    finally:
        db.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
