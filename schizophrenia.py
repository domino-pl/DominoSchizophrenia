#!/bin/python

import logging
from argparse import ArgumentParser
from enum import StrEnum
from pathlib import Path
from sys import exit

from src.db import SchizophreniaDB
from src.identity import Identity
from src.scantask import ScanTask
from src.synctask import SyncTask

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

log.setLevel(logging.DEBUG)


class ScanAction(StrEnum):
    START = "start"
    RESUME = "resume"
    ABORT = "abort"
    APPLY = "apply"


class SyncAction(StrEnum):
    PREPARE = "prepare"
    INIT_VIA = "init_via"
    RUN_VIA = "run_via"
    INIT_LWD = "init_lwd"
    RUN_LWD = "run_lwd"


def add_storage(db: SchizophreniaDB, name: str, storage_abs_path: Path):
    if not storage_abs_path.is_absolute():
        raise RuntimeError(f"Not absolute storage path: {storage_abs_path}.")

    if not storage_abs_path.is_dir():
        raise NotADirectoryError(f"Path {storage_abs_path} is not a directory.")

    identity = Identity.new()
    identity.save_to_storage_dir(storage_abs_path)

    db.insert_storage(name, storage_abs_path, identity)
    db.commit()
    log.info(f"Added storage {name} with absolute path {storage_abs_path}.")


def scan(db: SchizophreniaDB, storage_name: str, action: ScanAction):

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


def sync(
    db: SchizophreniaDB,
    src_storage_name: str,
    dst_storage_name: str,
    syncplan_file: Path,
    action: SyncAction,
    via: Path | None = None,
    lwd: Path | None = None,
):
    sync_task = SyncTask(
        db, src_storage_name, dst_storage_name, syncplan_file, via, lwd
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
    parser = ArgumentParser()
    commands = parser.add_subparsers(
        dest="command",
        required=True,
    )

    add_storage_parser = commands.add_parser("add_storage")
    add_storage_parser.add_argument("storage_name", type=str)
    add_storage_parser.add_argument("path", type=Path)

    scan_parser = commands.add_parser("scan")
    scan_parser.add_argument("action", choices=ScanAction)
    scan_parser.add_argument("storage_name", type=str)

    sync_parser = commands.add_parser("sync")
    sync_parser.add_argument("action", choices=SyncAction)
    sync_parser.add_argument("src_storage_name", type=str)
    sync_parser.add_argument("dst_storage_name", type=str)
    sync_parser.add_argument("syncplan_file", type=Path)
    sync_parser.add_argument("--via", type=Path)
    sync_parser.add_argument("--lwd", type=Path)

    args = parser.parse_args()
    db = SchizophreniaDB()

    try:
        if args.command == "add_storage":
            add_storage(db, args.storage_name, args.path.absolute())
        elif args.command == "scan":
            action: ScanAction = args.action
            scan(db, args.storage_name, action)
        elif args.command == "sync":
            action: SyncAction = args.action
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
    exit(main())
