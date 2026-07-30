#!/bin/python

import logging
from argparse import ArgumentParser
from enum import StrEnum
from pathlib import Path
from sys import exit

from src.identity import Identity
from src.scantask import ScanTask
from src.schizophrenia_db import SchizophreniaDB

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

log.setLevel(logging.DEBUG)


class ScanAction(StrEnum):
    START = "start"
    RESUME = "resume"
    ABORT = "abort"
    APPLY = "apply"


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

    scan_task = ScanTask(db, storage_name, allow_resume=action != ScanAction.START)

    match action:
        case ScanAction.START:
            scan_task.start()
        case ScanAction.RESUME:
            scan_task.resume()


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

    args = parser.parse_args()
    db = SchizophreniaDB()

    try:
        if args.command == "add_storage":
            add_storage(db, args.storage_name, args.path.absolute())
        elif args.command == "scan":
            action: ScanAction = args.action
            scan(db, args.storage_name, action)

    finally:
        db.close()

    return 0


if __name__ == "__main__":
    exit(main())
