# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Coordinate staged storage scans and apply their results to file records."""

import errno
import logging
import os
import stat
import sys
from datetime import UTC, datetime, timedelta
from hashlib import md5, sha256
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

from src.db.scan import Scan, ScanItemCheckSums, ScanListItem, ScanStep
from src.identity import IDENTITY_FILE_NAME
from src.tools import format_mtime, format_size, raise_walk_error

if TYPE_CHECKING:
    from pathlib import Path

    from src.db import SchizophreniaDB

log = logging.getLogger(__name__)


class InvalidScanStateError(RuntimeError):
    """Indicate that a scan is missing or has an unexpected completed stage."""

    def __init__(
        self,
        *,
        actual_step: ScanStep | None,
        expected_steps: tuple[ScanStep, ...],
    ) -> None:
        """Build a diagnostic from the actual and permitted scan stages.

        Args:
            actual_step: Completed stage, or `None` when no scan exists.
            expected_steps: Completed stages that permit the requested action.

        """
        self.actual_step = actual_step
        self.expected_steps = expected_steps
        expected = " or ".join(step.name for step in expected_steps)
        actual = "no scan" if actual_step is None else actual_step.name
        super().__init__(f"Invalid scan state: expected {expected}; got {actual}.")


class UnsupportedFileTypeError(OSError):
    """Indicate that a scanned path is a symlink or not a regular file."""

    def __init__(self, file_path: Path) -> None:
        """Build a diagnostic for an unsupported filesystem entry.

        Args:
            file_path: Path rejected by the scan's regular-file check.

        """
        self.file_path = file_path
        super().__init__(
            f"Unsupported file type: {file_path}; "
            "expected a regular file without symlinks.",
        )


class ScanTask:
    """List and identify storage files through resumable database scan stages.

    Preparation stages store scan results separately from known file records.
    Apply the prepared results explicitly with `apply()`.
    """

    INSERT_SIZE = 1000
    CHUNK_SIZE = 1024 * 1024
    CHECKSUMS_COMMIT_INTERVAL_S = 60

    def __init__(self, db: SchizophreniaDB, storage_name: str) -> None:
        """Bind the task to a database and a registered storage.

        Args:
            db: Database used to read storage details and persist scan results.
            storage_name: Name of the registered storage to scan.

        """
        self.db = db
        self.storage = self.db.select_storage(storage_name)

    def _find_last_storage_scan(self) -> Scan | None:
        """Look up the most recent scan for this storage.

        Returns:
            The latest scan, or `None` when the storage has no scans.

        """
        return self.db.scan_select_last_for_storage(self.storage.id)

    def _insert_new_scan(self) -> None:
        """Create and commit a scan after the previous scan ends.

        Raises:
            InvalidScanStateError: If the previous scan is neither applied nor aborted.

        """
        last_scan = self._find_last_storage_scan()
        expected_steps = (ScanStep.APPLY, ScanStep.ABORT)
        if last_scan and last_scan.finished_step not in expected_steps:
            raise InvalidScanStateError(
                actual_step=last_scan.finished_step,
                expected_steps=expected_steps,
            )

        log.debug("Insert new scan")
        self.db.scan_insert(self.storage.id)
        self.db.commit()
        log.info("New scan inserted")

    @staticmethod
    def _is_system_directory(directory: Path) -> bool:
        """Check the directory's Windows SYSTEM attribute.

        Returns:
            True if a recognized Windows directory has the SYSTEM flag.

        Raises:
            OSError: If the directory attributes cannot be read unexpectedly.

        """
        windows_system_dirnames = {"System Volume Information", "$RECYCLE.BIN"}

        if directory.name not in windows_system_dirnames:
            return False

        if sys.platform.startswith("win"):
            attributes = directory.stat(follow_symlinks=False).st_file_attributes
            return bool(attributes & stat.FILE_ATTRIBUTE_SYSTEM)

        if sys.platform.startswith("linux"):
            try:
                raw_attributes = os.getxattr(
                    directory, "system.ntfs_attrib_be", follow_symlinks=False,
                )
            except OSError as error:
                if error.errno in {errno.ENODATA, errno.ENOTSUP}:
                    return False
                raise

            ntfs_attributes_size = 4
            if len(raw_attributes) == ntfs_attributes_size:
                attributes = int.from_bytes(raw_attributes, "big")
                return bool(attributes & stat.FILE_ATTRIBUTE_SYSTEM)

        return False

    def _list(self) -> None:
        """Replace staged items with a file listing and commit the LIST stage.

        Store storage-relative paths, sizes, and modification times in batches.
        Directory read errors interrupt listing and propagate to the caller.

        Raises:
            InvalidScanStateError: If the latest scan is absent or not at INSERT.
            UnsupportedFileTypeError: If a path is a symlink or not a regular file.

        """
        scan = self._find_last_storage_scan()
        expected_steps = (ScanStep.INSERT,)
        if scan is None or scan.finished_step not in expected_steps:
            raise InvalidScanStateError(
                actual_step=scan.finished_step if scan is not None else None,
                expected_steps=expected_steps,
            )

        log.debug("Delete all scan items for storage %s.", self.storage.name)
        self.db.scan_delete_all_storage_scan_items(self.storage.id)

        log.info(
            "Begin file listing for storage %s in '%s'.",
            self.storage.name,
            self.storage.abs_path,
        )
        new_items: list[ScanListItem] = []
        for current_dir, dirnames, filenames in self.storage.abs_path.walk(
            top_down=True,
            on_error=raise_walk_error,
            follow_symlinks=False,
        ):
            subdirs_to_skip = [
                subdir
                for subdir in dirnames
                if self._is_system_directory(current_dir / subdir)
            ]
            for subdir in subdirs_to_skip:
                log.info("Skipping SYSTEM directory %s.", current_dir / subdir)
                dirnames.remove(subdir)

            for file in filenames:
                # The root identity file describes the storage, not user data.
                if current_dir == self.storage.abs_path and file == IDENTITY_FILE_NAME:
                    log.info(
                        "Skipping identity file %s in %s.",
                        IDENTITY_FILE_NAME,
                        IDENTITY_FILE_NAME,
                    )
                    continue

                file_path = current_dir / file
                if file_path.is_symlink() or not file_path.is_file():
                    raise UnsupportedFileTypeError(file_path)

                file_stat = file_path.stat()
                new_items.append(
                    ScanListItem(
                        rel_path=PurePosixPath(
                            file_path.relative_to(self.storage.abs_path).as_posix(),
                        ),
                        size=file_stat.st_size,
                        mtime=format_mtime(file_stat),
                    ),
                )

                log.debug(
                    "Found file '%s' (size=%s, mtime=%s).",
                    new_items[-1].rel_path,
                    format_size(new_items[-1].size),
                    new_items[-1].mtime,
                )

                if len(new_items) > self.INSERT_SIZE:
                    # Flush batches to keep the in-memory listing bounded.
                    self.db.scan_insert_scan_items(scan, new_items)
                    log.debug("Inserted %s new scan items.", len(new_items))
                    new_items = []

        self.db.scan_insert_scan_items(scan, new_items)
        log.debug("Inserted %s new scan items.", len(new_items))

        self.db.scan_update_step(scan.id, ScanStep.LIST)
        log.info("Finished file listing for scan.")
        self.db.commit()

    def _match(self) -> None:
        """Match staged paths to known files and commit the MATCH stage.

        Raises:
            InvalidScanStateError: If the latest scan is absent or not at LIST.

        """
        scan = self._find_last_storage_scan()
        expected_steps = (ScanStep.LIST,)
        if scan is None or scan.finished_step not in expected_steps:
            raise InvalidScanStateError(
                actual_step=scan.finished_step if scan is not None else None,
                expected_steps=expected_steps,
            )

        log.info(
            "Match new scan items with known files in storage %s.",
            self.storage.name,
        )
        self.db.scan_match_files(scan)
        self.db.scan_update_step(scan.id, ScanStep.MATCH)
        log.info("Finished matching.")
        self.db.commit()

    def _collect_sums(self) -> None:
        """Calculate pending MD5 and SHA-256 checksums after path matching.

        Commit accumulated checksums periodically and after each batch.
        Record the CHECKSUM stage once no files remain to be hashed.

        Raises:
            InvalidScanStateError: If the latest scan is absent or not at MATCH.

        """
        # TODO: Progress (per file and total)
        scan = self._find_last_storage_scan()
        expected_steps = (ScanStep.MATCH,)
        if scan is None or scan.finished_step not in expected_steps:
            raise InvalidScanStateError(
                actual_step=scan.finished_step if scan is not None else None,
                expected_steps=expected_steps,
            )

        log.info("Start checksums calculation.")
        last_commit_time = datetime.now(tz=UTC)
        while True:
            new_sums: list[ScanItemCheckSums] = []
            files = self.db.scan_select_files_for_checksum_calculation(scan)

            if len(files) == 0:
                break

            for file_info in files:
                log.debug("Calculating sums for '%s'.", file_info.rel_path)
                md5_hash = md5()  # ruff: ignore[hashlib-insecure-hash-function]
                sha256_hash = sha256()

                file_path = self.storage.abs_path / file_info.rel_path
                with file_path.open("rb") as file:
                    # Feed both hashes from the same bounded file chunks.
                    while chunk := file.read(self.CHUNK_SIZE):
                        md5_hash.update(chunk)
                        sha256_hash.update(chunk)

                new_sums.append(
                    ScanItemCheckSums(
                        item_id=file_info.item_id,
                        md5=md5_hash.hexdigest(),
                        sha256=sha256_hash.hexdigest(),
                    ),
                )

                log.debug(
                    "Calculated sums: md5=%s, sha256=%s.",
                    new_sums[-1].md5,
                    new_sums[-1].sha256,
                )

                if datetime.now(tz=UTC) > last_commit_time + timedelta(
                    seconds=self.CHECKSUMS_COMMIT_INTERVAL_S,
                ):
                    # Persist progress during long checksum batches.
                    self.db.scan_update_scan_items_sum(new_sums)
                    self.db.commit()
                    new_sums = []
                    last_commit_time = datetime.now(tz=UTC)

            self.db.scan_update_scan_items_sum(new_sums)
            self.db.commit()

        self.db.scan_update_step(scan.id, ScanStep.CHECKSUM)
        self.db.commit()

    def _compare(self) -> None:
        """Compare staged content with known files and commit COMPARE.

        Raises:
            InvalidScanStateError: If the latest scan is absent or not at CHECKSUM.

        """
        scan = self._find_last_storage_scan()
        expected_steps = (ScanStep.CHECKSUM,)
        if scan is None or scan.finished_step not in expected_steps:
            raise InvalidScanStateError(
                actual_step=scan.finished_step if scan is not None else None,
                expected_steps=expected_steps,
            )

        log.info(
            "Compare new scan items content with known files in storage %s.",
            self.storage.name,
        )
        self.db.scan_update_scan_items_content_changed(scan)
        self.db.scan_update_step(scan.id, ScanStep.COMPARE)
        self.db.commit()
        log.info("Finished comparing new scan items.")

    def _assign_uid(self) -> None:
        """Assign content identities to staged files and commit ASSIGN_UID.

        Raises:
            InvalidScanStateError: If the latest scan is absent or not at COMPARE.

        """
        scan = self._find_last_storage_scan()
        expected_steps = (ScanStep.COMPARE,)
        if scan is None or scan.finished_step not in expected_steps:
            raise InvalidScanStateError(
                actual_step=scan.finished_step if scan is not None else None,
                expected_steps=expected_steps,
            )

        # TODO: log
        self.db.scan_assign_scan_new_uid(scan.id)
        self.db.scan_update_step(scan.id, ScanStep.ASSIGN_UID)
        self.db.commit()
        log.info("Finished assigning UID.")

    def _start(self) -> None:
        """Create a scan and run its preparation stages."""
        self._insert_new_scan()
        self._resume()

    def _resume(self) -> None:
        """Run preparation stages not already completed by the latest scan.

        Raises:
            InvalidScanStateError: If no scan exists or a stage detects an
                unexpected scan state.

        """
        last_scan = self._find_last_storage_scan()

        if not last_scan:
            raise InvalidScanStateError(
                actual_step=None,
                expected_steps=tuple(ScanStep),
            )

        last_step = last_scan.finished_step

        # Use the saved checkpoint to skip stages completed before this run.
        if last_step >= ScanStep.LIST:
            log.info("Files already listed for scan.")
        else:
            self._list()

        if last_step >= ScanStep.MATCH:
            log.info("Files already matched for scan.")
        else:
            self._match()

        if last_step >= ScanStep.CHECKSUM:
            log.info("Checksum already calculated for scan.")
        else:
            self._collect_sums()

        if last_step >= ScanStep.COMPARE:
            log.info("Content already compared for scan.")
        else:
            self._compare()

        if last_step >= ScanStep.ASSIGN_UID:
            log.info("UID already assigned for scan.")
        else:
            self._assign_uid()

    def start(self) -> None:
        """Start a scan and prepare its results for a later `apply()` call.

        Completed preparation stages are committed as the scan advances.

        Propagate `InvalidScanStateError` if a previous scan has not ended.

        """
        self._start()

    def resume(self) -> None:
        """Resume preparation from the latest scan's saved stage.

        Completed preparation stages are committed as the scan advances.

        Propagate `InvalidScanStateError` if a stage requires another state.

        """
        self._resume()

    def apply(self) -> None:
        """Apply prepared scan results to known file records and commit APPLY.

        Raises:
            InvalidScanStateError: If no scan exists or its stage is not ASSIGN_UID.

        """
        scan = self._find_last_storage_scan()
        expected_steps = (ScanStep.ASSIGN_UID,)
        if scan is None:
            raise InvalidScanStateError(
                actual_step=None,
                expected_steps=expected_steps,
            )

        if scan.finished_step not in expected_steps:
            raise InvalidScanStateError(
                actual_step=scan.finished_step,
                expected_steps=expected_steps,
            )

        # Reconcile deletions, updates, and additions before marking completion.
        self.db.scan_delete_file_records_scanned_as_deleted(scan.id)
        self.db.scan_update_file_records_with_meta_changed(scan.id)
        self.db.scan_update_file_records_with_content_changed(scan.id)
        self.db.scan_insert_new_scanned_file_records(scan.id)
        self.db.scan_update_step(scan.id, ScanStep.APPLY)
        self.db.commit()
