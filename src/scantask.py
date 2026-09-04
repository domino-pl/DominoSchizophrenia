import logging
from datetime import UTC, datetime, timedelta
from hashlib import md5, sha256
from os import stat_result
from pathlib import PurePosixPath

from src.identity import IDENTITY_FILE_NAME
from src.db import SchizophreniaDB
from src.db.scan import Scan, ScanItemCheckSums, ScanListItem, ScanStep
from src.tools import format_mtime, format_size
import stat

log = logging.getLogger(__name__)


class ScanTask:
    INSERT_SIZE = 1000
    CHUNK_SIZE = 1024 * 1024
    CHECKSUMS_COMMIT_INTERVAL_S = 60

    def __init__(self, db: SchizophreniaDB, storage_name: str) -> None:
        self.db = db
        self.storage = self.db.select_storage(storage_name)

    def _find_last_storage_scan(self) -> Scan | None:
        return self.db.scan_select_last_for_storage(self.storage.id)

    def _insert_new_scan(self):
        last_scan = self._find_last_storage_scan()
        if last_scan and last_scan.finished_step not in (
            ScanStep.APPLY,
            ScanStep.ABORT,
        ):
            raise RuntimeError("Last scan not finished.")

        log.debug("Insert new scan")
        self.db.scan_insert(self.storage.id)
        self.db.commit()
        log.info("New scan inserted")

    def _list(self):
        scan = self._find_last_storage_scan()
        if scan is None or scan.finished_step != ScanStep.INSERT:
            raise RuntimeError("Last scan step is not insert.")

        log.debug(f"Delete all scan items for storage {self.storage.name}.")
        self.db.scan_delete_all_storage_scan_items(self.storage.id)

        log.info(
            f"Begin file listing for storage {self.storage.name} in '{self.storage.abs_path}'."
        )
        new_items: list[ScanListItem] = []
        for current_dir, dirnames, filenames in self.storage.abs_path.walk(top_down=True, on_error=None, follow_symlinks=False):
            for subdir in dirnames:
                if subdir not in ["System Volume Information", "$RECYCLE.BIN"]:
                    continue
                subdir_path = current_dir/subdir
                attrs = subdir_path.stat(follow_symlinks=False).st_file_attributes
                if attrs & stat.FILE_ATTRIBUTE_SYSTEM:
                    log.info(f"Skipping SYSTEM file/dir {subdir_path}.")
                    dirnames.remove(subdir)
                else:
                    log.info(f"File/dir {subdir_path} won't be skipped - not mark as SYSTEM.")

            for file in filenames:
                if current_dir == self.storage.abs_path and file == IDENTITY_FILE_NAME:
                    logging.info(f"Skipping identity file {IDENTITY_FILE_NAME} in {IDENTITY_FILE_NAME}.")
                    continue

                file_path = current_dir / file
                if file_path.is_symlink() or not file_path.is_file():
                    raise Exception(f"{file_path} is not a regular file")

                file_stat = file_path.stat()
                new_items.append(
                    ScanListItem(
                        rel_path=PurePosixPath(
                            file_path.relative_to(self.storage.abs_path).as_posix()
                        ),
                        size=file_stat.st_size,
                        mtime=format_mtime(file_stat),
                    )
                )

                log.debug(
                    f"Found file '{new_items[-1].rel_path}' (size={format_size(new_items[-1].size)}, mtime={new_items[-1].mtime})."
                )

                if len(new_items) > self.INSERT_SIZE:
                    self.db.scan_insert_scan_items(scan, new_items)
                    log.debug(f"Inserted {len(new_items)} new scan items.")
                    new_items = []

        self.db.scan_insert_scan_items(scan, new_items)
        log.debug(f"Inserted {len(new_items)} new scan items.")

        self.db.scan_update_step(scan.id, ScanStep.LIST)
        log.info("Finished file listing for scan.")
        self.db.commit()

    def _match(self) -> None:
        scan = self._find_last_storage_scan()
        if scan is None or scan.finished_step != ScanStep.LIST:
            raise RuntimeError("Last scan step is not list.")

        log.info(
            f"Match new scan items with known files in storage {self.storage.name}."
        )
        self.db.scan_match_files(scan)
        self.db.scan_update_step(scan.id, ScanStep.MATCH)
        log.info("Finished matching.")
        self.db.commit()

    def _collect_sums(self):
        # TODO: Progress (per file and total)
        scan = self._find_last_storage_scan()
        if scan is None or scan.finished_step != ScanStep.MATCH:
            raise RuntimeError("Last scan step is not match.")

        log.info("Start checksums calculation.")
        last_commit_time = datetime.now(tz=UTC)
        new_sums: list[ScanItemCheckSums] = []
        while True:
            files = self.db.scan_select_files_for_checksum_calculation(scan)

            if len(files) == 0:
                break

            for file_info in files:
                log.debug(f"Calculating sums for '{file_info.rel_path}'.")
                md5_hash = md5()
                sha256_hash = sha256()

                file_path = self.storage.abs_path / file_info.rel_path
                with file_path.open("rb") as file:
                    while chunk := file.read(self.CHUNK_SIZE):
                        md5_hash.update(chunk)
                        sha256_hash.update(chunk)

                new_sums.append(
                    ScanItemCheckSums(
                        item_id=file_info.item_id,
                        md5=md5_hash.hexdigest(),
                        sha256=sha256_hash.hexdigest(),
                    )
                )

                log.debug(
                    f"Calculated sums: md5={new_sums[-1].md5}, sha256={new_sums[-1].sha256}."
                )

                if datetime.now(tz=UTC) > last_commit_time + timedelta(
                    seconds=self.CHECKSUMS_COMMIT_INTERVAL_S
                ):
                    self.db.scan_update_scan_items_sum(new_sums)
                    self.db.commit()
                    new_sums = []
                    last_commit_time = datetime.now(tz=UTC)

            self.db.scan_update_scan_items_sum(new_sums)
            self.db.commit()

            self.db.scan_update_step(scan.id, ScanStep.CHECKSUM)
            self.db.commit()

    def _compare(self):
        scan = self._find_last_storage_scan()
        if scan is None or scan.finished_step != ScanStep.CHECKSUM:
            raise RuntimeError("Last scan step is not checksum.")

        log.info(
            f"Compare new scan items content with known files in storage {self.storage.name}."
        )
        self.db.scan_update_scan_items_content_changed(scan)
        self.db.scan_update_step(scan.id, ScanStep.COMPARE)
        self.db.commit()
        log.info("Finished comparing new scan items.")

    def _assign_uid(self):
        scan = self._find_last_storage_scan()
        if scan is None or scan.finished_step != ScanStep.COMPARE:
            raise RuntimeError("Last scan step is not checksum.")

        # TODO. log
        self.db.scan_assign_scan_new_uid(scan.id)
        self.db.scan_update_step(scan.id, ScanStep.ASSIGN_UID)
        self.db.commit()
        log.info("Finished assigning UID.")

    def _start(self) -> None:
        self._insert_new_scan()
        self._resume()

    def _resume(self) -> None:
        last_scan = self._find_last_storage_scan()
        last_step = last_scan.finished_step if last_scan else None

        if last_step and last_step >= ScanStep.LIST:
            log.info("Files already listed for scan.")
        else:
            self._list()

        if last_step and last_step >= ScanStep.MATCH:
            log.info("Files already matched for scan.")
        else:
            self._match()

        if last_step and last_step >= ScanStep.CHECKSUM:
            log.info("Checksum already calculated for scan.")
        else:
            self._collect_sums()

        if last_step and last_step >= ScanStep.COMPARE:
            log.info("Content already compared for scan.")
        else:
            self._compare()

        if last_step and last_step >= ScanStep.ASSIGN_UID:
            log.info("UID already assigned for scan.")
        else:
            self._assign_uid()

    def start(self) -> None:
        self._start()

    def resume(self) -> None:
        self._resume()

    def apply(self) -> None:
        scan = self._find_last_storage_scan()
        if scan is None:
            raise RuntimeError("No scan.")

        if scan.finished_step != ScanStep.COMPARE:
            raise RuntimeError("Scan is not ready to apply.")

        self.db.scan_delete_file_records_scanned_as_deleted(scan.id)
        self.db.scan_update_file_records_with_meta_changed(scan.id)
        self.db.scan_update_file_records_with_content_changed(scan.id)
        self.db.scan_insert_new_scanned_file_records(scan.id)
        self.db.scan_update_step(scan.id, ScanStep.APPLY)
        self.db.commit()
