import logging
from datetime import UTC, datetime, timedelta
from hashlib import md5, sha256
from os import stat_result

from src.schizophrenia_db import CheckSums, ListItem, Scan, ScanStep, SchizophreniaDB

log = logging.getLogger(__name__)


def format_size(size_bytes: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")

    size = float(size_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{int(size)} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024

    raise AssertionError("unreachable")


class ScanTask:
    INSERT_SIZE = 1000
    CHUNK_SIZE = 1024 * 1024
    CHECKSUMS_COMMIT_INTERVAL_S = 60

    def __init__(
        self, db: SchizophreniaDB, storage_name: str, allow_resume: bool = False
    ) -> None:
        self.db = db
        self.storage = self.db.select_storage(storage_name)

    def _find_last_storage_scan(self) -> Scan | None:
        return self.db.select_last_storage_scan(self.storage.id)

    def _format_mtime(self, stat: stat_result) -> str:
        return datetime.fromtimestamp(
            stat.st_mtime_ns / 1_000_000_000, tz=UTC
        ).isoformat(timespec="microseconds")

    def _insert_new_scan(self):
        last_scan = self._find_last_storage_scan()
        if last_scan and last_scan.finished_step not in (
            ScanStep.APPLY,
            ScanStep.ABORT,
        ):
            raise RuntimeError("Last scan not finished.")

        log.debug("Insert new scan")
        self.db.insert_scan(self.storage.id)
        self.db.commit()
        log.info("New scan inserted")

    def _list(self):
        scan = self._find_last_storage_scan()
        if scan is None or scan.finished_step != ScanStep.INSERT:
            raise RuntimeError("Last scan step is not insert.")

        log.debug(f"Delete all scan items for storage {self.storage.name}.")
        self.db.delete_all_storage_scan_items(self.storage.id)

        log.info(
            f"Begin file listing for storage {self.storage.name} in '{self.storage.abs_path}'."
        )
        new_items: list[ListItem] = []
        for file_path in self.storage.abs_path.rglob("*"):
            if file_path.is_symlink() or not file_path.is_file():
                continue

            stat = file_path.stat()
            new_items.append(
                ListItem(
                    rel_path=file_path.relative_to(self.storage.abs_path),
                    size=stat.st_size,
                    mtime=self._format_mtime(stat),
                )
            )

            log.debug(
                f"Found file '{new_items[-1].rel_path}' (size={format_size(new_items[-1].size)}, mtime={new_items[-1].mtime})."
            )

            if len(new_items) > self.INSERT_SIZE:
                self.db.insert_scan_items(scan, new_items)
                log.debug(f"Inserted {len(new_items)} new scan items.")
                new_items = []

        self.db.insert_scan_items(scan, new_items)
        log.debug(f"Inserted {len(new_items)} new scan items.")

        self.db.update_scan_step(scan.id, ScanStep.LIST)
        log.info("Finished file listing for scan.")
        self.db.commit()

    def _match(self) -> None:
        scan = self._find_last_storage_scan()
        if scan is None or scan.finished_step != ScanStep.LIST:
            raise RuntimeError("Last scan step is not list.")

        log.info(
            f"Match new scan items with known files in storage {self.storage.name}."
        )
        self.db.match_scan_files(scan)
        self.db.update_scan_step(scan.id, ScanStep.MATCH)
        log.info("Finished matching.")
        self.db.commit()

    def _collect_sums(self):
        # TODO: Progress (per file and total)
        scan = self._find_last_storage_scan()
        if scan is None or scan.finished_step != ScanStep.MATCH:
            raise RuntimeError("Last scan step is not match.")

        log.info("Start checksums calculation.")
        last_commit_time = datetime.now(tz=UTC)
        new_sums: list[CheckSums] = []
        while True:
            files = self.db.select_files_for_checksum_calculation(scan)

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
                    CheckSums(
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
                    self.db.update_scan_items_sum(new_sums)
                    self.db.commit()
                    new_sums = []
                    last_commit_time = datetime.now(tz=UTC)

            self.db.update_scan_items_sum(new_sums)
            self.db.commit()

            self.db.update_scan_step(scan.id, ScanStep.CHECKSUM)
            self.db.commit()

    def _compare(self):
        scan = self._find_last_storage_scan()
        if scan is None or scan.finished_step != ScanStep.CHECKSUM:
            raise RuntimeError("Last scan step is not checksum.")

        log.info(
            f"Compare new scan items content with known files in storage {self.storage.name}."
        )
        self.db.update_scan_items_content_changed(scan)
        self.db.update_scan_step(scan.id, ScanStep.COMPARE)
        self.db.commit()
        log.info("Finished comparing new scan items.")

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

        self.db.delete_file_records_scanned_as_deleted(scan.id)
        self.db.update_file_records_with_meta_changed(scan.id)
        self.db.update_file_records_with_content_changed(scan.id)
        self.db.insert_new_scanned_file_records(scan.id)
        self.db.update_scan_step(scan.id, ScanStep.APPLY)
        self.db.commit()