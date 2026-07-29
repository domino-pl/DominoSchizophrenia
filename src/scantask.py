import logging
from datetime import UTC, datetime, timedelta
from hashlib import md5, sha256
from os import stat_result

from src.schizophrenia_db import CheckSums, ListItem, ScanStep, SchizophreniaDB

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
        self.storage = db.select_storage(storage_name)
        self.scan = self._insert()

    def _format_mtime(self, stat: stat_result) -> str:
        return datetime.fromtimestamp(
            stat.st_mtime_ns / 1_000_000_000, tz=UTC
        ).isoformat(timespec="microseconds")

    def _insert(self):
        log.debug("Insert new scan")
        scan = self.db.insert_scan(self.storage.id)
        self.db.commit()
        log.info("New scan inserted")
        return scan

    def _list(self):

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
                self.db.insert_scan_items(self.scan, new_items)
                log.debug(f"Inserted {len(new_items)} new scan items.")
                new_items = []

        self.db.insert_scan_items(self.scan, new_items)
        log.debug(f"Inserted {len(new_items)} new scan items.")

        self.scan = self.db.update_scan_step(self.scan.id, ScanStep.LIST)
        log.info("Finished file listing for scan.")
        self.db.commit()

    def _match(self):
        log.info(f"Match new scan items with known files in storage {self.storage.name}.")
        self.db.match_scan_files(self.scan)
        self.scan = self.db.update_scan_step(self.scan.id, ScanStep.MATCH)
        log.info("Finished matching.")
        self.db.commit()

    def _collect_sums(self):
        #TODO: Progress (per file and total)
        log.info("Start checksums calculation.")
        last_commit_time = datetime.now(tz=UTC)
        new_sums: list[CheckSums] = []
        while True:
            files = self.db.select_files_for_checksum_calculation(self.scan)

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

                log.debug(f"Calculated sums: md5={new_sums[-1].md5}, sha256={new_sums[-1].sha256}.")

                if datetime.now(tz=UTC) > last_commit_time + timedelta(
                    seconds=self.CHECKSUMS_COMMIT_INTERVAL_S
                ):
                    self.db.update_scan_items_sum(new_sums)
                    self.db.commit()
                    new_sums = []
                    last_commit_time = datetime.now(tz=UTC)

            self.db.update_scan_items_sum(new_sums)
            self.db.commit()

            self.db.update_scan_items_content_changed(self.scan)
            self.db.commit()

            self.scan = self.db.update_scan_step(self.scan.id, ScanStep.CHECKSUM)
            self.db.commit()


    def start(self) -> None:
        if self.scan.finished_step > ScanStep.LIST:
            log.info("Files already listed for scan.")
        else:
            self._list()

        if self.scan.finished_step > ScanStep.MATCH:
            log.info("Files already matched for scan.")
        else:
            self._match()

        if self.scan.finished_step > ScanStep.CHECKSUM:
            log.info("Checksum already calculated for scan.")
        else:
            self._collect_sums()
