# Copyright (c) 2026 Dominik Przybysz <dominik@przybysz.dev>

"""Prepare, load, and execute synchronization plans across storage locations."""

import csv
import io
import os
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, NamedTuple, Self, override

from src.db.sync import SyncFileInfo, SyncFileStatus, SyncStrategy
from src.identity import Identity
from src.tools import (
    NO_PROGRESS,
    FileProgress,
    Progress,
    copy_with_hashes,
    format_mtime,
)

if TYPE_CHECKING:
    from src.db import SchizophreniaDB


class SyncFSActionPriority(StrEnum):
    """Define action kinds and their execution order."""

    OUT_CP2_VIA = "out_cp2_via"
    VIA_CP2_LWD = "via_cp2_lwd"
    IN_CP2_LWD = "in_cp2_lwd"
    CHANGED_MV2_LWD = "changed_mv2_lwd"
    DELETED_MV2_LWD = "deleted_mv2_lwd"
    MOVED_MV2_LWD = "moved_mv2_lwd"
    MOVED_MV2_DST = "moved_mv2_dst"
    IN_MV2_DST = "in_mv2_dst"
    OUT_MV2_DST = "out_mv2_dst"
    DELETED_RM = "deleted_rm"
    CHANGED_RM = "changed_rm"

    @property
    def order(self) -> int:
        """The numeric execution order for this priority."""
        return list(type(self)).index(self) * 10 + 10


class SyncStorageType(StrEnum):
    """Identify the source, destination, and staging locations."""

    SRC = "src"
    DST = "dst"
    VIA = "via"
    LWD_IN_COPY = "lwd_in_copy"
    LWD_OUT_COPY = "lwd_out_copy"
    LWD_MOVED = "lwd_moved"
    LWD_DELETED = "lwd_deleted"
    LWD_CHANGED = "lwd_changed"


class SyncStoragesType(NamedTuple):
    """Store the identity, path, and availability of a plan location."""

    identity: Identity
    path: Path
    available: bool


SyncStorages = dict[SyncStorageType, SyncStoragesType]


class SyncFSActionRow(NamedTuple):
    """Represent the CSV fields of a synchronization action."""

    operation_id: str
    priority_value: str
    src_storage_value: str
    dst_storage_value: str
    rel_path: str
    dst_rel_path: str
    mtime: str
    size: str
    md5: str
    sha256: str
    file_id: str
    uid: str
    move_dst_fid: str
    dst_mtime: str


class SyncPlanError(ValueError):
    """Report invalid synchronization plan or action-log data."""

    ACTION_FIELD_LINE_BREAK = "An action field cannot contain a line break"
    INVALID_ACTION_CSV = "Invalid action CSV"
    INVALID_ACTION_COLUMNS = "An action line must contain exactly 13 fields"
    INVALID_ACTION_PRIORITY = "Invalid action priority"
    UNSUPPORTED_ACTION_PRIORITY = "Unsupported action priority: {}"
    INVALID_ACTION_FIELD = "Invalid action field value"
    WRONG_ACTION_TYPE = "Priority {} does not identify an action of type {}"
    INVALID_STORAGE_CSV = "Invalid storage CSV at line {}"
    INVALID_STORAGE_COLUMNS = "A storage line must contain exactly 2 fields (line {})"
    INVALID_STORAGE_ENTRY = "Invalid storage entry at line {}"
    DUPLICATE_STORAGE = "Duplicate storage: {}"
    INVALID_ACTION = "Invalid action at line {}"
    DUPLICATE_ACTION = "Duplicate action number: {}"
    MISSING_STORAGE_IDENTITIES = "Missing storage identities: {}"
    DATABASE_IDENTITY_MISMATCH = (
        "Identity mismatch for {} storage: plan has {}, database has {}"
    )
    LOCATION_IDENTITY_MISMATCH = (
        "Identity mismatch for {} storage at {}: plan has {}, location has {}"
    )
    INVALID_ACTION_LOG_CSV = "Invalid action log CSV at line {}"
    INVALID_ACTION_LOG_COLUMNS = (
        "An action log line must contain exactly 2 fields (line {})"
    )
    INVALID_ACTION_NUMBER = "Invalid action number at line {}"
    INVALID_ACTION_STATUS = "Invalid action status at line {}"
    UNKNOWN_ACTION_NUMBER = "Unknown action number {} at line {}"
    MISSING_ACTION_LOG_SEPARATOR = "Missing {!r} separator in sync plan"


class SyncConfigurationError(ValueError):
    """Report a missing path required by a sync command."""

    MISSING_VIA = "--via is required for {}"
    MISSING_LWD = "--lwd is required for {}"


class SyncValidationError(RuntimeError):
    """Report a file or storage that fails action validation."""

    MISSING_SOURCE_FIELDS = "Source src_storage_type or file_info is None"
    UNAVAILABLE_SOURCE = "Source file not found"
    ABSOLUTE_SOURCE_PATH = "Source file path is absolute"
    SOURCE_MTIME_MISMATCH = (
        "Source modification time changed for {} (expected {}, got {})"
    )
    UNEXPECTED_DESTINATION_FIELDS = "dst_storage_type or dst_rel_path is not None"
    MISSING_DESTINATION_FIELDS = "dst_storage_type or dst_rel_path is None"
    UNAVAILABLE_DESTINATION = "Destination file storage does not found"
    MISSING_DESTINATION_IDENTITY = "Destination storage dispersed."
    ABSOLUTE_DESTINATION_PATH = "Destination file path is absolute"


class SyncExecutionError(RuntimeError):
    """Report failures executing or preparing synchronization actions."""

    MISSING_DESTINATION_PATH = "dst_path is None"
    SIZE_MISMATCH = "Size error"
    MD5_MISMATCH = "MD5 error"
    SHA256_MISMATCH = "SHA256 error"
    MISSING_SOURCE_ROW = "row.src is None"
    UNKNOWN_STATUS = "Unknown sync_status: {}"
    MISSING_SOURCE_OR_DONOR_ROW = "row.src or row.in_src is None"
    UNKNOWN_STRATEGY = "Unknown strategy {}"


class SyncFSBaseAction:
    """Validate, serialize, and run an action on synchronized files."""

    ACTION_LINE_COLUMNS = 14
    NEED_DST: bool
    PRIORITY: SyncFSActionPriority

    def __init__(
        self,
        src_storage_type: SyncStorageType,
        src_file_info: SyncFileInfo,
        dst_storage_type: SyncStorageType | None,
        dst_rel_path: PurePosixPath | None,
    ) -> None:
        """Set the source and optional destination fields for the action."""
        self._set_action_fields(
            src_storage_type,
            src_file_info,
            dst_storage_type,
            dst_rel_path,
        )

    def _set_action_fields(
        self,
        src_storage_type: SyncStorageType,
        src_file_info: SyncFileInfo,
        dst_storage_type: SyncStorageType | None,
        dst_rel_path: PurePosixPath | None,
    ) -> None:
        self.src_storage_type = src_storage_type
        self.file_info = src_file_info
        self.dst_storage_type = dst_storage_type
        self.dst_rel_path = dst_rel_path
        self.dst_mtime: str | None = None
        self.progress: Progress = NO_PROGRESS

    def dump(self, operation_id: int) -> str:
        """Serialize the action as a single CSV record.

        Returns:
            The CSV record without a line terminator.

        Raises:
            SyncPlanError: If any field contains a line break.

        """
        fields = SyncFSActionRow(
            operation_id=str(operation_id),
            priority_value=self.PRIORITY.value,
            src_storage_value=self.src_storage_type.value,
            dst_storage_value=(
                "" if self.dst_storage_type is None else self.dst_storage_type.value
            ),
            rel_path=self.file_info.rel_path.as_posix(),
            dst_rel_path=(
                "" if self.dst_rel_path is None else self.dst_rel_path.as_posix()
            ),
            mtime=self.file_info.mtime,
            size=str(self.file_info.size),
            md5=self.file_info.md5,
            sha256=self.file_info.sha256,
            file_id=str(self.file_info.id),
            uid=str(self.file_info.uid),
            move_dst_fid=(
                ""
                if self.file_info.move_dst_fid is None
                else str(self.file_info.move_dst_fid)
            ),
            dst_mtime=self.dst_mtime or "",
        )

        if any("\n" in field or "\r" in field for field in fields):
            raise SyncPlanError(SyncPlanError.ACTION_FIELD_LINE_BREAK)

        output = io.StringIO()
        csv.writer(output, lineterminator="").writerow(fields)
        return output.getvalue()

    @classmethod
    def load_str(cls, line: str) -> tuple[int, SyncFSBaseAction]:
        """Decode one CSV record and select its concrete action type.

        Returns:
            The operation ID and reconstructed action.

        Raises:
            SyncPlanError: If the record cannot identify a valid action.

        """
        try:
            rows = list(csv.reader([line], strict=True))
        except csv.Error as error:
            raise SyncPlanError(SyncPlanError.INVALID_ACTION_CSV) from error

        if len(rows) != 1 or len(rows[0]) != cls.ACTION_LINE_COLUMNS:
            raise SyncPlanError(SyncPlanError.INVALID_ACTION_COLUMNS)

        row = SyncFSActionRow(*rows[0])

        try:
            priority = SyncFSActionPriority(row.priority_value)
        except ValueError as error:
            raise SyncPlanError(SyncPlanError.INVALID_ACTION_PRIORITY) from error

        pending = list(SyncFSBaseAction.__subclasses__())
        while pending:
            action_type = pending.pop()
            pending.extend(action_type.__subclasses__())
            if action_type.__dict__.get("PRIORITY") == priority:
                return action_type.load_typed_str(row)

        raise SyncPlanError(SyncPlanError.UNSUPPORTED_ACTION_PRIORITY.format(priority))

    @classmethod
    def load_typed_str(cls, row: SyncFSActionRow) -> tuple[int, Self]:
        """Reconstruct an action of this type from parsed fields.

        Returns:
            The operation ID and reconstructed action.

        Raises:
            SyncPlanError: If a field is invalid or the priority does not match.

        """
        try:
            operation_id = int(row.operation_id)
            priority = SyncFSActionPriority(row.priority_value)
            src_storage_type = SyncStorageType(row.src_storage_value)
            dst_storage_type = (
                None
                if not row.dst_storage_value
                else SyncStorageType(row.dst_storage_value)
            )
            file_info = SyncFileInfo(
                id=int(row.file_id),
                uid=int(row.uid),
                rel_path=PurePosixPath(row.rel_path),
                mtime=row.mtime,
                size=int(row.size),
                md5=row.md5,
                sha256=row.sha256,
                move_dst_fid=(None if not row.move_dst_fid else int(row.move_dst_fid)),
            )
        except (TypeError, ValueError) as error:
            raise SyncPlanError(SyncPlanError.INVALID_ACTION_FIELD) from error

        if priority != cls.PRIORITY:
            raise SyncPlanError(
                SyncPlanError.WRONG_ACTION_TYPE.format(
                    priority,
                    cls.__name__,
                ),
            )

        action = cls.__new__(cls)
        cls._set_action_fields(
            action,
            src_storage_type,
            file_info,
            dst_storage_type,
            None if not row.dst_rel_path else PurePosixPath(row.dst_rel_path),
        )
        action.dst_mtime = row.dst_mtime or None
        return operation_id, action

    def execute(self, src_path: Path, dst_path: Path | None) -> None:
        """Apply the filesystem operation for this action.

        Raises:
            NotImplementedError: If the action type has no implementation.

        """
        raise NotImplementedError

    def validate_source(self, storages: SyncStorages) -> Path:
        """Check source availability, path, and file metadata.

        Returns:
            The validated source file path.

        Raises:
            SyncValidationError: If source validation fails.
            FileNotFoundError: If the source file does not exist.

        """
        if self.src_storage_type is None or self.file_info is None:
            raise SyncValidationError(SyncValidationError.MISSING_SOURCE_FIELDS)

        storage = storages.get(self.src_storage_type)
        if storage is None or not storage.available:
            raise SyncValidationError(SyncValidationError.UNAVAILABLE_SOURCE)

        if self.file_info.rel_path.is_absolute():
            raise SyncValidationError(SyncValidationError.ABSOLUTE_SOURCE_PATH)

        src_file = storage.path / self.file_info.rel_path

        if not src_file.exists():
            raise FileNotFoundError(src_file)
        if not src_file.is_file():
            raise SyncValidationError(src_file)

        file_stat = src_file.stat()
        mtime = format_mtime(file_stat)
        if mtime != self.file_info.mtime:
            raise SyncValidationError(
                SyncValidationError.SOURCE_MTIME_MISMATCH.format(
                    src_file, self.file_info.mtime, mtime,
                ),
            )
        if file_stat.st_size != self.file_info.size:
            raise SyncValidationError(src_file)

        return src_file

    def validate_destination(self, storages: SyncStorages) -> Path | None:
        """Check the destination path and storage identity.

        Returns:
            The destination path, or None if the action needs no destination.

        Raises:
            SyncValidationError: If destination validation fails.
            FileExistsError: If the destination already exists.

        """
        if not self.NEED_DST:
            if self.dst_storage_type is not None or self.dst_rel_path is not None:
                raise SyncValidationError(
                    SyncValidationError.UNEXPECTED_DESTINATION_FIELDS,
                )
            return None

        if self.dst_storage_type is None or self.dst_rel_path is None:
            raise SyncValidationError(SyncValidationError.MISSING_DESTINATION_FIELDS)

        storage = storages.get(self.dst_storage_type)
        if storage is None or not storage.available:
            raise SyncValidationError(SyncValidationError.UNAVAILABLE_DESTINATION)

        if not Identity.get_real_path(storage_path=storage.path).exists():
            raise SyncValidationError(SyncValidationError.MISSING_DESTINATION_IDENTITY)

        if self.dst_rel_path.is_absolute():
            raise SyncValidationError(SyncValidationError.ABSOLUTE_DESTINATION_PATH)

        dst_file = storage.path / self.dst_rel_path

        if dst_file.exists() or dst_file.is_symlink():
            raise FileExistsError(dst_file)

        return dst_file

    def run(self, storages: SyncStorages) -> None:
        """Validate the paths and execute the action."""
        src_path = self.validate_source(storages)
        dst_path = self.validate_destination(storages)
        self.execute(src_path, dst_path)

    def _report_completed_file(self) -> None:
        """Report the full size of a completed move or removal."""
        name = self.file_info.rel_path.as_posix()
        size = self.file_info.size
        self.progress.start_file(name, size)
        self.progress.advance(size)
        self.progress.finish_file()

    def _set_destination_mtime(self, path: Path) -> None:
        """Apply an explicitly planned destination timestamp."""
        if self.dst_mtime is None:
            return
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        elapsed = datetime.fromisoformat(self.dst_mtime) - epoch
        mtime_ns = (elapsed // timedelta(microseconds=1)) * 1_000
        os.utime(path, ns=(path.stat().st_atime_ns, mtime_ns))


class SyncFsActionCopy(SyncFSBaseAction):
    """Copy a file and verify the size and hashes of the copy."""

    NEED_DST = True

    @override
    def execute(self, src_path: Path, dst_path: Path | None) -> None:
        if dst_path is None:
            raise SyncExecutionError(SyncExecutionError.MISSING_DESTINATION_PATH)

        self.progress.start_file(
            self.file_info.rel_path.as_posix(), self.file_info.size,
        )
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        # Claim the path before enabling cleanup for this copy attempt.
        dst_path.touch(exist_ok=False)
        try:
            self._copy_and_verify(src_path, dst_path)
            self._set_destination_mtime(dst_path)
        except BaseException:
            dst_path.unlink(missing_ok=True)
            raise
        self.progress.finish_file()

    def _copy_and_verify(self, src_path: Path, dst_path: Path) -> None:
        """Copy the file and check its size and hashes against the plan.

        Raises:
            SyncExecutionError: If the copied size or a hash differs from the plan.

        """
        copy_result = copy_with_hashes(
            src_path,
            dst_path,
            preserve_mtime=True,
            on_chunk=self.progress.advance,
        )
        if copy_result.size != self.file_info.size:
            raise SyncExecutionError(SyncExecutionError.SIZE_MISMATCH)
        if copy_result.md5 != self.file_info.md5:
            raise SyncExecutionError(SyncExecutionError.MD5_MISMATCH)
        if copy_result.sha256 != self.file_info.sha256:
            raise SyncExecutionError(SyncExecutionError.SHA256_MISMATCH)


class SyncFsActionMove(SyncFSBaseAction):
    """Move a file to its planned destination."""

    NEED_DST = True

    @override
    def execute(self, src_path: Path, dst_path: Path | None) -> None:
        if dst_path is None:
            raise SyncExecutionError(SyncExecutionError.MISSING_DESTINATION_PATH)

        dst_path.parent.mkdir(parents=True, exist_ok=True)
        src_path.move(dst_path)
        try:
            self._set_destination_mtime(dst_path)
        except BaseException:
            dst_path.move(src_path)
            raise
        self._report_completed_file()


class SyncFsActionRemove(SyncFSBaseAction):
    """Delete a file from a staging directory."""

    NEED_DST = False

    @override
    def execute(self, src_path: Path, dst_path: Path | None) -> None:
        src_path.unlink()
        self._report_completed_file()


class SyncActionOutCopy2Via(SyncFsActionCopy):
    """Copy a source file to the transfer storage."""

    PRIORITY = SyncFSActionPriority.OUT_CP2_VIA

    def __init__(self, src_file_info: SyncFileInfo) -> None:
        """Build the action for the supplied file metadata."""
        super().__init__(
            SyncStorageType.SRC,
            src_file_info,
            SyncStorageType.VIA,
            src_file_info.rel_path,
        )


class SyncFSActionViaCopy2LWD(SyncFsActionCopy):
    """Copy a transferred source file to outgoing staging."""

    PRIORITY = SyncFSActionPriority.VIA_CP2_LWD

    def __init__(self, src_file_info: SyncFileInfo) -> None:
        """Build the action for the supplied file metadata."""
        super().__init__(
            SyncStorageType.VIA,
            src_file_info,
            SyncStorageType.LWD_OUT_COPY,
            src_file_info.rel_path,
        )


class SyncFSActionInCopy2LWD(SyncFsActionCopy):
    """Copy an existing destination file to incoming staging."""

    PRIORITY = SyncFSActionPriority.IN_CP2_LWD

    def __init__(
        self,
        src_file_info: SyncFileInfo,
        new_rel_path: PurePosixPath,
        dst_mtime: str | None = None,
    ) -> None:
        """Build the action with an explicit destination-relative path."""
        super().__init__(
            SyncStorageType.DST,
            src_file_info,
            SyncStorageType.LWD_IN_COPY,
            new_rel_path,
        )
        self.dst_mtime = dst_mtime if dst_mtime != src_file_info.mtime else None


class SyncFSActionChangedMV2LWD(SyncFsActionMove):
    """Move a changed destination file to changed staging."""

    PRIORITY = SyncFSActionPriority.CHANGED_MV2_LWD

    def __init__(self, src_file_info: SyncFileInfo) -> None:
        """Build the action for the supplied file metadata."""
        super().__init__(
            SyncStorageType.DST,
            src_file_info,
            SyncStorageType.LWD_CHANGED,
            src_file_info.rel_path,
        )


class SyncFSActionDeletedMV2LWD(SyncFsActionMove):
    """Move an obsolete destination file to deleted staging."""

    PRIORITY = SyncFSActionPriority.DELETED_MV2_LWD

    def __init__(self, src_file_info: SyncFileInfo) -> None:
        """Build the action for the supplied file metadata."""
        super().__init__(
            SyncStorageType.DST,
            src_file_info,
            SyncStorageType.LWD_DELETED,
            src_file_info.rel_path,
        )


class SyncFSActionMovedMV2LWD(SyncFsActionMove):
    """Move a reusable destination file to move staging."""

    PRIORITY = SyncFSActionPriority.MOVED_MV2_LWD

    def __init__(
        self,
        src_file_info: SyncFileInfo,
        new_rel_path: PurePosixPath,
        dst_mtime: str | None = None,
    ) -> None:
        """Build the action with an explicit destination-relative path."""
        super().__init__(
            SyncStorageType.DST,
            src_file_info,
            SyncStorageType.LWD_MOVED,
            new_rel_path,
        )
        self.dst_mtime = dst_mtime if dst_mtime != src_file_info.mtime else None


class SyncFSActionMovedMV2Dst(SyncFsActionMove):
    """Move a staged reusable file to its final destination."""

    PRIORITY = SyncFSActionPriority.MOVED_MV2_DST

    def __init__(self, src_file_info: SyncFileInfo) -> None:
        """Build the action for the supplied file metadata."""
        super().__init__(
            SyncStorageType.LWD_MOVED,
            src_file_info,
            SyncStorageType.DST,
            src_file_info.rel_path,
        )


class SyncFSActionInMV2Dst(SyncFsActionMove):
    """Move an incoming staged copy to its final destination."""

    PRIORITY = SyncFSActionPriority.IN_MV2_DST

    def __init__(self, src_file_info: SyncFileInfo) -> None:
        """Build the action for the supplied file metadata."""
        super().__init__(
            SyncStorageType.LWD_IN_COPY,
            src_file_info,
            SyncStorageType.DST,
            src_file_info.rel_path,
        )


class SyncFSActionOutMV2Dst(SyncFsActionMove):
    """Move an outgoing staged copy to its final destination."""

    PRIORITY = SyncFSActionPriority.OUT_MV2_DST

    def __init__(self, src_file_info: SyncFileInfo) -> None:
        """Build the action for the supplied file metadata."""
        super().__init__(
            SyncStorageType.LWD_OUT_COPY,
            src_file_info,
            SyncStorageType.DST,
            src_file_info.rel_path,
        )


class SyncFSActionRemoveDeleted(SyncFsActionRemove):
    """Remove a file from deleted staging."""

    PRIORITY = SyncFSActionPriority.DELETED_RM

    def __init__(self, src_file_info: SyncFileInfo) -> None:
        """Build the action for the supplied file metadata."""
        super().__init__(
            SyncStorageType.LWD_DELETED,
            src_file_info,
            None,
            None,
        )


class SyncFSActionRemoveChanged(SyncFsActionRemove):
    """Remove a file from changed staging."""

    PRIORITY = SyncFSActionPriority.CHANGED_RM

    def __init__(self, src_file_info: SyncFileInfo) -> None:
        """Build the action for the supplied file metadata."""
        super().__init__(
            SyncStorageType.LWD_CHANGED,
            src_file_info,
            None,
            None,
        )


class SyncTask:
    """Prepare a sync plan and run it across the transfer and local stages."""

    STORAGE_LINE_COLUMNS = 2
    ACTION_LINE_COLUMNS = 2
    ACTION_LOG_SEPARATOR = "--- actionlog ---"

    def __init__(  # ruff: ignore[too-many-arguments, too-many-positional-arguments]
        self,
        db: SchizophreniaDB,
        src_storage_name: str,
        dst_storage_name: str,
        syncplan_file: Path,
        via: Path | None = None,
        lwd: Path | None = None,
        *,
        progress: bool = False,
    ) -> None:
        """Bind the plan to its database storages and optional staging paths.

        Raises:
            FileNotFoundError: If a staging path does not exist.
            NotADirectoryError: If a staging path is not a directory.

        """
        self.db = db
        self.show_progress = progress
        self.src_storage = self.db.select_storage(src_storage_name)
        self.dst_storage = self.db.select_storage(dst_storage_name)
        self.sync_plan_file = syncplan_file
        self.via = via
        self.lwd = lwd

        if self.via is not None and not self.via.exists():
            raise FileNotFoundError(self.via)
        if self.via is not None and not self.via.is_dir():
            raise NotADirectoryError(self.via)
        if self.lwd is not None and not self.lwd.exists():
            raise FileNotFoundError(self.lwd)
        if self.lwd is not None and not self.lwd.is_dir():
            raise NotADirectoryError(self.lwd)

    def storage_type_to_path(self, storage_type: SyncStorageType) -> Path | None:
        """Resolve the configured path for a storage type.

        Returns:
            The storage path, or None if the location is unconfigured.

        """
        match storage_type:
            case SyncStorageType.SRC:
                return self.src_storage.abs_path
            case SyncStorageType.DST:
                return self.dst_storage.abs_path
            case SyncStorageType.VIA:
                return self.via
            case (
                SyncStorageType.LWD_IN_COPY
                | SyncStorageType.LWD_OUT_COPY
                | SyncStorageType.LWD_MOVED
                | SyncStorageType.LWD_DELETED
                | SyncStorageType.LWD_CHANGED
            ):
                if self.lwd is None:
                    return None
                return self.lwd / storage_type.value

    def load_sync_plan(  # ruff: ignore[complex-structure, too-many-branches]
        self,
    ) -> tuple[
        SyncStorages,
        dict[int, SyncFSBaseAction],
    ]:
        """Read the plan and validate the configured storage identities.

        Returns:
            The storage locations and actions indexed by operation ID.

        Raises:
            SyncPlanError: If the plan is invalid or a storage identity differs.

        """
        storage_identities: dict[SyncStorageType, Identity] = {}
        actions: dict[int, SyncFSBaseAction] = {}

        with self.sync_plan_file.open("r", encoding="utf-8", newline="") as plan_file:
            for line_number, line_raw in enumerate(plan_file, start=1):
                line = line_raw.rstrip("\r\n")

                if line_number <= len(SyncStorageType):
                    # noinspection DuplicatedCode
                    try:
                        rows = list(csv.reader([line], strict=True))
                    except csv.Error as error:
                        raise SyncPlanError(
                            SyncPlanError.INVALID_STORAGE_CSV.format(
                                line_number,
                            ),
                        ) from error

                    if len(rows) != 1 or len(rows[0]) != self.STORAGE_LINE_COLUMNS:
                        raise SyncPlanError(
                            SyncPlanError.INVALID_STORAGE_COLUMNS.format(
                                line_number,
                            ),
                        )

                    try:
                        storage_type = SyncStorageType(rows[0][0])
                        identity = Identity(rows[0][1])
                    except (TypeError, ValueError) as error:
                        raise SyncPlanError(
                            SyncPlanError.INVALID_STORAGE_ENTRY.format(
                                line_number,
                            ),
                        ) from error

                    if storage_type in storage_identities:
                        raise SyncPlanError(
                            SyncPlanError.DUPLICATE_STORAGE.format(
                                storage_type.value,
                            ),
                        )
                    storage_identities[storage_type] = identity
                    continue

                if line == self.ACTION_LOG_SEPARATOR:
                    break

                try:
                    operation_id, action = SyncFSBaseAction.load_str(line)
                except ValueError as error:
                    raise SyncPlanError(
                        SyncPlanError.INVALID_ACTION.format(
                            line_number,
                        ),
                    ) from error

                if operation_id in actions:
                    raise SyncPlanError(
                        SyncPlanError.DUPLICATE_ACTION.format(
                            operation_id,
                        ),
                    )
                actions[operation_id] = action

        missing_storages = set(SyncStorageType) - storage_identities.keys()
        if missing_storages:
            missing_names = ", ".join(
                storage_type.value for storage_type in missing_storages
            )
            raise SyncPlanError(
                SyncPlanError.MISSING_STORAGE_IDENTITIES.format(
                    missing_names,
                ),
            )

        database_identities = {
            SyncStorageType.SRC: self.src_storage.identity,
            SyncStorageType.DST: self.dst_storage.identity,
        }
        for storage_type, database_identity in database_identities.items():
            plan_identity = storage_identities[storage_type]
            if plan_identity != database_identity:
                raise SyncPlanError(
                    SyncPlanError.DATABASE_IDENTITY_MISMATCH.format(
                        storage_type.value,
                        plan_identity.to_str(),
                        database_identity.to_str(),
                    ),
                )

        storages: SyncStorages = {}
        for storage_type, plan_identity in storage_identities.items():
            storage_path = self.storage_type_to_path(storage_type)
            available = False

            if storage_path is not None:
                identity_file = Identity.get_real_path(storage_path)
                if identity_file.exists():
                    actual_identity = Identity.read_from_storage_dir(storage_path)
                    if actual_identity != plan_identity:
                        raise SyncPlanError(
                            SyncPlanError.LOCATION_IDENTITY_MISMATCH.format(
                                storage_type.value,
                                storage_path,
                                plan_identity.to_str(),
                                actual_identity.to_str(),
                            ),
                        )
                    available = True

            storages[storage_type] = SyncStoragesType(
                identity=plan_identity,
                path=storage_path if storage_path is not None else Path(),
                available=available,
            )

        return storages, actions

    def init_via(self) -> SyncStorages:
        """Write the plan identity to the transfer storage.

        Returns:
            The plan storage locations with transfer storage marked available.

        Raises:
            SyncConfigurationError: If the transfer path is missing.

        """
        via_path = self.storage_type_to_path(SyncStorageType.VIA)
        if via_path is None:
            raise SyncConfigurationError(
                SyncConfigurationError.MISSING_VIA.format("init_via"),
            )

        storages, _ = self.load_sync_plan()
        storages[SyncStorageType.VIA].identity.save_to_storage_dir(via_path)
        storages[SyncStorageType.VIA] = storages[SyncStorageType.VIA]._replace(
            available=True,
        )
        return storages

    def init_lwd(self) -> SyncStorages:
        """Create local staging directories and write their identities.

        Returns:
            The plan storage locations with local staging marked available.

        Raises:
            SyncConfigurationError: If the local staging path is missing.
            FileExistsError: If a staging directory already has an identity.

        """
        lwd_storage_types = tuple(
            storage_type
            for storage_type in SyncStorageType
            if storage_type.value.startswith("lwd_")
        )
        storage_dirs: dict[SyncStorageType, Path] = {}
        for storage_type in lwd_storage_types:
            storage_dir = self.storage_type_to_path(storage_type)
            if storage_dir is None:
                raise SyncConfigurationError(
                    SyncConfigurationError.MISSING_LWD.format(
                        "init_lwd",
                    ),
                )
            storage_dirs[storage_type] = storage_dir

        storages, _ = self.load_sync_plan()

        for storage_dir in storage_dirs.values():
            identity_file = Identity.get_real_path(storage_dir)
            if identity_file.exists():
                raise FileExistsError(identity_file)

        for storage_type, storage_dir in storage_dirs.items():
            storage_dir.mkdir(exist_ok=True)
            storages[storage_type].identity.save_to_storage_dir(storage_dir)
            storages[storage_type] = storages[storage_type]._replace(available=True)

        return storages

    def _load_completed_action_ids(self, action_ids: set[int]) -> set[int]:  # ruff: ignore[complex-structure]
        completed_action_ids: set[int] = set()
        separator_found = False

        with self.sync_plan_file.open("r", encoding="utf-8", newline="") as plan_file:
            for line_number, line_raw in enumerate(plan_file, start=1):
                line = line_raw.rstrip("\r\n")
                if not separator_found:
                    if line == self.ACTION_LOG_SEPARATOR:
                        separator_found = True
                    continue

                if not line:
                    continue

                # noinspection DuplicatedCode
                try:
                    rows = list(csv.reader([line], strict=True))
                except csv.Error as error:
                    raise SyncPlanError(
                        SyncPlanError.INVALID_ACTION_LOG_CSV.format(
                            line_number,
                        ),
                    ) from error

                if len(rows) != 1 or len(rows[0]) != self.ACTION_LINE_COLUMNS:
                    raise SyncPlanError(
                        SyncPlanError.INVALID_ACTION_LOG_COLUMNS.format(
                            line_number,
                        ),
                    )

                operation_id_value, status = rows[0]
                try:
                    operation_id = int(operation_id_value)
                except ValueError as error:
                    raise SyncPlanError(
                        SyncPlanError.INVALID_ACTION_NUMBER.format(
                            line_number,
                        ),
                    ) from error

                if status != "done":
                    raise SyncPlanError(
                        SyncPlanError.INVALID_ACTION_STATUS.format(
                            line_number,
                        ),
                    )
                if operation_id not in action_ids:
                    raise SyncPlanError(
                        SyncPlanError.UNKNOWN_ACTION_NUMBER.format(
                            operation_id,
                            line_number,
                        ),
                    )
                completed_action_ids.add(operation_id)

        if not separator_found:
            raise SyncPlanError(
                SyncPlanError.MISSING_ACTION_LOG_SEPARATOR.format(
                    self.ACTION_LOG_SEPARATOR,
                ),
            )

        return completed_action_ids

    def _append_completed_action(self, operation_id: int) -> None:
        with self.sync_plan_file.open("a", encoding="utf-8", newline="") as plan_file:
            csv.writer(plan_file, lineterminator="\n").writerow((operation_id, "done"))
            plan_file.flush()
            os.fsync(plan_file.fileno())

    def _run_actions(self, *, run_via_stage: bool) -> None:
        storages, actions = self.load_sync_plan()
        completed_action_ids = self._load_completed_action_ids(set(actions))

        pending: list[tuple[int, SyncFSBaseAction]] = []
        for operation_id, action in sorted(actions.items()):
            is_via_action = action.PRIORITY == SyncFSActionPriority.OUT_CP2_VIA
            if is_via_action != run_via_stage:
                continue
            if operation_id in completed_action_ids:
                continue

            pending.append((operation_id, action))

        progress = NO_PROGRESS
        if self.show_progress:
            progress = FileProgress(
                len(pending), sum(action.file_info.size for _, action in pending),
            )

        for operation_id, action in pending:
            action.progress = progress
            action.run(storages)
            self._append_completed_action(operation_id)

    def run_via(self) -> None:
        """Run pending transfer actions and record their completion.

        Raises:
            SyncConfigurationError: If the transfer path is missing.

        """
        if self.storage_type_to_path(SyncStorageType.VIA) is None:
            raise SyncConfigurationError(
                SyncConfigurationError.MISSING_VIA.format("run_via"),
            )
        self._run_actions(run_via_stage=True)

    def run_lwd(self) -> None:
        """Run pending local staging actions and record their completion.

        Raises:
            SyncConfigurationError: If the local staging path is missing.

        """
        if self.storage_type_to_path(SyncStorageType.LWD_IN_COPY) is None:
            raise SyncConfigurationError(
                SyncConfigurationError.MISSING_LWD.format(
                    "run_lwd",
                ),
            )
        self._run_actions(run_via_stage=False)

    def prepare(self) -> None:  # ruff: ignore[complex-structure, too-many-branches, too-many-statements]
        """Build and write a plan from the current source and destination scans.

        Raises:
            FileExistsError: If the plan file already exists.
            SyncExecutionError: If a database row cannot produce a valid action.

        """
        if self.sync_plan_file.exists():
            raise FileExistsError(self.sync_plan_file)

        with self.db.write_transaction():
            self.db.sync_clear()
            self.db.sync_create_table()
            self.db.sync_fill_table(self.src_storage.id, self.dst_storage.id)
            self.db.sync_mark_sync_state()
            self.db.sync_find_strategy()
            while self.db.sync_iter_solving_move_strategy():
                pass

        actions: list[SyncFSBaseAction] = []

        for row in self.db.sync_iter_entries():
            operation_actions: list[SyncFSBaseAction] = []
            src = row.src
            dst = row.dst
            in_src = row.in_src

            match row.sync_status:
                case SyncFileStatus.CHANGED:
                    if dst is None:
                        raise SyncExecutionError(SyncExecutionError.MISSING_SOURCE_ROW)
                    if dst.move_dst_fid is None:
                        operation_actions.extend((
                            SyncFSActionChangedMV2LWD(dst),
                            SyncFSActionRemoveChanged(dst),
                        ))
                case (
                    SyncFileStatus.DISAPPEARED
                    | SyncFileStatus.APPEARED
                    | SyncFileStatus.NOT_CHANGED
                ):
                    pass
                case _:
                    raise SyncExecutionError(
                        SyncExecutionError.UNKNOWN_STATUS.format(
                            row.sync_status,
                        ),
                    )

            match row.sync_strategy:
                case SyncStrategy.DO_NOTHING:
                    pass
                case SyncStrategy.OUT_COPY:
                    if src is None:
                        raise SyncExecutionError(SyncExecutionError.MISSING_SOURCE_ROW)
                    operation_actions.extend((
                        SyncActionOutCopy2Via(src),
                        SyncFSActionViaCopy2LWD(src),
                        SyncFSActionOutMV2Dst(src),
                    ))
                case SyncStrategy.IN_COPY:
                    if src is None or in_src is None:
                        raise SyncExecutionError(
                            SyncExecutionError.MISSING_SOURCE_OR_DONOR_ROW,
                        )
                    operation_actions.extend((
                        SyncFSActionInCopy2LWD(
                            in_src, src.rel_path, src.mtime,
                        ),
                        SyncFSActionInMV2Dst(src),
                    ))
                case SyncStrategy.MOVE_TARGET:
                    if src is None or in_src is None:
                        raise SyncExecutionError(
                            SyncExecutionError.MISSING_SOURCE_OR_DONOR_ROW,
                        )
                    operation_actions.extend((
                        SyncFSActionMovedMV2LWD(
                            in_src, src.rel_path, src.mtime,
                        ),
                        SyncFSActionMovedMV2Dst(src),
                    ))
                case SyncStrategy.DELETE:
                    if dst is None:
                        raise SyncExecutionError(SyncExecutionError.MISSING_SOURCE_ROW)
                    if dst.move_dst_fid is None:
                        operation_actions.extend((
                            SyncFSActionDeletedMV2LWD(dst),
                            SyncFSActionRemoveDeleted(dst),
                        ))
                case _:
                    raise SyncExecutionError(
                        SyncExecutionError.UNKNOWN_STRATEGY.format(
                            row.sync_strategy,
                        ),
                    )

            actions.extend(operation_actions)

        actions.sort(key=lambda _action: _action.PRIORITY.order)

        storage_identities = {
            SyncStorageType.SRC: self.src_storage.identity,
            SyncStorageType.DST: self.dst_storage.identity,
            **{
                storage_type: Identity.new()
                for storage_type in SyncStorageType
                if storage_type not in {SyncStorageType.SRC, SyncStorageType.DST}
            },
        }

        with self.sync_plan_file.open("x", encoding="utf-8", newline="") as plan_file:
            identity_writer = csv.writer(plan_file, lineterminator="\n")
            for storage_type in SyncStorageType:
                identity_writer.writerow(
                    (storage_type.value, storage_identities[storage_type].to_str()),
                )

            for operation_id, action in enumerate(actions, start=1):
                plan_file.write(action.dump(operation_id))
                plan_file.write("\n")

            plan_file.write(f"{self.ACTION_LOG_SEPARATOR}\n")
