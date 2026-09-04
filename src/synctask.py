import csv
import io
import os
from email.policy import default
from enum import StrEnum
from pathlib import Path, PurePath, PurePosixPath
from typing import NamedTuple, Self, TypedDict, override

from src.db import SchizophreniaDB
from src.db.storage import Storage
from src.db.sync import SyncFileInfo, SyncFileMeta, SyncFileStatus, SyncStrategy
from src.identity import Identity
from src.tools import copy_with_hashes, format_mtime


class SyncFSActionPriority(StrEnum):
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
    SRC = "src"
    DST = "dst"
    VIA = "via"
    LWD_IN_COPY = "lwd_in_copy"
    LWD_OUT_COPY = "lwd_out_copy"
    LWD_MOVED = "lwd_moved"
    LWD_DELETED = "lwd_deleted"
    LWD_CHANGED = "lwd_changed"


class SyncStoragesType(NamedTuple):
    identity: Identity
    path: Path
    available: bool


SyncStorages = dict[SyncStorageType, SyncStoragesType]


class SyncFSActionRow(NamedTuple):
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


class SyncFSBaseAction:
    NEED_DST: bool
    PRIORITY: SyncFSActionPriority

    def __init__(
        self,
        src_storage_type: SyncStorageType,
        src_file_info: SyncFileInfo,
        dst_storage_type: SyncStorageType | None,
        dst_rel_path: PurePath | None,
    ):
        self.src_storage_type = src_storage_type
        self.file_info = src_file_info
        self.dst_storage_type = dst_storage_type
        self.dst_rel_path = dst_rel_path

    def dump(self, operation_id: int) -> str:
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
        )

        if any("\n" in field or "\r" in field for field in fields):
            raise ValueError("An action field cannot contain a line break")

        output = io.StringIO()
        csv.writer(output, lineterminator="").writerow(fields)
        return output.getvalue()

    @staticmethod
    def load_str(line: str) -> tuple[int, "SyncFSBaseAction"]:
        try:
            rows = list(csv.reader([line], strict=True))
        except csv.Error as error:
            raise ValueError("Invalid action CSV") from error

        if len(rows) != 1 or len(rows[0]) != 13:
            raise ValueError("An action line must contain exactly 13 fields")

        row = SyncFSActionRow(*rows[0])

        try:
            priority = SyncFSActionPriority(row.priority_value)
        except ValueError as error:
            raise ValueError("Invalid action priority") from error

        pending = list(SyncFSBaseAction.__subclasses__())
        while pending:
            action_type = pending.pop()
            pending.extend(action_type.__subclasses__())
            if action_type.__dict__.get("PRIORITY") == priority:
                return action_type.load_typed_str(row)

        raise ValueError(f"Unsupported action priority: {priority}")

    @classmethod
    def load_typed_str(cls, row: SyncFSActionRow) -> tuple[int, Self]:
        try:
            operation_id = int(row.operation_id)
            priority = SyncFSActionPriority(row.priority_value)
            src_storage_type = SyncStorageType(row.src_storage_value)
            dst_storage_type = (
                None
                if row.dst_storage_value == ""
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
                move_dst_fid=(
                    None if row.move_dst_fid == "" else int(row.move_dst_fid)
                ),
            )
        except (TypeError, ValueError) as error:
            raise ValueError("Invalid action field value") from error

        if priority != cls.PRIORITY:
            raise ValueError(
                f"Priority {priority} does not identify an action of type {cls.__name__}"
            )

        action = cls.__new__(cls)
        SyncFSBaseAction.__init__(
            action,
            src_storage_type,
            file_info,
            dst_storage_type,
            None if row.dst_rel_path == "" else PurePosixPath(row.dst_rel_path),
        )
        return operation_id, action

    def execute(self, src_path: Path, dst_path: Path | None) -> None:
        raise NotImplementedError()

    def validate_source(self, storages: SyncStorages) -> Path:
        if self.src_storage_type is None or self.file_info is None:
            raise Exception("Source src_storage_type or file_info is None")

        storage = storages.get(self.src_storage_type)
        if storage is None or not storage.available:
            raise Exception("Source file not found")

        if self.file_info.rel_path.is_absolute():
            raise Exception("Source file path is absolute")

        src_file = storage.path / self.file_info.rel_path

        if not src_file.exists():
            raise FileNotFoundError(src_file)
        if not src_file.is_file():
            raise Exception(src_file)  # TODO: ex

        stat = os.stat(src_file)
        if format_mtime(stat) != self.file_info.mtime:
            print(f"Bad mtime for {src_file} - wanted {format_mtime(stat)} - got {self.file_info.mtime}.")
            if(input() != 'y'):
                raise Exception(src_file)  # TODO: ex
        if stat.st_size != self.file_info.size:
            raise Exception(src_file)  # TODO: ex

        return src_file

    def validate_destination(self, storages: SyncStorages) -> Path | None:
        if not self.NEED_DST:
            if self.dst_storage_type is not None or self.dst_rel_path is not None:
                raise Exception("dst_storage_type or dst_rel_path is not None")
            return None

        if self.dst_storage_type is None or self.dst_rel_path is None:
            raise Exception("dst_storage_type or dst_rel_path is None")

        storage = storages.get(self.dst_storage_type)
        if storage is None or not storage.available:
            raise Exception("Destination file storage does not found")

        if not Identity.get_real_path(storage_path=storage.path).exists():
            raise Exception("Destination storage dispersed.")

        if self.dst_rel_path.is_absolute():
            raise Exception("Destination file path is absolute")

        dst_file = storage.path / self.dst_rel_path

        if dst_file.exists():
            raise FileExistsError(dst_file)

        return dst_file

    def run(self, storages: SyncStorages):
        src_path = self.validate_source(storages)
        dst_path = self.validate_destination(storages)
        self.execute(src_path, dst_path)


class SyncFsActionCopy(SyncFSBaseAction):
    NEED_DST = True

    @override
    def execute(self, src_path: Path, dst_path: Path | None) -> None:
        if dst_path is None:
            raise RuntimeError("dst_path is None")

        dst_path.parent.mkdir(parents=True, exist_ok=True)
        copy_result = copy_with_hashes(src_path, dst_path, preserve_mtime=True)

        if copy_result.size != self.file_info.size:
            raise Exception("Size error")
        if copy_result.md5 != self.file_info.md5:
            raise Exception("MD5 error")
        if copy_result.sha256 != self.file_info.sha256:
            raise Exception("SHA256 error")


class SyncFsActionMove(SyncFSBaseAction):
    NEED_DST = True

    @override
    def execute(self, src_path: Path, dst_path: Path | None) -> None:
        if dst_path is None:
            raise RuntimeError("dst_path is None")

        dst_path.parent.mkdir(parents=True, exist_ok=True)
        src_path.move(dst_path)


class SyncFsActionRemove(SyncFSBaseAction):
    NEED_DST = False

    @override
    def execute(self, src_path: Path, dst_path: Path | None) -> None:
        src_path.unlink()


class SyncActionOutCopy2Via(SyncFsActionCopy):
    PRIORITY = SyncFSActionPriority.OUT_CP2_VIA

    def __init__(self, src_file_info: SyncFileInfo):
        super().__init__(
            SyncStorageType.SRC,
            src_file_info,
            SyncStorageType.VIA,
            src_file_info.rel_path,
        )


class SyncFSActionViaCopy2LWD(SyncFsActionCopy):
    PRIORITY = SyncFSActionPriority.VIA_CP2_LWD

    def __init__(self, src_file_info: SyncFileInfo):
        super().__init__(
            SyncStorageType.VIA,
            src_file_info,
            SyncStorageType.LWD_OUT_COPY,
            src_file_info.rel_path,
        )


class SyncFSActionInCopy2LWD(SyncFsActionCopy):
    PRIORITY = SyncFSActionPriority.IN_CP2_LWD

    def __init__(
        self, src_file_info: SyncFileInfo, new_rel_path: PurePosixPath
    ):
        super().__init__(
            SyncStorageType.DST,
            src_file_info,
            SyncStorageType.LWD_IN_COPY,
            new_rel_path,
        )


class SyncFSActionChangedMV2LWD(SyncFsActionMove):
    PRIORITY = SyncFSActionPriority.CHANGED_MV2_LWD

    def __init__(self, src_file_info: SyncFileInfo):
        super().__init__(
            SyncStorageType.DST,
            src_file_info,
            SyncStorageType.LWD_CHANGED,
            src_file_info.rel_path,
        )


class SyncFSActionDeletedMV2LWD(SyncFsActionMove):
    PRIORITY = SyncFSActionPriority.DELETED_MV2_LWD

    def __init__(self, src_file_info: SyncFileInfo):
        super().__init__(
            SyncStorageType.DST,
            src_file_info,
            SyncStorageType.LWD_DELETED,
            src_file_info.rel_path,
        )


class SyncFSActionMovedMV2LWD(SyncFsActionMove):
    PRIORITY = SyncFSActionPriority.MOVED_MV2_LWD

    def __init__(
        self, src_file_info: SyncFileInfo, new_rel_path: PurePosixPath
    ):
        super().__init__(
            SyncStorageType.DST,
            src_file_info,
            SyncStorageType.LWD_MOVED,
            new_rel_path,
        )


class SyncFSActionMovedMV2Dst(SyncFsActionMove):
    PRIORITY = SyncFSActionPriority.MOVED_MV2_DST

    def __init__(self, src_file_info: SyncFileInfo):
        super().__init__(
            SyncStorageType.LWD_MOVED,
            src_file_info,
            SyncStorageType.DST,
            src_file_info.rel_path,
        )


class SyncFSActionInMV2Dst(SyncFsActionMove):
    PRIORITY = SyncFSActionPriority.IN_MV2_DST

    def __init__(self, src_file_info: SyncFileInfo):
        super().__init__(
            SyncStorageType.LWD_IN_COPY,
            src_file_info,
            SyncStorageType.DST,
            src_file_info.rel_path,
        )


class SyncFSActionOutMV2Dst(SyncFsActionMove):
    PRIORITY = SyncFSActionPriority.OUT_MV2_DST

    def __init__(self, src_file_info: SyncFileInfo):
        super().__init__(
            SyncStorageType.LWD_OUT_COPY,
            src_file_info,
            SyncStorageType.DST,
            src_file_info.rel_path,
        )


class SyncFSActionRemoveDeleted(SyncFsActionRemove):
    PRIORITY = SyncFSActionPriority.DELETED_RM

    def __init__(self, src_file_info: SyncFileInfo):
        super().__init__(
            SyncStorageType.LWD_DELETED,
            src_file_info,
            None,
            None,
        )


class SyncFSActionRemoveChanged(SyncFsActionRemove):
    PRIORITY = SyncFSActionPriority.CHANGED_RM

    def __init__(self, src_file_info: SyncFileInfo):
        super().__init__(
            SyncStorageType.LWD_CHANGED,
            src_file_info,
            None,
            None,
        )


class SyncTask:
    ACTION_LOG_SEPARATOR = "--- actionlog ---"

    def __init__(
        self,
        db: SchizophreniaDB,
        src_storage_name: str,
        dst_storage_name: str,
        syncplan_file: Path,
        via: Path | None = None,
        lwd: Path | None = None,
    ) -> None:
        self.db = db
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

    def load_sync_plan(
        self,
    ) -> tuple[
        SyncStorages,
        dict[int, SyncFSBaseAction],
    ]:
        storage_identities: dict[SyncStorageType, Identity] = {}
        actions: dict[int, SyncFSBaseAction] = {}

        with self.sync_plan_file.open("r", encoding="utf-8", newline="") as plan_file:
            for line_number, line in enumerate(plan_file, start=1):
                line = line.rstrip("\r\n")

                if line_number <= len(SyncStorageType):
                    try:
                        rows = list(csv.reader([line], strict=True))
                    except csv.Error as error:
                        raise ValueError(
                            f"Invalid storage CSV at line {line_number}"
                        ) from error

                    if len(rows) != 1 or len(rows[0]) != 2:
                        raise ValueError(
                            f"A storage line must contain exactly 2 fields "
                            f"(line {line_number})"
                        )

                    try:
                        storage_type = SyncStorageType(rows[0][0])
                        identity = Identity(rows[0][1])
                    except (TypeError, ValueError) as error:
                        raise ValueError(
                            f"Invalid storage entry at line {line_number}"
                        ) from error

                    if storage_type in storage_identities:
                        raise ValueError(f"Duplicate storage: {storage_type.value}")
                    storage_identities[storage_type] = identity
                    continue

                if line == self.ACTION_LOG_SEPARATOR:
                    break

                try:
                    operation_id, action = SyncFSBaseAction.load_str(line)
                except ValueError as error:
                    raise ValueError(f"Invalid action at line {line_number}") from error

                if operation_id in actions:
                    raise ValueError(f"Duplicate action number: {operation_id}")
                actions[operation_id] = action

        missing_storages = set(SyncStorageType) - storage_identities.keys()
        if missing_storages:
            missing_names = ", ".join(
                storage_type.value for storage_type in missing_storages
            )
            raise ValueError(f"Missing storage identities: {missing_names}")

        database_identities = {
            SyncStorageType.SRC: self.src_storage.identity,
            SyncStorageType.DST: self.dst_storage.identity,
        }
        for storage_type, database_identity in database_identities.items():
            plan_identity = storage_identities[storage_type]
            if plan_identity != database_identity:
                raise ValueError(
                    f"Identity mismatch for {storage_type.value} storage: "
                    f"plan has {plan_identity.to_str()}, database has "
                    f"{database_identity.to_str()}"
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
                        raise ValueError(
                            f"Identity mismatch for {storage_type.value} storage "
                            f"at {storage_path}: plan has "
                            f"{plan_identity.to_str()}, location has "
                            f"{actual_identity.to_str()}"
                        )
                    available = True

            storages[storage_type] = SyncStoragesType(
                identity=plan_identity,
                path=storage_path if storage_path is not None else Path(),
                available=available,
            )

        return storages, actions

    def init_via(self) -> SyncStorages:
        via_path = self.storage_type_to_path(SyncStorageType.VIA)
        if via_path is None:
            raise ValueError("--via is required for init_via")

        storages, _ = self.load_sync_plan()
        storages[SyncStorageType.VIA].identity.save_to_storage_dir(via_path)
        storages[SyncStorageType.VIA] = storages[SyncStorageType.VIA]._replace(
            available=True
        )
        return storages

    def init_lwd(self) -> SyncStorages:
        lwd_storage_types = tuple(
            storage_type
            for storage_type in SyncStorageType
            if storage_type.value.startswith("lwd_")
        )
        storage_dirs: dict[SyncStorageType, Path] = {}
        for storage_type in lwd_storage_types:
            storage_dir = self.storage_type_to_path(storage_type)
            if storage_dir is None:
                raise ValueError("--lwd is required for init_lwd")
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

    def _load_completed_action_ids(self, action_ids: set[int]) -> set[int]:
        completed_action_ids: set[int] = set()
        separator_found = False

        with self.sync_plan_file.open("r", encoding="utf-8", newline="") as plan_file:
            for line_number, line in enumerate(plan_file, start=1):
                line = line.rstrip("\r\n")
                if not separator_found:
                    if line == self.ACTION_LOG_SEPARATOR:
                        separator_found = True
                    continue

                if line == "":
                    continue

                try:
                    rows = list(csv.reader([line], strict=True))
                except csv.Error as error:
                    raise ValueError(
                        f"Invalid action log CSV at line {line_number}"
                    ) from error

                if len(rows) != 1 or len(rows[0]) != 2:
                    raise ValueError(
                        f"An action log line must contain exactly 2 fields "
                        f"(line {line_number})"
                    )

                operation_id_value, status = rows[0]
                try:
                    operation_id = int(operation_id_value)
                except ValueError as error:
                    raise ValueError(
                        f"Invalid action number at line {line_number}"
                    ) from error

                if status != "done":
                    raise ValueError(f"Invalid action status at line {line_number}")
                if operation_id not in action_ids:
                    raise ValueError(
                        f"Unknown action number {operation_id} at line {line_number}"
                    )
                completed_action_ids.add(operation_id)

        if not separator_found:
            raise ValueError(
                f"Missing {self.ACTION_LOG_SEPARATOR!r} separator in sync plan"
            )

        return completed_action_ids

    def _append_completed_action(self, operation_id: int) -> None:
        with self.sync_plan_file.open("a", encoding="utf-8", newline="") as plan_file:
            csv.writer(plan_file, lineterminator="\n").writerow(
                (operation_id, "done")
            )
            plan_file.flush()
            os.fsync(plan_file.fileno())

    def _run_actions(self, run_via_stage: bool) -> None:
        storages, actions = self.load_sync_plan()
        completed_action_ids = self._load_completed_action_ids(set(actions))

        for operation_id, action in sorted(actions.items()):
            is_via_action = action.PRIORITY == SyncFSActionPriority.OUT_CP2_VIA
            if is_via_action != run_via_stage:
                continue
            if operation_id in completed_action_ids:
                continue

            action.run(storages)
            self._append_completed_action(operation_id)
            completed_action_ids.add(operation_id)

    def run_via(self) -> None:
        if self.storage_type_to_path(SyncStorageType.VIA) is None:
            raise ValueError("--via is required for run_via")
        self._run_actions(run_via_stage=True)

    def run_lwd(self) -> None:
        if self.storage_type_to_path(SyncStorageType.LWD_IN_COPY) is None:
            raise ValueError("--lwd is required for run_lwd")
        self._run_actions(run_via_stage=False)

    def prepare(self):
        if self.sync_plan_file.exists():
            raise FileExistsError(self.sync_plan_file)

        self.db.sync_clear()
        self.db.sync_create_table()
        self.db.sync_fill_table(self.src_storage.id, self.dst_storage.id)
        self.db.sync_mark_sync_state()
        self.db.sync_find_strategy()
        while self.db.sync_iter_solving_move_strategy():
            pass
        self.db.commit()

        actions: list[SyncFSBaseAction] = []

        for row in self.db.sync_iter_entries():
            operation_actions: list[SyncFSBaseAction] = []
            src = row.src
            dst = row.dst
            in_src = row.in_src

            match row.sync_status:
                case SyncFileStatus.CHANGED:
                    if dst is None:
                        raise RuntimeError("row.src is None")
                    if not dst.move_dst_fid:
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
                    raise RuntimeError(f"Unknown sync_status: {row.sync_status}")

            match row.sync_strategy:
                case SyncStrategy.DO_NOTHING:
                    pass
                case SyncStrategy.OUT_COPY:
                    if src is None:
                        raise RuntimeError("row.src is None")
                    operation_actions.extend((
                        SyncActionOutCopy2Via(src),
                        SyncFSActionViaCopy2LWD(src),
                        SyncFSActionOutMV2Dst(src),
                    ))
                case SyncStrategy.IN_COPY:
                    if src is None or in_src is None:
                        raise RuntimeError("row.src or row.in_src is None")
                    operation_actions.extend((
                        SyncFSActionInCopy2LWD(in_src, src.rel_path),
                        SyncFSActionInMV2Dst(src),
                    ))
                case SyncStrategy.MOVE_TARGET:
                    if src is None or in_src is None:
                        raise RuntimeError("row.src or row.in_src is None")
                    operation_actions.extend((
                        SyncFSActionMovedMV2LWD(in_src, src.rel_path),
                        SyncFSActionMovedMV2Dst(src),
                    ))
                case SyncStrategy.DELETE:
                    if dst is None:
                        raise RuntimeError("row.src is None")
                    if not dst.move_dst_fid:
                        operation_actions.extend((
                            SyncFSActionDeletedMV2LWD(dst),
                            SyncFSActionRemoveDeleted(dst),
                        ))
                case _:
                    raise RuntimeError(f"Unknown strategy {row.sync_strategy}")

            actions.extend(operation_actions)

        actions.sort(key=lambda action: action.PRIORITY.order)

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
