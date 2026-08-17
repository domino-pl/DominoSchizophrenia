from src.db.storage import SchizophreniaDBStorage


class SchizophreniaDBFile(SchizophreniaDBStorage):
    def _init_db(self):
        super()._init_db()

        sqls = [
            """
            CREATE TABLE IF NOT EXISTS uids
            (
                uid    INTEGER PRIMARY KEY AUTOINCREMENT,
                size   INTEGER NOT NULL,
                md5    TEXT    NOT NULL,
                sha256 TEXT    NOT NULL
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uids_uidx_size_md5_sha256 ON uids(size, md5, sha256)
            """,
            """
            CREATE TABLE IF NOT EXISTS file
            (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                storage_id INTEGER NOT NULL,
                rel_path   TEXT    NOT NULL,
                uid        INTEGER NOT NULL,
                size       INTEGER NOT NULL,
                mtime      TEXT    NOT NULL,
                md5        TEXT    NOT NULL,
                sha256     TEXT    NOT NULL
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS file_unique_idx_storage_path ON file(storage_id, rel_path)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_file_uid ON file(uid)
            """,
        ]
        self.execute_list(sqls)
