from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import re
import sqlite3
import stat
import subprocess
import sys
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import requests
import uvicorn
import yaml
from fastapi import FastAPI, HTTPException
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver


# ============================================================
# CONFIGURATION
# ============================================================

CONFIG_PATH = os.environ.get(
    "CAMERA_AGENT_CONFIG",
    "camera_agent_config.yaml"
)


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# STANDALONE mode (chạy riêng camera_agent.py):
#   tự load config từ file camera_agent_config.yaml (nếu có).
# MERGED mode (được import bởi AI Server - main.py):
#   main.py sẽ inject CONFIG (section 'camera_agent' trong config.yaml
#   của server) và gọi setup_logging() trước khi khởi động CameraAgent.
CONFIG: Optional[dict] = None

if os.path.exists(CONFIG_PATH):
    CONFIG = load_config(CONFIG_PATH)


# ============================================================
# LOGGING
# ============================================================

def setup_logging() -> logging.Logger:
    global LOGGER

    if CONFIG is None:
        # MERGED mode: config chưa được inject -> trả logger rỗng.
        # main.py sẽ gọi lại hàm này sau khi inject CONFIG.
        return logging.getLogger("CameraAgent")

    log_dir = Path(CONFIG["storage"]["log_dir"])
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("CameraAgent")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(threadName)s | %(message)s"
    )

    file_handler = logging.FileHandler(
        log_dir / "camera_agent.log",
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Gỡ handler cũ (nếu có) để inject lại config nhiều lần an toàn.
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    LOGGER = logger
    return logger


LOGGER = setup_logging()


# ============================================================
# CONSTANTS / VALIDATION
# ============================================================

SAFE_COMPONENT_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


from datetime import datetime, timedelta


def observed_now_iso() -> str:
    return datetime.now().astimezone().isoformat(
        timespec="milliseconds"
    )


def detect_image_format(path: Path) -> Optional[str]:
    try:
        with open(path, "rb") as f:
            header = f.read(32)

        if header.startswith(b"\xFF\xD8\xFF"):
            return "jpg"

        if header.startswith(b"\x89PNG\r\n\x1a\n"):
            return "png"

        if header.startswith(b"BM"):
            return "bmp"

        return None

    except Exception:
        return None


def safe_component(value: str, field_name: str) -> str:
    if not value or not SAFE_COMPONENT_PATTERN.match(value):
        raise ValueError(
            f"{field_name} contains invalid characters: {value}"
        )

    return value


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class CaptureFileInfo:
    path: str
    tube_type: str
    camera_side: str
    observed_at: str


@dataclass
class CandidateFile:
    path: Path
    first_seen_monotonic: float
    last_size: int = -1
    last_size_change_monotonic: float = 0.0


# ============================================================
# SQLITE DATABASE
# ============================================================

class CameraAgentDatabase:
    def __init__(self, database_path: str, agent_id: str):
        self.database_path = database_path
        self.agent_id = agent_id
        self.lock = threading.RLock()

        Path(database_path).parent.mkdir(
            parents=True,
            exist_ok=True
        )

        self.initialize()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.database_path,
            timeout=30,
            check_same_thread=False
        )

        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        return conn

    def initialize(self) -> None:
        with self.lock:
            conn = self.connect()

            try:
                # Migration: schema streams cu dung PK camera_side duy
                # nhat (mot row / side, co cot active_no) - khong cho
                # phep nhieu No chay song song. Schema cu khong the anh
                # xa sang (camera_side, tube_type) nen tao lai tu dau.
                existing_columns = {
                    row["name"]
                    for row in conn.execute(
                        "PRAGMA table_info(streams)"
                    ).fetchall()
                }

                if existing_columns and "tube_type" not in existing_columns:
                    conn.execute("DROP TABLE streams")

                    LOGGER.warning(
                        "Migrated streams table: old side-key schema "
                        "dropped, streams will be recreated per "
                        "(camera_side, tube_type) lazily"
                    )

                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS meta (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS streams (
                        camera_side TEXT NOT NULL,
                        tube_type TEXT NOT NULL,

                        next_step INTEGER NOT NULL DEFAULT 1,

                        cycle_number INTEGER NOT NULL DEFAULT 0,
                        current_cycle_id TEXT,

                        state TEXT NOT NULL DEFAULT 'OUT_OF_SYNC',

                        -- Anh dang cho ket qua AI cua stream nay. Chi mot
                        -- event duoc phep "active" tai mot thoi diem.
                        active_event_id TEXT,
                        active_step INTEGER,
                        result_wait_since REAL,

                        last_event_unix REAL,
                        last_file_path TEXT,
                        last_capture_timestamp TEXT,

                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

                        PRIMARY KEY (camera_side, tube_type)
                    );

                    CREATE TABLE IF NOT EXISTS events (
                        event_id TEXT PRIMARY KEY,
                        cycle_id TEXT NOT NULL,

                        file_path TEXT UNIQUE NOT NULL,
                        file_name TEXT NOT NULL,

                        camera_side TEXT NOT NULL,
                        tube_type TEXT NOT NULL,

                        step INTEGER NOT NULL,
                        capture_timestamp TEXT NOT NULL,

                        upload_status TEXT NOT NULL,
                        upload_attempts INTEGER NOT NULL DEFAULT 0,
                        last_upload_error TEXT,

                        -- Ket qua cuoi cung tra ve tu AI Server. NULL =
                        -- chua co ket qua (stream phai tiep tuc cho).
                        result_status TEXT,
                        result_error_code TEXT,
                        processing_error INTEGER NOT NULL DEFAULT 0,
                        result_received_at TEXT,

                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        uploaded_at TEXT
                    );

                    -- Hang doi anh ben vung: anh duoc nhan ngay khi file
                    -- on dinh, nhung CHUA duoc gan step cho den khi stream
                    -- cua no ranh (khong con anh nao cho ket qua AI).
                    CREATE TABLE IF NOT EXISTS pending_images (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,

                        file_path TEXT UNIQUE NOT NULL,
                        file_name TEXT NOT NULL,

                        camera_side TEXT NOT NULL,
                        tube_type TEXT NOT NULL,

                        observed_at TEXT NOT NULL,

                        status TEXT NOT NULL DEFAULT 'QUEUED',
                        event_id TEXT,
                        assigned_at TEXT,

                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS seen_files (
                        file_path TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS abort_requests (
                        cycle_id TEXT PRIMARY KEY,
                        camera_side TEXT NOT NULL,
                        tube_type TEXT NOT NULL,
                        error_code TEXT NOT NULL,

                        status TEXT NOT NULL DEFAULT 'PENDING',
                        attempts INTEGER NOT NULL DEFAULT 0,
                        last_error TEXT,

                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        sent_at TEXT
                    );

                    CREATE INDEX IF NOT EXISTS idx_events_upload
                    ON events(upload_status);

                    CREATE INDEX IF NOT EXISTS idx_events_cycle
                    ON events(cycle_id);

                    CREATE INDEX IF NOT EXISTS idx_pending_stream
                    ON pending_images(
                        camera_side, tube_type, status, id
                    );

                    CREATE INDEX IF NOT EXISTS idx_aborts_status
                    ON abort_requests(status);
                    """
                )

                # Migration nhe cho DB cu: bo sung cot moi cho state
                # machine "nhan anh -> cho ket qua -> moi gan step".
                self._migrate_columns(
                    conn,
                    "streams",
                    {
                        "active_event_id": "TEXT",
                        "active_step": "INTEGER",
                        "result_wait_since": "REAL",
                    },
                )
                self._migrate_columns(
                    conn,
                    "events",
                    {
                        "result_status": "TEXT",
                        "result_error_code": "TEXT",
                        "processing_error": (
                            "INTEGER NOT NULL DEFAULT 0"
                        ),
                        "result_received_at": "TEXT",
                    },
                )

                # Trang thai cu RUNNING duoc chuyen sang state machine moi.
                # Stream dang cho Step 3 (next_step=3) co the khoi phuc
                # active_event_id tu event Step 1 gan nhat cua cycle.
                old_running = conn.execute(
                    """
                    SELECT *
                    FROM streams
                    WHERE state = 'RUNNING'
                    """
                ).fetchall()

                for row in old_running:
                    next_step = int(row["next_step"] or 1)

                    if next_step == 1:
                        conn.execute(
                            """
                            UPDATE streams
                            SET
                                state = 'READY_STEP1',
                                next_step = 1,
                                active_event_id = NULL,
                                active_step = NULL,
                                result_wait_since = NULL,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE camera_side = ?
                            AND tube_type = ?
                            """,
                            (row["camera_side"], row["tube_type"]),
                        )
                        continue

                    event = conn.execute(
                        """
                        SELECT event_id, step
                        FROM events
                        WHERE cycle_id = ?
                        AND camera_side = ?
                        AND tube_type = ?
                        AND step = 1
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (
                            row["current_cycle_id"],
                            row["camera_side"],
                            row["tube_type"],
                        ),
                    ).fetchone()

                    if event is not None:
                        conn.execute(
                            """
                            UPDATE streams
                            SET
                                state = 'WAIT_STEP1_RESULT',
                                next_step = 3,
                                active_event_id = ?,
                                active_step = 1,
                                result_wait_since = ?,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE camera_side = ?
                            AND tube_type = ?
                            """,
                            (
                                event["event_id"],
                                time.time(),
                                row["camera_side"],
                                row["tube_type"],
                            ),
                        )
                    else:
                        conn.execute(
                            """
                            UPDATE streams
                            SET
                                state = 'READY_STEP3',
                                next_step = 3,
                                active_event_id = NULL,
                                active_step = NULL,
                                result_wait_since = NULL,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE camera_side = ?
                            AND tube_type = ?
                            """,
                            (row["camera_side"], row["tube_type"]),
                        )

                conn.commit()

            finally:
                conn.close()

    @staticmethod
    def _migrate_columns(
        conn: sqlite3.Connection,
        table: str,
        columns: dict[str, str],
    ) -> None:
        """Them cot moi cho DB da ton tai (SQLite ADD COLUMN)."""
        existing = {
            row["name"]
            for row in conn.execute(
                f"PRAGMA table_info({table})"
            ).fetchall()
        }

        for name, ddl in columns.items():
            if name not in existing:
                LOGGER.warning(
                    "Migrating table %s: add column %s",
                    table,
                    name,
                )
                conn.execute(
                    f"ALTER TABLE {table} ADD COLUMN {name} {ddl}"
                )

    # --------------------------------------------------------
    # META
    # --------------------------------------------------------

    def get_meta(self, key: str) -> Optional[str]:
        with self.lock:
            conn = self.connect()

            try:
                row = conn.execute(
                    """
                    SELECT value
                    FROM meta
                    WHERE key = ?
                    """,
                    (key,),
                ).fetchone()

                return row["value"] if row else None

            finally:
                conn.close()

    def set_meta(self, key: str, value: str) -> None:
        with self.lock:
            conn = self.connect()

            try:
                conn.execute(
                    """
                    INSERT INTO meta (key, value)
                    VALUES (?, ?)
                    ON CONFLICT(key)
                    DO UPDATE SET value = excluded.value
                    """,
                    (key, value),
                )
                conn.commit()

            finally:
                conn.close()

    # --------------------------------------------------------
    # FILE STATE
    # --------------------------------------------------------

    def is_file_seen(self, file_path: str) -> bool:
        with self.lock:
            conn = self.connect()

            try:
                row = conn.execute(
                    """
                    SELECT 1
                    FROM events
                    WHERE file_path = ?
                    """,
                    (file_path,),
                ).fetchone()

                if row:
                    return True

                row = conn.execute(
                    """
                    SELECT 1
                    FROM seen_files
                    WHERE file_path = ?
                    """,
                    (file_path,),
                ).fetchone()

                if row:
                    return True

                row = conn.execute(
                    """
                    SELECT 1
                    FROM pending_images
                    WHERE file_path = ?
                    """,
                    (file_path,),
                ).fetchone()

                return row is not None

            finally:
                conn.close()

    def mark_file_seen(
        self,
        file_path: str,
        status: str,
    ) -> None:
        with self.lock:
            conn = self.connect()

            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO seen_files (
                        file_path,
                        status
                    )
                    VALUES (?, ?)
                    """,
                    (file_path, status),
                )
                conn.commit()

            finally:
                conn.close()

    def bootstrap_files(self, file_paths: list[str]) -> None:
        if not file_paths:
            return

        with self.lock:
            conn = self.connect()

            try:
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO seen_files (
                        file_path,
                        status
                    )
                    VALUES (?, 'BOOTSTRAP_IGNORED')
                    """,
                    [(path,) for path in file_paths],
                )

                conn.commit()

            finally:
                conn.close()

    # --------------------------------------------------------
    # STREAM / SEQUENCE
    # --------------------------------------------------------

    def get_streams(self) -> list[dict]:
        with self.lock:
            conn = self.connect()

            try:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM streams
                    ORDER BY camera_side, tube_type
                    """
                ).fetchall()

                return [dict(row) for row in rows]

            finally:
                conn.close()

    def get_event(self, event_id: str) -> Optional[dict]:
        with self.lock:
            conn = self.connect()

            try:
                row = conn.execute(
                    """
                    SELECT *
                    FROM events
                    WHERE event_id = ?
                    """,
                    (event_id,),
                ).fetchone()

                return dict(row) if row else None

            finally:
                conn.close()

    def force_stream_ready(
        self,
        camera_side: str,
        tube_type: str,
        step: int = 1,
    ) -> None:
        """Dua mot stream ve trang thai ranh, khong dung toi hang doi."""
        step = 3 if int(step) == 3 else 1

        with self.lock:
            conn = self.connect()

            try:
                conn.execute(
                    """
                    UPDATE streams
                    SET
                        next_step = ?,
                        state = ?,
                        current_cycle_id = CASE
                            WHEN ? = 1 THEN NULL
                            ELSE current_cycle_id
                        END,
                        active_event_id = NULL,
                        active_step = NULL,
                        result_wait_since = NULL,
                        last_event_unix = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE camera_side = ?
                    AND tube_type = ?
                    """,
                    (
                        step,
                        (
                            "READY_STEP3"
                            if step == 3
                            else "READY_STEP1"
                        ),
                        step,
                        camera_side,
                        tube_type,
                    ),
                )
                conn.commit()

            finally:
                conn.close()

    def queue_abort(
        self,
        conn: sqlite3.Connection,
        cycle_id: str,
        camera_side: str,
        tube_type: str,
        error_code: str,
    ) -> None:
        conn.execute(
            """
            INSERT OR IGNORE INTO abort_requests (
                cycle_id,
                camera_side,
                tube_type,
                error_code,
                status
            )
            VALUES (?, ?, ?, ?, 'PENDING')
            """,
            (
                cycle_id,
                camera_side,
                tube_type,
                error_code,
            ),
        )

    # --------------------------------------------------------
    # STATE MACHINE: nhan anh -> hang doi -> gan step
    # --------------------------------------------------------
    #
    # Moi stream (camera_side, tube_type) co 4 trang thai:
    #   READY_STEP1        : ranh, anh ke tiep se la Step 1 cycle moi
    #   WAIT_STEP1_RESULT  : da upload Step 1, dang cho ket qua AI
    #   READY_STEP3        : Step 1 PASS, anh ke tiep se la Step 3
    #   WAIT_STEP3_RESULT  : da upload Step 3, dang cho ket qua AI
    #
    # Anh den bat ky luc nao deu duoc luu vao pending_images truoc.
    # Chi khi stream o READY_* moi lay anh dau tien (FIFO) ra gan step.
    # Timeout/HTTP 202 KHONG duoc dung de mo khoa stream: chi ket qua AI
    # da luu DB (callback hoac truy van lai) moi doi duoc trang thai.

    STEP1 = 1
    STEP3 = 3

    STATE_READY_STEP1 = "READY_STEP1"
    STATE_WAIT_STEP1_RESULT = "WAIT_STEP1_RESULT"
    STATE_READY_STEP3 = "READY_STEP3"
    STATE_WAIT_STEP3_RESULT = "WAIT_STEP3_RESULT"
    STATE_OUT_OF_SYNC = "OUT_OF_SYNC"

    WAIT_STATES = (STATE_WAIT_STEP1_RESULT, STATE_WAIT_STEP3_RESULT)

    def _ensure_stream_locked(
        self,
        conn: sqlite3.Connection,
        camera_side: str,
        tube_type: str,
    ) -> sqlite3.Row:
        stream = conn.execute(
            """
            SELECT *
            FROM streams
            WHERE camera_side = ?
            AND tube_type = ?
            """,
            (camera_side, tube_type),
        ).fetchone()

        if stream is not None:
            return stream

        conn.execute(
            """
            INSERT INTO streams (
                camera_side,
                tube_type,
                next_step,
                cycle_number,
                current_cycle_id,
                state,
                updated_at
            )
            VALUES (?, ?, 1, 0, NULL, 'READY_STEP1', CURRENT_TIMESTAMP)
            """,
            (camera_side, tube_type),
        )

        return conn.execute(
            """
            SELECT *
            FROM streams
            WHERE camera_side = ?
            AND tube_type = ?
            """,
            (camera_side, tube_type),
        ).fetchone()

    def _make_cycle_id(
        self,
        camera_side: str,
        tube_type: str,
        cycle_number: int,
    ) -> str:
        return (
            f"{self.agent_id}-"
            f"{camera_side}-"
            f"{tube_type}-"
            f"{cycle_number:09d}"
        )

    def enqueue_file(self, file_info: CaptureFileInfo) -> dict:
        """
        Luu anh vao hang doi ben vung, CHUA gan step.

        Tra ve:
        {
            "status": "QUEUED" | "ALREADY_SEEN",
            "pending_id": int | None,
            "file_info": CaptureFileInfo,
        }
        """
        with self.lock:
            conn = self.connect()

            try:
                conn.execute("BEGIN IMMEDIATE")

                seen = conn.execute(
                    """
                    SELECT 1
                    FROM events
                    WHERE file_path = ?
                    """,
                    (file_info.path,),
                ).fetchone()

                if not seen:
                    seen = conn.execute(
                        """
                        SELECT 1
                        FROM pending_images
                        WHERE file_path = ?
                        """,
                        (file_info.path,),
                    ).fetchone()

                if not seen:
                    seen = conn.execute(
                        """
                        SELECT 1
                        FROM seen_files
                        WHERE file_path = ?
                        """,
                        (file_info.path,),
                    ).fetchone()

                if seen:
                    conn.rollback()
                    return {
                        "status": "ALREADY_SEEN",
                        "pending_id": None,
                        "file_info": file_info,
                    }

                cursor = conn.execute(
                    """
                    INSERT INTO pending_images (
                        file_path,
                        file_name,
                        camera_side,
                        tube_type,
                        observed_at,
                        status
                    )
                    VALUES (?, ?, ?, ?, ?, 'QUEUED')
                    """,
                    (
                        file_info.path,
                        Path(file_info.path).name,
                        file_info.camera_side,
                        file_info.tube_type,
                        file_info.observed_at,
                    ),
                )

                pending_id = cursor.lastrowid
                conn.commit()

                return {
                    "status": "QUEUED",
                    "pending_id": pending_id,
                    "file_info": file_info,
                }

            except Exception:
                conn.rollback()
                raise

            finally:
                conn.close()

    def _dispatch_next_locked(
        self,
        conn: sqlite3.Connection,
        camera_side: str,
        tube_type: str,
        auto_recover_out_of_sync: bool = True,
    ) -> Optional[dict]:
        """
        Lay anh dau tien trong hang doi cua stream va gan step, neu
        stream dang ranh. Ham nay PHAI duoc goi trong transaction cua
        caller (conn da BEGIN IMMEDIATE).
        """
        stream = self._ensure_stream_locked(
            conn, camera_side, tube_type
        )

        state = str(stream["state"] or self.STATE_OUT_OF_SYNC)

        if state in self.WAIT_STATES:
            # Dang cho ket qua AI cua anh truoc -> giu nguyen hang doi.
            return None

        if state == self.STATE_OUT_OF_SYNC:
            if not auto_recover_out_of_sync:
                return None

            LOGGER.warning(
                "Auto-recover OUT_OF_SYNC side=%s no=%s -> READY_STEP1",
                camera_side,
                tube_type,
            )

            conn.execute(
                """
                UPDATE streams
                SET
                    state = 'READY_STEP1',
                    next_step = 1,
                    current_cycle_id = NULL,
                    active_event_id = NULL,
                    active_step = NULL,
                    result_wait_since = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE camera_side = ?
                AND tube_type = ?
                """,
                (camera_side, tube_type),
            )
            state = self.STATE_READY_STEP1

        # Tuong thich DB cu (state RUNNING).
        if state == "RUNNING":
            state = (
                self.STATE_READY_STEP3
                if int(stream["next_step"] or 1) == 3
                and stream["current_cycle_id"]
                else self.STATE_READY_STEP1
            )
            conn.execute(
                """
                UPDATE streams
                SET state = ?, updated_at = CURRENT_TIMESTAMP
                WHERE camera_side = ?
                AND tube_type = ?
                """,
                (state, camera_side, tube_type),
            )

        if state not in (
            self.STATE_READY_STEP1,
            self.STATE_READY_STEP3,
        ):
            LOGGER.error(
                "Unknown stream state side=%s no=%s state=%s -> "
                "reset READY_STEP1",
                camera_side,
                tube_type,
                state,
            )
            conn.execute(
                """
                UPDATE streams
                SET
                    state = 'READY_STEP1',
                    next_step = 1,
                    current_cycle_id = NULL,
                    active_event_id = NULL,
                    active_step = NULL,
                    result_wait_since = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE camera_side = ?
                AND tube_type = ?
                """,
                (camera_side, tube_type),
            )
            state = self.STATE_READY_STEP1

        pending = conn.execute(
            """
            SELECT *
            FROM pending_images
            WHERE camera_side = ?
            AND tube_type = ?
            AND status = 'QUEUED'
            ORDER BY id ASC
            LIMIT 1
            """,
            (camera_side, tube_type),
        ).fetchone()

        if pending is None:
            return None

        cycle_number = int(stream["cycle_number"] or 0)

        if state == self.STATE_READY_STEP1:
            assigned_step = self.STEP1
            cycle_number += 1
            cycle_id = self._make_cycle_id(
                camera_side, tube_type, cycle_number
            )
            new_state = self.STATE_WAIT_STEP1_RESULT
            new_next_step = 3

        else:
            assigned_step = self.STEP3
            cycle_id = stream["current_cycle_id"]

            if not cycle_id:
                LOGGER.error(
                    "READY_STEP3 without current_cycle_id side=%s "
                    "no=%s -> reset READY_STEP1",
                    camera_side,
                    tube_type,
                )
                conn.execute(
                    """
                    UPDATE streams
                    SET
                        state = 'READY_STEP1',
                        next_step = 1,
                        current_cycle_id = NULL,
                        active_event_id = NULL,
                        active_step = NULL,
                        result_wait_since = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE camera_side = ?
                    AND tube_type = ?
                    """,
                    (camera_side, tube_type),
                )
                return None

            new_state = self.STATE_WAIT_STEP3_RESULT
            new_next_step = 1

        event_id = f"{cycle_id}-S{assigned_step}"

        conn.execute(
            """
            INSERT INTO events (
                event_id,
                cycle_id,
                file_path,
                file_name,
                camera_side,
                tube_type,
                step,
                capture_timestamp,
                upload_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING_UPLOAD')
            """,
            (
                event_id,
                cycle_id,
                pending["file_path"],
                pending["file_name"],
                camera_side,
                tube_type,
                assigned_step,
                pending["observed_at"],
            ),
        )

        conn.execute(
            """
            UPDATE pending_images
            SET
                status = 'ASSIGNED',
                event_id = ?,
                assigned_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (event_id, pending["id"]),
        )

        conn.execute(
            """
            UPDATE streams
            SET
                next_step = ?,
                cycle_number = ?,
                current_cycle_id = ?,
                state = ?,
                active_event_id = ?,
                active_step = ?,
                result_wait_since = ?,
                last_event_unix = ?,
                last_file_path = ?,
                last_capture_timestamp = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE camera_side = ?
            AND tube_type = ?
            """,
            (
                new_next_step,
                cycle_number,
                cycle_id,
                new_state,
                event_id,
                assigned_step,
                time.time(),
                time.time(),
                pending["file_path"],
                pending["observed_at"],
                camera_side,
                tube_type,
            ),
        )

        return {
            "event_id": event_id,
            "cycle_id": cycle_id,
            "file_path": pending["file_path"],
            "file_name": pending["file_name"],
            "camera_side": camera_side,
            "tube_type": tube_type,
            "step": assigned_step,
            "capture_timestamp": pending["observed_at"],
            "upload_status": "PENDING_UPLOAD",
        }

    def dispatch_pending(
        self,
        camera_side: str,
        tube_type: str,
        auto_recover_out_of_sync: bool = True,
    ) -> Optional[dict]:
        """Gan anh ke tiep cua mot stream neu stream dang ranh."""
        with self.lock:
            conn = self.connect()

            try:
                conn.execute("BEGIN IMMEDIATE")

                event = self._dispatch_next_locked(
                    conn,
                    camera_side,
                    tube_type,
                    auto_recover_out_of_sync=auto_recover_out_of_sync,
                )

                conn.commit()
                return event

            except Exception:
                conn.rollback()
                raise

            finally:
                conn.close()

    def dispatch_all_pending(
        self,
        auto_recover_out_of_sync: bool = True,
    ) -> list[dict]:
        """Xu ly hang doi cua tat ca stream (dung khi startup/reset)."""
        assigned = []

        with self.lock:
            conn = self.connect()

            try:
                conn.execute("BEGIN IMMEDIATE")

                rows = conn.execute(
                    """
                    SELECT DISTINCT camera_side, tube_type
                    FROM pending_images
                    WHERE status = 'QUEUED'
                    ORDER BY id ASC
                    """
                ).fetchall()

                for row in rows:
                    event = self._dispatch_next_locked(
                        conn,
                        row["camera_side"],
                        row["tube_type"],
                        auto_recover_out_of_sync=(
                            auto_recover_out_of_sync
                        ),
                    )

                    if event is not None:
                        assigned.append(event)

                conn.commit()
                return assigned

            except Exception:
                conn.rollback()
                raise

            finally:
                conn.close()

    def assign_file_to_sequence(
        self,
        file_info: CaptureFileInfo,
        auto_recover_out_of_sync: bool = True,
    ) -> dict:
        """
        API tuong thich: nhan anh (enqueue) + thu gan step ngay neu
        stream dang ranh.

        Khac ban cu: neu stream dang cho ket qua AI, anh chi nam trong
        hang doi va tra ve status="QUEUED" (KHONG bi gan Step 3 som).
        """
        enqueue = self.enqueue_file(file_info)

        if enqueue["status"] == "ALREADY_SEEN":
            return {
                "status": "ALREADY_SEEN",
                "event": None,
                "abort": None,
                "message": "File already handled",
            }

        event = self.dispatch_pending(
            file_info.camera_side,
            file_info.tube_type,
            auto_recover_out_of_sync=auto_recover_out_of_sync,
        )

        if event is None:
            return {
                "status": "QUEUED",
                "event": None,
                "abort": None,
                "message": (
                    "File queued: stream is waiting for AI result "
                    "or is OUT_OF_SYNC"
                ),
            }

        return {
            "status": "ASSIGNED",
            "event": event,
            "abort": None,
            "message": "File assigned to sequence",
        }

    def complete_active_event(
        self,
        event_id: str,
        result_status: str,
        error_code: str = "",
        processing_error: bool = False,
    ) -> dict:
        """
        Ap dung ket qua AI cho dung event dang active cua stream, roi
        gan anh ke tiep trong hang doi (neu co) trong CUNG transaction.

        Idempotent: callback trung hoac den muon khong doi state lan nua.
        """
        result_status = str(result_status or "").upper()

        with self.lock:
            conn = self.connect()

            try:
                conn.execute("BEGIN IMMEDIATE")

                event = conn.execute(
                    """
                    SELECT *
                    FROM events
                    WHERE event_id = ?
                    """,
                    (event_id,),
                ).fetchone()

                if event is None:
                    conn.rollback()
                    return {
                        "action": "NOT_FOUND",
                        "event_id": event_id,
                        "next_event": None,
                    }

                step = int(event["step"])
                allowed = ("PASS", "FAIL") if step == 1 else ("OK", "NG")

                if result_status not in allowed:
                    conn.rollback()
                    return {
                        "action": "INVALID_STATUS",
                        "event_id": event_id,
                        "step": step,
                        "result_status": result_status,
                        "next_event": None,
                    }

                if event["result_status"] is not None:
                    conn.rollback()
                    return {
                        "action": "DUPLICATE",
                        "event_id": event_id,
                        "result_status": event["result_status"],
                        "next_event": None,
                    }

                # Luu ket qua vao event trong moi truong hop (ke ca
                # callback den muon) de giu lich su day du.
                conn.execute(
                    """
                    UPDATE events
                    SET
                        result_status = ?,
                        result_error_code = ?,
                        processing_error = ?,
                        result_received_at = CURRENT_TIMESTAMP
                    WHERE event_id = ?
                    AND result_status IS NULL
                    """,
                    (
                        result_status,
                        error_code,
                        1 if processing_error else 0,
                        event_id,
                    ),
                )

                stream = conn.execute(
                    """
                    SELECT *
                    FROM streams
                    WHERE camera_side = ?
                    AND tube_type = ?
                    """,
                    (event["camera_side"], event["tube_type"]),
                ).fetchone()

                if (
                    stream is None
                    or stream["active_event_id"] != event_id
                ):
                    conn.commit()
                    return {
                        "action": "IGNORED_STALE",
                        "event_id": event_id,
                        "result_status": result_status,
                        "next_event": None,
                    }

                if step == self.STEP1 and result_status == "PASS":
                    new_state = self.STATE_READY_STEP3
                    new_next_step = 3
                    keep_cycle_id = event["cycle_id"]
                else:
                    # Step 1 FAIL hoac Step 3 OK/NG: dong cycle.
                    new_state = self.STATE_READY_STEP1
                    new_next_step = 1
                    keep_cycle_id = None

                conn.execute(
                    """
                    UPDATE streams
                    SET
                        state = ?,
                        next_step = ?,
                        current_cycle_id = ?,
                        active_event_id = NULL,
                        active_step = NULL,
                        result_wait_since = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE camera_side = ?
                    AND tube_type = ?
                    """,
                    (
                        new_state,
                        new_next_step,
                        keep_cycle_id,
                        event["camera_side"],
                        event["tube_type"],
                    ),
                )

                next_event = self._dispatch_next_locked(
                    conn,
                    event["camera_side"],
                    event["tube_type"],
                )

                conn.commit()

                return {
                    "action": "APPLIED",
                    "event_id": event_id,
                    "step": step,
                    "result_status": result_status,
                    "stream_state": new_state,
                    "next_event": next_event,
                }

            except Exception:
                conn.rollback()
                raise

            finally:
                conn.close()

    def get_active_events_awaiting_result(self) -> list[dict]:
        """Cac event dang active nhung chua co ket qua AI (de poll lai)."""
        with self.lock:
            conn = self.connect()

            try:
                rows = conn.execute(
                    """
                    SELECT
                        e.*,
                        s.state AS stream_state,
                        s.result_wait_since AS result_wait_since
                    FROM streams s
                    JOIN events e ON e.event_id = s.active_event_id
                    WHERE s.state IN (
                        'WAIT_STEP1_RESULT',
                        'WAIT_STEP3_RESULT'
                    )
                    AND e.result_status IS NULL
                    ORDER BY s.result_wait_since ASC
                    """
                ).fetchall()

                return [dict(row) for row in rows]

            finally:
                conn.close()

    def warn_incomplete_streams(
        self,
        warn_sec: float,
    ) -> list[dict]:
        """
        Chỉ cảnh báo (không thay đổi trạng thái) các stream đang chờ
        ảnh/kết quả quá thời gian cho phép.

        Lưu ý: WAIT_* chỉ cảnh báo, KHÔNG bị coi là STEP 1 FAIL. Ảnh kế
        tiếp vẫn nằm trong hàng đợi và được retry kết quả.
        """
        warning_streams = []
        deadline = time.time() - warn_sec

        with self.lock:
            conn = self.connect()

            try:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM streams
                    WHERE state IN (
                        'READY_STEP3',
                        'WAIT_STEP1_RESULT',
                        'WAIT_STEP3_RESULT'
                    )
                    AND COALESCE(
                        result_wait_since,
                        last_event_unix
                    ) IS NOT NULL
                    AND COALESCE(
                        result_wait_since,
                        last_event_unix
                    ) < ?
                    """,
                    (deadline,),
                ).fetchall()

                for row in rows:
                    warning_streams.append(dict(row))

                return warning_streams

            except Exception:
                raise

            finally:
                conn.close()

    def timeout_incomplete_streams(
        self,
        timeout_sec: float,
    ) -> list[dict]:
        """
        Nếu đã Step 1 PASS nhưng quá lâu không có ảnh Step 3:
        - Cycle cũ bị abort E204.
        - Stream chuyển sang OUT_OF_SYNC (kỹ thuật viên reset thủ công).

        WAIT_STEP1_RESULT / WAIT_STEP3_RESULT KHÔNG bị timeout ở đây:
        mất mạng/không nhận được callback không được hiểu là STEP 1 FAIL.
        Hệ thống tiếp tục retry và cảnh báo cho đến khi có kết quả.
        """
        aborts = []
        deadline = time.time() - timeout_sec

        with self.lock:
            conn = self.connect()

            try:
                conn.execute("BEGIN IMMEDIATE")

                rows = conn.execute(
                    """
                    SELECT *
                    FROM streams
                    WHERE (
                        state = 'READY_STEP3'
                        OR (
                            state = 'RUNNING'
                            AND next_step = 3
                        )
                    )
                    AND COALESCE(
                        result_wait_since,
                        last_event_unix
                    ) IS NOT NULL
                    AND COALESCE(
                        result_wait_since,
                        last_event_unix
                    ) < ?
                    """,
                    (deadline,),
                ).fetchall()

                for row in rows:
                    cycle_id = row["current_cycle_id"]
                    side = row["camera_side"]
                    tube_type = row["tube_type"]

                    if cycle_id:
                        error_code = "E204_SEQUENCE_TIMEOUT"

                        self.queue_abort(
                            conn=conn,
                            cycle_id=cycle_id,
                            camera_side=side,
                            tube_type=tube_type,
                            error_code=error_code,
                        )

                        aborts.append(
                            {
                                "cycle_id": cycle_id,
                                "camera_side": side,
                                "tube_type": tube_type,
                                "error_code": error_code,
                            }
                        )

                    conn.execute(
                        """
                        UPDATE streams
                        SET
                            next_step = 1,
                            current_cycle_id = NULL,
                            state = 'OUT_OF_SYNC',
                            active_event_id = NULL,
                            active_step = NULL,
                            result_wait_since = NULL,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE camera_side = ?
                        AND tube_type = ?
                        """,
                        (side, tube_type),
                    )

                conn.commit()
                return aborts

            except Exception:
                conn.rollback()
                raise

            finally:
                conn.close()

    def reset_stream(self, camera_side: str) -> list[dict]:
        """
        Reset thủ công do kỹ thuật viên thực hiện: reset TẤT CẢ stream
        của side này (mọi No). Ảnh tiếp theo sau reset sẽ được xem là
        Step 1. Cycle nào đang dở của side bị queue abort E601.
        """
        if camera_side not in ("L", "R"):
            raise ValueError("camera_side must be L or R")

        with self.lock:
            conn = self.connect()

            try:
                conn.execute("BEGIN IMMEDIATE")

                streams = conn.execute(
                    """
                    SELECT *
                    FROM streams
                    WHERE camera_side = ?
                    """,
                    (camera_side,),
                ).fetchall()

                aborts = []

                for stream in streams:
                    if (
                        stream["current_cycle_id"]
                        and stream["state"] != "READY_STEP1"
                    ):
                        error_code = "E601_MANUAL_SEQUENCE_RESET"

                        self.queue_abort(
                            conn=conn,
                            cycle_id=stream["current_cycle_id"],
                            camera_side=camera_side,
                            tube_type=stream["tube_type"],
                            error_code=error_code,
                        )

                        aborts.append(
                            {
                                "cycle_id": stream["current_cycle_id"],
                                "camera_side": camera_side,
                                "tube_type": stream["tube_type"],
                                "error_code": error_code,
                            }
                        )

                conn.execute(
                    """
                    UPDATE streams
                    SET
                        next_step = 1,
                        current_cycle_id = NULL,
                        state = 'READY_STEP1',
                        active_event_id = NULL,
                        active_step = NULL,
                        result_wait_since = NULL,
                        last_event_unix = NULL,
                        last_file_path = NULL,
                        last_capture_timestamp = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE camera_side = ?
                    """,
                    (camera_side,),
                )

                conn.commit()
                return aborts

            except Exception:
                conn.rollback()
                raise

            finally:
                conn.close()

    def reset_stream_if_cycle(
        self,
        camera_side: str,
        expected_cycle_id: str,
    ) -> Optional[dict]:
        """
        Reset stream only if the current active cycle matches expected_cycle_id.

        Returns:
            The previous stream row dictionary if a reset was performed,
            otherwise None (no-op when the cycle no longer matches).
        """
        if camera_side not in ("L", "R"):
            raise ValueError("camera_side must be L or R")

        with self.lock:
            conn = self.connect()

            try:
                conn.execute("BEGIN IMMEDIATE")

                stream = conn.execute(
                    """
                    SELECT *
                    FROM streams
                    WHERE camera_side = ?
                    AND current_cycle_id = ?
                    """,
                    (camera_side, expected_cycle_id),
                ).fetchone()

                if stream is None:
                    conn.rollback()
                    return None

                conn.execute(
                    """
                    UPDATE streams
                    SET
                        next_step = 1,
                        current_cycle_id = NULL,
                        state = 'READY_STEP1',
                        active_event_id = NULL,
                        active_step = NULL,
                        result_wait_since = NULL,
                        last_event_unix = NULL,
                        last_file_path = NULL,
                        last_capture_timestamp = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE camera_side = ?
                    AND tube_type = ?
                    """,
                    (camera_side, stream["tube_type"]),
                )

                conn.commit()
                return dict(stream)

            except Exception:
                conn.rollback()
                raise

            finally:
                conn.close()

    # --------------------------------------------------------
    # UPLOAD QUEUE
    # --------------------------------------------------------

    def get_pending_uploads(self) -> list[dict]:
        with self.lock:
            conn = self.connect()

            try:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM events
                    WHERE upload_status IN (
                        'PENDING_UPLOAD',
                        'RETRY'
                    )
                    ORDER BY created_at ASC
                    """
                ).fetchall()

                return [dict(row) for row in rows]

            finally:
                conn.close()

    def mark_upload_result(
        self,
        event_id: str,
        success: bool,
        error_message: Optional[str] = None,
    ) -> None:
        with self.lock:
            conn = self.connect()

            try:
                if success:
                    status = "ACK"
                else:
                    status = "RETRY"

                conn.execute(
                    """
                    UPDATE events
                    SET
                        upload_status = ?,
                        upload_attempts = upload_attempts + 1,
                        last_upload_error = ?,
                        uploaded_at = CASE
                            WHEN ? = 1 THEN CURRENT_TIMESTAMP
                            ELSE uploaded_at
                        END
                    WHERE event_id = ?
                    """,
                    (
                        status,
                        error_message,
                        1 if success else 0,
                        event_id,
                    ),
                )

                conn.commit()

            finally:
                conn.close()

    # --------------------------------------------------------
    # ABORT QUEUE
    # --------------------------------------------------------

    def get_pending_aborts(self) -> list[dict]:
        with self.lock:
            conn = self.connect()

            try:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM abort_requests
                    WHERE status IN ('PENDING', 'RETRY')
                    ORDER BY created_at ASC
                    """
                ).fetchall()

                return [dict(row) for row in rows]

            finally:
                conn.close()

    def mark_abort_result(
        self,
        cycle_id: str,
        success: bool,
        error_message: Optional[str] = None,
    ) -> None:
        with self.lock:
            conn = self.connect()

            try:
                status = "ACK" if success else "RETRY"

                conn.execute(
                    """
                    UPDATE abort_requests
                    SET
                        status = ?,
                        attempts = attempts + 1,
                        last_error = ?,
                        sent_at = CASE
                            WHEN ? = 1 THEN CURRENT_TIMESTAMP
                            ELSE sent_at
                        END
                    WHERE cycle_id = ?
                    """,
                    (
                        status,
                        error_message,
                        1 if success else 0,
                        cycle_id,
                    ),
                )

                conn.commit()

            finally:
                conn.close()

    def get_status_summary(self) -> dict:
        with self.lock:
            conn = self.connect()

            try:
                upload_count = conn.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM events
                    WHERE upload_status IN (
                        'PENDING_UPLOAD',
                        'RETRY'
                    )
                    """
                ).fetchone()["count"]

                abort_count = conn.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM abort_requests
                    WHERE status IN ('PENDING', 'RETRY')
                    """
                ).fetchone()["count"]

                queued_count = conn.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM pending_images
                    WHERE status = 'QUEUED'
                    """
                ).fetchone()["count"]

                waiting_count = conn.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM streams
                    WHERE state IN (
                        'WAIT_STEP1_RESULT',
                        'WAIT_STEP3_RESULT'
                    )
                    """
                ).fetchone()["count"]

                return {
                    "pending_uploads": upload_count,
                    "pending_aborts": abort_count,
                    "queued_images": queued_count,
                    "streams_waiting_result": waiting_count,
                }

            finally:
                conn.close()


# ============================================================
# FILE PARSER
# ============================================================

def parse_capture_file(
    root_path: Path,
    file_path: Path,
) -> CaptureFileInfo:
    relative = file_path.relative_to(root_path)

    if len(relative.parts) < 3:
        raise ValueError(
            f"Unexpected path structure: {relative}"
        )

    camera_side = relative.parts[-2]
    tube_type = relative.parts[-3]

    if camera_side not in ("L", "R"):
        raise ValueError(
            f"Camera side must be L/R, got {camera_side}"
        )

    safe_component(tube_type, "tube_type")

    image_format = detect_image_format(file_path)

    if image_format is None:
        raise ValueError(
            "Unsupported image binary format"
        )

    return CaptureFileInfo(
        path=str(file_path),
        tube_type=tube_type,
        camera_side=camera_side,
        observed_at=observed_now_iso(),
    )


# ============================================================
# WATCHDOG EVENT HANDLER
# ============================================================

class CameraFolderEventHandler(FileSystemEventHandler):
    def __init__(self, agent: "CameraAgent"):
        super().__init__()
        self.agent = agent

    def on_created(self, event):
        if not event.is_directory:
            self.agent.add_candidate(Path(event.src_path))

    def on_modified(self, event):
        if not event.is_directory:
            self.agent.add_candidate(Path(event.src_path))

    def on_moved(self, event):
        if not event.is_directory:
            self.agent.add_candidate(Path(event.dest_path))


# ============================================================
# SMART SHARE POLLER
# ============================================================

# Thu muc co so entry khong vuot nguong nay duoc quet lai moi chu ky
# (re_hat) de bat ca file bi ghi de tai cho; chi phi thap nen an toan.
# Thu muc lon (hang nghin file) chi quet khi mtime cua no thay doi.
CHEAP_DIR_MAX_ENTRIES = 64

# Khoang nghi toi thieu giua hai chu ky cua poller (giay).
POLLER_MIN_INTERVAL_SEC = 0.2


@dataclass
class _DirState:
    """Trang thai da biet cua mot thu muc dang theo doi."""

    mtime_ns: int = 0
    scanned: bool = False
    entry_count: int = 0

    # file path - tuple (mtime_ns, size, content_hash) tai lan quet gan nhat
    files: dict = field(default_factory=dict)


class SmartSharePoller:
    """
    Poller THEO TANG thay the PollingObserver quet toan cay recursive.

    Van de cua PollingObserver tren share lon: moi chu ky no phai tao
    DirectorySnapshot cua TOAN BO cay thu muc (stat tung file qua SMB)
    roi so diff. Voi share hang tram GB / hang chuc nghin file, moi
    vong quet keo dai hang phut, agent nhu "dung" va anh moi khong
    duoc xu ly kip.

    Cach lam cua poller nay:
    - Moi chu ky: os.stat tung thu muc da biet (re) + os.scandir NHUNG
      thu muc MOI / DOI mtime / thu muc nho (entry_count khong vuot
      CHEAP_DIR_MAX_ENTRIES).
    - KHONG quet lai cay lich su khong thay doi, nen chi phi moi chu
      ky khong phu thuoc tong du lieu tren share.
    - Baseline theo ton-tai-khi-quet-lan-dau tung folder: LAN DAU
      quet mot thu muc, toan bo file dang ton tai duoc ghi nhan la
      "da xu ly", KHONG emit gi (bat ke mtime). Cac lan quet sau chi
      emit file MOI hoac bi ghi de sau baseline. Nho vay du lieu cu
      tren share khong bi xu ly lai va KHONG phu thuoc lech gio
      (clock skew) giua may chu share va may Agent.
    - Gioi han so scandir moi chu ky (max_dir_scans_per_cycle) de mot
      chu ky khong bao gio qua lau; phan con lai quet o chu ky sau.
    - watch_current_day: chi theo doi folder hom nay + hom qua (theo
      gio may Agent) ngay duoi root, bo qua cay ngay lich su khac.
    """

    def __init__(
        self,
        root_path: Path,
        on_candidate,
        poll_interval_sec: float = 2.0,
        max_dir_scans_per_cycle: int = 32,
        stop_event: Optional[threading.Event] = None,
        watch_current_day: bool = False,
        day_folder_format: str = "%Y-%m-%d",
        now_fn=None,
    ):
        self.root_path = Path(root_path)
        self.on_candidate = on_candidate

        # Chi theo doi folder NGAY HIEN TAI ngay duoi root (vd
        # 2026-09-11). Folder ngay lich su khong duoc track / stat
        # -> chi phi SMB moi chu ky gan nhu khong phu thuoc so nam
        # du lieu tren share. Sang ngay moi tu rollover state.
        self.watch_current_day = bool(watch_current_day)
        self.day_folder_format = str(day_folder_format)
        self.now_fn = now_fn or datetime.now
        self._day_name: Optional[str] = None
        self.poll_interval_sec = max(
            float(poll_interval_sec),
            POLLER_MIN_INTERVAL_SEC,
        )
        self.max_dir_scans_per_cycle = max(
            int(max_dir_scans_per_cycle),
            1,
        )

        self.stop_event = stop_event or threading.Event()
        self.dirs: dict = {}
        self._thread: Optional[threading.Thread] = None
        self.stats: dict = {
            "cycles": 0,
            "last_cycle_sec": 0.0,
            "last_candidates": 0,
            "deferred_dirs": 0,
            "dirs_tracked": 0,
            "day_folder": "",
        }

    # --------------------------------------------------------
    # DAY FOLDER FILTER
    # --------------------------------------------------------

    def _watched_day_names(self, now: datetime) -> list:
        """
        Ten cac folder ngay duoc theo doi khi watch_current_day bat:
        hom nay + hom qua (theo gio may Agent).

        May chu share thuong lech gio so voi may Agent: khi gio may
        camera cham hon, vua sau nua dem cua Agent camera van ghi vao
        folder ngay cu trong vai gio -> phai theo doi them folder hom
        qua neu khong se mat anh trong khung nay. Neu day_folder_format
        khong phan biet theo ngay (vd chi "%H") thi hai ten trung nhau
        -> chi con mot folder.
        """
        names = [now.strftime(self.day_folder_format)]

        yesterday = (now - timedelta(days=1)).strftime(
            self.day_folder_format
        )

        if yesterday not in names:
            names.append(yesterday)

        return names

    # --------------------------------------------------------
    # LIFECYCLE
    # --------------------------------------------------------

    def start(self):
        if self._thread is not None:
            return

        self._thread = threading.Thread(
            target=self._loop,
            name="Share-Poller",
            daemon=True,
        )
        self._thread.start()

        day_note = ""

        if self.watch_current_day:
            day_note = " day_filter=%s" % ",".join(
                str(self.root_path / name)
                for name in self._watched_day_names(self.now_fn())
            )

        LOGGER.info(
            "SmartSharePoller started. root=%s interval=%.1fs "
            "max_dir_scans_per_cycle=%s%s",
            self.root_path,
            self.poll_interval_sec,
            self.max_dir_scans_per_cycle,
            day_note,
        )

    def stop(self, timeout: float = 5.0):
        self.stop_event.set()

        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

        LOGGER.info("SmartSharePoller stopped")

    def _loop(self):
        while not self.stop_event.is_set():
            started = time.monotonic()

            try:
                candidates = self._run_cycle()
            except Exception:
                LOGGER.exception("Smart share poller cycle error")
                candidates = 0

            elapsed = time.monotonic() - started

            self.stats["cycles"] += 1
            self.stats["last_cycle_sec"] = elapsed
            self.stats["last_candidates"] = candidates
            self.stats["dirs_tracked"] = len(self.dirs)

            if candidates:
                LOGGER.info(
                    "Share poller: %s new/changed file(s) queued",
                    candidates,
                )

            if max(self.poll_interval_sec * 5.0, 30.0) < elapsed:
                LOGGER.warning(
                    "Share poller cycle took %.1fs (interval=%.1fs) "
                    "- xem xet tang max_dir_scans_per_cycle. "
                    "dirs_tracked=%s",
                    elapsed,
                    self.poll_interval_sec,
                    len(self.dirs),
                )

            self.stop_event.wait(self.poll_interval_sec)

    # --------------------------------------------------------
    # SCAN
    # --------------------------------------------------------

    def _run_cycle(self):
        root_key = str(self.root_path)

        if root_key not in self.dirs:
            self.dirs[root_key] = _DirState()

        if self.watch_current_day:
            day_names = self._watched_day_names(self.now_fn())
            today = day_names[0]
            self.stats["day_folder"] = today

            if today != self._day_name:
                # Rollover nua dem: chi don state cua folder ngay khong
                # con trong danh sach theo doi (hom nay + hom qua).
                # State folder hom qua duoc giu lai de camera con ghi
                # vao do sau nua dem cua Agent (gio may camera cham
                # hon) khong bi mat, va ep quet lai root o chu ky nay.
                if self._day_name is not None:
                    LOGGER.info(
                        "Share poller: day rollover %s -> %s, "
                        "watch day folders: %s",
                        self._day_name,
                        today,
                        ",".join(day_names),
                    )

                watched_keys = {
                    str(self.root_path / name) for name in day_names
                }

                for known in list(self.dirs):
                    if known == root_key:
                        continue

                    # Chi don nhanh cap 1 ngay duoi root ma khong con
                    # duoc theo doi (kem toan bo subtree cua no). Giu
                    # nguyen subtree cua folder hom nay + hom qua de
                    # file da biet khong bi emit lai sau rollover.
                    if (
                        os.path.dirname(known) == root_key
                        and known not in watched_keys
                    ):
                        self._prune_subtree(known)

                root_info = self.dirs.get(root_key)

                if root_info is not None:
                    root_info.scanned = False

                self._day_name = today

            # 1 stat re: track folder hom nay + hom qua ngay khi camera
            # tao no (khong cho mtime root doi). Chua co thi bo qua,
            # track loop ben duoi tu loi khi folder xuat hien.
            for day_name in day_names:
                day_path = self.root_path / day_name
                day_key = str(day_path)

                if day_key in self.dirs:
                    continue

                try:
                    st = os.stat(day_path)
                except OSError:
                    continue

                if st is not None and stat.S_ISDIR(st.st_mode):
                    self.dirs[day_key] = _DirState(
                        mtime_ns=st.st_mtime_ns
                    )

        scan_first: deque = deque()
        scan_later: deque = deque()

        for path, info in list(self.dirs.items()):
            try:
                st = os.stat(path)
            except OSError:
                # Thu muc bi xoa / share mat ket noi tam thoi, don
                # state. Chu ky sau tu discovery lai tu root. File da
                # xu ly khong bi xu ly lai nho dedup trong database.
                self._prune_subtree(path)
                continue

            if not stat.S_ISDIR(st.st_mode):
                self._prune_subtree(path)
                continue

            if not info.scanned or st.st_mtime_ns != info.mtime_ns:
                scan_first.append((path, st.st_mtime_ns))
            elif info.entry_count <= CHEAP_DIR_MAX_ENTRIES:
                scan_later.append((path, st.st_mtime_ns))

        budget = self.max_dir_scans_per_cycle
        candidates = 0
        deferred = 0

        for queue in (scan_first, scan_later):
            while queue:
                path, mtime_hint_ns = queue.popleft()

                if budget <= 0:
                    deferred += 1
                    continue

                budget -= 1

                try:
                    candidates += self._scan_dir(
                        path,
                        mtime_hint_ns,
                        scan_first,
                    )
                except OSError as exc:
                    LOGGER.warning(
                        "Share poller: cannot scan dir %s: %s",
                        path,
                        exc,
                    )

        if deferred:
            LOGGER.info(
                "Share poller: %s dir(s) deferred to next cycle "
                "(max_dir_scans_per_cycle=%s)",
                deferred,
                self.max_dir_scans_per_cycle,
            )

        self.stats["deferred_dirs"] = deferred

        return candidates

    def _scan_dir(self, path, mtime_hint_ns, discovered):
        # Track ngay tu dau: neu scandir loi thi 'scanned' van False,
        # chu ky sau tu quet lai.
        info = self.dirs.setdefault(path, _DirState())

        # Lan dau quet thu muc = baseline: moi file dang ton tai duoc
        # coi la "da xu ly" (khong emit). Neu da tung quet (ca truong
        # hop ep quet lai sau rollover thi state file van con) thi
        # chay so diff binh thuong.
        was_scanned = info.scanned or bool(info.files)

        subdirs: list = []
        files: list = []

        # Tren Windows/SMB, entry.stat() doc tu cache cua directory
        # listing nen khong ton them round trip mang.
        with os.scandir(path) as entries:
            for entry in entries:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        subdirs.append(
                            (
                                entry.path,
                                entry.stat(
                                    follow_symlinks=False
                                ).st_mtime_ns,
                            )
                        )

                    elif entry.is_file(follow_symlinks=False):
                        st = entry.stat(follow_symlinks=False)
                        files.append(
                            (entry.path, st.st_mtime_ns, st.st_size)
                        )

                except OSError:
                    continue

        # watch_current_day: tai cap root chi nhan entry trung ten
        # folder ngay duoc theo doi (hom nay + hom qua theo gio may
        # Agent - bu lech gio may chu share) -> folder ngay lich su
        # khac khong bao gio duoc track; prune o chu ky rollover tu
        # don state sot lai.
        if self.watch_current_day and path == str(self.root_path):
            watched = set(self._watched_day_names(self.now_fn()))
            subdirs = [
                (sub_path, sub_mtime)
                for sub_path, sub_mtime in subdirs
                if os.path.basename(sub_path) in watched
            ]

        # Luu mtime TRUOC lan quet: neu co file ghi vao trong luc scan
        # thi mtime thuc te se khac, chu ky sau tu quet lai (no-op).
        info.mtime_ns = mtime_hint_ns
        info.scanned = True

        current_files: dict = {}
        emitted = 0
        is_cheap_dir = len(files) + len(subdirs) <= CHEAP_DIR_MAX_ENTRIES

        for fpath, mtime_ns, size in files:
            content_hash = (
                self._file_content_hash(fpath) if is_cheap_dir else None
            )
            current_files[fpath] = (mtime_ns, size, content_hash)

            # Baseline lan dau: ghi nhan toan bo file hien co, khong
            # emit gi - bat ke mtime (du lieu cu tren share khong bi
            # xu ly lai, khong phai bu lech gio may chu share).
            if not was_scanned:
                continue

            if info.files.get(fpath) == (mtime_ns, size, content_hash):
                continue

            self.on_candidate(Path(fpath))
            emitted += 1

        info.files = current_files
        info.entry_count = len(files) + len(subdirs)

        listed = {sub_path for sub_path, _ in subdirs}

        for sub_path, sub_mtime in subdirs:
            sub_info = self.dirs.get(sub_path)

            if sub_info is None:
                self.dirs[sub_path] = _DirState(mtime_ns=sub_mtime)
                discovered.append((sub_path, sub_mtime))
                continue

            if not sub_info.scanned or sub_info.mtime_ns != sub_mtime:
                discovered.append((sub_path, sub_mtime))

        # Thu muc con khong con trong listing, don luon state cay con.
        for known in list(self.dirs):
            if known == path:
                continue

            if os.path.dirname(known) == path and known not in listed:
                self._prune_subtree(known)

        return emitted

    @staticmethod
    def _file_content_hash(path: str) -> Optional[str]:
        """Tra ve SHA-256 cua file trong folder nho de bat ghi de tai cho."""
        digest = hashlib.sha256()

        try:
            with open(path, "rb") as file_obj:
                for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
                    digest.update(chunk)
        except OSError:
            return None

        return digest.hexdigest()

    def _prune_subtree(self, path):
        prefix = path.rstrip("\\/") + os.sep

        for known in [
            key
            for key in self.dirs
            if key == path or key.startswith(prefix)
        ]:
            self.dirs.pop(known, None)


# ============================================================
# CAMERA AGENT
# ============================================================

class CameraAgent:
    def __init__(self, config: dict):
        self.config = config

        self.agent_id = safe_component(
            config["agent"]["agent_id"],
            "agent_id"
        )

        # root_path ưu tiên lấy từ network_share (UNC path).
        # Nếu không khai báo network_share thì dùng root_path local.
        self.network_share: Optional[dict] = config["agent"].get(
            "network_share"
        )
        self.share_username: Optional[str] = None
        self.share_password: Optional[str] = None
        self.poll_observer: bool = False

        if self.network_share:
            share_host = str(
                self.network_share.get("host", "")
            ).strip()
            share_name = str(
                self.network_share.get("share", "")
            ).strip()

            if not share_host or not share_name:
                raise ValueError(
                    "agent.network_share requires both "
                    "'host' and 'share'"
                )

            self.share_username = self.network_share.get("username")
            self.share_password = self.network_share.get("password")

            # SMB không gửi event realtime qua mạng -> mặc định
            # dùng PollingObserver để phát hiện file mới.
            self.poll_observer = bool(
                self.network_share.get("poll_observer", True)
            )

            subfolder = str(
                self.network_share.get("subfolder", "")
            ).strip().strip("/\\")

            self.root_path = Path(
                f"//{share_host}/{share_name}"
            )

            if subfolder:
                self.root_path = self.root_path / subfolder

            LOGGER.info(
                "Network share configured. root=%s user=%s "
                "poll_observer=%s",
                self.root_path,
                self.share_username,
                self.poll_observer,
            )

        else:
            root_path_value = config["agent"].get("root_path")

            if not root_path_value:
                raise ValueError(
                    "agent.root_path is required when "
                    "agent.network_share is not configured"
                )

            self.root_path = Path(root_path_value)

            # Tự động bật PollingObserver khi root_path là UNC
            # path (\\server\share hoặc //server/share) vì SMB
            # không gửi event realtime qua mạng. Với UNC, pathlib
            # chuẩn hóa cả '//' và '\\' thành drive '\\host\share'.
            if self.root_path.drive.startswith("\\\\"):
                self.poll_observer = True

                LOGGER.info(
                    "UNC root_path detected (%s). "
                    "PollingObserver enabled automatically.",
                    self.root_path,
                )

        # Chu kỳ quét (giây) của PollingObserver - chỉ có tác dụng
        # khi poll_observer bật (share mạng / UNC path). Mặc định 2s.
        self.poll_interval_sec = float(
            config["agent"].get("poll_interval_sec", 2.0)
        )

        if self.poll_interval_sec <= 0:
            raise ValueError(
                "agent.poll_interval_sec must be greater than 0"
            )

        # ==================================================
        # SMART POLLER - khac phuc agent "dung" khi network
        # share co hang tram GB / hang chuc folder. Bat mac
        # dinh khi poll_observer bat (share mang / UNC). Dat
        # smart_poller: false trong config de quay lai co che
        # PollingObserver quet toan cay (cach hoat dong cu).
        # ==================================================
        self.smart_poller_enabled = bool(
            config["agent"].get("smart_poller", True)
        )
        self.use_smart_poller = (
            self.poll_observer and self.smart_poller_enabled
        )

        self.max_dir_scans_per_cycle = int(
            config["agent"].get("max_dir_scans_per_cycle", 32)
        )

        if self.max_dir_scans_per_cycle <= 0:
            raise ValueError(
                "agent.max_dir_scans_per_cycle must be "
                "greater than 0"
            )

        # DEPRECATED - khong con dung. Baseline theo ton-tai-khi-
        # quet-lan-dau tung folder (SmartSharePoller) va bootstrap
        # every-start (legacy) da thay the loc theo mtime, nen lech
        # gio may chu share khong con anh huong. Van parse de config
        # cu khong crash; gia tri khac 0 chi duoc log lai mot lan.
        self.start_grace_sec = float(
            config["agent"].get("start_grace_sec", 0.0)
        )

        if self.start_grace_sec < 0:
            raise ValueError(
                "agent.start_grace_sec must not be negative"
            )

        if self.start_grace_sec:
            LOGGER.info(
                "agent.start_grace_sec=%s is deprecated and ignored: "
                "baseline-by-existence at startup replaces the mtime "
                "filter (share clock skew no longer matters).",
                self.start_grace_sec,
            )

        # Chi theo doi folder ngay hien tai + hom qua ngay duoi root
        # (vd 2026-09-11 + 2026-09-10) thay vi ca cay lich su: poller
        # chi track 2 folder nay, tu rollover khi sang ngay moi.
        # Giu folder hom qua de bu lech gio may chu share: camera
        # con ghi vao folder ngay cu sau nua dem cua Agent.
        self.watch_current_day = bool(
            config["agent"].get("watch_current_day", False)
        )
        self.day_folder_format = str(
            config["agent"].get("day_folder_format", "%Y-%m-%d")
        )

        self.file_stable_sec = float(
            config["agent"]["file_stable_sec"]
        )

        self.file_check_interval_sec = float(
            config["agent"]["file_check_interval_sec"]
        )

        self.reconcile_interval_sec = float(
            config["agent"]["reconcile_interval_sec"]
        )

        self.step_timeout_sec = float(
            config["agent"]["step_timeout_sec"]
        )

        step_timeout = self.step_timeout_sec
        self.cycle_timeout_sec = float(
            config["agent"].get(
                "cycle_timeout_sec",
                max(3 * step_timeout, 600),
            )
        )

        self.auto_recover_out_of_sync = bool(
            config["agent"].get(
                "auto_recover_out_of_sync",
                True,
            )
        )

        self.ai_base_url = (
            config["ai_server"]["base_url"].rstrip("/")
        )

        self.upload_timeout_sec = float(
            config["ai_server"]["upload_timeout_sec"]
        )

        self.retry_interval_sec = float(
            config["ai_server"]["retry_interval_sec"]
        )

        self.database = CameraAgentDatabase(
            database_path=config["storage"]["database_path"],
            agent_id=self.agent_id,
        )

        self.candidates: dict[str, CandidateFile] = {}
        self.candidate_lock = threading.Lock()

        self.stop_event = threading.Event()

        self.observer: Any = None
        self.share_poller: Any = None
        self.file_worker_thread: Optional[threading.Thread] = None
        self.reconcile_thread: Optional[threading.Thread] = None
        self.upload_thread: Optional[threading.Thread] = None
        self.timeout_thread: Optional[threading.Thread] = None

        self.http_session = requests.Session()
        self._recovery_warnings: dict[str, float] = {}

    # --------------------------------------------------------
    # NETWORK SHARE
    # --------------------------------------------------------

    def connect_network_share(self) -> None:
        if not self.network_share:
            return

        # Share đã truy cập được (credential đã có sẵn) -> bỏ qua.
        if self.root_path.exists():
            LOGGER.info(
                "Network share already accessible: %s",
                self.root_path,
            )
            return

        share_host = str(self.network_share["host"]).strip()
        share_name = str(self.network_share["share"]).strip()
        unc_target = f"\\\\{share_host}\\{share_name}"

        LOGGER.info(
            "Connecting network share \\\\%s\\%s (user=%s) ...",
            share_host,
            share_name,
            self.share_username,
        )

        if sys.platform != "win32":
            raise RuntimeError(
                f"Network share {unc_target} is not accessible "
                "and auto-connect via 'net use' requires Windows."
            )

        # LƯU Ý: không log command/password.
        # Luôn truyền password (kể cả rỗng) để 'net use' không
        # treo chờ nhập tay trong môi trường non-interactive.
        command = ["net", "use", unc_target]

        if self.share_username:
            command.append(f"/user:{self.share_username}")

        command.append(self.share_password or "")
        command.append("/persistent:no")

        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=30,
            )

        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"Timed out connecting network share {unc_target}. "
                "Check host IP, firewall and SMB port 445."
            )

        if result.returncode != 0:
            error_text = (
                result.stderr or result.stdout or ""
            ).strip()

            raise RuntimeError(
                f"Failed to connect network share {unc_target} "
                f"(net use exit code={result.returncode}): "
                f"{error_text}. Check host, share name, username, "
                "password, firewall and SMB port 445."
            )

        if not self.root_path.exists():
            raise RuntimeError(
                f"Network share connected but root path not found: "
                f"{self.root_path}. Check 'share' and 'subfolder' "
                "in network_share config."
            )

        LOGGER.info(
            "Network share connected: %s",
            self.root_path,
        )

    # --------------------------------------------------------
    # START / STOP
    # --------------------------------------------------------

    def ensure_root_path(self) -> None:
        """
        Dam bao thu muc watch (root_path) ton tai truoc khi start.

        - Local path: TU TAO khi chua co (kem cac thu muc cha) de
          server luon khoi dong duoc sau khi clone repo hoac xoa
          nham thu muc watch.
        - Network share / UNC: KHONG tu tao (thu muc nam tren may
          khac) -> bao loi kem goi y kiem tra SMB.
        """
        if self.root_path.is_dir():
            return

        # Ton tai nhung la file, khong phai thu muc -> cau hinh sai.
        if self.root_path.exists():
            raise RuntimeError(
                f"Root path is not a directory: {self.root_path}. "
                "Fix 'root_path' in config."
            )

        # UNC path (vd \\host\share) co .drive bat dau bang "\\".
        is_unc = self.root_path.drive.startswith("\\\\")

        if self.network_share or is_unc:
            raise RuntimeError(
                f"Root path not found on network share: "
                f"{self.root_path}. Check host, share name, "
                "'subfolder' in network_share config, credentials, "
                "firewall and SMB port 445."
            )

        try:
            self.root_path.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise RuntimeError(
                f"Cannot create watch directory {self.root_path}: "
                f"{exc}. Check 'root_path' in config and write "
                "permission."
            ) from exc

        LOGGER.info(
            "Watch directory auto-created: %s",
            self.root_path,
        )

    def start(self) -> None:
        self.connect_network_share()
        self.ensure_root_path()

        self.bootstrap_start_from_now()
        self.recover_after_restart()

        if self.use_smart_poller:
            # Share lon (network_share / UNC): dung
            # SmartSharePoller thay cho PollingObserver quet
            # toan cay recursive.
            self.observer = None

            self.share_poller = SmartSharePoller(
                root_path=self.root_path,
                on_candidate=self.add_candidate,
                poll_interval_sec=self.poll_interval_sec,
                max_dir_scans_per_cycle=(
                    self.max_dir_scans_per_cycle
                ),
                stop_event=self.stop_event,
                watch_current_day=self.watch_current_day,
                day_folder_format=self.day_folder_format,
            )
            self.share_poller.start()

            LOGGER.info(
                "Smart share poller enabled (PollingObserver "
                "skipped). root=%s interval=%ss "
                "max_dir_scans_per_cycle=%s",
                self.root_path,
                self.poll_interval_sec,
                self.max_dir_scans_per_cycle,
            )

            # reconcile_loop quet rglob TOAN BO share - chi ap
            # dung o che do cu; smart poller tu bu event theo
            # tung folder.
            self.reconcile_thread = None

            LOGGER.info(
                "Reconcile loop disabled "
                "(smart share poller active)"
            )

            self._start_common_workers()

            LOGGER.info(
                "Camera Agent started (smart poller). "
                "root=%s agent_id=%s",
                self.root_path,
                self.agent_id,
            )

            return

        event_handler = CameraFolderEventHandler(self)

        # Share qua mạng SMB/UNC không nhận event realtime
        # (ReadDirectoryChangesW không hoạt động trên UNC path)
        # -> dùng PollingObserver: tự động khi root_path là UNC
        #    path, hoặc khi 'poll_observer' bật trong config.
        if self.poll_observer:
            self.observer = PollingObserver(
                timeout=self.poll_interval_sec
            )
        else:
            self.observer = Observer()

        self.observer.schedule(
            event_handler,
            str(self.root_path),
            recursive=True,
        )
        self.observer.start()

        self.file_worker_thread = threading.Thread(
            target=self.file_worker_loop,
            name="File-Worker",
            daemon=True,
        )
        self.file_worker_thread.start()

        self.reconcile_thread = threading.Thread(
            target=self.reconcile_loop,
            name="Reconcile-Worker",
            daemon=True,
        )
        self.reconcile_thread.start()

        self.upload_thread = threading.Thread(
            target=self.upload_loop,
            name="Upload-Worker",
            daemon=True,
        )
        self.upload_thread.start()

        self.timeout_thread = threading.Thread(
            target=self.timeout_loop,
            name="Timeout-Worker",
            daemon=True,
        )
        self.timeout_thread.start()

        LOGGER.info(
            "Camera Agent started. root=%s agent_id=%s",
            self.root_path,
            self.agent_id,
        )

    def _start_common_workers(self):
        self.file_worker_thread = threading.Thread(
            target=self.file_worker_loop,
            name="File-Worker",
            daemon=True,
        )
        self.file_worker_thread.start()

        self.upload_thread = threading.Thread(
            target=self.upload_loop,
            name="Upload-Worker",
            daemon=True,
        )
        self.upload_thread.start()

        self.timeout_thread = threading.Thread(
            target=self.timeout_loop,
            name="Timeout-Worker",
            daemon=True,
        )
        self.timeout_thread.start()

    def stop(self) -> None:
        self.stop_event.set()

        if self.share_poller:
            self.share_poller.stop()

        if self.observer:
            self.observer.stop()
            self.observer.join(timeout=5)

        for thread in [
            self.file_worker_thread,
            self.reconcile_thread,
            self.upload_thread,
            self.timeout_thread,
        ]:
            if thread:
                thread.join(timeout=5)

        LOGGER.info("Camera Agent stopped")

    # --------------------------------------------------------
    # BOOTSTRAP
    # --------------------------------------------------------

    def iter_image_files(self):
        try:
            for path in self.root_path.rglob("*"):
                if path.is_file():
                    yield path

        except Exception:
            LOGGER.exception("Error while scanning root folder")

    def bootstrap_start_from_now(self) -> None:
        """
        Chot "moi file dang ton tai tai luc start = da xu ly" de
        du lieu cu khong bao gio bi xu ly lai:

        - Smart poller bat: bo qua - poller tu lap baseline theo
          ton-tai-khi-quet-lan-dau tung folder, khong can quet cay.
        - Legacy (PollingObserver + reconcile_loop): MOI LAN start
          deu quet rglob toan bo file duoi root (chi stat, khong doc
          noi dung file) va danh dau BOOTSTRAP_IGNORED. INSERT OR
          IGNORE nen idempotent, chi ton chi phi stat.

        Goi truoc khi bat Observer/poller trong start() de khong con
        khoang thoi gian lo: file tao sau buoc nay duoc Observer
        bat qua event on_created.
        """
        mode = self.config["agent"]["bootstrap_mode"]

        # Smart poller tu lap baseline tung folder khi khoi dong.
        # Voi share hang tram GB / hang chuc nghin file, buoc rglob
        # toan cay chinh la nguyen nhan server "dung" ngay sau buoc
        # ket noi network share.
        if self.use_smart_poller:
            LOGGER.warning(
                "Bootstrap skipped (smart poller active, "
                "mode=%s). Existing files are ignored by "
                "per-folder first-scan baseline - khong quet "
                "toan cay khi khoi dong.",
                mode,
            )

            return

        if mode != "start_from_now":
            LOGGER.warning(
                "Unsupported bootstrap mode=%s. "
                "Use start_from_now for safe industrial startup.",
                mode,
            )

        all_files = []

        for path in self.iter_image_files():
            all_files.append(str(path))

        self.database.bootstrap_files(all_files)

        LOGGER.warning(
            "Bootstrap completed. Existing files=%s were ignored. "
            "Streams will be recovered from DB queue/state.",
            len(all_files),
        )

    def recover_after_restart(self) -> None:
        """
        Khoi phuc hang doi va anh dang cho ket qua sau khi agent restart.

        - WAIT_* + active event con ton tai: giu nguyen de callback/
          truy van lai ket qua tiep tuc xu ly (khong reset, khong mat anh).
        - WAIT_* nhung event khong con: dua stream ve READY_STEP1.
        - OUT_OF_SYNC: chi auto-recover khi cau hinh cho phep.
        - Sau do gan anh dau tien trong hang doi cho moi stream ranh.
        """
        streams = self.database.get_streams()

        for stream in streams:
            side = stream["camera_side"]
            tube_type = stream["tube_type"]
            state = stream["state"]

            if state in (
                CameraAgentDatabase.STATE_WAIT_STEP1_RESULT,
                CameraAgentDatabase.STATE_WAIT_STEP3_RESULT,
            ):
                event_id = stream.get("active_event_id")
                event = (
                    self.database.get_event(event_id)
                    if event_id
                    else None
                )

                if event is None:
                    LOGGER.warning(
                        "Restart recovery: active event missing -> "
                        "READY_STEP1 side=%s no=%s event=%s",
                        side,
                        tube_type,
                        event_id,
                    )
                    self.database.force_stream_ready(
                        side, tube_type, step=1
                    )
                else:
                    LOGGER.warning(
                        "Restart recovery: keep waiting result "
                        "side=%s no=%s event=%s state=%s",
                        side,
                        tube_type,
                        event_id,
                        state,
                    )
                continue

            if state == CameraAgentDatabase.STATE_OUT_OF_SYNC:
                if self.auto_recover_out_of_sync:
                    LOGGER.warning(
                        "Restart recovery: auto-recover OUT_OF_SYNC "
                        "side=%s no=%s",
                        side,
                        tube_type,
                    )
                    self.database.force_stream_ready(
                        side, tube_type, step=1
                    )
                else:
                    LOGGER.warning(
                        "Restart recovery: keep OUT_OF_SYNC (manual "
                        "reset required) side=%s no=%s",
                        side,
                        tube_type,
                    )

        assigned = self.database.dispatch_all_pending(
            auto_recover_out_of_sync=self.auto_recover_out_of_sync
        )

        for event in assigned:
            LOGGER.warning(
                "Restart recovery: assigned queued image "
                "event=%s cycle=%s step=%s",
                event["event_id"],
                event["cycle_id"],
                event["step"],
            )

    def auto_reset_streams(self) -> None:
        """Tuong thich API cu: gio la recover_after_restart()."""
        self.recover_after_restart()

    # --------------------------------------------------------
    # FILE CANDIDATE MANAGEMENT
    # --------------------------------------------------------

    def add_candidate(self, path: Path) -> None:
        try:
            if not path.exists() or not path.is_file():
                return

            path_key = str(path)

            with self.candidate_lock:
                if path_key not in self.candidates:
                    now = time.monotonic()

                    self.candidates[path_key] = CandidateFile(
                        path=path,
                        first_seen_monotonic=now,
                        last_size=-1,
                        last_size_change_monotonic=now,
                    )

        except Exception:
            LOGGER.exception("add_candidate error path=%s", path)

    def remove_candidate(self, path: Path) -> None:
        with self.candidate_lock:
            self.candidates.pop(str(path), None)

    def get_ready_candidates(self) -> list[CandidateFile]:
        ready = []
        now = time.monotonic()

        with self.candidate_lock:
            for key, candidate in list(self.candidates.items()):
                path = candidate.path

                if not path.exists():
                    self.candidates.pop(key, None)
                    continue

                try:
                    size = path.stat().st_size
                except OSError:
                    continue

                if size <= 0:
                    continue

                if candidate.last_size != size:
                    candidate.last_size = size
                    candidate.last_size_change_monotonic = now
                    continue

                stable_time = (
                    now - candidate.last_size_change_monotonic
                )

                if stable_time < self.file_stable_sec:
                    continue

                # Kiểm tra có thể mở đọc file.
                try:
                    with open(path, "rb") as f:
                        f.read(64)

                except (PermissionError, OSError):
                    continue

                ready.append(candidate)

        # Normal operation uses file completion order.
        ready.sort(
            key=lambda item: item.first_seen_monotonic
        )

        return ready

    # --------------------------------------------------------
    # FILE WORKER
    # --------------------------------------------------------

    def file_worker_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                ready_candidates = self.get_ready_candidates()

                for candidate in ready_candidates:
                    self.process_stable_file(candidate.path)

            except Exception:
                LOGGER.exception("File worker loop error")

            self.stop_event.wait(
                self.file_check_interval_sec
            )

    def process_stable_file(self, path: Path) -> None:
        try:
            if self.database.is_file_seen(str(path)):
                self.remove_candidate(path)
                return

            file_info = parse_capture_file(
                root_path=self.root_path,
                file_path=path,
            )

            result = self.database.assign_file_to_sequence(
                file_info,
                auto_recover_out_of_sync=self.auto_recover_out_of_sync,
            )

            self.remove_candidate(path)

            if result["status"] == "ALREADY_SEEN":
                return

            if result["status"] == "BLOCKED":
                LOGGER.error(
                    "File blocked path=%s message=%s",
                    path,
                    result["message"],
                )

                if result["abort"]:
                    LOGGER.error(
                        "Abort queued: %s",
                        result["abort"],
                    )

                return

            if result["status"] == "QUEUED":
                LOGGER.info(
                    "Image queued (stream waiting) path=%s",
                    path,
                )
                return

            event = result["event"]

            if result.get("abort"):
                LOGGER.error(
                    "Abort queued: %s",
                    result["abort"],
                )

            LOGGER.info(
                "Assigned file event=%s cycle=%s side=%s no=%s step=%s upload=%s",
                event["event_id"],
                event["cycle_id"],
                event["camera_side"],
                event["tube_type"],
                event["step"],
                event["upload_status"],
            )

        except ValueError as ex:
            LOGGER.error(
                "Invalid capture file path=%s error=%s",
                path,
                ex,
            )

            self.database.mark_file_seen(
                str(path),
                "IGNORED_INVALID_PATH_OR_NAME",
            )

            self.remove_candidate(path)

        except Exception:
            LOGGER.exception(
                "Process stable file failed path=%s",
                path,
            )

    # --------------------------------------------------------
    # RECONCILE
    # --------------------------------------------------------

    def reconcile_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                files = []

                for path in self.iter_image_files():
                    if self.database.is_file_seen(str(path)):
                        continue

                    if detect_image_format(path) is None:
                        continue

                    try:
                        mtime_ns = path.stat().st_mtime_ns
                    except OSError:
                        continue

                    files.append((mtime_ns, path))

                files.sort(key=lambda item: item[0])

                for _, path in files:
                    self.add_candidate(path)

            except Exception:
                LOGGER.exception("Reconcile loop error")

            self.stop_event.wait(
                self.reconcile_interval_sec
            )

    # --------------------------------------------------------
    # TIMEOUT
    # --------------------------------------------------------

    def timeout_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                warnings = self.database.warn_incomplete_streams(
                    self.step_timeout_sec
                )

                for stream in warnings:
                    LOGGER.warning(
                        "Sequence pending beyond step_timeout_sec side=%s "
                        "no=%s cycle=%s step=%s idle_sec=%.1f",
                        stream["camera_side"],
                        stream["tube_type"],
                        stream["current_cycle_id"],
                        stream["next_step"],
                        time.time() - (stream["last_event_unix"] or 0),
                    )

                aborts = self.database.timeout_incomplete_streams(
                    self.cycle_timeout_sec
                )

                for abort in aborts:
                    LOGGER.error(
                        "Sequence timeout -> OUT_OF_SYNC: %s",
                        abort,
                    )

            except Exception:
                LOGGER.exception("Timeout loop error")

            self.stop_event.wait(1.0)

    # --------------------------------------------------------
    # UPLOAD TO AI SERVER
    # --------------------------------------------------------

    def upload_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.upload_pending_aborts()
                self.upload_pending_images()
                self.recover_pending_results()

            except Exception:
                LOGGER.exception("Upload loop error")

            self.stop_event.wait(
                self.retry_interval_sec
            )

    def upload_pending_images(self) -> None:
        events = self.database.get_pending_uploads()

        for event in events:
            if self.stop_event.is_set():
                return

            self.upload_one_image(event)

    def upload_one_image(self, event: dict) -> None:
        image_path = Path(event["file_path"])

        if not image_path.exists():
            error = "Local image file missing"

            LOGGER.error(
                "Upload failed event=%s: %s",
                event["event_id"],
                error,
            )

            self.database.mark_upload_result(
                event_id=event["event_id"],
                success=False,
                error_message=error,
            )

            return

        metadata = {
            "event_id": event["event_id"],
            "cycle_id": event["cycle_id"],
            "camera_side": event["camera_side"],
            "tube_type": event["tube_type"],
            "step": int(event["step"]),

            # Đây là thời gian Agent ghi nhận file đã hoàn tất.
            # Không phải timestamp lấy từ tên ảnh.
            "observed_at": event["capture_timestamp"],
        }

        url = f"{self.ai_base_url}/api/v1/images"

        try:
            with open(image_path, "rb") as f:
                files = {
                    "image": (
                        image_path.name,
                        f,
                        "application/octet-stream",
                    )
                }

                data = {
                    "metadata": json.dumps(
                        metadata,
                        ensure_ascii=False,
                    )
                }

                response = self.http_session.post(
                    url=url,
                    files=files,
                    data=data,
                    timeout=self.upload_timeout_sec,
                )

            if response.status_code not in (200, 202):
                raise RuntimeError(
                    f"AI HTTP={response.status_code}, "
                    f"body={response.text}"
                )

            body = response.json()

            if not body.get("received", False):
                raise RuntimeError(
                    f"AI response is not accepted: {body}"
                )

            self.database.mark_upload_result(
                event_id=event["event_id"],
                success=True,
            )

            LOGGER.info(
                "AI ACK image event=%s cycle=%s step=%s",
                event["event_id"],
                event["cycle_id"],
                event["step"],
            )

        except Exception as ex:
            self.database.mark_upload_result(
                event_id=event["event_id"],
                success=False,
                error_message=str(ex),
            )

            LOGGER.error(
                "AI upload failed event=%s error=%s",
                event["event_id"],
                ex,
            )

    def recover_pending_results(self) -> None:
        """
        Truy van lai ket qua AI da luu DB neu callback bi mat.

        Chi ap dung cho event dang active cua stream. HTTP 202 luc upload
        KHONG duoc dung de mo khoa: chi status PASS/FAIL/OK/NG tra ve tu
        AI Server moi doi state.
        """
        events = self.database.get_active_events_awaiting_result()

        for event in events:
            if self.stop_event.is_set():
                return

            event_id = event["event_id"]
            url = (
                f"{self.ai_base_url}/api/v1/events/"
                f"{event_id}/result"
            )

            try:
                response = self.http_session.get(
                    url,
                    timeout=self.upload_timeout_sec,
                )

                if response.status_code == 404:
                    # AI Server chua nhan duoc anh (dang retry upload)
                    # hoac phien ban cu chua co endpoint. Khong coi la
                    # STEP 1 FAIL.
                    self._warn_recovery(
                        event_id,
                        "result endpoint HTTP=404 (still waiting)",
                    )
                    continue

                if response.status_code != 200:
                    self._warn_recovery(
                        event_id,
                        f"result HTTP={response.status_code}",
                    )
                    continue

                body = response.json()

                if not body.get("found"):
                    self._warn_recovery(
                        event_id,
                        "AI Server does not know event yet",
                    )
                    continue

                status = body.get("status")
                error_code = body.get("error_code") or ""

                if not status:
                    # Dang xu ly: giu nguyen WAIT_* va tiep tuc poll.
                    continue

                result = self.database.complete_active_event(
                    event_id=event_id,
                    result_status=status,
                    error_code=error_code,
                    processing_error=bool(
                        body.get("processing_error")
                    ),
                )

                LOGGER.warning(
                    "Recovered AI result by polling event=%s status=%s "
                    "error=%s action=%s",
                    event_id,
                    status,
                    error_code,
                    result["action"],
                )

                if result.get("next_event"):
                    next_event = result["next_event"]
                    LOGGER.info(
                        "Recovered result unlocked next image "
                        "event=%s cycle=%s step=%s",
                        next_event["event_id"],
                        next_event["cycle_id"],
                        next_event["step"],
                    )

            except Exception as ex:
                self._warn_recovery(
                    event_id,
                    f"result poll failed: {ex}",
                )

    def _warn_recovery(self, event_id: str, message: str) -> None:
        """Cảnh báo có throttle để không spam log mỗi 2 giây."""
        now = time.time()
        last = self._recovery_warnings.get(event_id, 0.0)

        if now - last < 60.0:
            return

        self._recovery_warnings[event_id] = now
        LOGGER.warning(
            "Waiting AI result event=%s: %s (retry, NOT Step 1 FAIL)",
            event_id,
            message,
        )

    def upload_pending_aborts(self) -> None:
        aborts = self.database.get_pending_aborts()

        for abort in aborts:
            if self.stop_event.is_set():
                return

            self.upload_one_abort(abort)

    def upload_one_abort(self, abort: dict) -> None:
        url = f"{self.ai_base_url}/api/v1/cycles/abort"

        payload = {
            "cycle_id": abort["cycle_id"],
            "camera_side": abort["camera_side"],
            "tube_type": abort["tube_type"],
            "error_code": abort["error_code"],
        }

        try:
            response = self.http_session.post(
                url=url,
                json=payload,
                timeout=self.upload_timeout_sec,
            )

            if response.status_code not in (200, 201, 202):
                raise RuntimeError(
                    f"AI abort HTTP={response.status_code}, "
                    f"body={response.text}"
                )

            body = response.json()

            if not body.get("accepted", False):
                raise RuntimeError(
                    f"AI abort not accepted: {body}"
                )

            self.database.mark_abort_result(
                cycle_id=abort["cycle_id"],
                success=True,
            )

            LOGGER.warning(
                "AI ACK abort cycle=%s error=%s",
                abort["cycle_id"],
                abort["error_code"],
            )

        except Exception as ex:
            self.database.mark_abort_result(
                cycle_id=abort["cycle_id"],
                success=False,
                error_message=str(ex),
            )

            LOGGER.error(
                "AI abort upload failed cycle=%s error=%s",
                abort["cycle_id"],
                ex,
            )

    # --------------------------------------------------------
    # MANUAL RESET
    # --------------------------------------------------------

    def reset_sequence(self, camera_side: str) -> dict:
        aborts = self.database.reset_stream(camera_side)

        # Anh da nhan nhung chua gan step van duoc giu lai; sau reset
        # (neu co) chung duoc gan lai thanh Step 1 cua cycle moi.
        assigned = self.database.dispatch_all_pending(
            auto_recover_out_of_sync=self.auto_recover_out_of_sync
        )

        LOGGER.warning(
            "Manual sequence reset side=%s aborted_cycles=%s "
            "queued_assigned=%s",
            camera_side,
            aborts,
            len(assigned),
        )

        return {
            "camera_side": camera_side,
            "reset": True,
            "aborted_cycles": aborts,
            "assigned_after_reset": assigned,
            "message": (
                "Sequence reset complete. "
                "Queued/new images are treated as Step 1."
            ),
        }

    def apply_inspection_result(
        self,
        event_id: str,
        result_status: str,
        error_code: str = "",
        processing_error: bool = False,
    ) -> dict:
        """Ap dung ket qua AI (callback hoac poll lai) mot cach idempotent."""
        result = self.database.complete_active_event(
            event_id=event_id,
            result_status=result_status,
            error_code=error_code,
            processing_error=processing_error,
        )

        action = result.get("action")

        if action == "APPLIED":
            LOGGER.info(
                "AI result applied event=%s status=%s error=%s "
                "stream_state=%s",
                event_id,
                result_status,
                error_code,
                result.get("stream_state"),
            )
        else:
            LOGGER.warning(
                "AI result NOT applied event=%s status=%s action=%s",
                event_id,
                result_status,
                action,
            )

        next_event = result.get("next_event")

        if next_event:
            LOGGER.info(
                "Next queued image assigned after result event=%s "
                "cycle=%s step=%s",
                next_event["event_id"],
                next_event["cycle_id"],
                next_event["step"],
            )

        return result

    def handle_step1_fail(
        self,
        cycle_id: str,
        camera_side: str,
        error_code: str,
    ) -> None:
        """
        Tuong thich webhook cu. Khong con cancel event/anh nao.

        Chi dong cycle dang active cua stream tuong ung (neu con) de anh
        ke tiep trong hang doi duoc xu ly nhu Step 1 cua cycle moi.
        """
        target_stream = None

        for stream in self.database.get_streams():
            if (
                stream["camera_side"] == camera_side
                and stream["current_cycle_id"] == cycle_id
            ):
                target_stream = stream
                break

        if (
            target_stream is None
            or not target_stream.get("active_event_id")
        ):
            LOGGER.warning(
                "Step 1 FAIL ignored (cycle not active) cycle=%s side=%s",
                cycle_id,
                camera_side,
            )
            return

        self.apply_inspection_result(
            event_id=target_stream["active_event_id"],
            result_status="FAIL",
            error_code=error_code,
        )

    def get_health(self) -> dict:
        summary = self.database.get_status_summary()

        with self.candidate_lock:
            candidate_count = len(self.candidates)

        return {
            "status": "ok",
            "agent_id": self.agent_id,
            "root_path": str(self.root_path),
            "ai_server": self.ai_base_url,
            "candidate_files": candidate_count,
            "streams": self.database.get_streams(),
            **summary,
        }


# ============================================================
# FASTAPI LOCAL CONTROL API
# ============================================================

camera_agent: Optional[CameraAgent] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global camera_agent

    if CONFIG is None:
        raise RuntimeError(
            "Thiếu camera_agent_config.yaml (standalone mode). "
            "Trong merged mode, app này được main.py mount và "
            "CameraAgent do main.py khởi động/dừng."
        )

    camera_agent = CameraAgent(CONFIG)
    camera_agent.start()

    yield

    if camera_agent:
        camera_agent.stop()


app = FastAPI(
    title="Camera Capture Agent",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    if camera_agent is None:
        raise HTTPException(
            status_code=503,
            detail="Camera Agent is not started",
        )

    return camera_agent.get_health()


@app.get("/api/v1/streams")
def get_streams():
    if camera_agent is None:
        raise HTTPException(
            status_code=503,
            detail="Camera Agent is not started",
        )

    return {
        "streams": camera_agent.database.get_streams()
    }


@app.post("/api/v1/reset/{camera_side}")
def reset_sequence(camera_side: str):
    if camera_agent is None:
        raise HTTPException(
            status_code=503,
            detail="Camera Agent is not started",
        )

    camera_side = camera_side.upper()

    if camera_side not in ("L", "R"):
        raise HTTPException(
            status_code=400,
            detail="camera_side must be L or R",
        )

    return camera_agent.reset_sequence(camera_side)


@app.post("/api/v1/inspection-result")
def inspection_result(payload: dict):
    if camera_agent is None:
        raise HTTPException(
            status_code=503,
            detail="Camera Agent is not started",
        )

    event_id = payload.get("event_id")
    step = payload.get("step")
    error_code = payload.get("error_code") or ""
    processing_error = bool(payload.get("processing_error", False))

    if not event_id:
        return {"accepted": False, "action": "missing_event_id"}

    try:
        step = int(step)
    except (TypeError, ValueError):
        return {"accepted": False, "action": "invalid_step"}

    status = payload.get("status")

    if status is None:
        passed = payload.get("passed", True)

        if step == 1:
            status = "PASS" if passed else "FAIL"
        elif step == 3:
            status = "OK" if passed else "NG"
        else:
            return {"accepted": False, "action": "invalid_step"}

    status = str(status).upper()

    allowed = ("PASS", "FAIL") if step == 1 else ("OK", "NG")

    if step not in (1, 3) or status not in allowed:
        return {
            "accepted": False,
            "action": "invalid_step_or_status",
            "step": step,
            "status": status,
        }

    result = camera_agent.apply_inspection_result(
        event_id=str(event_id),
        result_status=status,
        error_code=str(error_code),
        processing_error=processing_error,
    )

    return {"accepted": True, **result}


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    if CONFIG is None:
        print(
            "Khong tim thay camera_agent_config.yaml. "
            "Chay standalone can file nay, hoac chay qua main.py (merged mode)."
        )
        sys.exit(1)

    uvicorn.run(
        app,
        host=CONFIG["local_api"]["host"],
        port=int(CONFIG["local_api"]["port"]),
        workers=1,
        reload=False,
    )
