from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import re
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
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


from datetime import datetime


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
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS meta (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS streams (
                        camera_side TEXT PRIMARY KEY,

                        active_no TEXT,
                        next_step INTEGER NOT NULL DEFAULT 1,

                        cycle_number INTEGER NOT NULL DEFAULT 0,
                        current_cycle_id TEXT,

                        state TEXT NOT NULL DEFAULT 'OUT_OF_SYNC',

                        last_event_unix REAL,
                        last_file_path TEXT,
                        last_capture_timestamp TEXT,

                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
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

                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        uploaded_at TEXT
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

                    CREATE INDEX IF NOT EXISTS idx_aborts_status
                    ON abort_requests(status);
                    """
                )

                for side in ("L", "R"):
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO streams (
                            camera_side,
                            state,
                            next_step,
                            cycle_number
                        )
                        VALUES (?, 'OUT_OF_SYNC', 1, 0)
                        """,
                        (side,),
                    )

                conn.commit()

            finally:
                conn.close()

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
                    ORDER BY camera_side
                    """
                ).fetchall()

                return [dict(row) for row in rows]

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

    def assign_file_to_sequence(
        self,
        file_info: CaptureFileInfo,
        auto_recover_out_of_sync: bool = True,
    ) -> dict:
        """
        Gán file mới vào Step 1 / Step 2 / Step 3.

        Return:
        {
            "status": "ASSIGNED" | "BLOCKED" | "ALREADY_SEEN",
            "event": {...} | None,
            "abort": {...} | None,
            "message": "..."
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
                        FROM seen_files
                        WHERE file_path = ?
                        """,
                        (file_info.path,),
                    ).fetchone()

                if seen:
                    conn.rollback()

                    return {
                        "status": "ALREADY_SEEN",
                        "event": None,
                        "abort": None,
                        "message": "File already handled",
                    }

                stream = conn.execute(
                    """
                    SELECT *
                    FROM streams
                    WHERE camera_side = ?
                    """,
                    (file_info.camera_side,),
                ).fetchone()

                if stream is None:
                    raise RuntimeError(
                        f"Stream not found: {file_info.camera_side}"
                    )

                # Khi đang OUT_OF_SYNC, mặc định không được tự suy luận ảnh mới.
                if stream["state"] != "RUNNING":
                    if not auto_recover_out_of_sync:
                        conn.execute(
                            """
                            INSERT INTO seen_files (
                                file_path,
                                status
                            )
                            VALUES (?, 'BLOCKED_OUT_OF_SYNC')
                            """,
                            (file_info.path,),
                        )

                        conn.commit()

                        return {
                            "status": "BLOCKED",
                            "event": None,
                            "abort": None,
                            "message": (
                                f"Camera {file_info.camera_side} is OUT_OF_SYNC. "
                                f"Manual reset required."
                            ),
                        }

                    LOGGER.warning(
                        "Auto-recovered OUT_OF_SYNC side=%s now at step=1 "
                        "cycle_number=%s",
                        file_info.camera_side,
                        stream["cycle_number"],
                    )

                    # Reset stream để ảnh hiện tại được xem là Step 1
                    # của cycle mới, giữ nguyên cycle_number.
                    conn.execute(
                        """
                        UPDATE streams
                        SET
                            active_no = NULL,
                            next_step = 1,
                            current_cycle_id = NULL,
                            state = 'RUNNING',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE camera_side = ?
                        """,
                        (file_info.camera_side,),
                    )

                    stream = conn.execute(
                        """
                        SELECT *
                        FROM streams
                        WHERE camera_side = ?
                        """,
                        (file_info.camera_side,),
                    ).fetchone()

                    if stream is None:
                        raise RuntimeError(
                            f"Stream not found: {file_info.camera_side}"
                        )

                next_step = int(stream["next_step"])
                active_no = stream["active_no"]
                current_cycle_id = stream["current_cycle_id"]
                cycle_number = int(stream["cycle_number"])

                abort_data = None

                # Nếu đang giữa Step 1/2/3 mà No bị thay đổi:
                # sequence không còn tin cậy.
                if next_step != 1 and active_no != file_info.tube_type:
                    if current_cycle_id:
                        error_code = "E201_SEQUENCE_INTERRUPTED_NO_CHANGED"

                        self.queue_abort(
                            conn=conn,
                            cycle_id=current_cycle_id,
                            camera_side=file_info.camera_side,
                            tube_type=active_no,
                            error_code=error_code,
                        )

                        abort_data = {
                            "cycle_id": current_cycle_id,
                            "camera_side": file_info.camera_side,
                            "tube_type": active_no,
                            "error_code": error_code,
                        }

                    conn.execute(
                        """
                        UPDATE streams
                        SET
                            active_no = NULL,
                            next_step = 1,
                            current_cycle_id = NULL,
                            state = 'OUT_OF_SYNC',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE camera_side = ?
                        """,
                        (file_info.camera_side,),
                    )

                    conn.execute(
                        """
                        INSERT INTO seen_files (
                            file_path,
                            status
                        )
                        VALUES (?, 'BLOCKED_SEQUENCE_INTERRUPTED')
                        """,
                        (file_info.path,),
                    )

                    conn.commit()

                    return {
                        "status": "BLOCKED",
                        "event": None,
                        "abort": abort_data,
                        "message": (
                            f"No changed during incomplete cycle. "
                            f"Camera {file_info.camera_side} is OUT_OF_SYNC."
                        ),
                    }

                # Step 1 luôn bắt đầu cycle mới.
                if next_step == 1:
                    cycle_number += 1

                    cycle_id = (
                        f"{self.agent_id}-"
                        f"{file_info.camera_side}-"
                        f"{cycle_number:09d}"
                    )

                    active_no = file_info.tube_type
                    assigned_step = 1

                else:
                    cycle_id = current_cycle_id
                    assigned_step = next_step

                if not cycle_id:
                    raise RuntimeError(
                        "Invalid sequence state: current cycle_id is empty"
                    )

                event_id = f"{cycle_id}-S{assigned_step}"

                if assigned_step == 2:
                    upload_status = "SKIPPED_STEP2"
                else:
                    upload_status = "PENDING_UPLOAD"

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
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_id,
                        cycle_id,
                        file_info.path,

                        # Chỉ lưu thông tin audit, không parse, không dùng logic.
                        Path(file_info.path).name,

                        file_info.camera_side,
                        file_info.tube_type,
                        assigned_step,

                        # Cột cũ capture_timestamp,
                        # nhưng lưu observed_at do Agent tạo.
                        file_info.observed_at,

                        upload_status,
                    ),
                )

                if assigned_step == 1:
                    new_next_step = 2
                    new_current_cycle_id = cycle_id
                    new_active_no = active_no

                elif assigned_step == 2:
                    new_next_step = 3
                    new_current_cycle_id = cycle_id
                    new_active_no = active_no

                else:
                    # Step 3 hoàn tất cycle.
                    new_next_step = 1
                    new_current_cycle_id = None
                    new_active_no = None

                conn.execute(
                    """
                    UPDATE streams
                    SET
                        active_no = ?,
                        next_step = ?,
                        cycle_number = ?,
                        current_cycle_id = ?,
                        state = 'RUNNING',
                        last_event_unix = ?,
                        last_file_path = ?,
                        last_capture_timestamp = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE camera_side = ?
                    """,
                    (
                        new_active_no,
                        new_next_step,
                        cycle_number,
                        new_current_cycle_id,
                        time.time(),
                        file_info.path,
                        file_info.observed_at,
                        file_info.camera_side,
                    ),
                )

                conn.commit()

                return {
                    "status": "ASSIGNED",
                    "event": {
                        "event_id": event_id,
                        "cycle_id": cycle_id,
                        "file_path": file_info.path,
                        "file_name": Path(file_info.path).name,
                        "camera_side": file_info.camera_side,
                        "tube_type": file_info.tube_type,
                        "step": assigned_step,
                        "capture_timestamp": file_info.observed_at,
                        "upload_status": upload_status,
                    },
                    "abort": None,
                    "message": "File assigned to sequence",
                }

            except Exception:
                conn.rollback()
                raise

            finally:
                conn.close()

    def warn_incomplete_streams(
        self,
        warn_sec: float,
    ) -> list[dict]:
        """
        Chỉ cảnh báo (không thay đổi trạng thái) các stream
        đang chờ Step 2/3 quá thời gian cho phép.
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
                    WHERE state = 'RUNNING'
                    AND next_step IN (2, 3)
                    AND last_event_unix IS NOT NULL
                    AND last_event_unix < ?
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
        Nếu đang chờ Step 2 hoặc Step 3 quá lâu:
        - Cycle cũ bị abort.
        - Stream chuyển sang OUT_OF_SYNC.
        - Kỹ thuật viên cần reset thủ công.
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
                    WHERE state = 'RUNNING'
                    AND next_step IN (2, 3)
                    AND last_event_unix IS NOT NULL
                    AND last_event_unix < ?
                    """,
                    (deadline,),
                ).fetchall()

                for row in rows:
                    cycle_id = row["current_cycle_id"]
                    active_no = row["active_no"]
                    side = row["camera_side"]

                    if cycle_id and active_no:
                        error_code = "E204_SEQUENCE_TIMEOUT"

                        self.queue_abort(
                            conn=conn,
                            cycle_id=cycle_id,
                            camera_side=side,
                            tube_type=active_no,
                            error_code=error_code,
                        )

                        aborts.append(
                            {
                                "cycle_id": cycle_id,
                                "camera_side": side,
                                "tube_type": active_no,
                                "error_code": error_code,
                            }
                        )

                    conn.execute(
                        """
                        UPDATE streams
                        SET
                            active_no = NULL,
                            next_step = 1,
                            current_cycle_id = NULL,
                            state = 'OUT_OF_SYNC',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE camera_side = ?
                        """,
                        (side,),
                    )

                conn.commit()
                return aborts

            except Exception:
                conn.rollback()
                raise

            finally:
                conn.close()

    def reset_stream(self, camera_side: str) -> Optional[dict]:
        """
        Reset thủ công do kỹ thuật viên thực hiện.
        Ảnh tiếp theo sau reset sẽ được xem là Step 1.
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
                    """,
                    (camera_side,),
                ).fetchone()

                if stream is None:
                    raise RuntimeError(
                        f"Stream not found: {camera_side}"
                    )

                abort_data = None

                if (
                    stream["current_cycle_id"]
                    and stream["active_no"]
                    and int(stream["next_step"]) != 1
                ):
                    error_code = "E601_MANUAL_SEQUENCE_RESET"

                    self.queue_abort(
                        conn=conn,
                        cycle_id=stream["current_cycle_id"],
                        camera_side=camera_side,
                        tube_type=stream["active_no"],
                        error_code=error_code,
                    )

                    abort_data = {
                        "cycle_id": stream["current_cycle_id"],
                        "camera_side": camera_side,
                        "tube_type": stream["active_no"],
                        "error_code": error_code,
                    }

                conn.execute(
                    """
                    UPDATE streams
                    SET
                        active_no = NULL,
                        next_step = 1,
                        current_cycle_id = NULL,
                        state = 'RUNNING',
                        last_event_unix = NULL,
                        last_file_path = NULL,
                        last_capture_timestamp = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE camera_side = ?
                    """,
                    (camera_side,),
                )

                conn.commit()
                return abort_data

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
                    """,
                    (camera_side,),
                ).fetchone()

                if stream is None:
                    conn.rollback()
                    return None

                if stream["current_cycle_id"] != expected_cycle_id:
                    conn.rollback()
                    return None

                conn.execute(
                    """
                    UPDATE streams
                    SET
                        active_no = NULL,
                        next_step = 1,
                        current_cycle_id = NULL,
                        state = 'RUNNING',
                        last_event_unix = NULL,
                        last_file_path = NULL,
                        last_capture_timestamp = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE camera_side = ?
                    """,
                    (camera_side,),
                )

                conn.commit()
                return dict(stream)

            except Exception:
                conn.rollback()
                raise

            finally:
                conn.close()

    def cancel_cycle_events(self, cycle_id: str) -> int:
        with self.lock:
            conn = self.connect()
            try:
                cursor = conn.execute(
                    """
                    UPDATE events
                    SET upload_status = 'CANCELLED'
                    WHERE cycle_id = ?
                    AND upload_status IN ('PENDING_UPLOAD', 'RETRY')
                    """,
                    (cycle_id,),
                )
                conn.commit()
                return cursor.rowcount
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

                return {
                    "pending_uploads": upload_count,
                    "pending_aborts": abort_count,
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
        self.file_worker_thread: Optional[threading.Thread] = None
        self.reconcile_thread: Optional[threading.Thread] = None
        self.upload_thread: Optional[threading.Thread] = None
        self.timeout_thread: Optional[threading.Thread] = None

        self.http_session = requests.Session()

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

    def start(self) -> None:
        self.connect_network_share()

        if not self.root_path.exists():
            raise RuntimeError(
                f"Root path not found: {self.root_path}. Check "
                "host, share name, credentials, firewall and "
                "SMB port 445."
            )

        self.bootstrap_if_needed()
        self.auto_reset_streams()

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

    def stop(self) -> None:
        self.stop_event.set()

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

    def bootstrap_if_needed(self) -> None:
        initialized = self.database.get_meta(
            "bootstrap_initialized"
        )

        if initialized == "true":
            return

        mode = self.config["agent"]["bootstrap_mode"]

        all_files = []

        for path in self.iter_image_files():
            if detect_image_format(path) is not None:
                all_files.append(str(path))

        if mode == "start_from_now":
            self.database.bootstrap_files(all_files)

            LOGGER.warning(
                "Bootstrap completed. Existing files=%s were ignored. "
                "Both L/R streams will be auto-reset on startup.",
                len(all_files),
            )

        else:
            LOGGER.warning(
                "Unsupported bootstrap mode=%s. "
                "Use start_from_now for safe industrial startup.",
                mode,
            )
            self.database.bootstrap_files(all_files)

        self.database.set_meta(
            "bootstrap_initialized",
            "true"
        )

    def auto_reset_streams(self) -> None:
        for side in ("L", "R"):
            abort = self.database.reset_stream(side)
            if abort:
                LOGGER.warning(
                    "Auto-reset on startup side=%s aborted_cycle=%s",
                    side,
                    abort,
                )
            else:
                LOGGER.info(
                    "Auto-reset on startup side=%s (no active cycle)",
                    side,
                )

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

            event = result["event"]

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
                        "cycle=%s step=%s idle_sec=%.1f",
                        stream["camera_side"],
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
        abort = self.database.reset_stream(camera_side)

        LOGGER.warning(
            "Manual sequence reset side=%s abort=%s",
            camera_side,
            abort,
        )

        return {
            "camera_side": camera_side,
            "reset": True,
            "aborted_cycle": abort,
            "message": (
                "Sequence reset complete. "
                "The next new image will be treated as Step 1."
            ),
        }

    def handle_step1_fail(self, cycle_id: str, camera_side: str, error_code: str) -> None:
        # 1. Cancel pending events (S2/S3) of the failed cycle
        self.database.cancel_cycle_events(cycle_id)

        # 2. Reset stream only if this cycle is still the active one
        stream = self.database.reset_stream_if_cycle(
            camera_side,
            cycle_id,
        )

        LOGGER.warning(
            "Step 1 FAIL handled cycle=%s error=%s side=%s events_cancelled=%s stream_reset=%s",
            cycle_id,
            error_code,
            camera_side,
            True,
            stream is not None,
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

    cycle_id = payload.get("cycle_id")
    step = payload.get("step")
    passed = payload.get("passed", True)
    camera_side = payload.get("camera_side")
    error_code = payload.get("error_code", "")

    if step != 1:
        return {"accepted": True, "action": "ignored_not_step1"}

    if passed:
        return {"accepted": True, "action": "ignored_passed"}

    camera_agent.handle_step1_fail(
        cycle_id=cycle_id,
        camera_side=camera_side,
        error_code=error_code,
    )

    return {"accepted": True, "action": "cycle_aborted_stream_reset"}


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