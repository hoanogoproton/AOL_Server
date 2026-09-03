import shutil
import csv
import hashlib
import json
import logging
import os
import queue
import re
import requests
import sqlite3
import threading
import time
import traceback
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional

import cv2
import serial
import yaml
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator
from ultralytics import YOLO

# MERGED MODE: Camera Agent chạy chung process với AI Server.
import camera_agent as camera_agent_module


# ============================================================
# CONFIGURATION
# ============================================================

CONFIG_PATH = os.environ.get("AI_CONFIG", "config.yaml")


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


CONFIG = load_config(CONFIG_PATH)


def setup_logging() -> logging.Logger:
    log_dir = Path(CONFIG["storage"]["base_dir"]) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("AIInspection")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(threadName)s | %(message)s"
    )

    file_handler = logging.FileHandler(
        log_dir / "inspection_server.log",
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger


LOGGER = setup_logging()


# ============================================================
# HELPERS
# ============================================================

SAFE_COMPONENT_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


def safe_component(value: str, field_name: str) -> str:
    if not value or not SAFE_COMPONENT_PATTERN.match(value):
        raise ValueError(
            f"{field_name} chỉ được chứa A-Z, a-z, 0-9, _, -, ."
        )
    return value


def utc_now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="milliseconds") + "Z"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()

    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)

    return h.hexdigest()


def crc16_modbus(data: bytes) -> int:
    crc = 0xFFFF

    for byte in data:
        crc ^= byte

        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1

    return crc & 0xFFFF


def detect_image_suffix(header: bytes) -> str:
    if header.startswith(b"\xFF\xD8\xFF"):
        return ".jpg"

    if header.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"

    if header.startswith(b"BM"):
        return ".bmp"

    raise ValueError("Unsupported or invalid image format")


# ============================================================
# DATA MODELS
# ============================================================

class ImageMetadata(BaseModel):
    event_id: str = Field(..., description="Mã duy nhất cho một ảnh")
    cycle_id: str = Field(..., description="Mã cycle chung Step 1 và Step 3")
    camera_side: Literal["L", "R"]
    tube_type: str
    step: Literal[1, 3]
    observed_at: str

    @field_validator("event_id", "cycle_id", "tube_type")
    @classmethod
    def validate_safe_component(cls, value: str) -> str:
        return safe_component(value, "metadata value")


class AbortCycleRequest(BaseModel):
    cycle_id: str
    camera_side: Literal["L", "R"]
    tube_type: str
    error_code: str

    @field_validator("cycle_id", "tube_type", "error_code")
    @classmethod
    def validate_safe_component(cls, value: str) -> str:
        return safe_component(value, "abort value")


@dataclass
class RoiRule:
    inspection_step: str
    roi_id: str
    model_name: str
    class_id: int
    class_name: str
    x_min: float
    y_min: float
    x_max: float
    y_max: float
    confidence: float
    check_mode: str
    min_count: int
    max_count: int


@dataclass
class Detection:
    class_id: int
    confidence: float
    x1: float
    y1: float
    x2: float
    y2: float

    @property
    def center_x(self) -> float:
        return (self.x1 + self.x2) / 2.0

    @property
    def center_y(self) -> float:
        return (self.y1 + self.y2) / 2.0


# ============================================================
# ROI RULE ENGINE
# ============================================================

class RoiRuleEngine:
    def __init__(self, csv_path: str):
        self.rules: list[RoiRule] = []
        self.load_rules(csv_path)

    @staticmethod
    def normalize_model_name(model_name: str) -> str:
        normalized = (model_name or "").strip().lower()
        aliases = {
            "aol": "best1",
            "best1": "best1",
            "aol_model": "best1",
        }
        return aliases.get(normalized, normalized)

    def load_rules(self, csv_path: str) -> None:
        self.rules.clear()

        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)

            for row in reader:
                self.rules.append(
                    RoiRule(
                        inspection_step=row["inspection_step"].strip(),
                        roi_id=row["roi_id"].strip(),
                        model_name=row["model_name"].strip(),
                        class_id=int(row["class_id"]),
                        class_name=row["class_name"].strip(),
                        x_min=float(row["compare_x_min"]),
                        y_min=float(row["compare_y_min"]),
                        x_max=float(row["compare_x_max"]),
                        y_max=float(row["compare_y_max"]),
                        confidence=float(row["confidence"]),
                        check_mode=row["check_mode"].strip(),
                        min_count=int(row["min_count"]),
                        max_count=int(row["max_count"]),
                    )
                )

        LOGGER.info("Loaded %s ROI rules from %s", len(self.rules), csv_path)

    def get_rules(self, inspection_step: str, model_name: str) -> list[RoiRule]:
        target_name = self.normalize_model_name(model_name)

        return [
            rule
            for rule in self.rules
            if rule.inspection_step == inspection_step
            and self.normalize_model_name(rule.model_name) == target_name
        ]

    @staticmethod
    def detection_inside_roi(det: Detection, rule: RoiRule) -> bool:
        # Quy tắc: tâm bounding box nằm trong ROI.
        return (
            rule.x_min <= det.center_x <= rule.x_max
            and rule.y_min <= det.center_y <= rule.y_max
        )

    def evaluate(
        self,
        inspection_step: str,
        model_name: str,
        detections: list[Detection],
    ) -> dict:
        rules = self.get_rules(inspection_step, model_name)

        if not rules:
            raise RuntimeError(
                f"Không tìm thấy ROI rule cho step={inspection_step}, "
                f"model={model_name}"
            )

        roi_results = []
        all_passed = True

        for rule in rules:
            matched = []

            for det in detections:
                if det.class_id != rule.class_id:
                    continue

                if det.confidence < rule.confidence:
                    continue

                if self.detection_inside_roi(det, rule):
                    matched.append(det)

            count = len(matched)

            passed = rule.min_count <= count <= rule.max_count
            if not passed:
                all_passed = False

            roi_results.append(
                {
                    "roi_id": rule.roi_id,
                    "class_id": rule.class_id,
                    "class_name": rule.class_name,
                    "count": count,
                    "min_count": rule.min_count,
                    "max_count": rule.max_count,
                    "passed": passed,
                    "confidence_threshold": rule.confidence,
                    "roi": {
                        "x_min": rule.x_min,
                        "y_min": rule.y_min,
                        "x_max": rule.x_max,
                        "y_max": rule.y_max,
                    },
                }
            )

        return {
            "inspection_step": inspection_step,
            "model_name": model_name,
            "passed": all_passed,
            "roi_results": roi_results,
        }


# ============================================================
# SQLITE DATABASE
# ============================================================

class InspectionDatabase:
    def __init__(self, database_path: str):
        self.database_path = database_path
        self.lock = threading.Lock()

        Path(database_path).parent.mkdir(parents=True, exist_ok=True)
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
                    CREATE TABLE IF NOT EXISTS cycles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        cycle_id TEXT UNIQUE NOT NULL,
                        camera_side TEXT NOT NULL,
                        tube_type TEXT NOT NULL,

                        step1_status TEXT,
                        step1_error TEXT,

                        step3_status TEXT,
                        step3_error TEXT,

                        final_result TEXT,
                        final_error TEXT,

                        com_status TEXT NOT NULL DEFAULT 'NONE',
                        com_updated_at TEXT,

                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS images (
                        event_id TEXT PRIMARY KEY,
                        cycle_id TEXT NOT NULL,
                        camera_side TEXT NOT NULL,
                        tube_type TEXT NOT NULL,
                        step INTEGER NOT NULL,
                        capture_timestamp TEXT NOT NULL,

                        image_path TEXT NOT NULL,
                        image_sha256 TEXT NOT NULL,
                        annotated_path TEXT,

                        image_status TEXT NOT NULL DEFAULT 'RECEIVED',
                        result_json TEXT,
                        error_code TEXT,

                        received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        processed_at TEXT
                    );

                    CREATE INDEX IF NOT EXISTS idx_images_cycle
                    ON images(cycle_id);

                    CREATE INDEX IF NOT EXISTS idx_images_status
                    ON images(image_status);

                    CREATE INDEX IF NOT EXISTS idx_cycles_com
                    ON cycles(com_status);
                    """
                )
                conn.commit()

            finally:
                conn.close()

    def insert_image(
        self,
        metadata: ImageMetadata,
        image_path: str,
        image_sha256: str,
    ) -> bool:
        with self.lock:
            conn = self.connect()

            try:
                conn.execute("BEGIN IMMEDIATE")

                conn.execute(
                    """
                    INSERT OR IGNORE INTO cycles (
                        cycle_id, camera_side, tube_type
                    )
                    VALUES (?, ?, ?)
                    """,
                    (
                        metadata.cycle_id,
                        metadata.camera_side,
                        metadata.tube_type,
                    ),
                )

                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO images (
                        event_id,
                        cycle_id,
                        camera_side,
                        tube_type,
                        step,
                        capture_timestamp,
                        image_path,
                        image_sha256,
                        image_status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'RECEIVED')
                    """,
                    (
                        metadata.event_id,
                        metadata.cycle_id,
                        metadata.camera_side,
                        metadata.tube_type,
                        metadata.step,

                        metadata.observed_at,

                        image_path,
                        image_sha256,
                    ),
                )

                inserted = cursor.rowcount == 1
                conn.commit()
                return inserted

            except Exception:
                conn.rollback()
                raise

            finally:
                conn.close()

    def get_image(self, event_id: str) -> Optional[dict]:
        with self.lock:
            conn = self.connect()

            try:
                row = conn.execute(
                    "SELECT * FROM images WHERE event_id = ?",
                    (event_id,),
                ).fetchone()

                return dict(row) if row else None

            finally:
                conn.close()

    def reset_processing_images(self) -> list[str]:
        with self.lock:
            conn = self.connect()

            try:
                conn.execute(
                    """
                    UPDATE images
                    SET image_status = 'RECEIVED'
                    WHERE image_status = 'PROCESSING'
                    """
                )

                rows = conn.execute(
                    """
                    SELECT event_id
                    FROM images
                    WHERE image_status = 'RECEIVED'
                    ORDER BY received_at ASC
                    """
                ).fetchall()

                conn.commit()
                return [row["event_id"] for row in rows]

            finally:
                conn.close()

    def mark_image_processing(self, event_id: str) -> None:
        with self.lock:
            conn = self.connect()

            try:
                conn.execute(
                    """
                    UPDATE images
                    SET image_status = 'PROCESSING'
                    WHERE event_id = ?
                    """,
                    (event_id,),
                )
                conn.commit()

            finally:
                conn.close()

    def update_image_result(
        self,
        event_id: str,
        image_status: str,
        result_json: dict,
        annotated_path: Optional[str],
        error_code: Optional[str],
    ) -> None:
        with self.lock:
            conn = self.connect()

            try:
                conn.execute(
                    """
                    UPDATE images
                    SET
                        image_status = ?,
                        result_json = ?,
                        annotated_path = ?,
                        error_code = ?,
                        processed_at = CURRENT_TIMESTAMP
                    WHERE event_id = ?
                    """,
                    (
                        image_status,
                        json.dumps(result_json, ensure_ascii=False),
                        annotated_path,
                        error_code,
                        event_id,
                    ),
                )
                conn.commit()

            finally:
                conn.close()

    def set_cycle_step_result(
        self,
        cycle_id: str,
        step: int,
        status: str,
        error_code: str,
    ) -> None:
        if step == 1:
            status_column = "step1_status"
            error_column = "step1_error"
        elif step == 3:
            status_column = "step3_status"
            error_column = "step3_error"
        else:
            raise ValueError("Step chỉ được là 1 hoặc 3")

        with self.lock:
            conn = self.connect()

            try:
                conn.execute(
                    f"""
                    UPDATE cycles
                    SET
                        {status_column} = ?,
                        {error_column} = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE cycle_id = ?
                    """,
                    (status, error_code, cycle_id),
                )
                conn.commit()

            finally:
                conn.close()

    def finalize_if_ready(self, cycle_id: str) -> Optional[dict]:
        """
        Chỉ final sau khi đã có kết quả cả Step 1 và Step 3.
        """
        with self.lock:
            conn = self.connect()

            try:
                conn.execute("BEGIN IMMEDIATE")

                row = conn.execute(
                    """
                    SELECT *
                    FROM cycles
                    WHERE cycle_id = ?
                    """,
                    (cycle_id,),
                ).fetchone()

                if row is None:
                    conn.rollback()
                    return None

                if row["final_result"] is not None:
                    conn.rollback()
                    return None

                # Step 1 FAIL → abort cycle ngay, không cần chờ Step 3
                step1_status = row["step1_status"]
                step3_status = row["step3_status"]

                if step1_status is not None and step1_status != "PASS":
                    final_result = "NG"
                    final_error = row["step1_error"] or "E110_PRE_GLUE_FAIL"
                    com_status = "PENDING"
                elif step1_status is None or step3_status is None:
                    conn.rollback()
                    return None
                else:
                    step1_pass = step1_status == "PASS"
                    step3_pass = step3_status == "OK"
                    if step1_pass and step3_pass:
                        final_result = "OK"
                        final_error = "NONE"
                    else:
                        final_result = "NG"
                        if not step1_pass:
                            final_error = row["step1_error"] or "E110_PRE_GLUE_FAIL"
                        else:
                            final_error = row["step3_error"] or "E300_FINAL_INSPECTION_FAIL"
                    com_status = "PENDING"

                cursor = conn.execute(
                    """
                    UPDATE cycles
                    SET
                        final_result = ?,
                        final_error = ?,
                        com_status = ?,
                        com_updated_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE cycle_id = ?
                    AND final_result IS NULL
                    """,
                    (
                        final_result,
                        final_error,
                        com_status,
                        cycle_id,
                    ),
                )

                if cursor.rowcount != 1:
                    conn.rollback()
                    return None

                updated = conn.execute(
                    """
                    SELECT *
                    FROM cycles
                    WHERE cycle_id = ?
                    """,
                    (cycle_id,),
                ).fetchone()

                conn.commit()
                return dict(updated)

            except Exception:
                conn.rollback()
                raise

            finally:
                conn.close()

    def delete_cycle(self, cycle_id: str) -> None:
        """
        Xóa luôn cycle khi Step 1 FAIL để ảnh tiếp theo là cycle mới.
        """
        with self.lock:
            conn = self.connect()

            try:
                conn.execute("BEGIN IMMEDIATE")

                conn.execute(
                    """
                    DELETE FROM cycles
                    WHERE cycle_id = ?
                    """,
                    (cycle_id,),
                )

                conn.commit()

            except Exception:
                conn.rollback()
                raise

            finally:
                conn.close()

    def timeout_cycles(self, timeout_seconds: int) -> list[dict]:
        """
        Nếu đã nhận Step 1 nhưng quá lâu không có Step 3,
        hoặc nhận Step 3 mà thiếu Step 1, final sẽ là NG.
        """
        timeout_payloads = []

        with self.lock:
            conn = self.connect()

            try:
                conn.execute("BEGIN IMMEDIATE")

                rows = conn.execute(
                    """
                    SELECT *
                    FROM cycles
                    WHERE final_result IS NULL
                    AND (
                        julianday('now') - julianday(created_at)
                    ) * 86400 >= ?
                    """,
                    (timeout_seconds,),
                ).fetchall()

                for row in rows:
                    if row["step1_status"] is None:
                        error_code = "E203_STEP1_MISSING"
                    elif row["step3_status"] is None:
                        error_code = "E204_STEP3_MISSING"
                    else:
                        continue

                    conn.execute(
                        """
                        UPDATE cycles
                        SET
                            final_result = 'NG',
                            final_error = ?,
                            com_status = 'PENDING',
                            com_updated_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE cycle_id = ?
                        AND final_result IS NULL
                        """,
                        (error_code, row["cycle_id"]),
                    )

                    updated = conn.execute(
                        """
                        SELECT *
                        FROM cycles
                        WHERE cycle_id = ?
                        """,
                        (row["cycle_id"],),
                    ).fetchone()

                    timeout_payloads.append(dict(updated))

                conn.commit()
                return timeout_payloads

            except Exception:
                conn.rollback()
                raise

            finally:
                conn.close()

    def pending_com_results(self, retry_interval_sec: int) -> list[dict]:
        with self.lock:
            conn = self.connect()

            try:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM cycles
                    WHERE com_status IN ('PENDING', 'RETRY')
                    AND (
                        com_updated_at IS NULL
                        OR (
                            julianday('now') - julianday(com_updated_at)
                        ) * 86400 >= ?
                    )
                    ORDER BY id ASC
                    """,
                    (retry_interval_sec,),
                ).fetchall()

                return [dict(row) for row in rows]

            finally:
                conn.close()

    def update_com_status(self, cycle_id: str, com_status: str) -> None:
        with self.lock:
            conn = self.connect()

            try:
                conn.execute(
                    """
                    UPDATE cycles
                    SET
                        com_status = ?,
                        com_updated_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE cycle_id = ?
                    """,
                    (com_status, cycle_id),
                )
                conn.commit()

            finally:
                conn.close()

    def get_cycle(self, cycle_id: str) -> Optional[dict]:
        with self.lock:
            conn = self.connect()

            try:
                row = conn.execute(
                    """
                    SELECT *
                    FROM cycles
                    WHERE cycle_id = ?
                    """,
                    (cycle_id,),
                ).fetchone()

                return dict(row) if row else None

            finally:
                conn.close()

    def list_cycles(self, limit: int = 50, offset: int = 0) -> list[dict]:
        with self.lock:
            conn = self.connect()

            try:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM cycles
                    ORDER BY id DESC
                    LIMIT ? OFFSET ?
                    """,
                    (limit, offset),
                ).fetchall()

                return [dict(row) for row in rows]

            finally:
                conn.close()

    def get_images_for_cycle(self, cycle_id: str) -> list[dict]:
        with self.lock:
            conn = self.connect()

            try:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM images
                    WHERE cycle_id = ?
                    ORDER BY step ASC, received_at ASC
                    """,
                    (cycle_id,),
                ).fetchall()

                return [dict(row) for row in rows]

            finally:
                conn.close()

    def get_recent_images(self, limit: int = 20) -> list[dict]:
        with self.lock:
            conn = self.connect()

            try:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM images
                    ORDER BY received_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()

                return [dict(row) for row in rows]

            finally:
                conn.close()

    def get_latest_processed(self) -> Optional[dict]:
        with self.lock:
            conn = self.connect()

            try:
                row = conn.execute(
                    """
                    SELECT *
                    FROM images
                    WHERE image_status = 'DONE'
                    AND annotated_path IS NOT NULL
                    ORDER BY processed_at DESC
                    LIMIT 1
                    """,
                ).fetchone()

                return dict(row) if row else None

            finally:
                conn.close()

    def abort_cycle(
        self,
        cycle_id: str,
        camera_side: str,
        tube_type: str,
        error_code: str,
    ) -> tuple[dict, bool]:
        with self.lock:
            conn = self.connect()

            try:
                conn.execute("BEGIN IMMEDIATE")

                conn.execute(
                    """
                    INSERT OR IGNORE INTO cycles (
                        cycle_id,
                        camera_side,
                        tube_type
                    )
                    VALUES (?, ?, ?)
                    """,
                    (
                        cycle_id,
                        camera_side,
                        tube_type,
                    ),
                )

                cursor = conn.execute(
                    """
                    UPDATE cycles
                    SET
                        final_result = 'NG',
                        final_error = ?,
                        com_status = 'PENDING',
                        com_updated_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE cycle_id = ?
                    AND final_result IS NULL
                    """,
                    (
                        error_code,
                        cycle_id,
                    ),
                )

                row = conn.execute(
                    """
                    SELECT *
                    FROM cycles
                    WHERE cycle_id = ?
                    """,
                    (cycle_id,),
                ).fetchone()

                conn.commit()

                return dict(row), cursor.rowcount == 1

            except Exception:
                conn.rollback()
                raise

            finally:
                conn.close()

    def abort_cycle_s1_fail(self, cycle_id: str, error_code: str) -> None:
        with self.lock:
            conn = self.connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """
                    UPDATE cycles
                    SET
                        final_result = 'ABORTED',
                        final_error = ?,
                        com_status = 'ABORTED',
                        com_updated_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE cycle_id = ?
                    AND final_result IS NULL
                    """,
                    (error_code, cycle_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()


# ============================================================
# SERIAL / ARDUINO GATEWAY
# ============================================================

class ArduinoSerialGateway:
    def __init__(self, config: dict):
        self.enabled = bool(config["enabled"])
        self.port = config["port"]
        self.baudrate = int(config["baudrate"])
        self.read_timeout_sec = float(config["read_timeout_sec"])
        self.ack_timeout_sec = float(config["ack_timeout_sec"])
        self.retry_count = int(config["retry_count"])
        self.retry_interval_sec = float(config["retry_interval_sec"])

        self.serial_conn: Optional[serial.Serial] = None
        self.lock = threading.Lock()

    def ensure_open(self) -> None:
        if not self.enabled:
            return

        if self.serial_conn is not None and self.serial_conn.is_open:
            return

        LOGGER.info("Opening serial port %s", self.port)

        self.serial_conn = serial.Serial(
            port=self.port,
            baudrate=self.baudrate,
            timeout=self.read_timeout_sec,
            write_timeout=1,
        )

        time.sleep(1.5)

        try:
            self.serial_conn.reset_input_buffer()
            self.serial_conn.reset_output_buffer()
        except Exception:
            pass

    def is_connected(self) -> bool:
        return bool(
            self.serial_conn is not None
            and self.serial_conn.is_open
        )

    def build_packet(self, cycle: dict) -> tuple[str, str]:
        sequence = f"{int(cycle['id']):08d}"

        body = (
            f"RESULT,"
            f"SEQ={sequence},"
            f"CYCLE={cycle['cycle_id']},"
            f"NO={cycle['tube_type']},"
            f"SIDE={cycle['camera_side']},"
            f"RESULT={cycle['final_result']},"
            f"ERROR={cycle['final_error']}"
        )

        crc = crc16_modbus(body.encode("ascii", errors="ignore"))
        packet = f"<{body},CRC={crc:04X}>\r\n"

        return sequence, packet

    def send_result(self, cycle: dict) -> bool:
        if not self.enabled:
            LOGGER.warning(
                "Serial disabled. Simulate ACK for cycle=%s",
                cycle["cycle_id"],
            )
            return True

        sequence, packet = self.build_packet(cycle)

        with self.lock:
            for attempt in range(1, self.retry_count + 1):
                try:
                    self.ensure_open()

                    assert self.serial_conn is not None

                    try:
                        self.serial_conn.reset_input_buffer()
                    except Exception:
                        pass

                    LOGGER.info(
                        "COM send attempt=%s seq=%s packet=%s",
                        attempt,
                        sequence,
                        packet.strip(),
                    )

                    self.serial_conn.write(packet.encode("ascii"))
                    self.serial_conn.flush()

                    deadline = time.monotonic() + self.ack_timeout_sec

                    while time.monotonic() < deadline:
                        line = self.serial_conn.readline()

                        if not line:
                            continue

                        text = line.decode(
                            "ascii",
                            errors="ignore"
                        ).strip()

                        LOGGER.info("COM RX: %s", text)

                        # Arduino phải trả ví dụ:
                        # <ACK,SEQ=00000001>
                        if "ACK" in text and f"SEQ={sequence}" in text:
                            LOGGER.info(
                                "COM ACK received seq=%s",
                                sequence,
                            )
                            return True

                except Exception as ex:
                    LOGGER.error(
                        "Serial error attempt=%s: %s",
                        attempt,
                        ex,
                    )

                    try:
                        if self.serial_conn is not None:
                            self.serial_conn.close()
                    except Exception:
                        pass

                    self.serial_conn = None

                time.sleep(self.retry_interval_sec)

        LOGGER.error(
            "COM ACK timeout after retries. cycle=%s",
            cycle["cycle_id"],
        )
        return False


# ============================================================
# INSPECTION SERVICE
# ============================================================

class InspectionService:
    def __init__(self, config: dict):
        self.config = config

        self.raw_dir = Path(config["storage"]["raw_dir"])
        self.annotated_dir = Path(config["storage"]["annotated_dir"])

        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.annotated_dir.mkdir(parents=True, exist_ok=True)

        self.ng_dir = Path(config["storage"]["base_dir"]) / "ng"
        self.ng_dir.mkdir(parents=True, exist_ok=True)

        self.database = InspectionDatabase(
            config["storage"]["database_path"]
        )

        self.rule_engine = RoiRuleEngine(
            config["roi"]["csv_path"]
        )

        self.serial_gateway = ArduinoSerialGateway(
            config["serial"]
        )

        self.model: Optional[YOLO] = None

        self.job_queue: queue.Queue[str] = queue.Queue()
        self.stop_event = threading.Event()

        self.worker_thread: Optional[threading.Thread] = None
        self.monitor_thread: Optional[threading.Thread] = None

        self.com_send_lock = threading.Lock()

    def get_model_name(self) -> str:
        weights_path = str(self.config["model"]["weights"])
        return Path(weights_path).stem

    def start(self) -> None:
        LOGGER.info("Loading YOLO model...")

        self.model = YOLO(
            self.config["model"]["weights"]
        )

        LOGGER.info(
            "YOLO model loaded: %s",
            self.config["model"]["weights"],
        )

        pending_events = self.database.reset_processing_images()

        for event_id in pending_events:
            self.job_queue.put(event_id)

        self.worker_thread = threading.Thread(
            target=self.worker_loop,
            name="YOLO-Worker",
            daemon=True,
        )
        self.worker_thread.start()

        self.monitor_thread = threading.Thread(
            target=self.monitor_loop,
            name="Cycle-Monitor",
            daemon=True,
        )
        self.monitor_thread.start()

        LOGGER.info("Inspection service started")

    def stop(self) -> None:
        self.stop_event.set()

        if self.worker_thread:
            self.worker_thread.join(timeout=5)

        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)

        LOGGER.info("Inspection service stopped")

    def enqueue_event(self, event_id: str) -> None:
        self.job_queue.put(event_id)

    def worker_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                event_id = self.job_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                self.process_event(event_id)
            except Exception:
                LOGGER.exception(
                    "Unhandled worker error event=%s",
                    event_id,
                )
            finally:
                self.job_queue.task_done()

    def monitor_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                timeout_sec = int(
                    self.config["sequence"]["server_cycle_timeout_sec"]
                )

                timeout_cycles = self.database.timeout_cycles(timeout_sec)

                for cycle in timeout_cycles:
                    LOGGER.error(
                        "Cycle timeout: %s",
                        cycle["cycle_id"],
                    )
                    self.send_com_result(cycle)

                retry_interval = int(
                    self.config["system"]["com_retry_interval_sec"]
                )

                pending = self.database.pending_com_results(
                    retry_interval
                )

                for cycle in pending:
                    self.send_com_result(cycle)

            except Exception:
                LOGGER.exception("Monitor loop error")

            self.stop_event.wait(1.0)

    def infer_detections(
        self,
        image_path: str,
    ) -> tuple[list[Detection], object]:
        if self.model is None:
            raise RuntimeError("YOLO model chưa được load")

        model_cfg = self.config["model"]

        results = self.model.predict(
            source=image_path,
            conf=float(model_cfg["min_predict_confidence"]),
            iou=float(model_cfg["iou_threshold"]),
            imgsz=int(model_cfg["imgsz"]),
            device=model_cfg["device"],
            verbose=False,
        )

        result = results[0]
        detections: list[Detection] = []

        if result.boxes is not None and len(result.boxes) > 0:
            xyxy = result.boxes.xyxy.cpu().numpy()
            confs = result.boxes.conf.cpu().numpy()
            classes = result.boxes.cls.cpu().numpy()

            for box, conf, cls_id in zip(xyxy, confs, classes):
                detections.append(
                    Detection(
                        class_id=int(cls_id),
                        confidence=float(conf),
                        x1=float(box[0]),
                        y1=float(box[1]),
                        x2=float(box[2]),
                        y2=float(box[3]),
                    )
                )

        return detections, result

    def validate_image_size(self, image) -> None:
        image_cfg = self.config["image"]

        if not bool(image_cfg["reject_wrong_resolution"]):
            return

        expected_width = int(image_cfg["expected_width"])
        expected_height = int(image_cfg["expected_height"])

        height, width = image.shape[:2]

        if width != expected_width or height != expected_height:
            raise RuntimeError(
                f"E202_IMAGE_SIZE_INVALID: "
                f"actual={width}x{height}, "
                f"expected={expected_width}x{expected_height}"
            )

    def get_error_code(
        self,
        step: int,
        evaluation: dict,
    ) -> str:
        failed_rois = [
            item["roi_id"]
            for item in evaluation["roi_results"]
            if not item["passed"]
        ]

        if not failed_rois:
            return "NONE"

        if step == 1:
            return "E110_PRE_GLUE_FAIL_" + "_".join(failed_rois)

        step3_map = {
            "roi_001": "E001_PRODUCT_OK_NOT_FOUND",
            "roi_002": "E101_LIQUID_ERROR_ROI_002",
            "roi_003": "E102_LIQUID_ERROR_ROI_003",
            "roi_004": "E103_LIQUID_ERROR_ROI_004",
        }

        errors = [
            step3_map.get(roi_id, f"E300_{roi_id}")
            for roi_id in failed_rois
        ]

        return "|".join(errors)

    def create_annotated_image(
        self,
        raw_image,
        detections: list[Detection],
        evaluation: dict,
        event: dict,
    ) -> str:
        image = raw_image.copy()

        # Vẽ ROI.
        for roi_result in evaluation["roi_results"]:
            roi = roi_result["roi"]

            color = (0, 200, 0) if roi_result["passed"] else (0, 0, 255)

            x1 = int(roi["x_min"])
            y1 = int(roi["y_min"])
            x2 = int(roi["x_max"])
            y2 = int(roi["y_max"])

            cv2.rectangle(image, (x1, y1), (x2, y2), color, 2)

            text = (
                f"{roi_result['roi_id']} "
                f"count={roi_result['count']} "
                f"[{roi_result['min_count']}-{roi_result['max_count']}]"
            )

            cv2.putText(
                image,
                text,
                (x1, max(25, y1 - 8)),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.55,
                color,
                2,
                cv2.LINE_AA,
            )

        # Vẽ YOLO bbox.
        for det in detections:
            x1 = int(det.x1)
            y1 = int(det.y1)
            x2 = int(det.x2)
            y2 = int(det.y2)

            color_map = {0: (0, 0, 255), 1: (0, 200, 0)}
            color = color_map.get(det.class_id, (255, 180, 0))

            cv2.rectangle(
                image,
                (x1, y1),
                (x2, y2),
                color,
                2,
            )

            label = f"class={det.class_id} conf={det.confidence:.3f}"

            cv2.putText(
                image,
                label,
                (x1, max(25, y1 - 5)),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.5,
                color,
                2,
                cv2.LINE_AA,
            )

        final_text = (
            f"Cycle={event['cycle_id']} "
            f"Step={event['step']} "
            f"Result={'PASS' if evaluation['passed'] else 'FAIL'}"
        )

        color = (0, 200, 0) if evaluation["passed"] else (0, 0, 255)

        cv2.putText(
            image,
            final_text,
            (20, 40),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.85,
            color,
            2,
            cv2.LINE_AA,
        )

        date_folder = datetime.now().strftime("%Y-%m-%d")
        output_dir = (
            self.annotated_dir
            / date_folder
            / event["camera_side"]
            / event["tube_type"]
        )
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / f"{event['event_id']}_annotated.jpg"

        success = cv2.imwrite(str(output_path), image)

        if not success:
            raise RuntimeError(
                f"Không thể lưu annotated image: {output_path}"
            )

        return str(output_path)

    def save_ng_screenshot(
        self,
        annotated_path: str,
        event: dict,
    ) -> Optional[str]:
        try:
            src = Path(annotated_path)
            if not src.exists():
                LOGGER.warning("NG source not found: %s", annotated_path)
                return None

            cycle_dir = self.ng_dir / str(event["cycle_id"])
            cycle_dir.mkdir(parents=True, exist_ok=True)

            dst = cycle_dir / f"{event['event_id']}_annotated_ng.jpg"
            shutil.copy2(str(src), str(dst))
            LOGGER.info("NG screenshot saved: %s", dst)
            return str(dst)
        except Exception as ex:
            LOGGER.error("Failed to save NG screenshot: %s", ex)
            return None

    def log_step3_csv(self, cycle: dict) -> None:
        try:
            log_dir = Path(self.config["storage"]["base_dir"]) / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)

            csv_path = log_dir / f"STEP3_log_{datetime.now().strftime('%Y-%m-%d')}.csv"

            write_header = not csv_path.exists()

            with open(csv_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow([
                        "datetime", "product_no", "step3_result", "step3_error_code"
                    ])
                writer.writerow([
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    cycle.get("tube_type", ""),
                    cycle.get("step3_status", ""),
                    cycle.get("step3_error", "NONE"),
                ])
        except Exception as ex:
            LOGGER.error("Failed to write STEP3 CSV: %s", ex)

    def process_event(self, event_id: str) -> None:
        event = self.database.get_image(event_id)

        if event is None:
            LOGGER.warning("Event not found: %s", event_id)
            return

        if event["image_status"] == "DONE":
            return

        self.database.mark_image_processing(event_id)

        step = int(event["step"])
        inspection_step = "STEP_1" if step == 1 else "STEP_3"

        try:
            raw_image = cv2.imread(event["image_path"])

            if raw_image is None:
                raise RuntimeError("E202_IMAGE_READ_FAIL")

            self.validate_image_size(raw_image)

            detections, _ = self.infer_detections(
                event["image_path"]
            )

            evaluation = self.rule_engine.evaluate(
                inspection_step=inspection_step,
                model_name=self.get_model_name(),
                detections=detections,
            )

            error_code = self.get_error_code(
                step,
                evaluation,
            )

            annotated_path = self.create_annotated_image(
                raw_image=raw_image,
                detections=detections,
                evaluation=evaluation,
                event=event,
            )

            if step == 3 and not evaluation["passed"]:
                self.save_ng_screenshot(annotated_path, event)

            result_payload = {
                "event_id": event_id,
                "cycle_id": event["cycle_id"],
                "camera_side": event["camera_side"],
                "tube_type": event["tube_type"],
                "step": step,
                "processed_at": utc_now_iso(),
                "detections": [
                    asdict(det)
                    for det in detections
                ],
                "evaluation": evaluation,
                "error_code": error_code,
            }

            if step == 1:
                step_status = "PASS" if evaluation["passed"] else "FAIL"
            else:
                step_status = "OK" if evaluation["passed"] else "NG"

            self.database.update_image_result(
                event_id=event_id,
                image_status="DONE",
                result_json=result_payload,
                annotated_path=annotated_path,
                error_code=error_code,
            )

            if step == 1 and step_status == "FAIL":
                self.database.set_cycle_step_result(
                    cycle_id=event["cycle_id"],
                    step=1,
                    status="FAIL",
                    error_code=error_code,
                )
                self.database.abort_cycle_s1_fail(
                    cycle_id=event["cycle_id"],
                    error_code=error_code,
                )
                self.notify_client_step1_result(event, step_status, error_code)
                LOGGER.info(
                    "Step 1 FAIL, cycle ABORTED cycle=%s error=%s",
                    event["cycle_id"],
                    error_code,
                )
            else:
                self.database.set_cycle_step_result(
                    cycle_id=event["cycle_id"],
                    step=step,
                    status=step_status,
                    error_code=error_code,
                )

                LOGGER.info(
                    "Inspection done event=%s cycle=%s step=%s status=%s error=%s",
                    event_id,
                    event["cycle_id"],
                    step,
                    step_status,
                    error_code,
                )

                final_cycle = self.database.finalize_if_ready(
                    event["cycle_id"]
                )

                if final_cycle:
                    self.send_com_result(final_cycle)

                    if step == 3:
                        self.log_step3_csv(final_cycle)

        except Exception as ex:
            LOGGER.error(
                "Inspection failed event=%s error=%s\n%s",
                event_id,
                ex,
                traceback.format_exc(),
            )

            error_code = "E301_INFERENCE_OR_IMAGE_ERROR"

            error_payload = {
                "event_id": event_id,
                "cycle_id": event["cycle_id"],
                "step": step,
                "error_code": error_code,
                "detail": str(ex),
                "processed_at": utc_now_iso(),
            }

            self.database.update_image_result(
                event_id=event_id,
                image_status="ERROR",
                result_json=error_payload,
                annotated_path=None,
                error_code=error_code,
            )

            if step == 1:
                step_status = "FAIL"
                self.database.set_cycle_step_result(
                    cycle_id=event["cycle_id"],
                    step=1,
                    status="FAIL",
                    error_code=error_code,
                )
                self.database.abort_cycle_s1_fail(
                    cycle_id=event["cycle_id"],
                    error_code=error_code,
                )
                self.notify_client_step1_result(event, step_status, error_code)
                LOGGER.info(
                    "Step 1 FAIL (exception), cycle ABORTED cycle=%s error=%s",
                    event["cycle_id"],
                    error_code,
                )
            else:
                step_status = "NG"

                self.database.set_cycle_step_result(
                    cycle_id=event["cycle_id"],
                    step=step,
                    status=step_status,
                    error_code=error_code,
                )

                final_cycle = self.database.finalize_if_ready(
                    event["cycle_id"]
                )

                if final_cycle:
                    self.send_com_result(final_cycle)

                    if step == 3:
                        self.log_step3_csv(final_cycle)

    def notify_client_step1_result(self, event: dict, step_status: str, error_code: str) -> None:
        client_cfg = self.config["client"]
        url = f"http://{client_cfg['host']}:{client_cfg['port']}/api/v1/inspection-result"
        payload = {
            "event_id": event["event_id"],
            "cycle_id": event["cycle_id"],
            "camera_side": event["camera_side"],
            "tube_type": event["tube_type"],
            "step": 1,
            "passed": step_status == "PASS",
            "error_code": error_code,
        }
        for attempt in range(1, 4):
            try:
                resp = requests.post(url, json=payload, timeout=3)
                if resp.ok:
                    LOGGER.info("Webhook S1 result sent to client cycle=%s passed=%s", event["cycle_id"], step_status == "PASS")
                    return
            except Exception as ex:
                LOGGER.warning("Webhook attempt %d failed: %s", attempt, ex)
                if attempt < 3:
                    time.sleep(0.5)
        LOGGER.error("Webhook S1 result FAILED after 3 retries cycle=%s", event["cycle_id"])

    def send_com_result(self, cycle: dict) -> None:
        """
        Arduino cần xử lý duplicate SEQ theo cơ chế idempotent:
        cùng SEQ gửi lại nhiều lần chỉ kích relay một lần.
        """
        with self.com_send_lock:
            if cycle["com_status"] == "ACK":
                return

            LOGGER.info(
                "Send final result cycle=%s result=%s error=%s",
                cycle["cycle_id"],
                cycle["final_result"],
                cycle["final_error"],
            )

            success = self.serial_gateway.send_result(cycle)

            if success:
                self.database.update_com_status(
                    cycle["cycle_id"],
                    "ACK",
                )

                LOGGER.info(
                    "COM result ACK cycle=%s",
                    cycle["cycle_id"],
                )
            else:
                self.database.update_com_status(
                    cycle["cycle_id"],
                    "RETRY",
                )

                LOGGER.error(
                    "COM result retry pending cycle=%s",
                    cycle["cycle_id"],
                )


# ============================================================
# FASTAPI APPLICATION
# ============================================================

service: Optional[InspectionService] = None


def start_camera_agent() -> None:
    """
    MERGED MODE: khởi động Camera Agent trong cùng process với AI Server.
    Config lấy từ section 'camera_agent' trong config.yaml.
    """
    agent_config = CONFIG.get("camera_agent")

    if not agent_config:
        LOGGER.warning(
            "Config khong co section 'camera_agent' -> "
            "Camera Agent se KHONG chay (che do AI Server don le)"
        )
        return

    camera_agent_module.CONFIG = agent_config
    camera_agent_module.setup_logging()

    agent = camera_agent_module.CameraAgent(agent_config)
    agent.start()

    # Các route của agent (mounted ở cuối file) đọc module-global này.
    camera_agent_module.camera_agent = agent

    LOGGER.info(
        "Camera Agent started (merged) root=%s network_share=%s",
        agent.root_path,
        bool(agent.network_share),
    )


def stop_camera_agent() -> None:
    if camera_agent_module.camera_agent is None:
        return

    try:
        camera_agent_module.camera_agent.stop()
    except Exception:
        LOGGER.exception("Camera Agent stop failed")
    finally:
        camera_agent_module.camera_agent = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global service

    service = InspectionService(CONFIG)
    service.start()

    start_camera_agent()

    yield

    stop_camera_agent()

    if service:
        service.stop()


app = FastAPI(
    title="AI Tube Inspection Server",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    if service is None:
        raise HTTPException(status_code=503, detail="Service not started")

    camera_agent_instance = camera_agent_module.camera_agent

    return {
        "status": "ok",
        "time": utc_now_iso(),
        "model_path": CONFIG["model"]["weights"],
        "serial_enabled": CONFIG["serial"]["enabled"],
        "serial_connected": service.serial_gateway.is_connected(),
        "queue_size": service.job_queue.qsize(),
        "client_url": f"http://{CONFIG['client']['host']}:{CONFIG['client']['port']}",
        "camera_agent": (
            camera_agent_instance.get_health()
            if camera_agent_instance is not None
            else None
        ),
    }


@app.get("/api/v1/cycles/{cycle_id}")
def get_cycle(cycle_id: str):
    if service is None:
        raise HTTPException(status_code=503, detail="Service not started")

    cycle_id = safe_component(cycle_id, "cycle_id")

    cycle = service.database.get_cycle(cycle_id)

    if not cycle:
        raise HTTPException(status_code=404, detail="Cycle not found")

    return cycle


@app.post("/api/v1/images", status_code=202)
async def upload_image(
    image: UploadFile = File(...),
    metadata: str = Form(...),
):
    if service is None:
        raise HTTPException(
            status_code=503,
            detail="Service not started",
        )

    try:
        parsed_metadata = ImageMetadata.model_validate_json(metadata)

    except Exception as ex:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid metadata: {ex}",
        )

    if parsed_metadata.step not in (1, 3):
        raise HTTPException(
            status_code=400,
            detail="AI server chỉ nhận Step 1 hoặc Step 3",
        )

    received_date = datetime.now().strftime("%Y-%m-%d")

    output_dir = (
        service.raw_dir
        / received_date
        / parsed_metadata.camera_side
        / parsed_metadata.tube_type
        / parsed_metadata.cycle_id
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    temp_path = output_dir / (
        f"{parsed_metadata.event_id}.uploading"
    )

    try:
        sha256 = hashlib.sha256()
        header = bytearray()

        with open(temp_path, "wb") as f:
            while True:
                chunk = await image.read(1024 * 1024)

                if not chunk:
                    break

                f.write(chunk)
                sha256.update(chunk)

                if len(header) < 32:
                    remain = 32 - len(header)
                    header.extend(chunk[:remain])

        image_suffix = detect_image_suffix(bytes(header))

        final_path = output_dir / (
            f"{parsed_metadata.event_id}{image_suffix}"
        )

        os.replace(temp_path, final_path)

        inserted = service.database.insert_image(
            metadata=parsed_metadata,
            image_path=str(final_path),
            image_sha256=sha256.hexdigest(),
        )

        if inserted:
            service.enqueue_event(parsed_metadata.event_id)

            LOGGER.info(
                "Image received event=%s cycle=%s step=%s",
                parsed_metadata.event_id,
                parsed_metadata.cycle_id,
                parsed_metadata.step,
            )

            return JSONResponse(
                status_code=202,
                content={
                    "received": True,
                    "duplicate": False,
                    "event_id": parsed_metadata.event_id,
                    "cycle_id": parsed_metadata.cycle_id,
                    "step": parsed_metadata.step,
                },
            )

        return JSONResponse(
            status_code=200,
            content={
                "received": True,
                "duplicate": True,
                "event_id": parsed_metadata.event_id,
                "cycle_id": parsed_metadata.cycle_id,
                "step": parsed_metadata.step,
            },
        )

    except Exception as ex:
        LOGGER.exception(
            "Upload failed event=%s error=%s",
            parsed_metadata.event_id,
            ex,
        )

        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass

        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {ex}",
        )


@app.post("/api/v1/cycles/abort")
def abort_cycle(request: AbortCycleRequest):
    if service is None:
        raise HTTPException(
            status_code=503,
            detail="Service not started",
        )

    cycle, newly_aborted = service.database.abort_cycle(
        cycle_id=request.cycle_id,
        camera_side=request.camera_side,
        tube_type=request.tube_type,
        error_code=request.error_code,
    )

    if newly_aborted:
        service.send_com_result(cycle)

    return {
        "accepted": True,
        "newly_aborted": newly_aborted,
        "cycle": cycle,
    }


# ============================================================
# WEB GUI
# ============================================================

STATIC_DIR = Path(__file__).parent / "static"


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/v1/cycles", name="list_cycles")
def list_cycles(limit: int = 50, offset: int = 0):
    if service is None:
        raise HTTPException(status_code=503, detail="Service not started")

    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    return {
        "cycles": service.database.list_cycles(limit, offset),
        "limit": limit,
        "offset": offset,
    }


@app.get("/api/v1/cycles/{cycle_id}/images", name="cycle_images")
def cycle_images(cycle_id: str):
    if service is None:
        raise HTTPException(status_code=503, detail="Service not started")

    cycle_id = safe_component(cycle_id, "cycle_id")

    cycle = service.database.get_cycle(cycle_id)

    if not cycle:
        raise HTTPException(status_code=404, detail="Cycle not found")

    images = service.database.get_images_for_cycle(cycle_id)

    for image in images:
        if image["result_json"]:
            try:
                image["result"] = json.loads(image["result_json"])
            except Exception:
                image["result"] = None
            del image["result_json"]

    return {"cycle": cycle, "images": images}


@app.get("/api/v1/images/{event_id}/annotated", name="annotated_image")
def annotated_image(event_id: str):
    if service is None:
        raise HTTPException(status_code=503, detail="Service not started")

    event_id = safe_component(event_id, "event_id")

    event = service.database.get_image(event_id)

    if not event:
        raise HTTPException(status_code=404, detail="Event not found")

    if not event["annotated_path"]:
        raise HTTPException(status_code=404, detail="Annotated image not available")

    path = Path(event["annotated_path"])

    if not path.exists():
        raise HTTPException(status_code=404, detail="Annotated image file missing")

    return FileResponse(path, media_type="image/jpeg")


@app.get("/api/v1/images/{event_id}/raw", name="raw_image")
def raw_image(event_id: str):
    if service is None:
        raise HTTPException(status_code=503, detail="Service not started")

    event_id = safe_component(event_id, "event_id")

    event = service.database.get_image(event_id)

    if not event:
        raise HTTPException(status_code=404, detail="Event not found")

    path = Path(event["image_path"])

    if not path.exists():
        raise HTTPException(status_code=404, detail="Raw image file missing")

    return FileResponse(path, media_type="image/jpeg")


@app.get("/api/v1/recent", name="recent_images")
def recent_images(limit: int = 20):
    if service is None:
        raise HTTPException(status_code=503, detail="Service not started")

    limit = max(1, min(limit, 100))

    images = service.database.get_recent_images(limit)

    for image in images:
        if image["result_json"]:
            try:
                image["result"] = json.loads(image["result_json"])
            except Exception:
                image["result"] = None
            del image["result_json"]

    return {"images": images}


@app.get("/api/v1/live", name="live_detection")
def live_detection():
    if service is None:
        raise HTTPException(status_code=503, detail="Service not started")

    image = service.database.get_latest_processed()

    if image is None:
        return {"image": None}

    if image["result_json"]:
        try:
            image["result"] = json.loads(image["result_json"])
        except Exception:
            image["result"] = None
        del image["result_json"]

    return {"image": image}


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ============================================================
# MERGED MODE: CAMERA AGENT (mount vào cùng app, cùng port)
# ============================================================
# Mount agent app ở CUỐI file:
# - Route của main (đăng ký trước) được ưu tiên khi trùng path
#   (ví dụ /health là của AI Server).
# - Route agent giữ nguyên URL cũ như khi chạy riêng:
#     GET  /api/v1/streams
#     POST /api/v1/reset/{camera_side}
#     POST /api/v1/inspection-result   (webhook Step 1 FAIL - loopback)
# Mount "/" phải nằm sau cùng để không che route/static ở trên.
app.mount("/", camera_agent_module.app)