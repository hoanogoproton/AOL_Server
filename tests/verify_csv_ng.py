"""
Verify nhanh cac thay doi CSV log + anh NG (khong can server):
1. sanitize_filename() lam sach ten file camera dung nhu thiet ke.
2. Migration DB cu -> tu dong them cot original_filename.
3. insert_image()/get_image() luu + doc lai original_filename.

Chay: .\\.venv\\Scripts\\python.exe tests\\verify_csv_ng.py
"""
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from main import (  # noqa: E402
    ImageMetadata,
    InspectionDatabase,
    sanitize_filename,
)


def check(name: str, actual, expected) -> None:
    status = "OK " if actual == expected else "FAIL"
    print(f"  [{status}] {name}: {actual!r} (expected {expected!r})")
    if actual != expected:
        raise SystemExit(1)


def test_sanitize_filename() -> None:
    print("== 1. sanitize_filename ==")
    check("ten binh thuong", sanitize_filename("IMG001.jpg"), "IMG001.jpg")
    check("co dau cach", sanitize_filename("IMG 001.jpg"), "IMG 001.jpg")
    check("ky tu dac biet", sanitize_filename("a<b>c|.jpg"), "a_b_c_.jpg")
    check(
        "path traversal",
        sanitize_filename(r"..\..\x\IMG001.jpg"),
        "IMG001.jpg",
    )
    check("rong", sanitize_filename(""), None)
    check("none", sanitize_filename(None), None)
    check("chi dau cham", sanitize_filename(".."), None)
    print("   OK")


OLD_SCHEMA = """
CREATE TABLE cycles (
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

CREATE TABLE images (
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
"""


def test_migration_and_roundtrip(tmp: Path) -> None:
    print("== 2. Migration DB cu + luu/doc original_filename ==")
    db_path = tmp / "old_schema.db"

    conn = sqlite3.connect(db_path)
    conn.executescript(OLD_SCHEMA)
    conn.commit()
    conn.close()

    db = InspectionDatabase(str(db_path))

    check_conn = sqlite3.connect(db_path)
    columns = {
        row[1]
        for row in check_conn.execute(
            "PRAGMA table_info(images)"
        ).fetchall()
    }
    check_conn.close()
    check("cot original_filename duoc them", "original_filename" in columns, True)

    metadata = ImageMetadata(
        event_id="TEST-L-000000001-S3",
        cycle_id="TEST-L-000000001",
        camera_side="L",
        tube_type="TESTNG",
        step=3,
        observed_at="2026-09-03T01:02:03.000Z",
    )

    inserted = db.insert_image(
        metadata=metadata,
        image_path=str(tmp / "TEST-L-000000001-S3.jpg"),
        image_sha256="deadbeef",
        original_filename="IMG001.jpg",
    )
    check("insert_image thanh cong", inserted, True)

    event = db.get_image("TEST-L-000000001-S3")
    check(
        "get_image tra ve original_filename",
        event["original_filename"],
        "IMG001.jpg",
    )

    inserted_again = db.insert_image(
        metadata=metadata,
        image_path=str(tmp / "TEST-L-000000001-S3.jpg"),
        image_sha256="deadbeef",
        original_filename="OTHER.jpg",
    )
    check("insert trung - khong ghi de", inserted_again, False)
    print("   OK")


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp_name:
        test_sanitize_filename()
        test_migration_and_roundtrip(Path(tmp_name))

    print("\nVERIFY CSV/NG CHANGES: ALL OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
