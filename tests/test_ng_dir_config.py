"""
Test cau hinh 'storage.ng_dir' + chinh sach luu anh:

- Khong cau hinh ng_dir -> log STEP3 chi ghi o <base_dir>/logs.
- Co cau hinh ng_dir -> log ghi kep: <base_dir>/logs + <ng_dir>/logs.
- Co cau hinh ng_dir nhung KHONG tao duoc (UNC share ao) -> fallback
  ve <base_dir>/ng, khong raise.
- save_ng_screenshot: copy anh annotated + raw sang ng_dir.
- cleanup_old_images: xoa anh cu hon 'image_retention_days' ngay,
  don thu muc rong; retention = 0 -> khong xoa gi.

Ghi chu: khoi tao InspectionService trong tmp_path de khong đụng dữ
liệu thật; serial disabled nen khong đụng COM.
"""

import csv
import os
import time
from pathlib import Path

import pytest

import main
from main import InspectionService


ROI_CSV_HEADER = (
    "inspection_step,roi_id,model_name,class_id,class_name,"
    "compare_x_min,compare_y_min,compare_x_max,compare_y_max,"
    "confidence,check_mode,min_count,max_count\n"
)


def make_config(tmp_path: Path, ng_dir=None, image_retention_days=None) -> dict:
    storage = {
        "base_dir": str(tmp_path / "data"),
        "raw_dir": str(tmp_path / "data" / "raw"),
        "annotated_dir": str(tmp_path / "data" / "annotated"),
        "database_path": str(tmp_path / "data" / "inspection.db"),
    }

    if ng_dir is not None:
        storage["ng_dir"] = ng_dir

    if image_retention_days is not None:
        storage["image_retention_days"] = image_retention_days

    roi_csv = tmp_path / "roi_rules.csv"
    roi_csv.write_text(ROI_CSV_HEADER, encoding="utf-8")

    return {
        "storage": storage,
        "roi": {"csv_path": str(roi_csv)},
        "serial": {
            "enabled": False,
            "port": "COM3",
            "baudrate": 115200,
            "retry_count": 1,
            "retry_interval_sec": 0.01,
        },
    }


def make_service(tmp_path: Path, ng_dir=None, image_retention_days=None) -> InspectionService:
    return InspectionService(
        make_config(tmp_path, ng_dir, image_retention_days)
    )


def test_no_ng_dir_keeps_default_layout(tmp_path):
    service = make_service(tmp_path)

    assert service.ng_dir == tmp_path / "data" / "ng"
    assert (tmp_path / "data" / "ng").is_dir()

    # Khong cau hinh ng_dir: log STEP3 chi ghi o <base_dir>/logs.
    assert service.step3_log_dirs() == [tmp_path / "data" / "logs"]


def test_ng_dir_configured_creates_dir_and_log_dir(tmp_path):
    ng_root = tmp_path / "ng_share"
    service = make_service(tmp_path, ng_dir=str(ng_root))

    assert service.ng_dir == Path(str(ng_root))
    assert ng_root.is_dir()

    # Log STEP3 ghi kep: local <base_dir>/logs + share <ng_dir>/logs.
    assert service.step3_log_dirs() == [
        tmp_path / "data" / "logs",
        ng_root / "logs",
    ]
    assert not (tmp_path / "data" / "ng").exists()


def test_unreachable_ng_dir_falls_back_to_local(tmp_path, monkeypatch):
    ng_root = Path(r"\\no-such-host-ao\no-such-share")

    real_mkdir = Path.mkdir

    def fake_mkdir(self, *args, **kwargs):
        # Chi cho UNC path ao that bai, cac path local van binh thuong.
        if str(self).startswith("\\\\no-such-host-ao"):
            raise OSError("network path not found")
        return real_mkdir(self, *args, **kwargs)

    monkeypatch.setattr(main.Path, "mkdir", fake_mkdir)

    service = make_service(tmp_path, ng_dir=str(ng_root))

    assert service.ng_dir == tmp_path / "data" / "ng"
    assert (tmp_path / "data" / "ng").is_dir()
    # Log van ghi kep: local + thu muc NG (da fallback ve local).
    assert service.step3_log_dirs() == [
        tmp_path / "data" / "logs",
        tmp_path / "data" / "ng" / "logs",
    ]


def test_ng_screenshot_and_csv_land_in_ng_dir(tmp_path):
    ng_root = tmp_path / "ng_share"
    service = make_service(tmp_path, ng_dir=str(ng_root))

    # Tao anh annotated + anh raw gia de save_ng_screenshot copy sang ng_dir.
    annotated = tmp_path / "data" / "annotated" / "evt1_annotated.jpg"
    annotated.write_bytes(b"fake-jpeg")

    raw = tmp_path / "data" / "raw" / "evt1.jpg"
    raw.write_bytes(b"fake-raw")

    dst = service.save_ng_screenshot(
        str(annotated),
        {
            "cycle_id": "CYC1",
            "event_id": "evt1",
            "original_filename": "IMG001.jpg",
            "image_path": str(raw),
        },
    )

    assert dst is not None
    assert Path(dst) == ng_root / "CYC1" / "IMG001_annotated.jpg"
    assert Path(dst).read_bytes() == b"fake-jpeg"

    # Anh raw NG (ban goc) cung duoc luu kep trong cung cycle dir.
    raw_dst = ng_root / "CYC1" / "IMG001.jpg"
    assert raw_dst.is_file()
    assert raw_dst.read_bytes() == b"fake-raw"

    service.log_step3_csv(
        {
            "tube_type": "A",
            "step3_status": "NG",
            "step3_error": "E301",
        },
        {
            "original_filename": "IMG001.jpg",
            "image_path": str(raw),
            "event_id": "evt1",
        },
    )

    # Ghi kep: ban sao local (data/logs) + ban sao share (ng_dir/logs).
    share_csv = list((ng_root / "logs").glob("STEP3_log_*.csv"))
    local_csv = list((tmp_path / "data" / "logs").glob("STEP3_log_*.csv"))
    assert len(share_csv) == 1
    assert len(local_csv) == 1

    with open(share_csv[0], "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0][-1] == "ng_image_name"
    assert rows[1][-1] == "IMG001_annotated.jpg"

    with open(local_csv[0], "r", encoding="utf-8", newline="") as f:
        local_rows = list(csv.reader(f))

    # Cung noi dung (bo cot datetime vi 2 lan ghi co the lệch giây).
    assert local_rows[1][1:] == rows[1][1:]


def test_ng_raw_copy_fallbacks(tmp_path):
    ng_root = tmp_path / "ng_share"
    service = make_service(tmp_path, ng_dir=str(ng_root))

    annotated = tmp_path / "data" / "annotated" / "evt2_annotated.jpg"
    annotated.write_bytes(b"fake-jpeg")

    # 1) Thieu original_filename -> dung event_id lam ten file
    #    (ca annotated lan raw).
    service.save_ng_screenshot(
        str(annotated),
        {"cycle_id": "CYC2", "event_id": "evt2"},
    )

    assert (ng_root / "CYC2" / "evt2_annotated.jpg").is_file()
    # Khong co image_path -> bo qua raw, khong loi.
    assert not (ng_root / "CYC2" / "evt2.jpg").exists()

    # 2) image_path tro toi file khong ton tai -> bo qua raw nhung
    #    van copy duoc annotated (ham tra ve duong dan annotated).
    dst = service.save_ng_screenshot(
        str(annotated),
        {
            "cycle_id": "CYC2",
            "event_id": "evt2",
            "image_path": str(tmp_path / "missing.jpg"),
        },
    )

    assert dst == str(ng_root / "CYC2" / "evt2_annotated.jpg")
    assert not (ng_root / "CYC2" / "evt2.jpg").exists()

    # 3) Co raw that -> luu raw giu duoi file goc (png o day).
    raw_png = tmp_path / "data" / "raw" / "evt3.png"
    raw_png.write_bytes(b"fake-png")

    service.save_ng_screenshot(
        str(annotated),
        {
            "cycle_id": "CYC2",
            "event_id": "evt3",
            "original_filename": "IMG003.jpg",
            "image_path": str(raw_png),
        },
    )

    # Raw giu duoi .png cua file goc, ten theo original_filename.
    raw_dst = ng_root / "CYC2" / "IMG003.png"
    assert raw_dst.is_file()
    assert raw_dst.read_bytes() == b"fake-png"


def test_cleanup_old_images_deletes_expired_files(tmp_path):
    service = make_service(tmp_path, ng_dir=str(tmp_path / "ng_share"))

    old_raw = (
        tmp_path / "data" / "raw" / "2026-01-01" / "L" / "A" / "C1"
        / "old.jpg"
    )
    old_raw.parent.mkdir(parents=True)
    old_raw.write_bytes(b"old")

    new_raw = (
        tmp_path / "data" / "raw" / "2026-01-01" / "L" / "A" / "C2"
        / "new.jpg"
    )
    new_raw.parent.mkdir(parents=True)
    new_raw.write_bytes(b"new")

    old_ann = tmp_path / "data" / "annotated" / "2026-01-01" / "old.jpg"
    old_ann.parent.mkdir(parents=True)
    old_ann.write_bytes(b"old")

    # Gia lap mtime cua file "cu": 2 ngay truoc (han giu 1 ngay).
    old_time = time.time() - 2 * 86400
    os.utime(old_raw, (old_time, old_time))
    os.utime(old_ann, (old_time, old_time))

    service.cleanup_old_images()

    assert not old_raw.exists()
    assert not old_ann.exists()
    assert new_raw.exists()

    # Thu muc tro nen rong duoc don sach.
    assert not (tmp_path / "data" / "annotated" / "2026-01-01").exists()
    assert not (
        tmp_path / "data" / "raw" / "2026-01-01" / "L" / "A" / "C1"
    ).exists()
    # Thu muc chua file con song khong bi don.
    assert (
        tmp_path / "data" / "raw" / "2026-01-01" / "L" / "A" / "C2"
    ).is_dir()


def test_cleanup_disabled_when_retention_zero(tmp_path):
    service = make_service(tmp_path, image_retention_days=0)

    old_raw = tmp_path / "data" / "raw" / "old.jpg"
    old_raw.parent.mkdir(parents=True, exist_ok=True)
    old_raw.write_bytes(b"old")

    old_time = time.time() - 5 * 86400
    os.utime(old_raw, (old_time, old_time))

    service.cleanup_old_images()

    # retention = 0 -> khong xoa gi ca.
    assert old_raw.exists()
