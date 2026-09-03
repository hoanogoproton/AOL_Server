"""
Test cau hinh 'storage.ng_dir' (thu muc luu anh NG + file log STEP3):

- Khong cau hinh ng_dir -> hanh vi cu: <base_dir>/ng va <base_dir>/logs.
- Co cau hinh ng_dir (thu muc local) -> tu tao thu muc, log dir la
  <ng_dir>/logs.
- Co cau hinh ng_dir nhung KHONG tao duoc (UNC share ao) -> fallback
  ve <base_dir>/ng, khong raise.

Ghi chu: khoi tao InspectionService trong tmp_path de khong đụng dữ
liệu thật; serial disabled nen khong đụng COM.
"""

import csv
from pathlib import Path

import pytest

import main
from main import InspectionService


ROI_CSV_HEADER = (
    "inspection_step,roi_id,model_name,class_id,class_name,"
    "compare_x_min,compare_y_min,compare_x_max,compare_y_max,"
    "confidence,check_mode,min_count,max_count\n"
)


def make_config(tmp_path: Path, ng_dir=None) -> dict:
    storage = {
        "base_dir": str(tmp_path / "data"),
        "raw_dir": str(tmp_path / "data" / "raw"),
        "annotated_dir": str(tmp_path / "data" / "annotated"),
        "database_path": str(tmp_path / "data" / "inspection.db"),
    }

    if ng_dir is not None:
        storage["ng_dir"] = ng_dir

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


def make_service(tmp_path: Path, ng_dir=None) -> InspectionService:
    return InspectionService(make_config(tmp_path, ng_dir))


def test_no_ng_dir_keeps_default_layout(tmp_path):
    service = make_service(tmp_path)

    assert service.ng_dir == tmp_path / "data" / "ng"
    assert (tmp_path / "data" / "ng").is_dir()

    # Hanh vi cu: log STEP3 van o <base_dir>/logs.
    assert service.step3_log_dir == tmp_path / "data" / "logs"
    assert not (tmp_path / "data" / "ng" / "logs").exists()


def test_ng_dir_configured_creates_dir_and_log_dir(tmp_path):
    ng_root = tmp_path / "ng_share"
    service = make_service(tmp_path, ng_dir=str(ng_root))

    assert service.ng_dir == Path(str(ng_root))
    assert ng_root.is_dir()

    # Log STEP3 chuyen vao <ng_dir>/logs (chua mkdir den khi ghi).
    assert service.step3_log_dir == ng_root / "logs"
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
    # Bat bien: co cau hinh ng_dir -> log dir LUON la <ng_dir>/logs,
    # ca khi ng_dir da fallback ve thu muc local.
    assert service.step3_log_dir == tmp_path / "data" / "ng" / "logs"


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

    csv_files = list((ng_root / "logs").glob("STEP3_log_*.csv"))
    assert len(csv_files) == 1

    with open(csv_files[0], "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0][-1] == "ng_image_name"
    assert rows[1][-1] == "IMG001_annotated.jpg"


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
