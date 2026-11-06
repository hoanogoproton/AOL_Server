"""Kiem tra fail-safe tin hieu NG cua Step 3.

- Loi doc anh / inference / ghi annotation van phai gui tin hieu NG.
- Tin hieu gui truoc annotation, khong phu thuoc archive thanh cong.
- Cycle da SKIP/ABORTED khong gui lai.
- Trang thai delivery ACK/UNCONFIRMED duoc ghi vao cycle.
"""

import json
from pathlib import Path
from types import SimpleNamespace

import main
from main import ImageMetadata, InspectionService


ROI_CSV_HEADER = (
    "inspection_step,roi_id,model_name,class_id,class_name,"
    "compare_x_min,compare_y_min,compare_x_max,compare_y_max,"
    "confidence,check_mode,min_count,max_count\n"
)


class FakeSignalSender:
    def __init__(self, confirmed=True):
        self.confirmed = confirmed
        self.calls = 0
        self.port = "COMX"

    def send_ng_signal(self):
        self.calls += 1
        return self.confirmed


def make_service(tmp_path: Path, sender) -> InspectionService:
    storage = {
        "base_dir": str(tmp_path / "data"),
        "raw_dir": str(tmp_path / "data" / "raw"),
        "annotated_dir": str(tmp_path / "data" / "annotated"),
        "database_path": str(tmp_path / "data" / "inspection.db"),
    }

    roi_csv = tmp_path / "roi_rules.csv"
    roi_csv.write_text(ROI_CSV_HEADER, encoding="utf-8")

    return InspectionService(
        {
            "storage": storage,
            "roi": {"csv_path": str(roi_csv)},
            "serial": {
                "enabled": False,
                "port": "COM3",
                "baudrate": 115200,
            },
        },
        signal_sender=sender,
    )


def add_event(service, event_id, cycle_id, step=3):
    metadata = ImageMetadata(
        event_id=event_id,
        cycle_id=cycle_id,
        camera_side="L",
        tube_type="No1",
        step=step,
        observed_at="2026-11-06T08:00:00",
    )
    service.database.insert_image(
        metadata,
        str(Path(service.raw_dir) / f"{event_id}.jpg"),
        "a" * 64,
        original_filename=f"{event_id}.jpg",
    )


def patch_ng_classifier_flow(service):
    service.infer_classifiers = lambda image: []
    service.rule_engine = SimpleNamespace(
        evaluate_classifiers=lambda **kwargs: {
            "passed": False,
            "roi_results": [],
        }
    )
    service.get_error_code = lambda *args: "E001_PRODUCT_OK_NOT_FOUND"
    service.save_ng_screenshot = lambda *args: None


def test_step3_inference_exception_requests_ng_signal(tmp_path, monkeypatch):
    """Loi doc anh/inference Step 3 -> van gui NG va final NG."""
    sender = FakeSignalSender(confirmed=True)
    service = make_service(tmp_path, sender)

    add_event(service, "cyc1-S3", "cyc1")
    service.database.set_cycle_step_result(
        cycle_id="cyc1", step=1, status="PASS", error_code="NONE"
    )

    def raise_inference_error(path):
        raise RuntimeError("model predict failed")

    monkeypatch.setattr(main.cv2, "imread", raise_inference_error)

    service.process_event("cyc1-S3")

    assert sender.calls == 1

    image = service.database.get_image("cyc1-S3")
    assert image["image_status"] == "ERROR"
    assert json.loads(image["result_json"])["ng_signal_status"] == "ACK"

    cycle = service.database.get_cycle("cyc1")
    assert cycle["step3_status"] == "NG"
    assert cycle["final_result"] == "NG"
    assert cycle["com_status"] == "ACK"


def test_step3_annotation_failure_still_requests_ng_signal(
    tmp_path, monkeypatch
):
    """Annotation ghi that bai KHONG duoc chan tin hieu NG Step 3."""
    sender = FakeSignalSender(confirmed=True)
    service = make_service(tmp_path, sender)

    add_event(service, "cyc2-S3", "cyc2")
    service.database.set_cycle_step_result(
        cycle_id="cyc2", step=1, status="PASS", error_code="NONE"
    )

    patch_ng_classifier_flow(service)

    def raise_annotate_error(**kwargs):
        raise RuntimeError("Khong the luu annotated image")

    service.create_annotated_image = raise_annotate_error
    monkeypatch.setattr(main.cv2, "imread", lambda path: object())

    service.process_event("cyc2-S3")

    assert sender.calls == 1

    image = service.database.get_image("cyc2-S3")
    assert image["image_status"] == "ERROR"

    cycle = service.database.get_cycle("cyc2")
    assert cycle["step3_status"] == "NG"
    assert cycle["final_result"] == "NG"
    assert cycle["com_status"] == "ACK"


def test_step3_ng_does_not_signal_when_cycle_already_skip(
    tmp_path, monkeypatch
):
    """Cycle da SKIP vi Step 1 FAIL -> Step 3 den muon khong gui lai."""
    sender = FakeSignalSender(confirmed=True)
    service = make_service(tmp_path, sender)

    add_event(service, "cyc3-S3", "cyc3")
    service.database.set_cycle_step_result(
        cycle_id="cyc3", step=1, status="FAIL", error_code="E110"
    )
    service.database.abort_cycle_s1_fail(
        cycle_id="cyc3", error_code="E110"
    )

    patch_ng_classifier_flow(service)
    service.create_annotated_image = lambda **kwargs: "fake_annotated.jpg"
    monkeypatch.setattr(main.cv2, "imread", lambda path: object())

    service.process_event("cyc3-S3")

    assert sender.calls == 0
    assert service.database.get_cycle("cyc3")["final_result"] == "SKIP"


def test_step3_missing_ack_records_unconfirmed(tmp_path, monkeypatch):
    """Mat ACK -> ghi UNCONFIRMED, khong tu retry/khong bao ACK gia."""
    sender = FakeSignalSender(confirmed=False)
    service = make_service(tmp_path, sender)

    add_event(service, "cyc4-S3", "cyc4")
    service.database.set_cycle_step_result(
        cycle_id="cyc4", step=1, status="PASS", error_code="NONE"
    )

    def raise_inference_error(path):
        raise RuntimeError("model predict failed")

    monkeypatch.setattr(main.cv2, "imread", raise_inference_error)

    service.process_event("cyc4-S3")

    assert sender.calls == 1
    cycle = service.database.get_cycle("cyc4")
    assert cycle["final_result"] == "NG"
    assert cycle["com_status"] == "UNCONFIRMED"


def test_step3_signal_is_not_replayed_for_same_event(
    tmp_path, monkeypatch
):
    """Cung mot event bi xu ly lai -> khong gui them lenh NG."""
    sender = FakeSignalSender(confirmed=True)
    service = make_service(tmp_path, sender)

    add_event(service, "cyc5-S3", "cyc5")
    service.database.set_cycle_step_result(
        cycle_id="cyc5", step=1, status="PASS", error_code="NONE"
    )

    def raise_inference_error(path):
        raise RuntimeError("model predict failed")

    monkeypatch.setattr(main.cv2, "imread", raise_inference_error)

    service.process_event("cyc5-S3")
    service.process_event("cyc5-S3")

    assert sender.calls == 1
