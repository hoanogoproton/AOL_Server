"""Kiem tra du lieu Liveview tach rieng theo camera side."""

from types import SimpleNamespace

from main import ImageMetadata, InspectionDatabase, InspectionService


def them_anh_da_xu_ly(db, event_id, cycle_id, side):
    """Them anh DONE co annotated de mo phong du lieu Liveview."""
    metadata = ImageMetadata(
        event_id=event_id,
        cycle_id=cycle_id,
        camera_side=side,
        tube_type="No1",
        step=1,
        observed_at="2026-10-13T08:00:00",
    )
    db.insert_image(metadata, "anh_goc.jpg", "a" * 64)
    db.update_image_result(
        event_id,
        "DONE",
        {"evaluation": {"passed": True}},
        "anh_annotated.jpg",
        None,
    )


def test_lay_anh_live_moi_nhat_tung_side_va_bo_qua_cycle_skip(tmp_path):
    """Liveview lay anh moi nhat L/R, nhung khong tra anh cua cycle SKIP."""
    db = InspectionDatabase(str(tmp_path / "inspection.db"))
    them_anh_da_xu_ly(db, "event-l-skip", "cycle-l-skip", "L")
    them_anh_da_xu_ly(db, "event-l-ok", "cycle-l-ok", "L")
    them_anh_da_xu_ly(db, "event-r-ok", "cycle-r-ok", "R")

    with db.connect() as conn:
        conn.execute(
            "UPDATE cycles SET final_result = 'SKIP' WHERE cycle_id = 'cycle-l-skip'"
        )
        conn.execute(
            "UPDATE images SET processed_at = '2026-10-13 08:00:04' WHERE event_id = 'event-l-skip'"
        )
        conn.execute(
            "UPDATE images SET processed_at = '2026-10-13 08:00:02' WHERE event_id = 'event-l-ok'"
        )
        conn.execute(
            "UPDATE images SET processed_at = '2026-10-13 08:00:03' WHERE event_id = 'event-r-ok'"
        )
        conn.commit()

    images = db.get_latest_processed_by_side()

    assert images["L"]["event_id"] == "event-l-ok"
    assert images["R"]["event_id"] == "event-r-ok"


def test_step1_fail_danh_dau_skip_truoc_khi_anh_done(monkeypatch):
    """Anh Step 1 FAIL khong duoc DONE truoc khi cycle da SKIP."""
    thu_tu = []
    event = {
        "event_id": "event-skip",
        "cycle_id": "cycle-skip",
        "camera_side": "L",
        "tube_type": "No1",
        "step": 1,
        "image_status": "QUEUED",
        "image_path": "anh_goc.jpg",
    }

    class DatabaseGia:
        def get_image(self, event_id):
            return event

        def mark_image_processing(self, event_id):
            thu_tu.append("processing")

        def set_cycle_step_result(self, **kwargs):
            thu_tu.append("step_fail")

        def abort_cycle_s1_fail(self, **kwargs):
            thu_tu.append("skip")

        def update_image_result(self, **kwargs):
            thu_tu.append("done")

    service = InspectionService.__new__(InspectionService)
    service.database = DatabaseGia()
    service.rule_engine = SimpleNamespace(
        evaluate_classifiers=lambda **kwargs: {"passed": False}
    )
    service.validate_image_size = lambda image: None
    service.infer_classifiers = lambda image: []
    service.create_annotated_image = lambda **kwargs: "anh_annotated.jpg"
    service.save_ng_screenshot = lambda *args: None
    service.get_error_code = lambda *args: "E110_PRE_GLUE_FAIL"
    service.notify_client_step1_result = lambda *args: None

    monkeypatch.setattr("main.cv2.imread", lambda path: object())

    service.process_event("event-skip")

    assert thu_tu == ["processing", "step_fail", "skip", "done"]
