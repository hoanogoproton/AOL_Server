"""Kiem tra du lieu Liveview tach rieng theo camera side."""

from main import ImageMetadata, InspectionDatabase


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
