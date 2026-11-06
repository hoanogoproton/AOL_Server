"""Test callback ket qua AI (outbox main.py + endpointCamera Agent)."""

from types import SimpleNamespace

import main
import camera_agent as camera_agent_module
from main import ImageMetadata, InspectionDatabase


def make_service_database(tmp_path) -> InspectionDatabase:
    return InspectionDatabase(
        str(tmp_path / "inspection.db")
    )


def test_client_callback_outbox_is_idempotent(tmp_path):
    db = make_service_database(tmp_path)
    payload = {
        "event_id": "cyc1-S1",
        "step": 1,
        "status": "PASS",
    }

    first = db.queue_client_callback("cyc1-S1", payload)
    second = db.queue_client_callback("cyc1-S1", payload)

    assert first == second

    rows = db.get_pending_client_callbacks()
    assert len(rows) == 1
    assert rows[0]["event_id"] == "cyc1-S1"

    db.mark_client_callback_result(first, True)
    assert db.get_pending_client_callbacks() == []


def test_event_result_endpoint_returns_saved_status(tmp_path, monkeypatch):
    db = make_service_database(tmp_path)

    metadata = ImageMetadata(
        event_id="cyc1-S1",
        cycle_id="cyc1",
        camera_side="L",
        tube_type="No1",
        step=1,
        observed_at="2026-11-06T08:00:00",
    )
    db.insert_image(
        metadata,
        str(tmp_path / "cyc1-S1.jpg"),
        "a" * 64,
        original_filename="cyc1-S1.jpg",
    )

    # Chua co ket qua AI -> status None (agent phai tiep tuc cho).
    monkeypatch.setattr(main, "service", SimpleNamespace(database=db))
    result = main.event_result("cyc1-S1")
    assert result["found"] is True
    assert result["status"] is None

    # Da co ket qua DB -> tra dung status cho agent poll lai.
    db.set_cycle_step_result(
        cycle_id="cyc1",
        step=1,
        status="FAIL",
        error_code="E110_PRE_GLUE_FAIL",
    )
    result = main.event_result("cyc1-S1")
    assert result["status"] == "FAIL"
    assert result["error_code"] == "E110_PRE_GLUE_FAIL"


def test_inspection_result_endpoint_accepts_all_four_outcomes(monkeypatch):
    calls = []

    fake_agent = SimpleNamespace(
        apply_inspection_result=lambda **kwargs: (
            calls.append(kwargs) or {"action": "APPLIED"}
        )
    )
    monkeypatch.setattr(
        camera_agent_module, "camera_agent", fake_agent
    )

    payloads = [
        ("e1", 1, "PASS"),
        ("e2", 1, "FAIL"),
        ("e3", 3, "OK"),
        ("e4", 3, "NG"),
    ]

    for event_id, step, status in payloads:
        response = camera_agent_module.inspection_result(
            {
                "event_id": event_id,
                "cycle_id": "cyc",
                "camera_side": "L",
                "tube_type": "No1",
                "step": step,
                "status": status,
                "error_code": "E_TEST",
            }
        )
        assert response["accepted"] is True
        assert response["action"] == "APPLIED"

    assert [
        (call["event_id"], call["result_status"])
        for call in calls
    ] == [
        ("e1", "PASS"),
        ("e2", "FAIL"),
        ("e3", "OK"),
        ("e4", "NG"),
    ]


def test_inspection_result_endpoint_rejects_wrong_status_for_step(
    monkeypatch,
):
    calls = []

    fake_agent = SimpleNamespace(
        apply_inspection_result=lambda **kwargs: (
            calls.append(kwargs) or {"action": "APPLIED"}
        )
    )
    monkeypatch.setattr(
        camera_agent_module, "camera_agent", fake_agent
    )

    # Step 1 khong the co status OK.
    response = camera_agent_module.inspection_result(
        {
            "event_id": "e1",
            "step": 1,
            "status": "OK",
        }
    )

    assert response["accepted"] is False
    assert calls == []
