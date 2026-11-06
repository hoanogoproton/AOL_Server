"""
Test state machine moi cho Camera Agent.

Nguyen tac:
- Anh den duoc luu vao pending_images truoc, CHUA gan step.
- Moi stream (camera_side, tube_type) chi co mot anh dang cho ket qua AI.
- Step 1 FAIL -> dong cycle, anh ke tiep van la Step 1 (khong bi huy).
- Step 1 PASS -> anh ke tiep trong hang doi la Step 3 cung cycle.
- Step 3 OK/NG -> dong cycle, anh ke tiep la Step 1 cycle moi.
- Callback trung/muon khong doi state lan nua.

Dung CameraAgentDatabase truc tiep (SQLite that trong tmp_path).
"""

import sqlite3
import time

from camera_agent import CameraAgentDatabase, CaptureFileInfo


def make_db(tmp_path) -> CameraAgentDatabase:
    return CameraAgentDatabase(
        database_path=str(tmp_path / "agent.db"),
        agent_id="T1",
    )


def make_file_info(
    path,
    tube_type,
    camera_side="L",
    observed_at="2026-09-12T08:00:00.000+07:00",
) -> CaptureFileInfo:
    return CaptureFileInfo(
        path=str(path),
        tube_type=tube_type,
        camera_side=camera_side,
        observed_at=observed_at,
    )


def add_image(db, no, name, side="L", root=""):
    """Nhan anh vao hang doi + thu gan step ngay neu stream ranh."""
    path = f"{root}/{no}/{side}/{name}.jpg"
    return db.assign_file_to_sequence(
        make_file_info(path, tube_type=no, camera_side=side)
    )


def assign_step(db, no, step, side="L", root=""):
    name = f"s{step}_{int(time.time() * 1000000)}"
    result = add_image(db, no, name, side=side, root=root)

    assert result["status"] == "ASSIGNED", result
    assert result["event"]["step"] == step
    return result["event"]


def queue_image(db, no, name, side="L", root=""):
    """Them anh vao hang doi khi stream dang cho ket qua."""
    result = add_image(db, no, name, side=side, root=root)
    assert result["status"] == "QUEUED", result
    return result


def complete(db, event_id, status, error_code="E_TEST"):
    return db.complete_active_event(
        event_id=event_id,
        result_status=status,
        error_code=error_code,
    )


def get_stream(db, side, no) -> dict:
    return next(
        stream
        for stream in db.get_streams()
        if stream["camera_side"] == side
        and stream["tube_type"] == no
    )


def expire_stream(db, side, no, seconds=100000.0) -> None:
    with db.lock:
        conn = db.connect()

        try:
            conn.execute(
                """
                UPDATE streams
                SET
                    last_event_unix = ?,
                    result_wait_since = ?
                WHERE camera_side = ?
                AND tube_type = ?
                """,
                (time.time() - seconds, time.time() - seconds, side, no),
            )
            conn.commit()

        finally:
            conn.close()


def test_multiple_nos_are_independent_and_wait_in_queue(tmp_path):
    """No1/No2 doc lap: S1 cua No nao dang cho thi S3 cua No do vao queue."""
    db = make_db(tmp_path)

    no1_s1 = assign_step(db, "No1", 1, root=str(tmp_path))
    no2_s1 = assign_step(db, "No2", 1, root=str(tmp_path))

    # Anh tiep theo cua No1 chua duoc gan Step 3 khi S1 chua co ket qua.
    queued_no1 = queue_image(db, "No1", "no1_next", root=str(tmp_path))
    assert queued_no1["event"] is None

    # No2 cung vay, nhung hai stream khong anh huong nhau.
    queue_image(db, "No2", "no2_next", root=str(tmp_path))

    assert get_stream(db, "L", "No1")["next_step"] == 3
    assert get_stream(db, "L", "No2")["next_step"] == 3
    assert db.get_status_summary()["queued_images"] == 2

    # No1 PASS -> anh dang cho cua No1 thanh Step 3 cung cycle.
    applied = complete(db, no1_s1["event_id"], "PASS")
    assert applied["action"] == "APPLIED"
    assert applied["next_event"]["step"] == 3
    assert applied["next_event"]["cycle_id"] == no1_s1["cycle_id"]

    # No2 van dang cho ket qua; Step 3 cua No2 chua duoc gan.
    assert get_stream(db, "L", "No2")["state"] == "WAIT_STEP1_RESULT"

    # No2 PASS -> Step 3 cua No2.
    applied = complete(db, no2_s1["event_id"], "PASS")
    assert applied["next_event"]["step"] == 3
    assert applied["next_event"]["cycle_id"] == no2_s1["cycle_id"]

    # Step 3 OK/NG deu dong cycle.
    no1_s3 = applied_no1_s3 = db.get_event(
        f"{no1_s1['cycle_id']}-S3"
    )
    assert no1_s3 is not None
    result = complete(db, no1_s3["event_id"], "OK")
    assert result["action"] == "APPLIED"
    assert get_stream(db, "L", "No1")["state"] == "READY_STEP1"

    no2_s3 = db.get_event(f"{no2_s1['cycle_id']}-S3")
    result = complete(db, no2_s3["event_id"], "NG")
    assert result["action"] == "APPLIED"
    assert get_stream(db, "L", "No2")["state"] == "READY_STEP1"

    # Khong co abort nao bi queue.
    assert db.get_pending_aborts() == []


def test_step1_fail_never_cancels_later_images(tmp_path):
    """
    FAIL lien tiep 5 anh: ca 5 deu duoc xu ly nhu Step 1, khong anh nao
    bi canceled/bo qua, va cycle_id khong trung.
    """
    db = make_db(tmp_path)

    events = [assign_step(db, "No1", 1, root=str(tmp_path))]
    queued = [
        queue_image(db, "No1", f"queued_{i}", root=str(tmp_path))
        for i in range(4)
    ]

    # Anh thu 2..5 dang nam trong queue, chua bi gan Step 3.
    assert all(item["event"] is None for item in queued)

    applied_steps = [1]
    cycle_ids = {events[0]["cycle_id"]}

    for expected_cycle in range(2, 6):
        previous = events[-1]
        result = complete(db, previous["event_id"], "FAIL")

        assert result["action"] == "APPLIED"
        assert result["next_event"] is not None
        assert result["next_event"]["step"] == 1
        assert result["next_event"]["cycle_id"] == (
            f"T1-L-No1-{expected_cycle:09d}"
        )

        events.append(result["next_event"])
        applied_steps.append(result["next_event"]["step"])
        cycle_ids.add(result["next_event"]["cycle_id"])

    assert applied_steps == [1, 1, 1, 1, 1]
    assert len(cycle_ids) == 5
    assert queue_image(
        db, "No1", "queued_after_fails", root=str(tmp_path)
    )["event"] is None

    # Anh tiep theo PASS -> anh ke tiep trong queue la Step 3 cung cycle.
    result = complete(db, events[-1]["event_id"], "PASS")
    assert result["next_event"]["step"] == 3
    assert result["next_event"]["cycle_id"] == events[-1]["cycle_id"]

    # Tat ca 5 event Step 1 deu con trong DB (khong bi CANCELLED).
    for event in events:
        row = db.get_event(event["event_id"])
        assert row is not None
        assert row["result_status"] == (
            "PASS" if event is events[-1] else "FAIL"
        )


def test_pass_fail_pass_maps_to_steps_1_1_3(tmp_path):
    """Chuoi FAIL -> FAIL -> PASS -> anh tiep theo phai la Step 3."""
    db = make_db(tmp_path)

    first = assign_step(db, "No1", 1, root=str(tmp_path))
    queue_image(db, "No1", "b", root=str(tmp_path))
    queue_image(db, "No1", "c", root=str(tmp_path))
    queue_image(db, "No1", "d", root=str(tmp_path))
    queue_image(db, "No1", "e", root=str(tmp_path))

    second = complete(db, first["event_id"], "FAIL")["next_event"]
    assert second["step"] == 1

    third = complete(db, second["event_id"], "FAIL")["next_event"]
    assert third["step"] == 1

    fourth = complete(db, third["event_id"], "PASS")["next_event"]
    assert fourth["step"] == 3
    assert fourth["cycle_id"] == third["cycle_id"]

    # Step 3 OK -> anh tiep theo la Step 1 cycle moi.
    fifth = complete(db, fourth["event_id"], "OK")["next_event"]
    assert fifth["step"] == 1
    assert fifth["cycle_id"] != fourth["cycle_id"]


def test_duplicate_and_late_callbacks_are_idempotent(tmp_path):
    db = make_db(tmp_path)

    s1 = assign_step(db, "No1", 1, root=str(tmp_path))
    queue_image(db, "No1", "next", root=str(tmp_path))

    first = complete(db, s1["event_id"], "PASS")
    assert first["action"] == "APPLIED"

    s3 = db.get_event(f"{s1['cycle_id']}-S3")
    assert s3 is not None

    # Callback Step 1 trung (den muon sau khi Step 3 da duoc gan).
    duplicate = complete(db, s1["event_id"], "PASS")
    assert duplicate["action"] in ("DUPLICATE", "IGNORED_STALE")
    assert duplicate["next_event"] is None

    stream = get_stream(db, "L", "No1")
    assert stream["active_event_id"] == s3["event_id"]
    assert stream["state"] == "WAIT_STEP3_RESULT"

    # Callback Step 3 trung sau khi cycle dong.
    complete(db, s3["event_id"], "OK")
    late = complete(db, s3["event_id"], "NG")
    assert late["action"] in ("DUPLICATE", "IGNORED_STALE")
    assert get_stream(db, "L", "No1")["state"] == "READY_STEP1"


def test_unknown_event_callback_is_ignored(tmp_path):
    db = make_db(tmp_path)
    result = complete(db, "khong-ton-tai-S1", "PASS")
    assert result["action"] == "NOT_FOUND"
    assert result["next_event"] is None


def test_restart_restores_queue_and_active_event(tmp_path):
    db = make_db(tmp_path)

    s1 = assign_step(db, "No1", 1, root=str(tmp_path))
    queue_image(db, "No1", "next", root=str(tmp_path))

    # Agent restart: mo lai cung DB (initialize/migration chay lai).
    db2 = make_db(tmp_path)

    stream = get_stream(db2, "L", "No1")
    assert stream["state"] == "WAIT_STEP1_RESULT"
    assert stream["active_event_id"] == s1["event_id"]

    # Anh dang cho van con trong queue, khong bi gan som.
    assert db2.get_status_summary()["queued_images"] == 1

    # Ket qua den sau restart van ap dung dung event.
    result = db2.complete_active_event(
        event_id=s1["event_id"],
        result_status="PASS",
        error_code="NONE",
    )
    assert result["action"] == "APPLIED"
    assert result["next_event"]["step"] == 3
    assert result["next_event"]["cycle_id"] == s1["cycle_id"]


def test_fail_on_one_stream_does_not_affect_others(tmp_path):
    """L/No01 FAIL khong anh huong R/No01 va L/No02."""
    db = make_db(tmp_path)

    l_no1 = assign_step(db, "No1", 1, side="L", root=str(tmp_path))
    r_no1 = assign_step(db, "No1", 1, side="R", root=str(tmp_path))
    l_no2 = assign_step(db, "No2", 1, side="L", root=str(tmp_path))

    queue_image(db, "No1", "l_next", side="L", root=str(tmp_path))
    queue_image(db, "No1", "r_next", side="R", root=str(tmp_path))
    queue_image(db, "No2", "l2_next", side="L", root=str(tmp_path))

    # Chi L/No01 FAIL.
    result = complete(db, l_no1["event_id"], "FAIL")
    assert result["next_event"]["step"] == 1

    assert get_stream(db, "R", "No1")["state"] == "WAIT_STEP1_RESULT"
    assert get_stream(db, "L", "No2")["state"] == "WAIT_STEP1_RESULT"
    assert get_stream(db, "R", "No1")["active_event_id"] == (
        r_no1["event_id"]
    )
    assert get_stream(db, "L", "No2")["active_event_id"] == (
        l_no2["event_id"]
    )


def test_timeout_never_turns_waiting_result_into_failure(tmp_path):
    """WAIT_STEP1_RESULT qua lau chi canh bao, khong abort/reset."""
    db = make_db(tmp_path)

    s1 = assign_step(db, "No1", 1, root=str(tmp_path))
    expire_stream(db, "L", "No1")

    warnings = db.warn_incomplete_streams(warn_sec=10)
    assert [w["tube_type"] for w in warnings] == ["No1"]

    assert db.timeout_incomplete_streams(timeout_sec=10) == []

    stream = get_stream(db, "L", "No1")
    assert stream["state"] == "WAIT_STEP1_RESULT"
    assert stream["active_event_id"] == s1["event_id"]

    # Van co the ap dung ket qua binh thuong sau canh bao.
    result = complete(db, s1["event_id"], "PASS")
    assert result["action"] == "APPLIED"


def test_timeout_ready_step3_still_aborts_that_stream_only(tmp_path):
    db = make_db(tmp_path)

    no1_s1 = assign_step(db, "No1", 1, root=str(tmp_path))
    no2_s1 = assign_step(db, "No2", 1, root=str(tmp_path))

    # No1 PASS nhung khong co anh Step 3 nao den.
    complete(db, no1_s1["event_id"], "PASS")
    assert get_stream(db, "L", "No1")["state"] == "READY_STEP3"

    expire_stream(db, "L", "No1")

    aborts = db.timeout_incomplete_streams(timeout_sec=10)
    assert len(aborts) == 1
    assert aborts[0]["tube_type"] == "No1"

    assert get_stream(db, "L", "No1")["state"] == "OUT_OF_SYNC"
    assert get_stream(db, "L", "No2")["state"] == "WAIT_STEP1_RESULT"

    # No2 van xu ly binh thuong.
    complete(db, no2_s1["event_id"], "PASS")


def test_manual_reset_keeps_queued_images_and_restarts_step1(tmp_path):
    db = make_db(tmp_path)

    s1 = assign_step(db, "No1", 1, root=str(tmp_path))
    queue_image(db, "No1", "after_reset", root=str(tmp_path))

    aborts = db.reset_stream("L")
    assert len(aborts) == 1
    assert get_stream(db, "L", "No1")["state"] == "READY_STEP1"

    assigned = db.dispatch_all_pending()
    assert len(assigned) == 1
    assert assigned[0]["step"] == 1

    # Cycle cu (s1) khong con active; anh moi la cycle tiep theo.
    assert assigned[0]["cycle_id"] == "T1-L-No1-000000002"


def test_manual_reset_aborts_wait_step3_result(tmp_path):
    """Reset tay phai huy ca cycle dang cho ket qua Step 3."""
    db = make_db(tmp_path)

    s1 = assign_step(db, "No1", 1, root=str(tmp_path))
    complete(db, s1["event_id"], "PASS")

    s3 = assign_step(db, "No1", 3, root=str(tmp_path))
    assert get_stream(db, "L", "No1")["state"] == "WAIT_STEP3_RESULT"

    queue_image(db, "No1", "after_reset_step3", root=str(tmp_path))

    aborts = db.reset_stream("L")
    assert len(aborts) == 1
    assert aborts[0]["cycle_id"] == s3["cycle_id"]
    assert aborts[0]["error_code"] == "E601_MANUAL_SEQUENCE_RESET"

    stream = get_stream(db, "L", "No1")
    assert stream["state"] == "READY_STEP1"
    assert stream["active_event_id"] is None

    # Anh dang queue duoc gan lai la Step 1 cua cycle moi.
    assigned = db.dispatch_all_pending()
    assert len(assigned) == 1
    assert assigned[0]["step"] == 1
    assert assigned[0]["cycle_id"] != s3["cycle_id"]


def test_streams_created_lazily_per_side_and_no(tmp_path):
    db = make_db(tmp_path)

    assert db.get_streams() == []

    add_image(db, "No2", "first", root=str(tmp_path))
    add_image(db, "No1", "second", root=str(tmp_path))

    streams = db.get_streams()

    assert [(s["camera_side"], s["tube_type"]) for s in streams] == [
        ("L", "No1"),
        ("L", "No2"),
    ]

    stream_no1 = get_stream(db, "L", "No1")
    assert stream_no1["state"] == "WAIT_STEP1_RESULT"
    assert stream_no1["cycle_number"] == 1
    assert stream_no1["current_cycle_id"] == "T1-L-No1-000000001"
    assert stream_no1["active_event_id"] == "T1-L-No1-000000001-S1"


def test_blocked_out_of_sync_only_blocks_that_no_and_keeps_queue(tmp_path):
    db = make_db(tmp_path)

    no1_s1 = assign_step(db, "No1", 1, root=str(tmp_path))
    expire_stream(db, "L", "No1")
    complete(db, no1_s1["event_id"], "PASS")
    expire_stream(db, "L", "No1")
    db.timeout_incomplete_streams(timeout_sec=10)

    # Anh moi cua No1 van duoc nhan vao queue, khong bi danh dau seen.
    # auto_recover=True se tu dua OUT_OF_SYNC ve READY_STEP1 nen o day
    # kiem tra che do yeu cau reset tay (khong mat anh).
    queued = db.assign_file_to_sequence(
        make_file_info(
            f"{tmp_path}/No1/L/after_timeout.jpg", "No1", "L"
        ),
        auto_recover_out_of_sync=False,
    )
    assert queued["status"] == "QUEUED"

    # No2 tren cung side van chay binh thuong.
    result = add_image(db, "No2", "other", root=str(tmp_path))
    assert result["status"] == "ASSIGNED"
    assert result["event"]["step"] == 1

    # Manual reset -> anh dang queue cua No1 duoc gan Step 1.
    db.reset_stream("L")
    assigned = db.dispatch_all_pending()

    assert any(
        event["tube_type"] == "No1" and event["step"] == 1
        for event in assigned
    )


# Schema streams TRUOC khi doi key (PK camera_side duy nhat, co cot
# active_no) - dung de kiem tra migration tu dong khi khoi dong.
OLD_STREAMS_SCHEMA = """
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
"""


def test_migration_from_old_streams_schema(tmp_path):
    database_path = tmp_path / "agent.db"

    conn = sqlite3.connect(str(database_path))

    try:
        conn.executescript(OLD_STREAMS_SCHEMA)
        conn.execute(
            """
            INSERT INTO streams (
                camera_side, active_no, next_step, cycle_number,
                current_cycle_id, state
            )
            VALUES ('L', 'No1', 1, 0, NULL, 'RUNNING')
            """
        )
        conn.commit()

    finally:
        conn.close()

    db = CameraAgentDatabase(
        database_path=str(database_path),
        agent_id="T1",
    )

    conn = db.connect()

    try:
        columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(streams)"
            ).fetchall()
        }

    finally:
        conn.close()

    assert "active_no" not in columns
    assert "tube_type" in columns
    assert "active_event_id" in columns

    result = add_image(db, "No1", "new", root=str(tmp_path))

    assert result["status"] == "ASSIGNED"
    assert result["event"]["step"] == 1
    assert result["event"]["cycle_id"] == "T1-L-No1-000000001"
