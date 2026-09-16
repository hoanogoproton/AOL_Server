"""
Test chay song song nhieu No tren cung camera side.

Bug cu: bang streams chi co mot row theo camera_side (PK side, cot
active_no). Khi anh S1 cua No.2 den trong khi stream dang cho S2/S3
cua No.1, dieu kien active_no != tube_type khop -> queue abort
E201_SEQUENCE_INTERRUPTED_NO_CHANGED cho cycle No.1 -> server dat
final_result='NG' cho No.1 du Step 1 cua No.1 la OK. Voi cac No xen
ke lien tuc (song song), E201 ping-pong: anh ke tiep cua moi No lai
abort cycle cua No kia -> NG lan truyen.

Fix: key stream doi thanh (camera_side, tube_type). Moi No co chu
trinh S1->S2->S3 doc lap; anh cua No khac khong con lam abort cycle
cua No hien tai. Cycle_id them tube_type de khong trung giua cac No
vi counter cycle tang doc lap theo (side, no).

Ghi chu: dung CameraAgentDatabase truc tiep (SQLite that trong
tmp_path), khong can khoi tao CameraAgent day du.
"""

import sqlite3
import time

from camera_agent import CameraAgentDatabase, CaptureFileInfo


def make_db(tmp_path) -> CameraAgentDatabase:
    return CameraAgentDatabase(
        database_path=str(tmp_path / "agent.db"),
        agent_id="T1",
    )


def make_file_info(path, tube_type, camera_side="L") -> CaptureFileInfo:
    return CaptureFileInfo(
        path=str(path),
        tube_type=tube_type,
        camera_side=camera_side,
        observed_at="2026-09-12T08:00:00.000+07:00",
    )


def assign_step(db, no, step, side="L", root="") -> dict:
    """Gan 1 anh step cho tube, tra ve ket qua assign."""
    return db.assign_file_to_sequence(
        make_file_info(
            f"{root}/{no}/{side}/s{step}.jpg",
            tube_type=no,
            camera_side=side,
        )
    )


def assign_cycle(db, no, side="L", root="", cycle_number=1) -> dict:
    """Gan day du 3 step cho tube, tra ve ket qua step cuoi."""
    result = None

    for step in (1, 2, 3):
        result = assign_step(db, no, step, side=side, root=root)

    expected = f"T1-{side}-{no}-{cycle_number:09d}"

    assert result["status"] == "ASSIGNED"
    assert result["event"]["cycle_id"] == expected
    assert result["event"]["step"] == 3

    return result


def get_stream(db, side, no) -> dict:
    return next(
        stream
        for stream in db.get_streams()
        if stream["camera_side"] == side
        and stream["tube_type"] == no
    )


def expire_stream(db, side, no, seconds=100000.0) -> None:
    """Gia lap stream khong nhan anh moi tu rat lau."""
    with db.lock:
        conn = db.connect()

        try:
            conn.execute(
                """
                UPDATE streams
                SET last_event_unix = ?
                WHERE camera_side = ?
                AND tube_type = ?
                """,
                (time.time() - seconds, side, no),
            )
            conn.commit()

        finally:
            conn.close()


def test_interleaved_nos_all_assigned_no_abort(tmp_path):
    """
    Scenario chinh: 2 No xen ke S1, S1, S2, S2, S3, S3 tren cung side.
    Tat ca deu ASSIGNED dung step, KHONG abort nao duoc queue.
    """
    db = make_db(tmp_path)

    order = [
        ("No1", 1),
        ("No2", 1),
        ("No1", 2),
        ("No2", 2),
        ("No1", 3),
        ("No2", 3),
    ]

    for no, step in order:
        result = assign_step(db, no, step, root=str(tmp_path))

        assert result["status"] == "ASSIGNED", result
        assert result["abort"] is None
        assert result["event"]["step"] == step
        assert result["event"]["cycle_id"] == f"T1-L-{no}-000000001"
        assert result["event"]["tube_type"] == no

    # Khong co E201/E204 nao duoc queue cho 2 cycle.
    assert db.get_pending_aborts() == []

    # Ca 2 stream hoan tat: san sang Step 1 cua cycle moi.
    for no in ("No1", "No2"):
        stream = get_stream(db, "L", no)

        assert stream["state"] == "RUNNING"
        assert stream["next_step"] == 1
        assert stream["current_cycle_id"] is None
        assert stream["cycle_number"] == 1

    # Ca 6 anh deu co event (khong file nao bi nuot).
    for no in ("No1", "No2"):
        for step in (1, 2, 3):
            assert db.is_file_seen(f"{tmp_path}/{no}/L/s{step}.jpg")


def test_cycle_id_contains_tube_type_and_counters_independent(tmp_path):
    """
    Cycle_id phai chua tube_type: counter cycle tang doc lap theo
    (side, no) nen khong co tube_type se trung cycle_id giua cac No.
    """
    db = make_db(tmp_path)

    # No1 + No2 song song tren L, No1 tren R: 3 counter doc lap.
    for no in ("No1", "No2"):
        assign_cycle(db, no, side="L", root=str(tmp_path))

    assign_cycle(db, "No1", side="R", root=str(tmp_path))

    # No1 chay lai sau khi xong: counter cua No1 tang doc lap, khong
    # dung chung so voi No2.
    result = assign_step(db, "No1", 1, root=f"{tmp_path}/rerun")

    assert result["status"] == "ASSIGNED"
    assert result["event"]["step"] == 1
    assert result["event"]["cycle_id"] == "T1-L-No1-000000002"

    assert get_stream(db, "L", "No1")["cycle_number"] == 2
    assert get_stream(db, "L", "No2")["cycle_number"] == 1
    assert get_stream(db, "R", "No1")["cycle_number"] == 1


def test_step1_fail_only_affects_that_no(tmp_path):
    """
    Step 1 FAIL (AI ket luan): cancel_cycle_events + reset_stream_if_cycle
    chi tac dung dung cycle do; stream cua No khac tren cung side nguyen
    ven va van cho Step 2 binh thuong.
    """
    db = make_db(tmp_path)

    result_no1 = assign_step(db, "No1", 1, root=str(tmp_path))
    result_no2 = assign_step(db, "No2", 1, root=str(tmp_path))

    cycle_no1 = result_no1["event"]["cycle_id"]
    cycle_no2 = result_no2["event"]["cycle_id"]

    # handle_step1_fail: cancel events cua cycle FAIL + reset stream
    # neu cycle van la cycle active.
    db.cancel_cycle_events(cycle_no1)
    reset = db.reset_stream_if_cycle("L", cycle_no1)

    assert reset is not None
    assert reset["tube_type"] == "No1"

    # Stream No2 khong bi anh huong.
    stream_no2 = get_stream(db, "L", "No2")

    assert stream_no2["state"] == "RUNNING"
    assert stream_no2["next_step"] == 2
    assert stream_no2["current_cycle_id"] == cycle_no2

    # S2 cua No2 van duoc gan dung cycle cu.
    result = assign_step(db, "No2", 2, root=str(tmp_path))

    assert result["status"] == "ASSIGNED"
    assert result["event"]["cycle_id"] == cycle_no2
    assert result["event"]["step"] == 2

    # reset_stream_if_cycle khong khop cycle No1 lan nua.
    assert db.reset_stream_if_cycle("L", cycle_no1) is None

    # Stream No1 da reset: S1 moi la Step 1 cua cycle moi.
    result = assign_step(db, "No1", 1, root=f"{tmp_path}/again")

    assert result["status"] == "ASSIGNED"
    assert result["event"]["step"] == 1
    assert result["event"]["cycle_id"] == "T1-L-No1-000000002"


def test_timeout_only_aborts_stuck_stream(tmp_path):
    """
    Timeout E204 chi abort dung stream (side, no) bi ket; cac stream
    No khac tren cung side khong anh huong. Sau timeout, anh S1 moi
    cua No bi ket tu recover thanh Step 1 (auto_recover).
    """
    db = make_db(tmp_path)

    # No1 da den S1+S2 (dang cho S3), No2 moi den S1 (dang cho S2).
    assign_step(db, "No1", 1, root=str(tmp_path))
    assign_step(db, "No1", 2, root=str(tmp_path))
    assign_step(db, "No2", 1, root=str(tmp_path))

    # No1 khong co anh moi tu rat lau.
    expire_stream(db, "L", "No1")

    # Warning chi danh cho stream No1, ke ca tube_type.
    warnings = db.warn_incomplete_streams(warn_sec=500)
    assert [w["tube_type"] for w in warnings] == ["No1"]

    # E204 chi abort dung stream bi ket.
    aborts = db.timeout_incomplete_streams(timeout_sec=1800)

    assert len(aborts) == 1
    assert aborts[0]["tube_type"] == "No1"
    assert aborts[0]["error_code"] == "E204_SEQUENCE_TIMEOUT"
    assert aborts[0]["cycle_id"] == "T1-L-No1-000000001"

    assert get_stream(db, "L", "No1")["state"] == "OUT_OF_SYNC"

    stream_no2 = get_stream(db, "L", "No2")

    assert stream_no2["state"] == "RUNNING"
    assert stream_no2["next_step"] == 2

    # Che do can reset tay: anh khong phai S1 cua stream OUT_OF_SYNC
    # bi BLOCKED, khong lam anh huong No khac.
    result = db.assign_file_to_sequence(
        make_file_info(f"{tmp_path}/late/No1/L/s3.jpg", "No1", "L"),
        auto_recover_out_of_sync=False,
    )
    assert result["status"] == "BLOCKED"

    # Anh S1 moi cua No1 tu recover thanh Step 1 cycle moi.
    result = assign_step(db, "No1", 1, root=f"{tmp_path}/next")

    assert result["status"] == "ASSIGNED"
    assert result["event"]["step"] == 1
    assert result["event"]["cycle_id"] == "T1-L-No1-000000002"

    # No2 van hoan tat cycle binh thuong sau su co cua No1.
    result = assign_step(db, "No2", 2, root=str(tmp_path))
    assert result["event"]["step"] == 2

    result = assign_step(db, "No2", 3, root=str(tmp_path))
    assert result["event"]["cycle_id"] == "T1-L-No2-000000001"
    assert result["event"]["step"] == 3


def test_reset_stream_side_aborts_all_pending_cycles(tmp_path):
    """
    Reset tay 1 side: moi cycle dở cua MOI No tren side do bi queue
    abort E601; tat ca stream cua side ve RUNNING + Step 1. Side kia
    khong anh huong.
    """
    db = make_db(tmp_path)

    # L: No1 cho S3, No2 cho S2. R: No1 cho S2 - khong lien quan.
    assign_step(db, "No1", 1, root=str(tmp_path))
    assign_step(db, "No1", 2, root=str(tmp_path))
    assign_step(db, "No2", 1, root=str(tmp_path))
    assign_step(db, "No1", 1, side="R", root=str(tmp_path))

    aborts = db.reset_stream("L")

    assert len(aborts) == 2
    assert {a["tube_type"] for a in aborts} == {"No1", "No2"}
    assert all(
        a["error_code"] == "E601_MANUAL_SEQUENCE_RESET"
        for a in aborts
    )

    pending = db.get_pending_aborts()
    assert len(pending) == 2
    assert {a["tube_type"] for a in pending} == {"No1", "No2"}

    # Tat ca stream cua L ve RUNNING + Step 1.
    for no in ("No1", "No2"):
        stream = get_stream(db, "L", no)

        assert stream["state"] == "RUNNING"
        assert stream["next_step"] == 1
        assert stream["current_cycle_id"] is None

    # Stream R khong bi reset.
    stream_r = get_stream(db, "R", "No1")

    assert stream_r["next_step"] == 2
    assert stream_r["current_cycle_id"] == "T1-R-No1-000000001"


def test_streams_created_lazily_per_side_and_no(tmp_path):
    """
    Khong seed row mac dinh: stream per (side, no) duoc tao lazily khi
    anh dau tien cua cap (side, no) den; get_streams sap xep theo
    (camera_side, tube_type).
    """
    db = make_db(tmp_path)

    assert db.get_streams() == []

    # Thu tu den: No2 truoc No1 de kiem tra ORDER BY.
    assign_step(db, "No2", 1, root=str(tmp_path))
    assign_step(db, "No1", 1, root=str(tmp_path))

    streams = db.get_streams()

    assert [(s["camera_side"], s["tube_type"]) for s in streams] == [
        ("L", "No1"),
        ("L", "No2"),
    ]

    # Row dau tien: Step 1 da gan, state RUNNING, counter tu 0 -> 1.
    stream_no1 = get_stream(db, "L", "No1")

    assert stream_no1["state"] == "RUNNING"
    assert stream_no1["next_step"] == 2
    assert stream_no1["cycle_number"] == 1
    assert stream_no1["current_cycle_id"] == "T1-L-No1-000000001"


def test_blocked_out_of_sync_only_blocks_that_no(tmp_path):
    """
    Che do yeu cau reset tay (auto_recover_out_of_sync=False): stream
    OUT_OF_SYNC chi chan anh cua dung No do; No khac tren cung side
    van chay binh thuong.
    """
    db = make_db(tmp_path)

    assign_step(db, "No1", 1, root=str(tmp_path))
    expire_stream(db, "L", "No1")

    # No1 ket thuc bat thuong -> E204 dua stream ve OUT_OF_SYNC.
    aborts = db.timeout_incomplete_streams(timeout_sec=1800)
    assert len(aborts) == 1

    # Anh moi cua No1 bi BLOCKED + danh dau seen.
    result = db.assign_file_to_sequence(
        make_file_info(f"{tmp_path}/next/No1/L/s1.jpg", "No1", "L"),
        auto_recover_out_of_sync=False,
    )

    assert result["status"] == "BLOCKED"
    assert result["abort"] is None
    assert "No1" in result["message"]

    # No2 tren cung side van chay binh thuong.
    result = assign_step(db, "No2", 1, root=str(tmp_path))

    assert result["status"] == "ASSIGNED"
    assert result["event"]["step"] == 1

    # File BLOCKED bi danh dau seen: khong duoc xu ly lai.
    result = db.assign_file_to_sequence(
        make_file_info(f"{tmp_path}/next/No1/L/s1.jpg", "No1", "L"),
        auto_recover_out_of_sync=False,
    )
    assert result["status"] == "ALREADY_SEEN"


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
    """
    DB cu (schema streams theo camera_side) tu duoc migrate khi khoi
    tao: bang cu bi drop, bang moi theo (side, tube_type) duoc tao va
    anh moi cua bat ky No nao deu bat dau cycle moi binh thuong.
    """
    database_path = tmp_path / "agent.db"

    # Tao DB kieu cu voi 1 row gia lap cua side L.
    conn = sqlite3.connect(str(database_path))

    try:
        conn.executescript(OLD_STREAMS_SCHEMA)
        conn.execute(
            """
            INSERT INTO streams (
                camera_side, active_no, next_step, cycle_number,
                current_cycle_id, state
            )
            VALUES ('L', 'No1', 2, 7, 'CAM01-L-000000007', 'RUNNING')
            """
        )
        conn.commit()

    finally:
        conn.close()

    db = CameraAgentDatabase(
        database_path=str(database_path),
        agent_id="T1",
    )

    # Schema moi: khong con cot active_no, co tube_type trong PK.
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

    # State cu da mat (chap nhan - stream la transient): anh moi cua
    # No1 bat dau cycle moi theo counter moi, khong loi.
    result = assign_step(db, "No1", 1, root=str(tmp_path))

    assert result["status"] == "ASSIGNED"
    assert result["event"]["step"] == 1
    assert result["event"]["cycle_id"] == "T1-L-No1-000000001"
