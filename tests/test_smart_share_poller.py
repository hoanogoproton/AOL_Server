"""
Test SmartSharePoller - khac phuc agent "dung" khi network share co
hang tram GB / hang chuc folder:

- File ghi sau thoi diem start duoc phat hien (emit).
- File co mtime nho hon start KHONG emit (start_from_now per folder).
- File moi them vao folder cu: chi file moi duoc emit.
- Budget: so scandir moi chu ky gioi han, phan con lai chu ky sau.
- Folder bi xoa: don state, chu ky sau khong con quet.
- File bi ghi de (in-place) trong folder nho: phat hien nho quet re.
- Share mat ket noi / root chua co: khong crash, chu ky sau thu lai.
- CameraAgent.bootstrap_if_needed: smart poller bat - khong quet cay.
- watch_current_day: chi theo doi folder hom nay + hom qua (theo gio
  may Agent) ngay duoi root, bo qua cay ngay lich su khac, tu
  rollover nua dem, ho tro gio khong pad.
"""

import os
import shutil
import time
from datetime import datetime
from pathlib import Path

import pytest

from camera_agent import CameraAgent, SmartSharePoller


class _FakeDatabase:
    def __init__(self):
        self.meta = {}
        self.bootstrap_calls = []

    def get_meta(self, key):
        return self.meta.get(key)

    def set_meta(self, key, value):
        self.meta[key] = value

    def bootstrap_files(self, file_paths):
        self.bootstrap_calls.append(list(file_paths))


def make_poller(root, **kwargs):
    emitted = []
    poller = SmartSharePoller(
        root_path=root,
        on_candidate=emitted.append,
        **kwargs,
    )
    return poller, emitted


def write_image(path, fill=b"\x00"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\xFF\xD8\xFF" + fill * 32)


def age_path(path, seconds=86400.0):
    old = time.time() - seconds
    os.utime(path, (old, old))


def test_new_file_after_start_is_emitted(tmp_path):
    root = tmp_path / "share"
    (root / "T1" / "L").mkdir(parents=True)

    poller, emitted = make_poller(root)

    # Ghi file SAU khi tao poller, mtime khong nho hon start_ns.
    write_image(root / "T1" / "L" / "a.jpg")

    assert poller._run_cycle() == 1
    assert emitted == [root / "T1" / "L" / "a.jpg"]

    # Chu ky sau khong con gi moi, khong emit lap lai.
    assert poller._run_cycle() == 0
    assert emitted == [root / "T1" / "L" / "a.jpg"]


def test_old_file_not_emitted_start_from_now(tmp_path):
    root = tmp_path / "share"
    target = root / "T1" / "L"
    write_image(target / "old.jpg")

    age_path(target / "old.jpg")
    age_path(target)
    age_path(root / "T1")
    age_path(root)

    poller, emitted = make_poller(root)

    assert poller._run_cycle() == 0
    assert emitted == []

    # File van duoc ghi nhan vao state de khong quet vo ich lan sau.
    state = poller.dirs[str(target)]
    assert state.scanned is True
    assert str(target / "old.jpg") in state.files

    assert poller._run_cycle() == 0
    assert emitted == []


def test_new_file_in_old_folder_only_new_emitted(tmp_path):
    root = tmp_path / "share"
    target = root / "T1" / "L"
    write_image(target / "old.jpg")

    age_path(target / "old.jpg")
    age_path(target)
    age_path(root / "T1")
    age_path(root)

    poller, emitted = make_poller(root)
    assert poller._run_cycle() == 0
    assert emitted == []

    # Camera ghi anh moi, mtime folder doi, chi file moi duoc emit.
    write_image(target / "new.jpg")

    assert poller._run_cycle() == 1
    assert emitted == [target / "new.jpg"]
    assert poller._run_cycle() == 0


def test_budget_rotates_scans_across_cycles(tmp_path):
    root = tmp_path / "share"
    root.mkdir()

    for i in range(5):
        (root / ("T%d" % i) / "L").mkdir(parents=True)

    poller, emitted = make_poller(
        root,
        max_dir_scans_per_cycle=2,
    )

    for i in range(5):
        write_image(root / ("T%d" % i) / "L" / "a.jpg")

    total = 0
    for _ in range(20):
        per_cycle = poller._run_cycle()

        # Moi chu ky khong vuot budget (moi file den tu 1 lan quet).
        assert per_cycle <= 2

        total += per_cycle

        if total == 5:
            break

    assert total == 5
    assert len(emitted) == 5


def test_deleted_folder_is_pruned(tmp_path):
    root = tmp_path / "share"
    (root / "T1" / "L").mkdir(parents=True)

    poller, emitted = make_poller(root)

    # Ghi file sau khi tao poller de mtime moi hon start_ns.
    write_image(root / "T1" / "L" / "a.jpg")
    assert poller._run_cycle() == 1

    shutil.rmtree(root / "T1")

    poller._run_cycle()

    assert not any(
        key.startswith(str(root / "T1")) for key in poller.dirs
    )
    assert poller._run_cycle() == 0
    assert len(emitted) == 1


def test_in_place_change_in_small_folder_detected(tmp_path):
    root = tmp_path / "share"
    (root / "T1" / "L").mkdir(parents=True)

    poller, emitted = make_poller(root)

    # Ghi file sau khi tao poller de mtime moi hon start_ns.
    write_image(root / "T1" / "L" / "a.jpg")
    assert poller._run_cycle() == 1

    # Ghi de noi dung: folder chi co 1 entry nen duoc quet re moi
    # chu ky ke ca khi mtime folder khong doi.
    write_image(root / "T1" / "L" / "a.jpg", fill=b"\x01")

    assert poller._run_cycle() == 1
    assert emitted == [
        root / "T1" / "L" / "a.jpg",
        root / "T1" / "L" / "a.jpg",
    ]


def test_poller_start_stop_thread(tmp_path):
    root = tmp_path / "share"
    root.mkdir()

    poller, emitted = make_poller(root, poll_interval_sec=0.2)
    poller.start()
    assert poller._thread.is_alive()

    thread = poller._thread

    poller.stop(timeout=2.0)
    assert not thread.is_alive()
    assert emitted == []


def test_root_missing_is_tolerated(tmp_path):
    # Share mat ket noi / chua mount: khong crash, chu ky sau thu lai.
    root = tmp_path / "khong-ton-tai"

    poller, emitted = make_poller(root)
    assert poller._run_cycle() == 0

    (root / "T1" / "L").mkdir(parents=True)
    write_image(root / "T1" / "L" / "a.jpg")

    assert poller._run_cycle() == 1


# ------------------------------------------------------------
# bootstrap_if_needed khi smart poller bat
# ------------------------------------------------------------

def make_bootstrap_agent(use_smart_poller):
    agent = CameraAgent.__new__(CameraAgent)
    agent.config = {"agent": {"bootstrap_mode": "start_from_now"}}
    agent.database = _FakeDatabase()
    agent.use_smart_poller = use_smart_poller
    agent.root_path = Path(".")
    return agent


def test_bootstrap_skipped_when_smart_poller_active(tmp_path):
    agent = make_bootstrap_agent(use_smart_poller=True)
    agent.root_path = tmp_path

    write_image(tmp_path / "T1" / "L" / "old.jpg")

    agent.bootstrap_if_needed()

    assert agent.database.meta["bootstrap_initialized"] == "true"
    # Khong quet cay: khong file nao bi danh dau qua bootstrap.
    assert agent.database.bootstrap_calls == []


def test_bootstrap_skips_when_already_initialized(tmp_path):
    agent = make_bootstrap_agent(use_smart_poller=True)
    agent.database.meta["bootstrap_initialized"] = "true"
    agent.root_path = tmp_path

    agent.bootstrap_if_needed()

    assert agent.database.meta["bootstrap_initialized"] == "true"


def test_bootstrap_old_path_no_walk_when_root_missing(tmp_path):
    # use_smart_poller=False + root khong ton tai: rglob tra ve rong,
    # khong goi bootstrap_files (FakeDatabase khong co method nay,
    # neu code co goi se raise AttributeError va test fail).
    agent = make_bootstrap_agent(use_smart_poller=False)
    agent.root_path = tmp_path / "khong-ton-tai"

    agent.bootstrap_if_needed()

    assert agent.database.meta["bootstrap_initialized"] == "true"
    # Nhanh cu: van goi bootstrap_files (rong vi root khong co).
    assert agent.database.bootstrap_calls == [[]]


# ------------------------------------------------------------
# Parse config trong CameraAgent.__init__
# ------------------------------------------------------------

def make_full_config(tmp, agent_extra=None):
    agent_cfg = {
        "agent_id": "T1",
        "root_path": str(tmp / "watch"),
        "network_share": None,
        "poll_interval_sec": 1.0,
        "bootstrap_mode": "start_from_now",
        "file_stable_sec": 0.1,
        "file_check_interval_sec": 0.1,
        "reconcile_interval_sec": 5,
        "step_timeout_sec": 10,
    }
    agent_cfg.update(agent_extra or {})

    return {
        "agent": agent_cfg,
        "storage": {"database_path": str(tmp / "agent.db")},
        "ai_server": {
            "base_url": "http://127.0.0.1:1",
            "upload_timeout_sec": 1,
            "retry_interval_sec": 1,
        },
    }


def test_init_smart_poller_defaults_for_network_share(tmp_path):
    config = make_full_config(
        tmp_path,
        {"network_share": {"host": "h", "share": "s"}},
    )

    agent = CameraAgent(config)

    assert agent.poll_observer is True
    assert agent.use_smart_poller is True
    assert agent.max_dir_scans_per_cycle == 32
    assert agent.start_grace_sec == 0.0
    assert agent.watch_current_day is False
    assert agent.day_folder_format == "%Y-%m-%d"
    assert agent.share_poller is None


def test_init_smart_poller_off_for_local_path(tmp_path):
    config = make_full_config(tmp_path)

    agent = CameraAgent(config)

    assert agent.poll_observer is False
    assert agent.use_smart_poller is False


def test_init_rejects_bad_max_dir_scans(tmp_path):
    config = make_full_config(
        tmp_path,
        {
            "network_share": {"host": "h", "share": "s"},
            "max_dir_scans_per_cycle": 0,
        },
    )

    with pytest.raises(ValueError):
        CameraAgent(config)


# ------------------------------------------------------------
# watch_current_day: theo doi folder hom nay + hom qua
# ------------------------------------------------------------

def test_watch_current_day_ignores_old_day_folders(tmp_path):
    root = tmp_path / "share"
    old1 = root / "2026-09-08"
    old2 = root / "2026-09-09"
    yesterday_dir = root / "2026-09-10"
    today_dir = root / "2026-09-11"

    now_holder = {"now": datetime(2026, 9, 11, 10, 0, 0)}

    write_image(old1 / "9" / "No.1" / "L" / "old1.jpg")
    write_image(old2 / "10" / "No.2" / "R" / "old2.jpg")
    write_image(yesterday_dir / "22" / "No.3" / "L" / "y_old.jpg")
    write_image(today_dir / "9" / "No.1" / "L" / "today_old.jpg")

    poller, emitted = make_poller(
        root,
        watch_current_day=True,
        now_fn=lambda: now_holder["now"],
    )

    # Cay ngay lich su (truoc hom qua) khong duoc track / emit.
    assert poller._run_cycle() == 0
    assert emitted == []
    assert not any("2026-09-08" in k for k in poller.dirs)
    assert not any("2026-09-09" in k for k in poller.dirs)

    # Folder hom qua van duoc track (camera gio cham hon co the con
    # ghi vao do), nhung file cu hon start khong bi emit lai.
    assert str(yesterday_dir) in poller.dirs

    l_state = poller.dirs[str(yesterday_dir / "22" / "No.3" / "L")]
    assert (
        str(yesterday_dir / "22" / "No.3" / "L" / "y_old.jpg")
        in l_state.files
    )

    # Anh moi ghi vao folder hom qua SAU khi start -> emit.
    write_image(yesterday_dir / "22" / "No.3" / "L" / "y_new.jpg")

    assert poller._run_cycle() == 1
    assert emitted == [
        yesterday_dir / "22" / "No.3" / "L" / "y_new.jpg",
    ]

    # Anh moi ghi vao folder hom nay van emit binh thuong.
    write_image(today_dir / "9" / "No.1" / "L" / "today_new.jpg")

    assert poller._run_cycle() == 1
    assert emitted == [
        yesterday_dir / "22" / "No.3" / "L" / "y_new.jpg",
        today_dir / "9" / "No.1" / "L" / "today_new.jpg",
    ]
    assert str(root) in poller.dirs
    assert str(today_dir) in poller.dirs
    assert not any("2026-09-08" in k for k in poller.dirs)
    assert not any("2026-09-09" in k for k in poller.dirs)


def test_watch_current_day_folder_created_later(tmp_path):
    root = tmp_path / "share"
    now_holder = {"now": datetime(2026, 9, 11, 10, 0, 0)}

    poller, emitted = make_poller(
        root,
        watch_current_day=True,
        now_fn=lambda: now_holder["now"],
    )

    # Folder ngay hom nay chua ton tai khi start: khong crash,
    # khong track gi.
    assert poller._run_cycle() == 0
    assert emitted == []
    assert not any("2026-09-11" in k for k in poller.dirs)

    # Camera tao folder ngay + ghi anh: chu ky ke tiet lo va emit.
    today_dir = root / "2026-09-11"

    write_image(today_dir / "9" / "No.1" / "L" / "a.jpg")

    assert poller._run_cycle() == 1
    assert emitted == [
        today_dir / "9" / "No.1" / "L" / "a.jpg",
    ]
    assert str(today_dir) in poller.dirs


def test_watch_current_day_rollover_prunes_old_day_state(tmp_path):
    root = tmp_path / "share"
    now_holder = {"now": datetime(2026, 9, 11, 23, 59, 50)}

    day0 = root / "2026-09-10"  # hom qua (theo gio Agent)
    day1 = root / "2026-09-11"  # hom nay (theo gio Agent)
    (day0 / "21" / "No.0" / "L").mkdir(parents=True)
    (day1 / "9" / "No.1" / "L").mkdir(parents=True)

    poller, emitted = make_poller(
        root,
        watch_current_day=True,
        now_fn=lambda: now_holder["now"],
    )

    # Ghi file SAU khi tao poller de mtime khong nho hon start_ns.
    write_image(day1 / "9" / "No.1" / "L" / "night.jpg")

    assert poller._run_cycle() == 1
    assert str(day1) in poller.dirs
    assert str(day0) in poller.dirs

    # Sang ngay moi: state folder TRUOC hom qua (day0) bi don, folder
    # hom nay cu (day1 - gio la hom qua) van duoc giu de camera con
    # ghi vao do, folder ngay moi (day2) duoc theo doi.
    now_holder["now"] = datetime(2026, 9, 12, 0, 0, 10)

    write_image(day1 / "9" / "No.1" / "L" / "after_midnight.jpg")

    poller._run_cycle()

    assert str(day1) in poller.dirs
    assert not any("2026-09-10" in k for k in poller.dirs)
    assert poller.stats["day_folder"] == "2026-09-12"

    # Anh ghi vao folder hom qua sau rollover van duoc emit (co the
    # tre 1-2 chu ky do co che quet re cua poller, khong mat du lieu).
    for _ in range(5):
        poller._run_cycle()

        if len(emitted) == 2:
            break

    assert emitted == [
        day1 / "9" / "No.1" / "L" / "night.jpg",
        day1 / "9" / "No.1" / "L" / "after_midnight.jpg",
    ]

    # Camera bat dau ghi vao folder ngay moi.
    day2 = root / "2026-09-12"

    write_image(day2 / "1" / "No.2" / "R" / "morning.jpg")

    for _ in range(10):
        poller._run_cycle()

        if len(emitted) == 3:
            break

    assert emitted == [
        day1 / "9" / "No.1" / "L" / "night.jpg",
        day1 / "9" / "No.1" / "L" / "after_midnight.jpg",
        day2 / "1" / "No.2" / "R" / "morning.jpg",
    ]
    assert str(day2) in poller.dirs


def test_watch_current_day_tracks_yesterday_for_late_camera(tmp_path):
    # May camera gio CHAM hon may Agent: vua sau nua dem cua Agent,
    # camera van ghi vao folder ngay cu. Folder hom qua phai van duoc
    # theo doi de anh trong khung nay khong bi mat.
    root = tmp_path / "share"
    now_holder = {"now": datetime(2026, 9, 12, 0, 0, 30)}

    yesterday_dir = root / "2026-09-11"
    (yesterday_dir / "23" / "No.1" / "L").mkdir(parents=True)

    poller, emitted = make_poller(
        root,
        watch_current_day=True,
        now_fn=lambda: now_holder["now"],
    )

    # Chu ky dau: track folder hom qua + hom nay (chua co file moi).
    assert poller._run_cycle() == 0

    # Camera (van o ngay 09-11 theo gio cua no) ghi anh moi.
    write_image(yesterday_dir / "23" / "No.1" / "L" / "late.jpg")

    assert poller._run_cycle() == 1
    assert emitted == [
        yesterday_dir / "23" / "No.1" / "L" / "late.jpg",
    ]

    # Sang ngay ke tiep: folder 09-11 (bay gio la truoc hom qua) bi
    # prune, khong con track.
    now_holder["now"] = datetime(2026, 9, 13, 0, 0, 30)

    poller._run_cycle()

    assert not any("2026-09-11" in k for k in poller.dirs)
    assert poller.stats["day_folder"] == "2026-09-13"


def test_watch_current_day_deep_unpadded_hour_folder(tmp_path):
    root = tmp_path / "share"
    now_holder = {"now": datetime(2026, 9, 11, 9, 30, 0)}

    poller, emitted = make_poller(
        root,
        watch_current_day=True,
        now_fn=lambda: now_holder["now"],
    )

    # Gio khong pad so 0 ("9" chu khong phai "09"): moi cap duoi
    # folder ngay duoc poller kham pha dong nhu binh thuong.
    deep = root / "2026-09-11" / "9" / "No.1" / "L"

    write_image(deep / "a.jpg")

    assert poller._run_cycle() == 1
    assert emitted == [deep / "a.jpg"]
    assert str(deep) in poller.dirs
    assert poller._run_cycle() == 0
