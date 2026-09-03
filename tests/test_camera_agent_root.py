"""
Test CameraAgent.ensure_root_path():

- Local path chua co -> tu tao (kem thu muc cha).
- Local path da ton tai -> noop.
- root_path la file -> bao loi cau hinh sai.
- Network share / UNC chua co -> KHONG tu tao, bao loi SMB.

Ghi chu: khoi tao agent bang __new__ de tranh side-effect cua
__init__ (database, threads, ...).
"""

from pathlib import Path

import pytest

import camera_agent
from camera_agent import CameraAgent


def make_agent(root_path, network_share=None) -> CameraAgent:
    agent = CameraAgent.__new__(CameraAgent)
    agent.root_path = Path(root_path)
    agent.network_share = network_share
    return agent


def test_local_root_path_auto_created(tmp_path):
    target = tmp_path / "watch"

    agent = make_agent(target)
    agent.ensure_root_path()

    assert target.is_dir()


def test_local_root_path_nested_auto_created(tmp_path):
    target = tmp_path / "data" / "test_ng" / "watch"

    agent = make_agent(target)
    agent.ensure_root_path()

    assert target.is_dir()


def test_existing_dir_is_noop(tmp_path):
    agent = make_agent(tmp_path)

    agent.ensure_root_path()  # khong raise

    assert tmp_path.is_dir()


def test_root_path_is_file_raises(tmp_path):
    file_path = tmp_path / "watch"
    file_path.write_text("not a dir", encoding="utf-8")

    agent = make_agent(file_path)

    with pytest.raises(RuntimeError) as exc_info:
        agent.ensure_root_path()

    assert "not a directory" in str(exc_info.value)


def test_unc_root_path_not_created(monkeypatch):
    # Gia lap is_dir/exists de khong probe mang that o UNC fake host.
    monkeypatch.setattr(camera_agent.Path, "is_dir", lambda self: False)
    monkeypatch.setattr(camera_agent.Path, "exists", lambda self: False)

    agent = make_agent(
        r"\\some-host\some-share\sub",
        network_share={"host": "some-host", "share": "some-share"},
    )

    with pytest.raises(RuntimeError) as exc_info:
        agent.ensure_root_path()

    assert "network share" in str(exc_info.value)


def test_network_share_local_path_not_created(tmp_path):
    # network_share dang bat -> khong tu tao du khi path la local-style.
    agent = make_agent(
        tmp_path / "watch",
        network_share={"host": "h", "share": "s"},
    )

    with pytest.raises(RuntimeError) as exc_info:
        agent.ensure_root_path()

    assert "network share" in str(exc_info.value)
    assert not (tmp_path / "watch").exists()