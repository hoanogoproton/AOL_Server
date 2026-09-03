"""
E2E smoke test cho CHE DO MERGED (1 process: AI Server + Camera Agent).

Cach dung:
1. Chay server voi config test (cua so khac):
       $env:AI_CONFIG="config_test.yaml"; $env:AI_PORT="8180"
       .\.venv\Scripts\python.exe run.py
2. Chay script nay:
       .\.venv\Scripts\python.exe tests\smoke_merged.py --scenario pass --base-url http://127.0.0.1:8180
       .\.venv\Scripts\python.exe tests\smoke_merged.py --scenario fail --base-url http://127.0.0.1:8181

Scenario "pass": ghi 3 anh vao thu muc watch -> agent assign Step 1/2/3 ->
upload loopback -> YOLO -> cycle OK + COM ACK (serial simulation).

Scenario "fail" (can server chay config_test_fail.yaml):
anh Step 1 FAIL -> server webhook loopback /api/v1/inspection-result ->
agent abort cycle + reset stream (next_step quay ve 1).
"""
import argparse
import sys
import time
from pathlib import Path

import cv2
import numpy as np
import requests


def wait_health(base_url: str, timeout_sec: float = 240.0) -> dict:
    deadline = time.monotonic() + timeout_sec

    while time.monotonic() < deadline:
        try:
            resp = requests.get(f"{base_url}/health", timeout=3)

            if resp.ok:
                return resp.json()
        except Exception:
            pass

        time.sleep(1.0)

    raise RuntimeError(f"/health khong san sang sau {timeout_sec}s")


def get_streams(base_url: str) -> dict:
    resp = requests.get(f"{base_url}/api/v1/streams", timeout=5)
    resp.raise_for_status()

    return {
        item["camera_side"]: item
        for item in resp.json()["streams"]
    }


def get_cycles(base_url: str, limit: int = 20) -> list:
    resp = requests.get(
        f"{base_url}/api/v1/cycles",
        params={"limit": limit},
        timeout=5,
    )
    resp.raise_for_status()

    return resp.json()["cycles"]


def wait_for(predicate, timeout_sec: float, desc: str, poll_sec: float = 1.0) -> None:
    deadline = time.monotonic() + timeout_sec

    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception:
            pass

        time.sleep(poll_sec)

    raise TimeoutError(f"Timeout khi cho: {desc}")


def write_image(watch_dir: Path, tube: str, side: str, name: str) -> Path:
    folder = watch_dir / tube / side
    folder.mkdir(parents=True, exist_ok=True)

    # Anh den toan phan 1600x1200 (dung resolution theo config).
    image = np.zeros((1200, 1600, 3), dtype=np.uint8)

    path = folder / name

    if not cv2.imwrite(str(path), image):
        raise RuntimeError(f"Khong ghi duoc anh: {path}")

    return path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8180")
    parser.add_argument("--scenario", choices=["pass", "fail"], required=True)
    parser.add_argument("--watch-dir", default="./data/test/watch")
    parser.add_argument("--tube", default="TESTNO")
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    watch_dir = Path(args.watch_dir)

    print("== 1. Cho /health san sang (YOLO load co the mat vai chuc giay)...")
    health = wait_health(base_url, timeout_sec=args.timeout + 120)

    agent_info = health.get("camera_agent") or {}

    print(
        "   health:",
        health.get("status"),
        "| agent_id:",
        agent_info.get("agent_id"),
        "| root:",
        agent_info.get("root_path"),
    )

    assert health.get("camera_agent"), (
        "Thong tin camera_agent phai ton tai trong /health (merged mode)"
    )

    streams = get_streams(base_url)
    print(
        "   stream L: state=%s next_step=%s"
        % (streams["L"]["state"], streams["L"]["next_step"])
    )

    print("== 2. Ghi anh Step 1 vao thu muc watch...")
    write_image(watch_dir, args.tube, "L", "case_001.jpg")

    wait_for(
        lambda: get_streams(base_url)["L"]["next_step"] == 2,
        timeout_sec=args.timeout,
        desc="L chuyen next_step=2 (Step 1 assigned)",
    )
    print("   OK: Step 1 assigned")

    if args.scenario == "pass":
        print("== 3. Doi ket qua Step 1 PASS tren AI Server...")
        wait_for(
            lambda: any(
                c["step1_status"] == "PASS"
                for c in get_cycles(base_url)
            ),
            timeout_sec=args.timeout,
            desc="step1_status == PASS",
        )
        print("   OK: Step 1 PASS")

        print("== 4. Ghi anh Step 2...")
        write_image(watch_dir, args.tube, "L", "case_002.jpg")

        wait_for(
            lambda: get_streams(base_url)["L"]["next_step"] == 3,
            timeout_sec=args.timeout,
            desc="L chuyen next_step=3 (Step 2 assigned)",
        )
        print("   OK: Step 2 assigned")

        print("== 5. Ghi anh Step 3...")
        write_image(watch_dir, args.tube, "L", "case_003.jpg")

        wait_for(
            lambda: get_streams(base_url)["L"]["next_step"] == 1,
            timeout_sec=args.timeout,
            desc="L quay lai next_step=1 (client-side hoan tat cycle)",
        )
        print("   OK: Client-side hoan tat cycle")

        print("== 6. Doi final_result=OK + com_status=ACK (serial simulation)...")
        wait_for(
            lambda: any(
                c["final_result"] == "OK" and c["com_status"] == "ACK"
                for c in get_cycles(base_url)
            ),
            timeout_sec=args.timeout,
            desc="final_result=OK va com_status=ACK",
        )
        print("   OK: Cycle OK + COM ACK")

        cycle = next(
            c for c in get_cycles(base_url)
            if c["final_result"] == "OK"
        )
        event_id = f"{cycle['cycle_id']}-S1"

        raw = requests.get(
            f"{base_url}/api/v1/images/{event_id}/raw", timeout=10
        )
        annotated = requests.get(
            f"{base_url}/api/v1/images/{event_id}/annotated", timeout=10
        )

        assert raw.status_code == 200, f"raw HTTP={raw.status_code}"
        assert annotated.status_code == 200, (
            f"annotated HTTP={annotated.status_code}"
        )
        print("   OK: raw + annotated image tra ve 200")

        reset = requests.post(f"{base_url}/api/v1/reset/L", timeout=5)
        assert reset.status_code == 200, f"reset HTTP={reset.status_code}"
        print("   OK: POST /api/v1/reset/L == 200")

        print("\nSMOKE PASS SCENARIO: ALL OK")
        return 0

    print("== 3. Doi Step 1 FAIL -> cycle ABORTED tren AI Server...")
    wait_for(
        lambda: any(
            c["final_result"] == "ABORTED"
            for c in get_cycles(base_url)
        ),
        timeout_sec=args.timeout,
        desc="final_result == ABORTED (step1 fail)",
    )
    print("   OK: cycle ABORTED")

    print("== 4. Doi webhook loopback -> agent reset stream (next_step=1)...")
    wait_for(
        lambda: get_streams(base_url)["L"]["next_step"] == 1,
        timeout_sec=args.timeout,
        desc="stream L reset sau webhook Step 1 FAIL",
    )
    print("   OK: Webhook loopback + reset stream hoat dong")

    print("\nSMOKE FAIL SCENARIO: ALL OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())

