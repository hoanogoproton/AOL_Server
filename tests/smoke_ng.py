"""
E2E smoke test - SCENARIO NG: Step 3 phat hien san pham loi.

Kiem chung: anh Step 3 khong co san pham OK -> step3_status='NG'
-> cycle final_result='NG' -> server goi signal_sender.send_ng_signal()
(serial disabled trong test nen chi log "Skip NG signal").

Cach dung:
1. Chay server voi config test:
       $env:AI_CONFIG="config_ng_test.yaml"
       .\\.venv\\Scripts\\python.exe run.py      # listen 8182
2. Chay script nay:
       .\\.venv\\Scripts\\python.exe tests\\smoke_ng.py --base-url http://127.0.0.1:8182
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
    parser.add_argument("--base-url", default="http://127.0.0.1:8182")
    parser.add_argument("--watch-dir", default="./data/test_ng/watch")
    parser.add_argument("--tube", default="TESTNG")
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    watch_dir = Path(args.watch_dir)

    print("== 1. Cho /health san sang (YOLO load co the mat vai chuc giay)...")
    wait_health(base_url, timeout_sec=args.timeout + 120)
    print("   OK: Server san sang")

    print("== 2. Ghi anh Step 1...")
    write_image(watch_dir, args.tube, "L", "ng_001.jpg")

    wait_for(
        lambda: get_streams(base_url)["L"]["next_step"] == 2,
        timeout_sec=args.timeout,
        desc="L chuyen next_step=2 (Step 1 assigned)",
    )
    print("   OK: Step 1 assigned")

    print("== 3. Doi Step 1 PASS tren AI Server...")
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
    write_image(watch_dir, args.tube, "L", "ng_002.jpg")

    wait_for(
        lambda: get_streams(base_url)["L"]["next_step"] == 3,
        timeout_sec=args.timeout,
        desc="L chuyen next_step=3 (Step 2 assigned)",
    )
    print("   OK: Step 2 assigned")

    print("== 5. Ghi anh Step 3 (anh den -> 0 san pham OK -> Step 3 se NG)...")
    write_image(watch_dir, args.tube, "L", "ng_003.jpg")

    print("== 6. Doi final_result=NG + step3_status=NG...")
    wait_for(
        lambda: any(
            c["final_result"] == "NG" and c["step3_status"] == "NG"
            for c in get_cycles(base_url)
        ),
        timeout_sec=args.timeout,
        desc="final_result=NG va step3_status=NG",
    )
    print("   OK: Step 3 NG -> cycle NG (tin hieu '0' da duoc kich hoat)")

    cycle = next(
        c for c in get_cycles(base_url)
        if c["final_result"] == "NG"
    )
    assert cycle["final_error"] != "NONE", "NG cycle phai co error code"
    print(f"   OK: final_error={cycle['final_error']}")

    print("\nSMOKE NG SCENARIO: ALL OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
