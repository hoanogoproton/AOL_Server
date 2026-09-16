from main import app, CONFIG, LOGGER
import uvicorn
import os
import threading
import time
import urllib.request
import webbrowser

# Thời gian tối đa chờ server sống (giây) trước khi bỏ qua việc mở trình duyệt.
OPEN_BROWSER_TIMEOUT_SEC = 60
OPEN_BROWSER_POLL_INTERVAL_SEC = 0.5


def open_browser_when_ready(host: str, port: int) -> None:
    """
    Tự mở trình duyệt Web UI sau khi server KHỞI CHẠY THÀNH CÔNG.

    Hàm chạy trong background thread (start trước khi uvicorn chặn main
    thread). Poll /health qua loopback đến khi trả 200 mới gọi
    webbrowser.open -> trình duyệt chỉ mở khi server thực sự sống
    (service + model đã start xong). Nếu exe boot lỗi / port bị chiếm,
    thread tự bỏ sau OPEN_BROWSER_TIMEOUT_SEC — không mở trang lỗi.

    Web UI luôn mở qua 127.0.0.1 (localhost) kể cả khi server listen
    0.0.0.0 — gõ URL '0.0.0.0' trên Windows sẽ không hoạt động.
    """
    loopback_host = "127.0.0.1" if host in ("0.0.0.0", "::", "", "*") else host
    health_url = f"http://{loopback_host}:{port}/health"
    ui_url = f"http://{loopback_host}:{port}/"

    deadline = time.time() + OPEN_BROWSER_TIMEOUT_SEC

    while time.time() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=2) as resp:
                if resp.status == 200:
                    try:
                        webbrowser.open(ui_url)
                        LOGGER.info("Web UI tu dong mo: %s", ui_url)
                    except Exception:
                        LOGGER.exception(
                            "Khong mo duoc trinh duyet cho %s", ui_url
                        )
                    return
        except Exception:
            # Server chưa sống (đang boot model / port chưa bind) -> thử lại.
            pass

        time.sleep(OPEN_BROWSER_POLL_INTERVAL_SEC)

    LOGGER.warning(
        "Server chua san sang sau %s gi -> bo qua viec tu mo trinh duyet "
        "(mo tay: %s)",
        OPEN_BROWSER_TIMEOUT_SEC,
        ui_url,
    )


def open_browser_enabled() -> bool:
    """
    Quyết định có bật 'tự mở trình duyệt' hay không:
    - Env AI_OPEN_BROWSER luôn thắng config:
        AI_OPEN_BROWSER=0 (hoặc false/no) -> TẮT
        AI_OPEN_BROWSER=1 (hoặc true/yes) -> BẬT
    - Không có env -> lấy 'server.open_browser' trong config (mặc định: BẬT).
    """
    env_value = os.environ.get("AI_OPEN_BROWSER")

    if env_value is not None:
        return env_value.strip().lower() in ("1", "true", "yes", "on")

    return bool((CONFIG.get("server") or {}).get("open_browser", True))


if __name__ == "__main__":
    # Ưu tiên env AI_HOST/AI_PORT, nếu không có thì lấy section 'server'
    # trong config.yaml -> port luôn khớp với webhook/loopback của agent.
    server_cfg = CONFIG.get("server") or {}

    host = os.environ.get("AI_HOST", str(server_cfg.get("host", "0.0.0.0")))
    port = int(os.environ.get("AI_PORT", server_cfg.get("port", 8080)))

    # Tính năng mới: sau khi server khởi chạy thành công (health 200) sẽ
    # tự mở trang localhost. Poll chạy ở background thread vì uvicorn.run
    # chặn main thread cho đến khi server dừng.
    if open_browser_enabled():
        threading.Thread(
            target=open_browser_when_ready,
            args=(host, port),
            name="OpenBrowser",
            daemon=True,
        ).start()
    else:
        LOGGER.info(
            "Tu mo trinh duyet DANG TAT "
            "(AI_OPEN_BROWSER / server.open_browser)"
        )

    # Bắt buộc workers=1: agent + server dùng chung trạng thái trong process.
    uvicorn.run(app, host=host, port=port, workers=1)