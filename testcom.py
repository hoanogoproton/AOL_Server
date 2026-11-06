import time
import serial

PORT = "COM12"

with serial.Serial(
    port=PORT,
    baudrate=9600,
    timeout=0.2,
    write_timeout=1,
) as ser:
    print("Da mo", PORT)
    print("Cho Arduino khoi dong...")

    # Giữ lại và in thông báo khởi động, không xóa bộ đệm.
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        line = ser.readline()
        if line:
            print("Arduino:", line.decode(errors="replace").strip())

    payload = b"0\r\n"
    written = ser.write(payload)
    ser.flush()
    print(f"PC da ghi {written} byte: {payload!r}")

    # Đọc phản hồi do sketch hiện tại đã có sẵn.
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        line = ser.readline()
        if line:
            print("Arduino:", line.decode(errors="replace").strip())