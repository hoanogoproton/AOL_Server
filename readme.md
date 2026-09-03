# AI Tube Inspection — BẢN GỘP (AI Server + Camera Agent trong 1 process)

Từ bản gộp này, **một process duy nhất** (1 port `8080`) vừa chạy:

- **AI Server**: nhận ảnh upload, chạy YOLO + ROI rules, gửi kết quả qua COM Arduino, Web GUI quan sát.
- **Camera Agent** (`camera_agent.py`): theo dõi thư mục ảnh camera (**network share** hoặc local),
  quản lý sequence Step 1→2→3, upload ảnh lên AI Server qua loopback,
  nhận webhook Step 1 FAIL để abort cycle + reset stream.

## Cài thư viện

```
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r .\requirements.txt
```

## Chạy (khuyến nghị dùng venv)

```
.\.venv\Scripts\python.exe run.py
```

hoặc:

```
uvicorn main:app --host 0.0.0.0 --port 8080 --workers 1
```

hoặc double-click `start_server.bat`.

> ⚠️ Bắt buộc chạy `workers=1`: agent + server dùng chung trạng thái trong cùng process.

## URL sau khi gộp (cùng port 8080)

| Chức năng | URL |
|---|---|
| Web GUI | http://localhost:8080/ |
| Health (kèm trạng thái camera agent) | http://localhost:8080/health |
| Streams của agent | http://localhost:8080/api/v1/streams |
| Reset sequence tay | POST http://localhost:8080/api/v1/reset/L (hoặc /R) |
| Upload ảnh (agent tự gọi - loopback) | POST /api/v1/images |
| Webhook Step 1 FAIL (loopback) | POST /api/v1/inspection-result |
| Abort cycle (agent tự gọi - loopback) | POST /api/v1/cycles/abort |

## Cấu hình (config.yaml)

- Section `camera_agent` (mới — cấu hình của Camera Agent):
  - Đọc ảnh qua **network share máy khác**: giữ mục `network_share`
    và điền `host`, `share`, `subfolder`, `username`, `password` thật.
  - Đọc thư mục **local**: xóa mục `network_share` (hoặc đặt `network_share: null`),
    điền `root_path` (ví dụ `D:/JPG1`).
  - `ai_server.base_url` luôn là `http://127.0.0.1:8080` (loopback tới chính nó).
- Section `client`: để `127.0.0.1:8080` (webhook Step 1 FAIL loopback).
- `serial`: COM port của Arduino cắm vào máy này.

## Build exe (PyInstaller — 1 exe duy nhất)

```
pyinstaller AIInspectionServer.spec --noconfirm
```

Kết quả: `dist\AIInspectionServer\AIInspectionServer.exe`.
Copy kèm cạnh exe: `config.yaml`, `roi_rules.csv`, `models\AOLv2.pt`
(thư mục `static` đã được đóng gói sẵn trong exe).

## Test nhanh E2E (không đụng dữ liệu thật)

`run.py` tự lấy host/port từ section `server` trong config (env `AI_HOST`/`AI_PORT`
vẫn override được nếu muốn).

Scenario 1 — cycle hoàn chỉnh OK (rule test cho phép ảnh rỗng PASS):

```
$env:AI_CONFIG="config_test.yaml"; .\.venv\Scripts\python.exe run.py     # listen 8180
# cửa sổ khác:
.\.venv\Scripts\python.exe tests\smoke_merged.py --scenario pass --base-url http://127.0.0.1:8180
```

Scenario 2 — Step 1 FAIL (webhook loopback + abort + reset stream):

```
$env:AI_CONFIG="config_test_fail.yaml"; .\.venv\Scripts\python.exe run.py   # listen 8181
# cửa sổ khác:
.\.venv\Scripts\python.exe tests\smoke_merged.py --scenario fail --base-url http://127.0.0.1:8181
```

> ⚠️ Khi deploy exe: `server.port` trong `config.yaml` phải trùng với port exe đang
> lắng nghe (đã là 8080 mặc định) vì agent upload/webhook đều đi qua port đó.
> Build lại exe sau khi có bản `run.py` mới (tự đọc port từ config).
