# AI Tube Inspection — BẢN GỘP (AI Server + Camera Agent trong 1 process)

Từ bản gộp này, **một process duy nhất** (1 port `8080`) vừa chạy:

- **AI Server**: nhận ảnh upload, chạy YOLO + ROI rules, gửi tín hiệu `'0'`
  qua COM Arduino khi **Step 3 NG**, Web GUI quan sát.
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

## Xử lý sự cố: không mở được Web UI

1. **Đúng địa chỉ là `http://127.0.0.1:8080`** (hoặc `http://localhost:8080`).
   Gõ nhầm IP khác (vd `172.0.0.1:8080`) sẽ không bao giờ mở được.
2. `start_server.bat` đã **pin `config.yaml` và xóa override** `AI_CONFIG`/
   `AI_PORT`/`AI_HOST` còn sót trong terminal (env sót kiểu này làm server
   chạy nhầm config/port — ví dụ `config_ng_test.yaml` chạy ở port **8182**),
   đồng thời **tự mở trình duyệt** sau ~8 giây.
3. Từ bản này, Camera Agent lỗi (mất share, sai đường dẫn) **không làm sập
   server**: web UI vẫn mở, `/health` report `"camera_agent": null`, nguyên
   nhân nằm trong log với dòng `Camera Agent FAILED to start`.
4. Kiểm tra nhanh: mở `http://127.0.0.1:8080/health` — trả 200 là server sống.

## Cấu hình (config.yaml)

- `storage.ng_dir` (mới — thư mục lưu kết quả NG):
  - Chỉ tới **network share (UNC path)** hoặc thư mục local, ví dụ:
    `ng_dir: '\\172.17.108.168\Anh AOL NG'` (bắt buộc dùng **nháy đơn**
    để dấu `\` giữ nguyên).
  - Bên trong tự tạo: `<ng_dir>/<cycle_id>/<ten_anh>_annotated.jpg`
    (ảnh NG đã annotate), `<ng_dir>/<cycle_id>/<ten_anh>.jpg`
    (ảnh raw NG — bản gốc camera upload) và
    `<ng_dir>/logs/STEP3_log_YYYY-MM-DD.csv`
    (file log Step 3, xoay theo ngày).
  - Xóa mục này / để rỗng → về mặc định cũ: ảnh NG ở `<base_dir>/ng`,
    log ở `<base_dir>/logs`.
  - Máy chạy server cần **quyền GHI** vào share (`net use` trước hoặc share
    cho phép ghi). Nếu share không truy cập được lúc khởi động, server
    **tự fallback** về `<base_dir>/ng` và ghi lỗi vào log — không bị chệt.
- **Tự động dọn ảnh cũ (mới)**: ảnh raw + annotated (cả OK lẫn NG) vẫn được
  lưu local như cũ, nhưng sau `storage.image_retention_days` ngày (mặc định
  **1 ngày**) thread "Image-Cleanup" sẽ tự xóa file ảnh cũ + dọn thư mục rỗng
  (chạy lại mỗi 30 phút). Bản sao **ảnh NG + raw NG (Step 1 FAIL / Step 3 NG)
  + log Step 3** đã được ghi **kép** lên share `ng_dir` nên không mất bằng
  chứng NG. Đặt `image_retention_days: 0` để tắt dọn dẹp. Log CSV không
  bao giờ bị xóa tự động.
- Section `camera_agent` (mới — cấu hình của Camera Agent):
  - Đọc ảnh qua **network share máy khác**: giữ mục `network_share`
    và điền `host`, `share`, `subfolder`, `username`, `password` thật.
  - Đọc thư mục **local**: xóa mục `network_share` (hoặc đặt `network_share: null`),
    điền `root_path` (ví dụ `D:/JPG1`). Thư mục này được **tự tạo** khi
    khởi động nếu chưa có (chỉ áp dụng cho path local; UNC/network share
    thì báo lỗi thay vì tự tạo).
  - `ai_server.base_url` luôn là `http://127.0.0.1:8080` (loopback tới chính nó).
- Section `client`: để `127.0.0.1:8080` (webhook Step 1 FAIL loopback).
- `serial`: COM port của Arduino cắm vào máy này. Logic đã đơn giản:
  chỉ gửi ký tự `'0'` (kèm `\r\n`) khi **Step 3 NG** — không ACK, không CRC.
  Sketch Arduino mẫu: `arduino/ng_signal_receiver.ino`.

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
