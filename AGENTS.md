# AGENTS.md

AI tube-inspection system (Vietnamese industrial app). One process serves both the AI server and the camera agent ("merged mode"). All UI strings, log messages, config comments, and test docstrings are **Vietnamese** (often diacritic-free ASCII, e.g. "khong dau") — keep that convention; do not translate.

## Run
- Use the committed venv: `.\.venv\Scripts\python.exe` (Python 3.11, pytest 9.1.1).
- Start: `.\.venv\Scripts\python.exe run.py`. Host/port come from `server` in `config.yaml`; env overrides: `AI_CONFIG` (config file), `AI_HOST`/`AI_PORT`, `AI_OPEN_BROWSER=0` (disable auto-opening the web UI).
- Merged mode is single-process and single-worker: agent + server share in-process state (`service`, `camera_agent` globals). Never run uvicorn with `workers > 1`.

## Architecture
- `main.py` = FastAPI AI server. It mounts `camera_agent.py`'s own FastAPI app at `/` (bottom of `main.py`), so agent routes (`/api/v1/streams`, `/api/v1/reset/{L|R}`, `/api/v1/inspection-result`) are served on the same port. main.py's lifespan starts/stops the `CameraAgent`.
- Cycle = 2 images per tube: image 1 = Step 1 marker + step 1 detect; image 2 = Step 3. The agent watches a folder / UNC share, uploads to the loopback AI server, and aborts the cycle when the server webhooks Step 1 FAIL.
- Step 3 NG → serial sends a single `'0'` + CRLF over COM (fire-and-forget, no ACK). Arduino sample: `arduino/ng_signal_receiver.ino`.
- `SmartSharePoller` (in `camera_agent.py`, not to be confused with the old `PollingObserver` path): files present at a folder's first scan are baselined as "already processed" and never re-emitted. With `watch_current_day: true` only today's + yesterday's `%Y-%m-%d` folders directly under root are polled.

## Config
- `config.yaml` is the production config (NG output goes to UNC share `\\172.17.108.168\Anh AOL NG`). Use **single quotes** for UNC paths in YAML; unreachable shares fall back to `<base_dir>/ng` at startup, not an error.
- Test configs redirect everything local and run different ports: `config_test.yaml` → 8180 (`roi_rules_test_pass.csv`), `config_test_fail.yaml` → 8181. Never point them at real data; note `watch_current_day` is `false` in tests because smoke tests write `<root>/<tube>/<side>/file.jpg`.
- `data/`, `models/`, `dist/`, `build/` are gitignored: model weights (`models/AOLv2.pt`) are **not in git** — the server won't start without them, but unit tests don't load the model.
- Deliberate ordering: `sequence.server_cycle_timeout_sec` (1900) > agent `cycle_timeout_sec` (1800) so the server fail-safe fires after the agent's E204 abort. Don't "fix" this.
- Ignore `config copy.yaml` and `data copy/` — stale copies; `config.yaml` is authoritative.

## Tests
- Unit tests import `main` / `camera_agent` directly, so run pytest from the repo root (no conftest.py / pytest.ini / pyproject): `.\.venv\Scripts\python.exe -m pytest tests\`.
- Importing `main` has side effects: it loads config at import time (defaults to `config.yaml`) and creates `./data/logs/`. The YOLO model loads only inside `InspectionService.__init__`, so unit tests pass without the `.pt` file.
- E2E smoke (two windows): start the server with a test config (`$env:AI_CONFIG="config_test.yaml"; ... run.py` → 8180), then `.\.venv\Scripts\python.exe tests\smoke_merged.py --scenario pass --base-url http://127.0.0.1:8180` (fail scenario targets 8181; see `readme.md` "Test nhanh E2E").

## Build
- `pyinstaller AIInspectionServer.spec --noconfirm` → `dist\AIInspectionServer\AIInspectionServer.exe` (`static/` is bundled). Copy `config.yaml`, `roi_rules.csv`, `models\AOLv2.pt` beside the exe. Build again whenever `run.py` changes the port logic.