@echo off
rem ============================================================
rem  Khoi dong AI Inspection Server (merged: AI Server + Camera
rem  Agent) - che do PRODUCTION: config.yaml, port 8080.
rem  Bat nay PIN config va xoa override AI_CONFIG/AI_PORT/AI_HOST
rem  con sot trong moi truong -> luon dung dung config + dung port.
rem ============================================================
cd /d "%~dp0"

set "AI_CONFIG=config.yaml"
set "AI_PORT="
set "AI_HOST="

echo ============================================================
echo  AI Inspection Server (production)
echo  Web UI : http://127.0.0.1:8080
echo  Health : http://127.0.0.1:8080/health
echo  Dung server: dong cua so nay hoac nhan CTRL+C
echo ============================================================

rem Tu mo webui da xu ly trong run.py (poll /health den khi 200 moi
rem mo trinh duyet -> khong con hack cho 8 giay nua). Tinh nang nay
rem BON theo config server.open_browser (hoac env AI_OPEN_BROWSER).
rem Xoa override AI_OPEN_BROWSER con sot trong terminal:
set "AI_OPEN_BROWSER="

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" run.py
) else (
    python run.py
)
pause
