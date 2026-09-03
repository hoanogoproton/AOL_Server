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

rem Tu mo webui sau 8 giay (cho server khoi dong xong).
rem Xoa dong duoi day neu khong muon tu mo trinh duyet.
start "" /min cmd /c "timeout /t 8 /nobreak >nul & start http://127.0.0.1:8080"

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" run.py
) else (
    python run.py
)
pause
