@echo off
cd /d "%~dp0"
echo Starting AI Inspection Server (merged: AI Server + Camera Agent) on port 8080...
if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" run.py
) else (
    python run.py
)
pause
