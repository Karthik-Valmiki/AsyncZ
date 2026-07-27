@echo off
echo Starting AsyncZ...

:: Activate venv and start Uvicorn in a new window
start "AsyncZ - API Server" cmd /k "cd /d "%~dp0" && Myenv\Scripts\activate && uvicorn app.main:app --host 127.0.0.1 --port 8000"

:: Small delay so the API starts before the worker connects
timeout /t 2 /nobreak >nul

:: Activate venv and start ARQ worker in a new window
start "AsyncZ - Worker" cmd /k "cd /d "%~dp0" && Myenv\Scripts\activate && python -m arq app.worker.WorkerSettings"

echo Both processes started. Close the opened windows to stop them.
