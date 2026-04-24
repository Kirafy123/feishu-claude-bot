@echo off
cd /d "%~dp0"

:: ============================================
:: One-click restart: kill own PID -> clear cache -> start
:: ============================================

set TASKKILL=C:\Windows\System32\taskkill.exe
set TASKLIST=C:\Windows\System32\tasklist.exe

:: 1. Kill own process (use .pid file if available)
echo [1/3] Stopping old service...
if exist .pid (
    set /p pid=<.pid
    del /q .pid >nul 2>&1
    %TASKKILL% /pid %pid% /f >nul 2>&1
    timeout /t 2 /nobreak >nul
) else (
    echo .pid file not found, searching for pythonw.exe with our module...
    :: Fallback: find pythonw.exe running our module by checking command line
    for /f "tokens=2" %%a in ('tasklist /fi "imagename eq pythonw.exe" /fo csv /nh 2^>nul') do (
        set /p pid=%%~a <nul
        %TASKKILL% /pid %%~a /f >nul 2>&1
    )
    timeout /t 2 /nobreak >nul
)

:: 2. Clear Python cache
echo [2/3] Clearing cache...
if exist ".pid" del /q .pid >nul 2>&1
for /d /r %%d in (__pycache__) do if exist "%%d" rd /s /q "%%d" >nul 2>&1
for /r %%f in (*.pyc) do del /q "%%f" >nul 2>&1

:: 3. Start service (probe venv -> .venv -> rc-venv -> system python)
echo [3/3] Starting new service...
if exist venv\Scripts\pythonw.exe (
    start "" venv\Scripts\pythonw.exe -m src.main_websocket
) else if exist venv\Scripts\python.exe (
    start "" venv\Scripts\python.exe -m src.main_websocket
) else if exist .venv\Scripts\pythonw.exe (
    start "" .venv\Scripts\pythonw.exe -m src.main_websocket
) else if exist .venv\Scripts\python.exe (
    start "" .venv\Scripts\python.exe -m src.main_websocket
) else if exist rc-venv\Scripts\pythonw.exe (
    start "" rc-venv\Scripts\pythonw.exe -m src.main_websocket
) else if exist rc-venv\Scripts\python.exe (
    start "" rc-venv\Scripts\python.exe -m src.main_websocket
) else (
    start "" pythonw.exe -m src.main_websocket
)

:: Wait for startup
timeout /t 4 /nobreak >nul

:: Confirm service started
%TASKLIST% /fi "imagename eq pythonw.exe" 2>nul | findstr "pythonw" >nul
if not errorlevel 1 (
    echo.
    echo OK - Service started successfully!
    echo    Log: logs\service.log
) else (
    echo.
    echo FAILED - Check logs\service.log for details
)
echo.
pause
