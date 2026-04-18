@echo off
cd /d "%~dp0"

:: 通过 PID 文件停止旧进程
if exist .pid (
    set /p pid=<.pid
    tasklist /fi "pid eq %pid%" 2>nul | find "%pid%" >nul
    if not errorlevel 1 (
        echo [1/2] 正在停止服务 (PID: %pid%)...
        taskkill /pid %pid% /f >nul 2>&1
        timeout /t 2 /nobreak >nul
    )
    del .pid
)

:: 如果没有 PID 文件，尝试停止 pythonw 进程
tasklist /fi "imagename eq pythonw.exe" 2>nul | find "pythonw" >nul
if not errorlevel 1 (
    echo [1/2] 正在停止服务 (pythonw)...
    taskkill /f /im pythonw.exe >nul 2>&1
    timeout /t 2 /nobreak >nul
)

:: 使用 pythonw.exe 后台启动（无控制台窗口）
echo [2/2] 正在启动服务...
if exist rc-venv\Scripts\pythonw.exe (
    start "" rc-venv\Scripts\pythonw.exe -m src.main_websocket
) else if exist rc-venv\Scripts\python.exe (
    start "" rc-venv\Scripts\python.exe -m src.main_websocket
) else (
    start "" pythonw.exe -m src.main_websocket
)

:: 等待启动并获取 PID
timeout /t 3 /nobreak >nul
for /f "tokens=2" %%a in ('tasklist /fi "imagename eq pythonw.exe" /fo list ^| findstr "PID:"') do (
    echo %%a > .pid
)

echo.
echo ✅ 服务已重启，日志: logs\service.log
echo.
