@echo off
cd /d "%~dp0"

:: 检查是否已运行
tasklist /fi "imagename eq pythonw.exe" 2>nul | find /i "pythonw" >nul
if not errorlevel 1 (
    echo 服务已在运行
    exit /b 1
)

:: 确保日志目录存在
if not exist logs mkdir logs

:: 启动服务（探测 venv -> .venv -> rc-venv -> 系统 python）
if exist venv\Scripts\pythonw.exe (
    start "" venv\Scripts\pythonw.exe -m src.main_websocket
) else if exist .venv\Scripts\pythonw.exe (
    start "" .venv\Scripts\pythonw.exe -m src.main_websocket
) else if exist rc-venv\Scripts\pythonw.exe (
    start "" rc-venv\Scripts\pythonw.exe -m src.main_websocket
) else (
    start "" pythonw.exe -m src.main_websocket
)

echo 服务已启动，日志: logs\service.log
