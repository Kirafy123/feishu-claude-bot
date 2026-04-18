@echo off
cd /d "%~dp0"

:: 检查是否已运行
tasklist /fi "imagename eq python.exe" 2>nul | find /i "python" >nul
if not errorlevel 1 (
    echo 服务已在运行
    exit /b 1
)

:: 使用虚拟环境启动
if exist venv\Scripts\pythonw.exe (
    start /b venv\Scripts\pythonw.exe -m src.main_websocket >> logs\service.log 2>&1
) else (
    start /b python -m src.main_websocket >> logs\service.log 2>&1
)

echo 服务已启动，日志: logs\service.log
