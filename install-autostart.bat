@echo off
setlocal enabledelayedexpansion

echo ====== Claude Code 飞书机器人 - 开机自启动安装 ======
echo.
echo 此脚本需要管理员权限注册 Windows 任务计划
echo.

:: 获取当前脚本所在目录的绝对路径（自动适配安装位置）
set "SCRIPT_DIR=%~dp0"
set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"
set "BAT_PATH=%SCRIPT_DIR%\start.bat"

echo 安装路径: %SCRIPT_DIR%
echo.

:: 动态生成 XML 文件（替换硬编码路径）
(
echo ^<?xml version="1.0" encoding="UTF-8"?^>
echo ^<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task"^>
echo   ^<RegistrationInfo^>
echo     ^<Description^>Claude Code 飞书机器人服务^</Description^>
echo   ^</RegistrationInfo^>
echo   ^<Triggers^>
echo     ^<LogonTrigger^>
echo       ^<Enabled^>true^</Enabled^>
echo     ^</LogonTrigger^>
echo   ^</Triggers^>
echo   ^<Principals^>
echo     ^<Principal id="Author"^>
echo       ^<LogonType^>InteractiveToken^</LogonType^>
echo       ^<RunLevel^>HighestAvailable^</RunLevel^>
echo     ^</Principal^>
echo   ^</Principals^>
echo   ^<Settings^>
echo     ^<MultipleInstancesPolicy^>IgnoreNew^</MultipleInstancesPolicy^>
echo     ^<DisallowStartIfOnBatteries^>false^</DisallowStartIfOnBatteries^>
echo     ^<StopIfGoingOnBatteries^>false^</StopIfGoingOnBatteries^>
echo     ^<AllowHardTerminate^>false^</AllowHardTerminate^>
echo     ^<StartWhenAvailable^>true^</StartWhenAvailable^>
echo     ^<Enabled^>true^</Enabled^>
echo   ^</Settings^>
echo   ^<Actions Context="Author"^>
echo     ^<Exec^>
echo       ^<Command^>!BAT_PATH!^</Command^>
echo       ^<WorkingDirectory^>!SCRIPT_DIR!^</WorkingDirectory^>
echo     ^</Exec^>
echo   ^</Actions^>
echo ^</Task^>
) > "%TEMP%\feishu-bot-autostart.xml"

schtasks /Create /TN "ClaudeCodeFeishuBot" /XML "%TEMP%\feishu-bot-autostart.xml" /F
del "%TEMP%\feishu-bot-autostart.xml"

if %errorlevel% equ 0 (
    echo.
    echo 成功！服务已设置为开机自动启动
    echo 下次重启电脑后会自动启动
    echo.
    echo 手动启动: start.bat
    echo 手动停止: stop.bat
    echo 管理任务: taskschd.msc → 找到 "ClaudeCodeFeishuBot"
) else (
    echo.
    echo 失败！请右键此脚本 → "以管理员身份运行"
    pause
)
