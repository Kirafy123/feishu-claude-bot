@echo off
echo ====== Claude Code 飞书机器人 - 开机自启动安装 ======
echo.
echo 此脚本需要管理员权限注册 Windows 任务计划
echo.

schtasks /Create /TN "ClaudeCodeFeishuBot" /XML "%~dp0feishu-bot-autostart.xml" /F

if %errorlevel% equ 0 (
    echo.
    echo 成功！服务已设置为开机自动启动
    echo 下次重启电脑后会自动启动
    echo.
    echo 手动启动: start.bat
    echo 手动停止:  stop.bat
    echo 管理任务:  taskschd.msc → 找到 "ClaudeCodeFeishuBot"
) else (
    echo.
    echo 失败！请右键此脚本 → "以管理员身份运行"
    pause
)
