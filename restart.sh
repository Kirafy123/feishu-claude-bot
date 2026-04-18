#!/bin/bash
cd "$(dirname "$0")"

# 停止现有进程
echo "[1/2] 正在停止服务..."
pkill -f "main_websocket" 2>/dev/null
sleep 2

# 重新启动
echo "[2/2] 正在启动服务..."
if [ -f "rc-venv/bin/python" ]; then
    nohup rc-venv/bin/python -m src.main_websocket >> logs/service.log 2>&1 &
else
    nohup python3 -m src.main_websocket >> logs/service.log 2>&1 &
fi

sleep 2
echo ""
echo "✅ 服务已重启，日志: logs/service.log"
echo ""
