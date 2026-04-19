#!/bin/bash
cd "$(dirname "$0")"

# 停止现有进程
echo "[1/2] 正在停止服务..."
pkill -f "main_websocket" 2>/dev/null
sleep 2

# 选择 Python 解释器（同 start.sh）
if [ -n "$PYTHON_BIN" ] && command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    PY="$PYTHON_BIN"
elif [ -x ".venv/bin/python" ]; then
    PY=".venv/bin/python"
elif [ -x "venv/bin/python" ]; then
    PY="venv/bin/python"
elif command -v python3.11 >/dev/null 2>&1; then
    PY="python3.11"
elif command -v python3 >/dev/null 2>&1; then
    PY="python3"
else
    echo "❌ 找不到 Python 解释器" >&2
    exit 1
fi

echo "[2/2] 正在启动服务 ($PY)..."
mkdir -p logs
nohup "$PY" -m src.main_websocket >> logs/service.log 2>&1 &
echo $! > .pid

sleep 2
echo ""
echo "✅ 服务已重启 (PID: $(cat .pid))，日志: logs/service.log"
echo ""
