#!/bin/bash

cd "$(dirname "$0")"

# 检查是否已运行
if [ -f .pid ]; then
    pid=$(cat .pid)
    if ps -p $pid > /dev/null 2>&1; then
        echo "服务已在运行 (PID: $pid)"
        exit 1
    fi
fi

# 选择 Python 解释器：优先用户指定 > venv > python3.11 > python3 > python
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
elif command -v python >/dev/null 2>&1; then
    PY="python"
else
    echo "❌ 找不到 Python 解释器，请设置 PYTHON_BIN 环境变量" >&2
    exit 1
fi

echo "使用 Python: $PY"

# 后台启动
nohup "$PY" -m src.main_websocket > log.log 2>&1 &
echo $! > .pid

echo "服务已启动 (PID: $!)"
echo "日志: tail -f log.log"
