#!/usr/bin/env bash
# 本地启动读经站：在「终端.app」里执行
#   chmod +x scripts/run-server.sh && ./scripts/run-server.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if ! command -v node >/dev/null 2>&1; then
  echo ""
  echo "【错误】当前环境找不到 node 命令。"
  echo "请先安装 Node.js（建议 LTS）：https://nodejs.org/"
  echo "安装后请完全退出并重新打开「终端」，再运行本脚本。"
  echo ""
  exit 1
fi

echo "node: $(command -v node)  ($(node -v))"
if [[ ! -d node_modules ]]; then
  echo "未找到 node_modules，正在执行 npm install …"
  npm install
fi

PORT="${PORT:-3000}"
if lsof -ti:"$PORT" >/dev/null 2>&1; then
  echo "端口 ${PORT} 已被占用，正在尝试结束占用进程 …"
  lsof -ti:"$PORT" | xargs kill -9 2>/dev/null || true
  sleep 1
fi

echo "启动 server.js（http://localhost:${PORT}/）…"
exec node server.js
