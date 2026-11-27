from __future__ import annotations
import os
from pathlib import Path

PROJECT_NAME = "舱隙"
DB_PATH = Path(os.environ.get("PODRIFT_DB", str(Path(__file__).resolve().parents[2] / "data" / "podrift.db")))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# 启停对砸时序参数（毫秒），可用环境变量覆盖，夹具里用小值
GATE_TIMEOUT_MS = int(os.environ.get("PODRIFT_GATE_TIMEOUT_MS", "3000"))
LOCK_TTL_MS = int(os.environ.get("PODRIFT_LOCK_TTL_MS", "5000"))
LOCK_WAIT_MS = int(os.environ.get("PODRIFT_LOCK_WAIT_MS", "8000"))
POLL_MS = int(os.environ.get("PODRIFT_POLL_MS", "40"))
CONTENTION_WINDOW_MS = int(os.environ.get("PODRIFT_CONTENTION_WINDOW_MS", "4000"))
