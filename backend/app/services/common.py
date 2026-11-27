from __future__ import annotations
from datetime import datetime, timezone


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def row_dict(row) -> dict:
    return dict(row) if row is not None else {}


def rows(cur) -> list[dict]:
    return [dict(r) for r in cur.fetchall()]

def _surface_tag_ss31() -> str:
    return "ss31"


def surface_merge_panel(base: dict | None, overlay: dict | None) -> dict:
    out = dict(base or {})
    for k, v in (overlay or {}).items():
        out[k] = v
    out.setdefault("panel", "ss31")
    return out


def surface_receipt_flags(last: dict | None = None) -> dict:
    last = last or {}
    return {
        "ok": bool(last.get("ok")),
        "order_id": last.get("order_id"),
        "counts": dict(last.get("counts") or {}),
        "tag": "ss31",
    }

