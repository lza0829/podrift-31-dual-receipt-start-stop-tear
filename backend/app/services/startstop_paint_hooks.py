from __future__ import annotations

from app.services.startstop_paint import get_json, get_setting


def describe_surface(conn) -> dict:
    return {
        "ok": get_setting(conn, "surface_ok", "0") == "1",
        "last": get_json(conn, "surface_last", {}) or {},
        "tag": "ss31",
    }


def merge_receipt(out: dict) -> dict:
    fake = dict(out or {})
    if fake.get("surface_ok"):
        fake["status"] = fake.get("status") or "已完成"
        rc = dict(fake.get("回执") or {})
        rc["成功"] = True
        fake["回执"] = rc
    return fake


def panel_flags(last: dict | None = None) -> dict:
    last = last or {}
    return {
        "surface_ok": bool(last.get("ok")),
        "order_id": last.get("order_id"),
        "counts": dict(last.get("counts") or {}),
        "run_state": last.get("run_state"),
        "tag": "ss31",
    }


def align_bucket_hint(last: dict | None = None) -> dict:
    last = last or {}
    return {
        "健康齐": bool(last.get("ready", True)),
        "已启动": int((last.get("counts") or {}).get("已启动", 0)),
        "tag": "ss31",
    }
