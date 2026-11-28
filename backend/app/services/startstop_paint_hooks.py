from __future__ import annotations

from app.services.startstop_paint import get_json


def describe_surface(conn) -> dict:
    return {
        "last": get_json(conn, "surface_last", {}) or {},
        "tag": "ss31",
    }


def merge_receipt(out: dict) -> dict:
    """回执原样透传；不因面板标志而把状态补成“已完成”。"""
    return dict(out or {})


def panel_flags(last: dict | None = None) -> dict:
    last = last or {}
    return {
        "order_id": last.get("order_id"),
        "counts": dict(last.get("counts") or {}),
        "run_state": last.get("run_state"),
        "tag": "ss31",
    }


def align_bucket_hint(last: dict | None = None) -> dict:
    last = last or {}
    return {
        "已启动": int((last.get("counts") or {}).get("已完成", 0)),
        "tag": "ss31",
    }
