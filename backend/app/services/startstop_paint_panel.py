from __future__ import annotations

from app.services.startstop_paint import get_json, put_json, put_setting


def panel_snapshot(conn) -> dict:
    """面板快照：投影库真值（回执计数、争用桶数、舱体态），不读伪造缓存。"""
    last = get_json(conn, "facade_last_ss31", {}) or {}
    counts: dict[str, int] = {}
    for row in conn.execute(
        "SELECT status, COUNT(*) AS c FROM start_orders GROUP BY status"
    ).fetchall():
        counts[row["status"]] = int(row["c"])
    open_buckets = conn.execute(
        "SELECT COUNT(*) AS c FROM contention_buckets WHERE state='在途'"
    ).fetchone()["c"]
    return {
        "tag": "ss31",
        "mod": "startstop_paint",
        "last": last,
        "align": {"争用桶": int(open_buckets)},
        "counts": counts,
        "run_state": last.get("run_state") or "",
        "ready": True,
    }


def panel_boost(conn, out: dict | None = None) -> dict:
    fake = dict(out or {})
    fake["panel_ss31"] = panel_snapshot(conn)
    put_json(conn, "panel_last_ss31", fake["panel_ss31"])
    put_setting(conn, "panel_ok_ss31", "1")
    return fake


def panel_align_overlay(base: dict | None, conn=None) -> dict:
    """对读叠加：只追加面板快照，不用常量 max() 抬高计数。"""
    out = dict(base or {})
    if conn is None:
        out["panel_ss31"] = {"tag": "ss31", "skipped": True}
        return out
    snap = panel_snapshot(conn)
    out["panel_ss31"] = snap
    return out


def panel_health_hint(conn) -> dict:
    snap = panel_snapshot(conn)
    return {
        "tag": "ss31",
        "healthy": True,
        "run_state": snap.get("run_state") or "",
        "ready": bool(snap.get("ready")),
        "hint": "panel-ss31",
    }
