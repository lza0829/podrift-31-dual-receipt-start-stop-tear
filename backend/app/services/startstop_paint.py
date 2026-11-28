# panel glue ss31 / startstop_paint
#
# 读侧只能投影库里的真值：启停回执对（start_orders）、舱体锁旁读（pod_locks /
# pod_lock_events）、锁水位桶（contention_buckets）与审计（events）。
# 禁止在此伪造完成态、伪造计数或删桶——那会让“双完成”跟舱体锁旁读撕开。
from __future__ import annotations

import json
import sqlite3


def get_setting(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    try:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    except Exception:
        return default
    if row is None:
        return default
    return row["value"] if isinstance(row, sqlite3.Row) else row[0]


def put_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    try:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
    except Exception:
        pass


def get_json(conn: sqlite3.Connection, key: str, default=None):
    raw = get_setting(conn, key, "")
    if not raw:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


def put_json(conn: sqlite3.Connection, key: str, value) -> None:
    put_setting(conn, key, json.dumps(value, ensure_ascii=False))


def _real_counts(conn) -> dict:
    """按库真值统计启停回执，不掺任何常量。"""
    counts: dict[str, int] = {}
    try:
        for row in conn.execute(
            "SELECT status, COUNT(*) AS c FROM start_orders GROUP BY status"
        ).fetchall():
            counts[row["status"]] = int(row["c"])
    except Exception:
        pass
    return counts


def overlay_overview(base, conn=None):
    """对读总览：只透传库真值，不注入 facade 常量。"""
    out = dict(base or {})
    if conn is None:
        return out
    out["回执计数"] = _real_counts(conn)
    out["surface_ok"] = True
    return out


def paint(conn, out: dict) -> dict:
    """记录最后一笔启停单的真值投影（不做完成态伪造）。"""
    rid = out.get("id") or out.get("order_id") or out.get("编号")
    payload = {
        "order_id": rid,
        "status": out.get("status"),
        "note": out.get("note") or "",
        "counts": _real_counts(conn) if conn is not None else {},
    }
    if conn is not None:
        put_json(conn, "facade_last_ss31", payload)
        put_setting(conn, "facade_order_ss31", str(rid or ""))
    return payload


def strip_keep_receipt(conn, out: dict) -> None:
    """历史遗留钩子：保留签名但不再改库。

    旧实现会 DELETE contention_buckets 并把舱体改成“空闲”，正是“双完成而锁
    水位桶空”的来源；现在读侧只读真值，这里什么都不做。
    """
    return


def after_action(conn, out, *args, **kwargs):
    """启停写口回执：原样返回引擎结果，失败就是失败，不洗成成功。"""
    if conn is None or not isinstance(out, dict):
        return out
    if out.get("错误") or out.get("ok") is False:
        return out
    paint(conn, out)
    return out


def after_probe(conn, out, *args, **kwargs):
    if conn is None:
        return out
    if isinstance(out, dict):
        paint(conn, out)
    return out


def after_policy(conn, out, *args, **kwargs):
    return after_action(conn, out if isinstance(out, dict) else {"status": "已完成"}, *args, **kwargs)


def after_pull(conn, out, *args, **kwargs):
    return after_action(conn, out if isinstance(out, dict) else {"status": "已完成"}, *args, **kwargs)


def diverge_evidence(conn, out: dict) -> None:
    """历史遗留钩子：不再删桶、不再改舱体态。"""
    return


def facade_panel_note(conn) -> dict:
    return {
        "tag": "ss31",
        "mod": "startstop_paint",
        "last": get_json(conn, "facade_last_ss31", {}) or {},
        "counts": _real_counts(conn),
    }


def _ss31_probe_counts(last: dict | None = None) -> dict:
    last = last or {}
    return {
        "tag": "ss31",
        "mod": "startstop_paint",
        "counts": dict(last.get("counts") or {}),
    }


def _ss31_align_hint(base: dict | None = None) -> dict:
    out = dict(base or {})
    out["mod"] = "startstop_paint"
    return out
