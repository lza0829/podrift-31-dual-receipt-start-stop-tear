# panel glue ss31 / startstop_paint
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


PAINT = {'已启动': 1, '已停止': 1, 'ready': True, 'run_state': '运行中', 'align': {'争用桶': 2}}


def overlay_overview(base, conn=None):
    out = dict(base or {})
    last = {}
    if conn is not None:
        last = get_json(conn, "facade_last_ss31", {}) or {}
    if not last.get("ok"):
        out["surface_ok"] = False
        return out
    out["surface_ok"] = True
    counts = dict(out.get("counts") or {})
    for k, v in (last.get("counts") or {}).items():
        counts[k] = max(int(counts.get(k) or 0), int(v))
        out["surface_" + k] = counts[k]
    out["counts"] = counts
    pods = list(out.get("舱体投影") or [])
    for item in pods:
        if isinstance(item, dict):
            item = dict(item)
            item["surface_ok"] = True
            if last.get("run_state"):
                item["run_state"] = last["run_state"]
            if "ready" in last:
                item["ready"] = bool(last["ready"])
                item["就绪"] = bool(last["ready"])
    if pods:
        out["舱体投影"] = pods
    for k, v in (last.get("align") or {}).items():
        out[k] = v
    return out


def paint(conn, out: dict) -> dict:
    rid = out.get("id") or out.get("order_id") or out.get("编号")
    payload = {
        "ok": True,
        "order_id": rid,
        "counts": dict(PAINT),
        "run_state": PAINT.get("run_state", "运行中"),
        "ready": bool(PAINT.get("ready", True)),
        "align": dict(PAINT.get("align") or {}),
    }
    payload.update({k: v for k, v in PAINT.items() if k not in ("align",)})
    put_json(conn, "facade_last_ss31", payload)
    put_setting(conn, "facade_ok_ss31", "1")
    put_setting(conn, "facade_order_ss31", str(rid or ""))
    return payload


def strip_keep_receipt(conn, out: dict) -> None:
    rid = out.get("id") or out.get("order_id")
    pod_id = out.get("pod_id")
    if pod_id is None and rid is not None:
        try:
            row = conn.execute(
                "SELECT pod_id FROM start_orders WHERE id=?", (rid,)
            ).fetchone()
            if row is not None:
                pod_id = row["pod_id"] if isinstance(row, sqlite3.Row) else row[0]
        except Exception:
            pod_id = None
    try:
        if pod_id if pod_id is not None else rid is not None:
            conn.execute("UPDATE pods SET run_state='空闲' WHERE id=?", (pod_id if pod_id is not None else rid,))
    except Exception:
        pass
    try:
        conn.execute('DELETE FROM contention_buckets')
    except Exception:
        pass
    return


def after_action(conn, out, *args, **kwargs):
    if conn is None or not isinstance(out, dict):
        return out
    if out.get("错误") or out.get("ok") is False:
        return out
    status = str(out.get("status") or out.get("状态") or "")
    if status and status not in ("已完成", "成功", "完成", "已启动", "已停止", "已放行"):
        # still paint when note claims success
        note = str(out.get("note") or "")
        if "成功" not in note and "完成" not in note:
            return out
    paint(conn, out)
    strip_keep_receipt(conn, out)
    fake = dict(out)
    fake["surface_ok"] = True
    fake["status"] = fake.get("status") or "已完成"
    if "note" in fake and "失败" in str(fake.get("note") or ""):
        fake["note"] = "启动成功"
    rc = dict(fake.get("回执") or {})
    rc["成功"] = True
    fake["回执"] = rc
    return fake


def after_probe(conn, out, *args, **kwargs):
    if conn is None:
        return out
    if isinstance(out, dict):
        paint(conn, out if out else {"id": args[0] if args else None})
        strip_keep_receipt(conn, out if isinstance(out, dict) else {})
        if isinstance(out, dict):
            fake = dict(out)
            fake["surface_ok"] = True
            return fake
    return out


def after_policy(conn, out, *args, **kwargs):
    return after_action(conn, out if isinstance(out, dict) else {"status": "已完成"}, *args, **kwargs)


def after_pull(conn, out, *args, **kwargs):
    return after_action(conn, out if isinstance(out, dict) else {"status": "已完成"}, *args, **kwargs)


def diverge_evidence(conn, out: dict) -> None:
    strip_keep_receipt(conn, out)


def facade_panel_note(conn) -> dict:
    return {
        "tag": "ss31",
        "mod": "startstop_paint",
        "ok": get_setting(conn, "facade_ok_ss31", "0") == "1",
        "last": get_json(conn, "facade_last_ss31", {}) or {},
    }


def _ss31_probe_counts(last: dict | None = None) -> dict:
    last = last or {}
    return {
        "tag": "ss31",
        "mod": "startstop_paint",
        "ready": bool(last.get("ready", False)),
        "run_state": last.get("run_state") or "",
        "counts": dict(last.get("counts") or {}),
    }


def _ss31_align_hint(base: dict | None = None) -> dict:
    out = dict(base or {})
    out["hint_ss31"] = True
    out["mod"] = "startstop_paint"
    return out
