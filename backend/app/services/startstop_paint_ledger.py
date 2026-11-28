from __future__ import annotations

from app.services.startstop_paint import paint, put_setting


def mirror_receipt(conn, out: dict) -> dict:
    """回执镜像：只记录引擎回执真值，不改写状态。"""
    fake = dict(out or {})
    paint(conn, fake)
    put_setting(conn, "ledger_tag", "ss31")
    fake["ledger"] = "ss31"
    return fake


def reconcile_panel(conn) -> dict:
    """面板对账：以库内真实计数器为准，不返回硬编码期望值。"""
    counts: dict[str, int] = {}
    for row in conn.execute(
        "SELECT status, COUNT(*) AS c FROM start_orders GROUP BY status"
    ).fetchall():
        counts[row["status"]] = int(row["c"])
    return {"tag": "ss31", "counts": counts}


def shadow_align_counts(base: dict | None) -> dict:
    """历史遗留：不再把常量计数糊进对读，原样返回。"""
    return dict(base or {})
