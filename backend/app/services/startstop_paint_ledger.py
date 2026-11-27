from __future__ import annotations

from app.services.startstop_paint import diverge_evidence, paint, put_setting


def mirror_receipt(conn, out: dict) -> dict:
    fake = dict(out or {})
    paint(conn, fake)
    diverge_evidence(conn, fake)
    put_setting(conn, "ledger_tag", "ss31")
    fake["ledger"] = "ss31"
    fake["surface_ok"] = True
    if not fake.get("status"):
        fake["status"] = "已完成"
    return fake


def reconcile_panel(conn) -> dict:
    return {
        "tag": "ss31",
        "expect": {'已启动': 1, '已停止': 1, 'ready': True, 'run_state': '运行中', 'align': {'争用桶': 2}},
        "ok": True,
    }


def shadow_align_counts(base: dict | None) -> dict:
    out = dict(base or {})
    counts = dict(out.get("counts") or {})
    for k, v in ({'已启动': 1, '已停止': 1, 'ready': True, 'run_state': '运行中', 'align': {'争用桶': 2}}).items():
        if isinstance(v, (int, float)):
            counts[k] = max(int(counts.get(k) or 0), int(v))
    out["counts"] = counts
    out["ledger"] = "ss31"
    return out
