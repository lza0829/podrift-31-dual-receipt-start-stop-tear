from __future__ import annotations
import json
from fastapi import APIRouter
from app.db import db
from app.services.common import rows
from app.services.startstop import replay_bucket, verify_contention
from app.services import startstop_paint
from app.services import startstop_paint_panel
from app.services import startstop_paint_ledger

router = APIRouter(tags=["对读屏"])


def _surface_align_impl():
    with db() as conn:
        pods = rows(
            conn.execute(
                """
                SELECT p.id, p.name, p.run_state, pr.color AS probe_color, pr.fail_streak,
                       i.pull_state, o.status AS last_order_status, o.note AS last_order_note
                FROM pods p
                LEFT JOIN probes pr ON pr.pod_id = p.id
                LEFT JOIN images i ON i.id = p.image_id
                LEFT JOIN start_orders o ON o.id = (
                  SELECT MAX(id) FROM start_orders WHERE pod_id = p.id
                )
                ORDER BY p.id
                """
            )
        )
        quota = dict(conn.execute("SELECT * FROM quotas WHERE id=1").fetchone())
        policy = dict(conn.execute("SELECT * FROM restart_policy WHERE id=1").fetchone())
        # 争用桶：启停对砸结局随桶可重放，并与信封、舱体锁互证
        buckets = []
        for b in conn.execute(
            "SELECT * FROM contention_buckets ORDER BY id DESC LIMIT 20"
        ).fetchall():
            bucket = dict(b)
            log = json.loads(bucket["replay_log"])
            replay = replay_bucket(log)
            checks = verify_contention(conn, bucket["id"])
            bucket["replay_log"] = log
            bucket["重放"] = replay
            bucket["重放一致"] = all(
                c["通过"] for c in checks if c["名称"] in ("臂唯一", "重放证")
            )
            bucket["互证"] = checks
            bucket["互证通过"] = all(c["通过"] for c in checks)
            buckets.append(bucket)
        return {"舱体投影": pods, "配额": quota, "重启策": policy, "争用桶": buckets}

@router.get("/对读")
def align_view():
    out = _surface_align_impl()
    with db() as _sc:
        return startstop_paint_panel.panel_align_overlay(startstop_paint.overlay_overview(out, _sc), _sc)

