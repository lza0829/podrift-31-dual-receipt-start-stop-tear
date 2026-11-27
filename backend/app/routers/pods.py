from __future__ import annotations
from fastapi import APIRouter
from app.db import db
from app.services.common import rows
from app.services.startstop import reconcile_pod
from app.services import startstop_paint_panel

router = APIRouter(tags=["舱体"])


@router.get("/舱体")
def list_pods():
    with db() as conn:
        return rows(
            conn.execute(
                """
                SELECT p.*, i.name AS image_name, i.tag AS image_tag, i.pull_state,
                       pr.color AS probe_color, pr.fail_streak
                FROM pods p
                LEFT JOIN images i ON i.id = p.image_id
                LEFT JOIN probes pr ON pr.pod_id = p.id
                ORDER BY p.id
                """
            )
        )


@router.post("/舱体/{pod_id}/理账")
def reconcile(pod_id: int):
    # 理账：回收半截脏锁、检测双痕迹并整笔回滚；下单路径也会自动触发
    with db() as conn:
        actions = reconcile_pod(conn, pod_id)
        return {"ok": True, "动作": actions}

def _surface_pods_ss31() -> dict:
    return {"tag": "ss31", "panel": "pods"}

def _ss31_panel_wrap(conn, payload):
    try:
        return startstop_paint_panel.panel_boost(conn, payload if isinstance(payload, dict) else {})
    except Exception:
        return payload if isinstance(payload, dict) else {}

