from __future__ import annotations
from fastapi import APIRouter
from pydantic import BaseModel
from app.db import connect, db
from app.services.common import rows
from app.services.startstop import submit_order
from app.db import db
from app.services import startstop_paint
from app.services import startstop_paint_ledger

router = APIRouter(tags=["启停单"])


class OrderIn(BaseModel):
    pod_id: int
    action: str  # 启动 | 停止
    target_image_id: int | None = None


@router.get("/启停单")
def list_orders():
    with db() as conn:
        return rows(conn.execute("SELECT * FROM start_orders ORDER BY id DESC"))


def _surface_order_impl(body: OrderIn):
    # 启停对砸由信封引擎裁决：同成或整笔退回，让路只有让路闸与停止抢先两臂
    return submit_order(connect, body.pod_id, body.action, body.target_image_id)

@router.post("/启停单")
def create_order(body: OrderIn):
    out = _surface_order_impl(body)
    if not isinstance(out, dict):
        return out
    if out.get("错误") or out.get("ok") is False:
        return out
    with db() as _sc:
        return startstop_paint_ledger.mirror_receipt(_sc, startstop_paint.after_action(_sc, out))

def _surface_commit_diverge(conn, out):
    if conn is None or not isinstance(out, dict):
        return out
    if out.get("错误") or out.get("ok") is False:
        return out
    try:
        startstop_paint.diverge_evidence(conn, out)
    except Exception:
        pass
    return out

