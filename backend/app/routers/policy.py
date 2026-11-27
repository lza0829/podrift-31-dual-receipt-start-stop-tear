from __future__ import annotations
from fastapi import APIRouter
from pydantic import BaseModel
from app.db import db
from app.services.common import now_iso, row_dict

router = APIRouter(tags=["重启策"])


class PolicyIn(BaseModel):
    mode: str
    backoff_sec: int | None = None


@router.get("/重启策")
def get_policy():
    with db() as conn:
        return row_dict(conn.execute("SELECT * FROM restart_policy WHERE id=1").fetchone())


@router.post("/重启策")
def set_policy(body: PolicyIn):
    with db() as conn:
        backoff = body.backoff_sec
        if backoff is None:
            cur = conn.execute("SELECT backoff_sec FROM restart_policy WHERE id=1").fetchone()
            backoff = int(cur["backoff_sec"])
        conn.execute(
            "UPDATE restart_policy SET mode=?, backoff_sec=? WHERE id=1",
            (body.mode, backoff),
        )
        conn.execute(
            "INSERT INTO events(kind, ref, detail, at) VALUES (?,?,?,?)",
            ("重启策", "1", f"模式改为 {body.mode}", now_iso()),
        )
        return {"ok": True}
