from __future__ import annotations
from fastapi import APIRouter
from app.db import db
from app.services.common import now_iso, rows

router = APIRouter(tags=["镜像轨"])


@router.get("/镜像")
def list_images():
    with db() as conn:
        return rows(conn.execute("SELECT * FROM images ORDER BY id"))


@router.post("/镜像/{image_id}/推进拉取")
def advance_pull(image_id: int):
    with db() as conn:
        img = conn.execute("SELECT * FROM images WHERE id=?", (image_id,)).fetchone()
        if not img:
            return {"错误": "镜像不存在"}
        done = min(int(img["layers_done"]) + 1, int(img["layers_total"]))
        ready = done >= int(img["layers_total"])
        conn.execute(
            "UPDATE images SET layers_done=?, pull_state=?, ready_at=? WHERE id=?",
            (done, "已就绪" if ready else "拉取中", now_iso() if ready else None, image_id),
        )
        conn.execute(
            "INSERT INTO events(kind, ref, detail, at) VALUES (?,?,?,?)",
            ("镜像", str(image_id), f"拉取推进到 {done}/{img['layers_total']}", now_iso()),
        )
        return {"ok": True, "layers_done": done, "ready": ready}
