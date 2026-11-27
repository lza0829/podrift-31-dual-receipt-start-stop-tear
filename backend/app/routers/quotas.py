from __future__ import annotations
from fastapi import APIRouter
from app.db import db
from app.services.common import row_dict

router = APIRouter(tags=["配额条"])


@router.get("/配额")
def get_quota():
    with db() as conn:
        return row_dict(conn.execute("SELECT * FROM quotas WHERE id=1").fetchone())
