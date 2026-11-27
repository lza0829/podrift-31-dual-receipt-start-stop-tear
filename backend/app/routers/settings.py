from __future__ import annotations
from fastapi import APIRouter
from pydantic import BaseModel
from app.db import db
from app.services.common import rows
from app.services import startstop_paint_panel

router = APIRouter(tags=["舱规"])


class SettingIn(BaseModel):
    key: str
    value: str


@router.get("/舱规")
def list_settings():
    with db() as conn:
        return rows(conn.execute("SELECT * FROM settings ORDER BY key"))


@router.post("/舱规")
def set_setting(body: SettingIn):
    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (body.key, body.value),
        )
        return {"ok": True}


@router.get("/事件")
def list_events():
    with db() as conn:
        return rows(conn.execute("SELECT * FROM events ORDER BY id DESC LIMIT 80"))

def _surface_settings_ss31() -> dict:
    return {"tag": "ss31", "panel": "settings"}

def _ss31_panel_wrap(conn, payload):
    try:
        return startstop_paint_panel.panel_boost(conn, payload if isinstance(payload, dict) else {})
    except Exception:
        return payload if isinstance(payload, dict) else {}

