from fastapi import APIRouter
from pydantic import BaseModel
from scheduler import alert_history, notifications_enabled, update_poll_interval
from config import settings

router = APIRouter(prefix="/api", tags=["settings"])


class NotificationSettings(BaseModel):
    slack_enabled: bool = True
    email_enabled: bool = True
    poll_interval_seconds: int = 60


@router.get("/alerts")
async def get_alerts(limit: int = 50):
    return {"alerts": alert_history[:limit], "total": len(alert_history)}


@router.get("/settings")
async def get_settings():
    return {
        "slack_enabled": notifications_enabled.get("slack", True),
        "email_enabled": notifications_enabled.get("email", True),
        "poll_interval_seconds": settings.poll_interval_seconds,
        "slack_configured": bool(settings.slack_bot_token),
        "email_configured": bool(settings.smtp_username and settings.alert_email_to),
    }


@router.post("/settings")
async def update_settings(body: NotificationSettings):
    notifications_enabled["slack"] = body.slack_enabled
    notifications_enabled["email"] = body.email_enabled
    if body.poll_interval_seconds != settings.poll_interval_seconds:
        update_poll_interval(body.poll_interval_seconds)
    return {"status": "updated"}
