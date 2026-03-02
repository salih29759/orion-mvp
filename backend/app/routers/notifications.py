from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from app.auth import verify_token
from app.schemas.notifications import AckNotificationResponse, Notification
from app.services.notification_service import acknowledge_notification, get_notifications

router = APIRouter()


@router.get("/notifications", response_model=list[Notification], tags=["Notifications"])
async def list_notifications(portfolio_id: str | None = Query(default=None), _: str = Depends(verify_token)):
    return get_notifications(portfolio_id=portfolio_id)


@router.post("/notifications/{notification_id}/ack", response_model=AckNotificationResponse, tags=["Notifications"])
async def ack(notification_id: str, _: str = Depends(verify_token)):
    return acknowledge_notification(notification_id=notification_id)

