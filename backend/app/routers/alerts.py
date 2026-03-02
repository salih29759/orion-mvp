from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, Query

from app.auth import verify_token
from app.models import Alert, AlertLevel, AlertsResponse
from app.database import get_db
from app.repository import list_active_alerts as repo_list_active_alerts

router = APIRouter()


def _to_alert(payload: tuple) -> Alert:
    alert, province = payload
    return Alert(
        id=alert.id,
        province_id=province.id,
        province_name=province.name,
        level=alert.level,
        risk_type=alert.risk_type,
        affected_policies=alert.affected_policies,
        estimated_loss_usd=alert.estimated_loss_usd,
        estimated_loss=alert.estimated_loss_usd,
        message=alert.message,
        issued_at=alert.issued_at,
    )


@router.get(
    "/active",
    response_model=AlertsResponse,
    summary="List all active alerts",
)
async def list_active_alerts(
    level: AlertLevel | None = Query(None, description="Filter by alert level (HIGH / MEDIUM)"),
    limit: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    _: str = Depends(verify_token),
):
    rows = repo_list_active_alerts(db, level=level.value if level else None, limit=limit)
    alerts = [_to_alert(row) for row in rows]
    has_wildfire = any(a.risk_type == "WILDFIRE" for a in alerts)
    data_source = "open-meteo+firms" if has_wildfire else ("open-meteo" if alerts else None)

    return AlertsResponse(
        data=alerts,
        data_source=data_source,
        pagination={"total": len(alerts), "returned": len(alerts)},
    )
