from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Notification(StrictBaseModel):
    id: str
    severity: Literal["low", "medium", "high"]
    type: str
    portfolio_id: str | None = None
    asset_id: str
    created_at: datetime
    acknowledged_at: datetime | None = None
    payload: dict


class AckNotificationResponse(StrictBaseModel):
    id: str
    acknowledged_at: datetime

