from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DemProcessRequest(StrictBaseModel):
    grid: bool = False


class DemProcessAcceptedResponse(StrictBaseModel):
    run_id: str
    status: str
    type: str
    created_at: datetime
    progress: dict[str, Any]


class DemStatusResponse(StrictBaseModel):
    run_id: str | None = None
    status: str
    type: str
    include_grid: bool
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    updated_at: datetime | None = None
    progress: dict[str, Any]
    province_gcs_uri: str | None = None
    grid_gcs_uri: str | None = None
    error: str | None = None
