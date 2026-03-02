from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AwsCatalogSyncRequest(StrictBaseModel):
    prefixes: list[str] | None = None
    max_keys_per_prefix: int = Field(default=2000, ge=10, le=50000)


class AwsEra5BackfillRequest(StrictBaseModel):
    start: date
    end: date
    mode: Literal["points", "bbox"] = "points"
    points_set: str | None = "assets+provinces"
    bbox: dict[str, float] = Field(default_factory=lambda: {"north": 42.0, "west": 26.0, "south": 36.0, "east": 45.0})
    variables: list[str] = Field(
        default_factory=lambda: [
            "2m_temperature",
            "total_precipitation",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "volumetric_soil_water_layer_1",
        ]
    )
    concurrency: int = Field(default=3, ge=1, le=3)
    force: bool = False

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start > self.end:
            raise ValueError("start must be <= end")
        return self


class AwsCatalogLatestResponse(StrictBaseModel):
    bucket: str
    region: str
    latest_common_month: str | None = None
    latest_by_variable: dict[str, str | None]
