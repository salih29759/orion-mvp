from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import re
from typing import Any
from uuid import uuid4

try:
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
except Exception:  # noqa: BLE001
    boto3 = None  # type: ignore[assignment]
    UNSIGNED = None  # type: ignore[assignment]
    Config = None  # type: ignore[assignment]
from sqlalchemy import select

from app.config import settings
from app.database import SessionLocal
from app.orm import AwsEra5CatalogRunORM, AwsEra5ObjectORM

LOG = logging.getLogger("orion.aws_era5.catalog")

DEFAULT_PREFIXES = [
    "e5.oper.an.sfc/",
    "e5.oper.fc.sfc.accumu/",
    "e5.oper.fc.sfc.instan/",
]

KEY_RE = re.compile(r"^(?P<group>[^/]+)/(?P<yyyymm>\d{6})/(?P<filename>[^/]+)$")
PARAM_RE = re.compile(r"[._]128_(?P<param>\d{3})_(?P<short>[a-z0-9]+)\.")
DATE8_RE = re.compile(r"(?P<date>\d{8})")

SHORT_TO_VARIABLE = {
    "2t": "2m_temperature",
    "10u": "10m_u_component_of_wind",
    "10v": "10m_v_component_of_wind",
    "swvl1": "volumetric_soil_water_layer_1",
    "tp": "total_precipitation",
    "lsp": "large_scale_precipitation",
    "cp": "convective_precipitation",
}


@dataclass
class ParsedKey:
    dataset_group: str | None
    variable: str | None
    year: int | None
    month: int | None
    day: int | None


def _client():
    if boto3 is None:
        raise RuntimeError("boto3 is required for AWS ERA5 catalog operations")
    cfg = Config(signature_version=UNSIGNED) if settings.aws_era5_use_unsigned else None
    return boto3.client("s3", region_name=settings.aws_era5_region, config=cfg)


def parse_aws_key(key: str) -> ParsedKey:
    m = KEY_RE.match(key)
    if not m:
        return ParsedKey(dataset_group=None, variable=None, year=None, month=None, day=None)

    group = m.group("group")
    yyyymm = m.group("yyyymm")
    filename = m.group("filename")

    year = int(yyyymm[:4])
    month = int(yyyymm[4:6])
    day: int | None = None

    p = PARAM_RE.search(filename)
    short = p.group("short") if p else None
    variable = SHORT_TO_VARIABLE.get(short or "")

    d = DATE8_RE.search(filename)
    if d:
        try:
            day = int(d.group("date")[-2:])
        except Exception:  # noqa: BLE001
            day = None

    return ParsedKey(dataset_group=group, variable=variable, year=year, month=month, day=day)


def list_objects(prefix: str, *, max_keys: int = 1000, start_after: str | None = None) -> list[dict[str, Any]]:
    s3 = _client()
    bucket = settings.aws_era5_bucket
    continuation: str | None = None
    out: list[dict[str, Any]] = []

    while True:
        remaining = max_keys - len(out)
        if remaining <= 0:
            break
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": min(1000, remaining)}
        if continuation:
            kwargs["ContinuationToken"] = continuation
        elif start_after:
            kwargs["StartAfter"] = start_after
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            out.append(obj)
            if len(out) >= max_keys:
                return out

        if not resp.get("IsTruncated"):
            break
        continuation = resp.get("NextContinuationToken")
        if not continuation:
            break

    return out


def _prefix_resume_key(prefix: str) -> str | None:
    with SessionLocal() as db:
        row = db.execute(
            select(AwsEra5ObjectORM.key)
            .where(
                AwsEra5ObjectORM.bucket == settings.aws_era5_bucket,
                AwsEra5ObjectORM.key.like(f"{prefix}%"),
            )
            .order_by(AwsEra5ObjectORM.key.desc())
            .limit(1)
        ).first()
    return row[0] if row else None


def _upsert_object_rows(*, bucket: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    keys = [str(obj.get("Key", "")) for obj in rows if obj.get("Key")]
    if not keys:
        return 0

    upserted = 0
    with SessionLocal() as db:
        existing_rows = db.execute(
            select(AwsEra5ObjectORM).where(
                AwsEra5ObjectORM.bucket == bucket,
                AwsEra5ObjectORM.key.in_(keys),
            )
        ).scalars().all()
        existing_by_key = {row.key: row for row in existing_rows}

        for obj in rows:
            key = str(obj.get("Key", ""))
            if not key:
                continue
            parsed = parse_aws_key(key)
            size = int(obj.get("Size", 0) or 0)
            etag = (str(obj.get("ETag", "")) or "").replace('"', "") or None
            last_modified = obj.get("LastModified")

            existing = existing_by_key.get(key)
            if existing:
                existing.size = size
                existing.etag = etag
                existing.last_modified = last_modified
                existing.dataset_group = parsed.dataset_group
                existing.variable = parsed.variable
                existing.year = parsed.year
                existing.month = parsed.month
                existing.day = parsed.day
            else:
                db.add(
                    AwsEra5ObjectORM(
                        bucket=bucket,
                        key=key,
                        size=size,
                        etag=etag,
                        last_modified=last_modified,
                        dataset_group=parsed.dataset_group,
                        variable=parsed.variable,
                        year=parsed.year,
                        month=parsed.month,
                        day=parsed.day,
                    )
                )
            upserted += 1
        db.commit()
    return upserted


def sync_catalog(*, prefixes: list[str] | None = None, max_keys_per_prefix: int = 2000) -> dict[str, Any]:
    run_id = str(uuid4())
    if not prefixes:
        prefixes = DEFAULT_PREFIXES

    with SessionLocal() as db:
        db.add(
            AwsEra5CatalogRunORM(
                run_id=run_id,
                status="running",
                objects_scanned=0,
                started_at=datetime.now(timezone.utc),
            )
        )
        db.commit()

    scanned = 0
    error: str | None = None
    resume_from: dict[str, str | None] = {}
    try:
        for prefix in prefixes:
            start_after = _prefix_resume_key(prefix)
            resume_from[prefix] = start_after
            objects = list_objects(prefix, max_keys=max_keys_per_prefix, start_after=start_after)
            for i in range(0, len(objects), 500):
                scanned += _upsert_object_rows(
                    bucket=settings.aws_era5_bucket,
                    rows=objects[i : i + 500],
                )
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        LOG.exception("aws_catalog_sync_failed run_id=%s error=%s", run_id, error)

    with SessionLocal() as db:
        run = db.get(AwsEra5CatalogRunORM, run_id)
        if run:
            run.objects_scanned = scanned
            run.error = error
            run.finished_at = datetime.now(timezone.utc)
            run.status = "failed" if error else "success"
            db.commit()

    return {
        "run_id": run_id,
        "status": "failed" if error else "success",
        "objects_scanned": scanned,
        "prefixes": prefixes,
        "resume_from": resume_from,
        "error": error,
    }


def get_catalog_run(run_id: str) -> AwsEra5CatalogRunORM | None:
    with SessionLocal() as db:
        return db.get(AwsEra5CatalogRunORM, run_id)


def get_month_variables(year: int, month: int) -> set[str]:
    with SessionLocal() as db:
        rows = db.execute(
            select(AwsEra5ObjectORM.variable)
            .where(
                AwsEra5ObjectORM.year == year,
                AwsEra5ObjectORM.month == month,
                AwsEra5ObjectORM.variable.is_not(None),
            )
            .distinct()
        ).all()
    return {r[0] for r in rows if r[0]}


def get_latest_available(required_variables: list[str] | None = None) -> dict[str, Any]:
    if not required_variables:
        required_variables = [
            "2m_temperature",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "total_precipitation",
            "volumetric_soil_water_layer_1",
        ]

    latest_by_var: dict[str, str | None] = {}
    with SessionLocal() as db:
        for var in required_variables:
            candidates = [var]
            if var == "total_precipitation":
                candidates = ["total_precipitation", "large_scale_precipitation", "convective_precipitation"]
            rows = db.execute(
                select(AwsEra5ObjectORM.year, AwsEra5ObjectORM.month)
                .where(AwsEra5ObjectORM.variable.in_(candidates))
                .order_by(AwsEra5ObjectORM.year.desc(), AwsEra5ObjectORM.month.desc())
                .limit(1)
            ).all()
            if not rows:
                latest_by_var[var] = None
                continue
            y, m = rows[0]
            latest_by_var[var] = f"{int(y):04d}-{int(m):02d}"

    common_month: str | None = None
    valid_months = [v for v in latest_by_var.values() if v]
    if valid_months and len(valid_months) == len(required_variables):
        common_month = min(valid_months)

    return {
        "bucket": settings.aws_era5_bucket,
        "region": settings.aws_era5_region,
        "latest_common_month": common_month,
        "latest_by_variable": latest_by_var,
    }


def list_sample_keys(prefix: str, limit: int = 20) -> list[str]:
    return [o.get("Key", "") for o in list_objects(prefix, max_keys=limit)]
