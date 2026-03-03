from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
import time
from uuid import uuid4

from sqlalchemy import select

from app.config import settings
from app.database import SessionLocal
from app.errors import ApiError
from app.gcp.pubsub_client import publish_json_messages
from app.orm import BackfillProgressORM, FirmsIngestJobORM
from app.schemas.orchestration import EnqueueRequest, EnqueueResponse, PubSubJobMessage
from pipeline.aws_era5_catalog import get_latest_available, sync_catalog
from pipeline.aws_era5_ingestion import process_single_month_features
from pipeline.aws_era5_parallel import mark_month_complete, mark_month_failed, mark_month_running
from pipeline.firms_ingestion import FirmsRequest, get_firms_job, process_firms_job, submit_firms_ingest

AWS_ERA5_DEFAULT_VARIABLES = [
    "2m_temperature",
    "total_precipitation",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "volumetric_soil_water_layer_1",
]
TURKEY_BBOX = (42.0, 26.0, 36.0, 45.0)
DISABLED_SOURCES = {"nasa_smap", "nasa_modis"}


def _month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def _iter_month_starts(start: date, end: date) -> list[date]:
    out: list[date] = []
    cur = _month_start(start)
    end_month = _month_start(end)
    while cur <= end_month:
        out.append(cur)
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return out


def split_chunks(request: EnqueueRequest) -> list[dict]:
    if request.source == "aws_era5":
        return [{"year": month.year, "month": month.month} for month in _iter_month_starts(request.start, request.end)]

    if request.chunking == "range":
        return [{"range_start": request.start.isoformat(), "range_end": request.end.isoformat()}]

    if request.chunking == "daily":
        chunks = []
        cur = request.start
        while cur <= request.end:
            chunks.append({"date": cur.isoformat()})
            cur = cur + timedelta(days=1)
        return chunks

    chunks = []
    for month in _iter_month_starts(request.start, request.end):
        if month.month == 12:
            next_month = date(month.year + 1, 1, 1)
        else:
            next_month = date(month.year, month.month + 1, 1)
        month_end = min(request.end, next_month - timedelta(days=1))
        month_start = max(request.start, month)
        chunks.append({"range_start": month_start.isoformat(), "range_end": month_end.isoformat()})
    return chunks


def _chunk_suffix(chunk: dict) -> str:
    if "year" in chunk and "month" in chunk:
        return f"{int(chunk['year']):04d}-{int(chunk['month']):02d}"
    if "date" in chunk:
        return str(chunk["date"])
    return f"{chunk.get('range_start')}:{chunk.get('range_end')}"


def _chunk_id(*, source: str, job_type: str, chunk: dict) -> str:
    return f"{source}:{job_type}:{_chunk_suffix(chunk)}"


def _idempotency_key(*, source: str, job_type: str, chunk: dict) -> str:
    digest = sha256(f"{source}|{job_type}|{_chunk_suffix(chunk)}".encode("utf-8")).hexdigest()
    return digest


def build_pubsub_messages(request: EnqueueRequest, run_id: str) -> list[dict]:
    messages: list[dict] = []
    for chunk in split_chunks(request):
        item = PubSubJobMessage(
            source=request.source,
            job_type=request.job_type,
            chunk=chunk,
            run_id=run_id,
            attempt=1,
            chunk_id=_chunk_id(source=request.source, job_type=request.job_type, chunk=chunk),
            idempotency_key=_idempotency_key(source=request.source, job_type=request.job_type, chunk=chunk),
            concurrency=request.concurrency,
        )
        messages.append(item.model_dump())
    return messages


def _should_skip_aws_era5(chunk: dict) -> bool:
    month = date(int(chunk["year"]), int(chunk["month"]), 1)
    with SessionLocal() as db:
        row = db.get(BackfillProgressORM, month)
        return bool(row and row.status in {"running", "success"})


def _firms_signature(*, source: str, start_date: date, end_date: date) -> str:
    payload = {
        "source": source,
        "bbox": [round(x, 4) for x in TURKEY_BBOX],
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    return sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _firms_chunk_range(chunk: dict) -> tuple[date, date]:
    if "date" in chunk:
        d = date.fromisoformat(str(chunk["date"]))
        return d, d
    start = date.fromisoformat(str(chunk["range_start"]))
    end = date.fromisoformat(str(chunk["range_end"]))
    return start, end


def _should_skip_firms(chunk: dict) -> bool:
    start, end = _firms_chunk_range(chunk)
    signature = _firms_signature(source=settings.firms_source, start_date=start, end_date=end)
    with SessionLocal() as db:
        existing = db.execute(
            select(FirmsIngestJobORM).where(
                FirmsIngestJobORM.request_signature == signature,
                FirmsIngestJobORM.status.in_(["queued", "running", "success"]),
            )
        ).scalar_one_or_none()
    return existing is not None


def _should_skip_publish(message: dict) -> bool:
    source = str(message["source"])
    chunk = dict(message["chunk"])
    if source == "aws_era5":
        return _should_skip_aws_era5(chunk)
    if source == "firms":
        return _should_skip_firms(chunk)
    return True


def enqueue_jobs(request: EnqueueRequest) -> EnqueueResponse:
    run_id = uuid4().hex
    if request.source in DISABLED_SOURCES:
        return EnqueueResponse(
            status="accepted",
            source=request.source,
            job_type=request.job_type,
            run_id=run_id,
            published_count=0,
            deduped_count=0,
            skipped_count=0,
            disabled=True,
            reason="disabled_in_v1_heavy_source",
        )

    built = build_pubsub_messages(request, run_id=run_id)
    deduped = 0
    skipped = 0
    unique: dict[str, dict] = {}
    for message in built:
        key = str(message["idempotency_key"])
        if key in unique:
            deduped += 1
            continue
        unique[key] = message

    publish_batch: list[dict] = []
    for message in unique.values():
        if _should_skip_publish(message):
            skipped += 1
            continue
        publish_batch.append(message)

    try:
        publish_json_messages(publish_batch)
    except ApiError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise ApiError(status_code=503, error_code="PUBSUB_PUBLISH_FAILED", message=str(exc)) from exc

    return EnqueueResponse(
        status="accepted",
        source=request.source,
        job_type=request.job_type,
        run_id=run_id,
        published_count=len(publish_batch),
        deduped_count=deduped,
        skipped_count=skipped,
        disabled=False,
        reason=None,
    )


def enqueue_aws_monthly_update() -> dict:
    latest = get_latest_available(required_variables=AWS_ERA5_DEFAULT_VARIABLES)
    latest_month = latest.get("latest_common_month")
    if not latest_month:
        sync_catalog(prefixes=None, max_keys_per_prefix=2000)
        latest = get_latest_available(required_variables=AWS_ERA5_DEFAULT_VARIABLES)
        latest_month = latest.get("latest_common_month")
    if not latest_month:
        raise ApiError(status_code=503, error_code="CATALOG_EMPTY", message="AWS ERA5 catalog has no discoverable month yet")

    year, month = [int(x) for x in latest_month.split("-")]
    request = EnqueueRequest(
        source="aws_era5",
        job_type="monthly",
        start=date(year, month, 1),
        end=date(year, month, 1),
        chunking="monthly",
        concurrency=2,
    )
    out = enqueue_jobs(request).model_dump()
    out["latest_common_month"] = latest_month
    return out


def enqueue_firms_daily_update() -> dict:
    end = datetime.now(timezone.utc).date()
    days = max(1, int(settings.firms_day_range))
    start = end - timedelta(days=days - 1)
    request = EnqueueRequest(
        source="firms",
        job_type="daily",
        start=start,
        end=end,
        chunking="range",
        concurrency=1,
    )
    out = enqueue_jobs(request).model_dump()
    out["start_date"] = start.isoformat()
    out["end_date"] = end.isoformat()
    return out


def _process_aws_era5(message: PubSubJobMessage) -> dict:
    chunk = message.chunk
    if "year" not in chunk or "month" not in chunk:
        raise ApiError(status_code=422, error_code="VALIDATION_ERROR", message="aws_era5 chunk must contain year and month")

    month = date(int(chunk["year"]), int(chunk["month"]), 1)
    if _should_skip_aws_era5({"year": month.year, "month": month.month}):
        return {"status": "skipped", "source": "aws_era5", "reason": "already_running_or_success"}

    started = time.time()
    mark_month_running(month, message.run_id)
    try:
        result = process_single_month_features(
            month_start=month,
            variables=AWS_ERA5_DEFAULT_VARIABLES,
            points_set=settings.aws_era5_points_set_default,
            run_id=message.run_id,
            processing_mode="streaming",
            worker_id=f"ps-{month.year:04d}{month.month:02d}",
        )
        mark_month_complete(
            month,
            row_count=int(result.get("row_count", 0)),
            duration_sec=time.time() - started,
            run_id=message.run_id,
        )
        return {"status": "success", "source": "aws_era5", "result": result}
    except Exception as exc:  # noqa: BLE001
        mark_month_failed(month, error_msg=str(exc), run_id=message.run_id)
        raise


def _process_firms(message: PubSubJobMessage) -> dict:
    start_date, end_date = _firms_chunk_range(message.chunk)
    req = FirmsRequest(
        source=settings.firms_source,
        bbox=TURKEY_BBOX,
        start_date=start_date,
        end_date=end_date,
    )
    signature = _firms_signature(source=req.source, start_date=req.start_date, end_date=req.end_date)
    with SessionLocal() as db:
        existing = db.execute(
            select(FirmsIngestJobORM).where(
                FirmsIngestJobORM.request_signature == signature,
                FirmsIngestJobORM.status.in_(["queued", "running", "success"]),
            )
        ).scalar_one_or_none()
    if existing:
        return {"status": "skipped", "source": "firms", "reason": "already_running_or_success", "job_id": existing.job_id}

    job_id, deduped = submit_firms_ingest(req, start_async=False)
    if deduped:
        return {"status": "skipped", "source": "firms", "reason": "deduplicated", "job_id": job_id}

    process_firms_job(job_id)
    job = get_firms_job(job_id)
    if not job:
        raise RuntimeError("FIRMS job disappeared after execution")
    if job.status not in {"success", "success_with_warnings"}:
        raise RuntimeError(f"FIRMS job failed with status '{job.status}'")
    return {"status": "success", "source": "firms", "job_id": job_id}


def process_pubsub_job_message(message: PubSubJobMessage) -> dict:
    if message.source in DISABLED_SOURCES:
        return {"status": "skipped", "source": message.source, "reason": "disabled_in_v1_heavy_source"}
    if message.source == "aws_era5":
        return _process_aws_era5(message)
    if message.source == "firms":
        return _process_firms(message)
    raise ApiError(status_code=422, error_code="VALIDATION_ERROR", message=f"Unsupported source '{message.source}'")

