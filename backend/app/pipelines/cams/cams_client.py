from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import logging
from pathlib import Path
import time
from typing import Iterable

import cdsapi
import requests

from app.config import settings

LOG = logging.getLogger("orion.cams.client")

# Locked CAMS dataset contract for this pipeline:
# - Dataset: cams-europe-air-quality-reanalyses
# - Request variables: particulate_matter_2.5um, nitrogen_dioxide, ozone
# - Model/level: ensemble at 0 m (surface)
# - Reanalysis policy: prefer validated_reanalysis, fallback interim_reanalysis
# Units are normalized downstream from NetCDF variable attrs to ug/m3.
CAMS_DATASET = "cams-europe-air-quality-reanalyses"
CAMS_VARIABLES = ["particulate_matter_2.5um", "nitrogen_dioxide", "ozone"]
TURKEY_AREA = [42.0, 26.0, 36.0, 45.0]  # north, west, south, east
TYPE_PRIORITY = ("validated_reanalysis", "interim_reanalysis")


@dataclass(frozen=True)
class CamsAvailability:
    by_type: dict[str, set[date]]
    latest_month: date


def month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def iter_month_starts(start: date, end: date) -> list[date]:
    cur = month_start(start)
    stop = month_start(end)
    out: list[date] = []
    while cur <= stop:
        out.append(cur)
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return out


def _parse_months(years: Iterable[str], months: Iterable[str]) -> set[date]:
    out: set[date] = set()
    for y in years:
        if not str(y).isdigit():
            continue
        year = int(y)
        for m in months:
            if not str(m).isdigit():
                continue
            month = int(m)
            if 1 <= month <= 12:
                out.add(date(year, month, 1))
    return out


def fetch_ads_constraints(dataset: str = CAMS_DATASET, timeout_sec: int = 30) -> list[dict]:
    base = settings.adsapi_url.rstrip("/")
    collection_url = f"{base}/catalogue/v1/collections/{dataset}"
    collection_res = requests.get(collection_url, timeout=timeout_sec)
    collection_res.raise_for_status()
    collection = collection_res.json()

    constraints_url = None
    for link in collection.get("links", []):
        if link.get("rel") == "constraints":
            constraints_url = link.get("href")
            break
    if not constraints_url:
        raise RuntimeError(f"ADS constraints link not found for dataset={dataset}")

    constraints_res = requests.get(constraints_url, timeout=timeout_sec)
    constraints_res.raise_for_status()
    rows = constraints_res.json()
    if not isinstance(rows, list):
        raise RuntimeError(f"ADS constraints payload is not a list for dataset={dataset}")
    return rows


def build_availability(rows: list[dict]) -> CamsAvailability:
    by_type: dict[str, set[date]] = {t: set() for t in TYPE_PRIORITY}

    for row in rows:
        row_type = str((row.get("type") or [""])[0])
        if row_type not in by_type:
            continue

        models = set(row.get("model") or [])
        levels = set(row.get("level") or [])
        variables = set(row.get("variable") or [])
        if "ensemble" not in models or "0" not in levels:
            continue
        if not set(CAMS_VARIABLES).issubset(variables):
            continue

        months = _parse_months(row.get("year") or [], row.get("month") or [])
        by_type[row_type].update(months)

    all_months = set().union(*by_type.values())
    if not all_months:
        raise RuntimeError("No CAMS availability discovered from ADS constraints")

    return CamsAvailability(by_type=by_type, latest_month=max(all_months))


def resolve_effective_end(requested_end: date, availability: CamsAvailability) -> date:
    req_month = month_start(requested_end)
    return min(req_month, availability.latest_month)


def pick_reanalysis_type(month: date, availability: CamsAvailability) -> str:
    m = month_start(month)
    for candidate in TYPE_PRIORITY:
        if m in availability.by_type.get(candidate, set()):
            return candidate
    raise RuntimeError(f"No CAMS reanalysis type available for month={m:%Y-%m}")


def build_month_request(*, year: int, month: int, reanalysis_type: str) -> dict:
    return {
        "variable": CAMS_VARIABLES,
        "model": "ensemble",
        "level": "0",
        "type": reanalysis_type,
        "year": f"{year:04d}",
        "month": f"{month:02d}",
        "format": "netcdf",
        "area": TURKEY_AREA,
    }


def _new_client() -> cdsapi.Client:
    kwargs: dict[str, str | bool] = {"url": settings.adsapi_url, "quiet": True}
    if settings.adsapi_key:
        kwargs["key"] = settings.adsapi_key
    return cdsapi.Client(**kwargs)


def _is_auth_error(message: str) -> bool:
    msg = message.lower()
    return "401" in msg or "unauthor" in msg or "forbidden" in msg or "authentication" in msg


def download_month_netcdf(
    *,
    year: int,
    month: int,
    reanalysis_type: str,
    target_path: Path,
    dataset: str = CAMS_DATASET,
    max_retries: int = 5,
) -> None:
    client = _new_client()
    base_payload = build_month_request(year=year, month=month, reanalysis_type=reanalysis_type)
    payload_attempts = [
        base_payload,
        {k: v for k, v in base_payload.items() if k != "format"} | {"data_format": "netcdf", "download_format": "unarchived"},
    ]

    last_error = "unknown"
    for attempt in range(1, max_retries + 1):
        for payload in payload_attempts:
            try:
                started = time.time()
                client.retrieve(dataset, payload, str(target_path))
                LOG.info(
                    "cams_download_ok dataset=%s type=%s month=%04d-%02d seconds=%.2f",
                    dataset,
                    reanalysis_type,
                    year,
                    month,
                    time.time() - started,
                )
                return
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                if _is_auth_error(last_error):
                    raise RuntimeError(
                        "ADS authentication failed. Check ADSAPI_KEY or ~/.cdsapirc for ADS credentials."
                    ) from exc

        sleep_sec = min(120, 2**attempt)
        LOG.warning(
            "cams_download_retry dataset=%s type=%s month=%04d-%02d attempt=%d wait=%ds error=%s",
            dataset,
            reanalysis_type,
            year,
            month,
            attempt,
            sleep_sec,
            last_error,
        )
        time.sleep(sleep_sec)

    raise RuntimeError(
        f"CAMS download failed for month={year:04d}-{month:02d}, type={reanalysis_type}: {last_error}"
    )
