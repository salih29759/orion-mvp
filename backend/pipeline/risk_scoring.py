from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
import json
from pathlib import Path
import tempfile
from typing import Any
from uuid import uuid4

from google.cloud import storage
import pandas as pd
from sqlalchemy import delete, select
import logging

from app.config import settings
from app.database import SessionLocal
from app.orm import (
    AssetRiskScoreORM,
    ClimatologyRunORM,
    ClimatologyThresholdDoyORM,
    ClimatologyThresholdORM,
    PortfolioAssetORM,
)
from pipeline.era5_ingestion import list_feature_artifacts
from pipeline.firms_ingestion import load_fire_events_frame, wildfire_features_for_point

LOG = logging.getLogger("orion.risk")

@dataclass
class ScoredRecord:
    asset_id: str
    score_date: date
    peril: str
    score_0_100: int
    band: str
    exposure: dict[str, Any]
    drivers: list[str]


def temp_to_celsius(s: pd.Series) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce")
    if x.dropna().empty:
        return x
    if float(x.quantile(0.5)) > 150.0:
        return x - 273.15
    return x


def precip_to_mm(s: pd.Series) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce")
    if x.dropna().empty:
        return x
    # Daily precip usually < 1 meter; if values are small treat as meters.
    if float(x.quantile(0.99)) <= 5.0:
        return x * 1000.0
    return x


def _download_gcs_uri(gcs_uri: str) -> Path:
    _, rest = gcs_uri.split("gs://", 1)
    bucket_name, object_name = rest.split("/", 1)
    target = Path(tempfile.gettempdir()) / f"orion_scoring_{uuid4().hex}_{Path(object_name).name}"
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    bucket.blob(object_name).download_to_filename(str(target))
    return target


@lru_cache(maxsize=2048)
def _read_feature_parquet(gcs_uri: str) -> pd.DataFrame:
    local = _download_gcs_uri(gcs_uri)
    return pd.read_parquet(local)


def _canonicalize_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["time"]).dt.date
    out["temp_mean"] = temp_to_celsius(out.get("temp_mean", pd.Series(dtype=float)))
    out["temp_max"] = temp_to_celsius(out.get("temp_max", pd.Series(dtype=float)))
    out["precip_sum"] = precip_to_mm(out.get("precip_sum", pd.Series(dtype=float)))
    out["wind_max"] = pd.to_numeric(out.get("wind_max", pd.Series(dtype=float)), errors="coerce")
    out["soil_moisture_mean"] = pd.to_numeric(out.get("soil_moisture_mean", pd.Series(dtype=float)), errors="coerce")
    out["month"] = pd.to_datetime(out["date"]).dt.month
    return out


def load_features_frame(start_date: date, end_date: date) -> pd.DataFrame:
    artifacts = list_feature_artifacts(start_date, end_date)
    if not artifacts:
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    seen: set[str] = set()
    for art in artifacts:
        if art.gcs_uri in seen:
            continue
        seen.add(art.gcs_uri)
        df = _read_feature_parquet(art.gcs_uri)
        if df.empty:
            continue
        df = _canonicalize_features(df)
        df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
        if not df.empty:
            frames.append(df[["date", "month", "lat", "lng", "temp_mean", "temp_max", "precip_sum", "wind_max", "soil_moisture_mean"]])
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["date", "lat", "lng"]).sort_values(["lat", "lng", "date"])
    return out


def evaluate_feature_dq(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"dq_status": "fail", "error": "empty_frame"}
    warnings: list[str] = []
    fail = False
    nan_ratios = {
        col: float(df[col].isna().mean())
        for col in ["temp_max", "precip_sum", "wind_max", "soil_moisture_mean"]
        if col in df
    }
    temp_min = float(df["temp_max"].min()) if "temp_max" in df and not df["temp_max"].dropna().empty else None
    temp_max = float(df["temp_max"].max()) if "temp_max" in df and not df["temp_max"].dropna().empty else None
    precip_min = float(df["precip_sum"].min()) if "precip_sum" in df and not df["precip_sum"].dropna().empty else None
    wind_min = float(df["wind_max"].min()) if "wind_max" in df and not df["wind_max"].dropna().empty else None
    soil_min = float(df["soil_moisture_mean"].min()) if "soil_moisture_mean" in df and not df["soil_moisture_mean"].dropna().empty else None
    soil_max = float(df["soil_moisture_mean"].max()) if "soil_moisture_mean" in df and not df["soil_moisture_mean"].dropna().empty else None

    if temp_min is not None and temp_min < -40:
        warnings.append(f"temp_max below expected range: {temp_min}")
    if temp_max is not None and temp_max > 60:
        warnings.append(f"temp_max above expected range: {temp_max}")
    if precip_min is not None and precip_min < 0:
        fail = True
        warnings.append(f"precip_sum negative: {precip_min}")
    if wind_min is not None and wind_min < 0:
        fail = True
        warnings.append(f"wind_max negative: {wind_min}")
    if soil_min is not None and soil_min < 0:
        warnings.append(f"soil_moisture below 0: {soil_min}")
    if soil_max is not None and soil_max > 1:
        warnings.append(f"soil_moisture above 1: {soil_max}")

    dq_status = "fail_dq" if fail else ("success_with_warnings" if warnings else "pass")
    return {"dq_status": dq_status, "nan_ratios": nan_ratios, "warnings": warnings}


def build_climatology(
    *,
    baseline_start: date,
    baseline_end: date,
    climatology_version: str,
    level: str = "month",
) -> dict[str, Any]:
    t0 = datetime.now(timezone.utc)
    frame = load_features_frame(baseline_start, baseline_end)
    if frame.empty:
        raise RuntimeError("No feature data found for climatology baseline range")
    # Rolling windows per cell.
    frame = frame.sort_values(["lat", "lng", "date"]).copy()
    frame["precip_7d_sum"] = frame.groupby(["lat", "lng"])["precip_sum"].transform(lambda s: s.rolling(7, min_periods=1).sum())
    frame["precip_30d_sum"] = frame.groupby(["lat", "lng"])["precip_sum"].transform(lambda s: s.rolling(30, min_periods=1).sum())
    frame["doy"] = pd.to_datetime(frame["date"]).dt.dayofyear

    def q(v: pd.Series, p: float) -> float | None:
        w = v.dropna()
        if w.empty:
            return None
        return float(w.quantile(p))

    rows: list[dict[str, Any]] = []
    group_cols = ["lat", "lng", "doy"] if level == "doy" else ["lat", "lng", "month"]
    for keys, g in frame.groupby(group_cols, as_index=False):
        lat = float(keys[0])
        lng = float(keys[1])
        key_value = int(keys[2])
        rows.append(
            {
                "climatology_version": climatology_version,
                "cell_lat": lat,
                "cell_lng": lng,
                "month": key_value if level == "month" else None,
                "doy": key_value if level == "doy" else None,
                "temp_max_p95": q(g["temp_max"], 0.95),
                "wind_max_p95": q(g["wind_max"], 0.95),
                "precip_1d_p95": q(g["precip_sum"], 0.95),
                "precip_1d_p99": q(g["precip_sum"], 0.99),
                "precip_7d_p95": q(g["precip_7d_sum"], 0.95),
                "precip_7d_p99": q(g["precip_7d_sum"], 0.99),
                "precip_30d_p10": q(g["precip_30d_sum"], 0.10),
                "soil_moisture_p10": q(g["soil_moisture_mean"], 0.10),
            }
        )
    th_df = pd.DataFrame(rows)
    if th_df.empty:
        raise RuntimeError("Climatology thresholds could not be computed")

    # Persist to GCS parquet.
    if not settings.era5_gcs_bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")
    out_local = Path(tempfile.gettempdir()) / f"orion_climatology_{uuid4().hex}.parquet"
    th_df.to_parquet(out_local, index=False)
    object_name = f"climatology/era5_land/{climatology_version}/{level}/thresholds.parquet"
    client = storage.Client()
    blob = client.bucket(settings.era5_gcs_bucket).blob(object_name)
    blob.upload_from_filename(str(out_local))
    gcs_uri = f"gs://{settings.era5_gcs_bucket}/{object_name}"

    run_id = str(uuid4())
    with SessionLocal() as db:
        # idempotent replace for the same version
        db.execute(delete(ClimatologyThresholdORM).where(ClimatologyThresholdORM.climatology_version == climatology_version))
        db.execute(delete(ClimatologyThresholdDoyORM).where(ClimatologyThresholdDoyORM.climatology_version == climatology_version))
        existing = db.execute(
            select(ClimatologyRunORM).where(ClimatologyRunORM.climatology_version == climatology_version).limit(1)
        ).scalar_one_or_none()
        if existing:
            existing.status = "success"
            existing.row_count = len(th_df)
            existing.thresholds_gcs_uri = gcs_uri
            existing.error = None
        else:
            db.add(
                ClimatologyRunORM(
                    run_id=run_id,
                    climatology_version=climatology_version,
                    dataset="era5-land",
                    baseline_start=baseline_start,
                    baseline_end=baseline_end,
                    level=level,
                    status="success",
                    row_count=len(th_df),
                    thresholds_gcs_uri=gcs_uri,
                    error=None,
                )
            )
        if level == "doy":
            db.add_all([ClimatologyThresholdDoyORM(**{k: v for k, v in r.items() if k != "month"}) for r in rows])
        else:
            db.add_all([ClimatologyThresholdORM(**{k: v for k, v in r.items() if k != "doy"}) for r in rows])
        db.commit()

    LOG.info(
        json.dumps(
            {
                "event": "climatology_build",
                "climatology_version": climatology_version,
                "baseline_start": baseline_start.isoformat(),
                "baseline_end": baseline_end.isoformat(),
                "level": level,
                "row_count": len(rows),
                "thresholds_gcs_uri": gcs_uri,
                "duration_seconds": (datetime.now(timezone.utc) - t0).total_seconds(),
            }
        )
    )
    return {"run_id": run_id, "climatology_version": climatology_version, "row_count": len(rows), "thresholds_gcs_uri": gcs_uri}


def get_thresholds(cell_lat: float, cell_lng: float, dt: date, climatology_version: str) -> dict[str, Any] | None:
    with SessionLocal() as db:
        doy = int(pd.Timestamp(dt).dayofyear)
        exact_doy = db.execute(
            select(ClimatologyThresholdDoyORM).where(
                ClimatologyThresholdDoyORM.climatology_version == climatology_version,
                ClimatologyThresholdDoyORM.cell_lat == cell_lat,
                ClimatologyThresholdDoyORM.cell_lng == cell_lng,
                ClimatologyThresholdDoyORM.doy == doy,
            )
        ).scalar_one_or_none()
        if exact_doy is not None:
            row = exact_doy
            return {
                "temp_max_p95": row.temp_max_p95,
                "wind_max_p95": row.wind_max_p95,
                "precip_1d_p95": row.precip_1d_p95,
                "precip_1d_p99": row.precip_1d_p99,
                "precip_7d_p95": row.precip_7d_p95,
                "precip_7d_p99": row.precip_7d_p99,
                "precip_30d_p10": row.precip_30d_p10,
                "soil_moisture_p10": row.soil_moisture_p10,
            }

        month = dt.month
        exact = db.execute(
            select(ClimatologyThresholdORM).where(
                ClimatologyThresholdORM.climatology_version == climatology_version,
                ClimatologyThresholdORM.cell_lat == cell_lat,
                ClimatologyThresholdORM.cell_lng == cell_lng,
                ClimatologyThresholdORM.month == month,
            )
        ).scalar_one_or_none()
        row = exact
        if row is None:
            cand = db.execute(
                select(ClimatologyThresholdORM).where(
                    ClimatologyThresholdORM.climatology_version == climatology_version,
                    ClimatologyThresholdORM.month == month,
                )
            ).scalars().all()
            if cand:
                row = min(cand, key=lambda r: abs(r.cell_lat - cell_lat) + abs(r.cell_lng - cell_lng))
        if row is None:
            return None
        return {
            "temp_max_p95": row.temp_max_p95,
            "wind_max_p95": row.wind_max_p95,
            "precip_1d_p95": row.precip_1d_p95,
            "precip_1d_p99": row.precip_1d_p99,
            "precip_7d_p95": row.precip_7d_p95,
            "precip_7d_p99": row.precip_7d_p99,
            "precip_30d_p10": row.precip_30d_p10,
            "soil_moisture_p10": row.soil_moisture_p10,
        }


def _band(score: int) -> str:
    if score >= 80:
        return "Extreme"
    if score >= 60:
        return "Major"
    if score >= 40:
        return "Moderate"
    if score >= 20:
        return "Minor"
    return "Minimal"


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> int:
    return int(max(lo, min(hi, round(v))))


def score_heat(hot_days_30d: int, max_excess_c: float) -> int:
    return _clamp((hot_days_30d / 30.0) * 80.0 + max(0.0, max_excess_c) * 4.0)


def score_rain(precip_7d_mm: float, max_1d_mm: float, precip_7d_p95: float, precip_7d_p99: float, precip_1d_p99: float) -> int:
    return _clamp(
        (precip_7d_mm / max(precip_7d_p95, 1e-6)) * 45
        + (precip_7d_mm / max(precip_7d_p99, 1e-6)) * 25
        + (max_1d_mm / max(precip_1d_p99, 1e-6)) * 30
    )


def score_wind(windy_days_30d: int, max_wind_mps: float, wind_p95: float) -> int:
    return _clamp((windy_days_30d / 30.0) * 70 + (max_wind_mps / max(wind_p95, 1e-6)) * 30)


def score_drought(precip_30d_mm: float, precip_30d_p10: float, soil_now: float, soil_p10: float) -> int:
    dry_precip_ratio = max(0.0, 1.0 - (precip_30d_mm / max(precip_30d_p10, 1e-6)))
    dry_soil_ratio = max(0.0, 1.0 - (soil_now / max(soil_p10, 1e-6)))
    return _clamp(dry_precip_ratio * 55 + dry_soil_ratio * 45)


def score_wildfire(
    nearest_7d_km: float | None,
    fires_10km_24h: int,
    fires_10km_7d: int,
    max_frp_20km_7d: float | None,
) -> int:
    if nearest_7d_km is None:
        return 0
    dist_component = max(0.0, (50.0 - min(nearest_7d_km, 50.0)) / 50.0) * 55.0
    count_component = min(fires_10km_24h * 25.0 + fires_10km_7d * 6.0, 35.0)
    frp = max_frp_20km_7d or 0.0
    frp_component = min((frp / 200.0) * 10.0, 10.0)
    return _clamp(dist_component + count_component + frp_component)


def _nearest_coords(asset_lat: float, asset_lng: float, coords_df: pd.DataFrame) -> tuple[float, float]:
    nearest = (
        coords_df.assign(_dist=(coords_df["lat"] - asset_lat).abs() + (coords_df["lng"] - asset_lng).abs())
        .sort_values("_dist")
        .head(1)
    )
    return float(nearest.iloc[0]["lat"]), float(nearest.iloc[0]["lng"])


def _score_single_asset(
    asset_id: str,
    lat: float,
    lon: float,
    df: pd.DataFrame,
    climatology_version: str,
    include_perils: list[str],
    fire_events_df: pd.DataFrame,
) -> list[ScoredRecord]:
    coords = df[["lat", "lng"]].drop_duplicates()
    lat0, lng0 = _nearest_coords(lat, lon, coords)
    sub = df[(df["lat"] == lat0) & (df["lng"] == lng0)].sort_values("date").reset_index(drop=True)
    if sub.empty:
        return []
    out: list[ScoredRecord] = []
    for i in range(len(sub)):
        row = sub.iloc[i]
        dt = row["date"]
        t = get_thresholds(lat0, lng0, dt, climatology_version)
        if not t:
            continue
        w30 = sub.iloc[max(0, i - 29) : i + 1]
        w7 = sub.iloc[max(0, i - 6) : i + 1]
        w30_precip = float(w30["precip_sum"].sum())
        w7_precip = float(w7["precip_sum"].sum())

        # Heat
        if "heat" in include_perils:
            hot_days = int((w30["temp_max"] > (t["temp_max_p95"] or 9999)).sum())
            max_excess = float(max(0.0, (w30["temp_max"] - (t["temp_max_p95"] or 9999)).max()))
            heat_score = score_heat(hot_days, max_excess)
            out.append(
                ScoredRecord(
                    asset_id=asset_id,
                    score_date=dt,
                    peril="heat",
                    score_0_100=heat_score,
                    band=_band(heat_score),
                    exposure={"hot_days_30d": hot_days, "max_excess_c": round(max_excess, 2)},
                    drivers=[f"{hot_days} hot days in last 30d above local p95", f"max exceedance {max_excess:.2f}C"],
                )
            )

        # Heavy rain proxy
        if "rain" in include_perils:
            max_1d = float(w7["precip_sum"].max())
            p7p95 = t["precip_7d_p95"] or 1.0
            p7p99 = t["precip_7d_p99"] or p7p95
            p1p99 = t["precip_1d_p99"] or 1.0
            rain_score = score_rain(w7_precip, max_1d, p7p95, p7p99, p1p99)
            out.append(
                ScoredRecord(
                    asset_id=asset_id,
                    score_date=dt,
                    peril="rain",
                    score_0_100=rain_score,
                    band=_band(rain_score),
                    exposure={"precip_7d_mm": round(w7_precip, 2), "max_1d_mm": round(max_1d, 2)},
                    drivers=[
                        f"7d precip {w7_precip:.1f}mm vs local p95 {p7p95:.1f}",
                        f"max 1d precip {max_1d:.1f}mm vs local p99 {p1p99:.1f}",
                    ],
                )
            )

        # Wind
        if "wind" in include_perils:
            windy_days = int((w30["wind_max"] > (t["wind_max_p95"] or 9999)).sum())
            max_wind = float(w30["wind_max"].max())
            wind_score = score_wind(windy_days, max_wind, t["wind_max_p95"] or 1.0)
            out.append(
                ScoredRecord(
                    asset_id=asset_id,
                    score_date=dt,
                    peril="wind",
                    score_0_100=wind_score,
                    band=_band(wind_score),
                    exposure={"windy_days_30d": windy_days, "max_wind_mps": round(max_wind, 2)},
                    drivers=[
                        f"{windy_days} windy days in last 30d above local p95",
                        f"max wind {max_wind:.1f} m/s",
                    ],
                )
            )

        # Drought
        if "drought" in include_perils:
            soil_now = float(w30["soil_moisture_mean"].mean())
            p30p10 = t["precip_30d_p10"] or 1.0
            soil_p10 = t["soil_moisture_p10"] or 0.1
            dry_soil_ratio = max(0.0, 1.0 - (soil_now / max(soil_p10, 1e-6)))
            drought_score = score_drought(w30_precip, p30p10, soil_now, soil_p10)
            out.append(
                ScoredRecord(
                    asset_id=asset_id,
                    score_date=dt,
                    peril="drought",
                    score_0_100=drought_score,
                    band=_band(drought_score),
                    exposure={"precip_30d_mm": round(w30_precip, 2), "soil_moisture_anom_ratio": round(dry_soil_ratio, 4)},
                    drivers=[
                        f"30d precip {w30_precip:.1f}mm vs dry-tail p10 {p30p10:.1f}",
                        f"soil moisture ratio to p10: {soil_now:.3f}/{soil_p10:.3f}",
                    ],
                )
            )

        # Wildfire
        if "wildfire" in include_perils:
            ref_end = datetime.combine(dt, time.max, tzinfo=timezone.utc)
            feat = wildfire_features_for_point(lat=lat, lon=lon, events_df=fire_events_df, window_end=ref_end)
            nearest24 = feat["nearest_fire_distance_km_24h"]
            nearest7 = feat["nearest_fire_distance_km_7d"]
            count24 = int(feat["fires_within_10km_count_24h"])
            count7 = int(feat["fires_within_10km_count_7d"])
            max_frp7 = feat["max_frp_within_20km_7d"]
            wf_score = score_wildfire(nearest7, count24, count7, max_frp7)
            drivers = [
                f"{count24} active fires within 10km in last 24h",
                (
                    f"nearest fire {nearest7:.1f}km away in last 7d"
                    if nearest7 is not None
                    else "no nearby fires detected in last 7d"
                ),
            ]
            out.append(
                ScoredRecord(
                    asset_id=asset_id,
                    score_date=dt,
                    peril="wildfire",
                    score_0_100=wf_score,
                    band=_band(wf_score),
                    exposure={
                        "nearest_fire_distance_km_24h": nearest24,
                        "nearest_fire_distance_km_7d": nearest7,
                        "fires_within_10km_count_24h": count24,
                        "fires_within_10km_count_7d": count7,
                        "max_frp_within_20km_7d": max_frp7,
                    },
                    drivers=drivers,
                )
            )
    return out


def _upsert_portfolio_assets(portfolio_id: str, assets: list[dict[str, Any]]) -> None:
    if not assets:
        return
    with SessionLocal() as db:
        for a in assets:
            found = db.execute(
                select(PortfolioAssetORM).where(
                    PortfolioAssetORM.portfolio_id == portfolio_id,
                    PortfolioAssetORM.asset_id == a["asset_id"],
                )
            ).scalar_one_or_none()
            if found:
                found.lat = a["lat"]
                found.lon = a["lon"]
            else:
                db.add(
                    PortfolioAssetORM(
                        portfolio_id=portfolio_id,
                        asset_id=a["asset_id"],
                        lat=a["lat"],
                        lon=a["lon"],
                    )
                )
        db.commit()


def batch_score_assets(
    *,
    assets: list[dict[str, Any]],
    start_date: date,
    end_date: date,
    climatology_version: str,
    persist: bool = True,
    include_perils: list[str] | None = None,
) -> dict[str, Any]:
    if include_perils is None:
        include_perils = ["heat", "rain", "wind", "drought"]
    include_perils = [p for p in include_perils if p in {"heat", "rain", "wind", "drought", "wildfire"}]
    if not include_perils:
        include_perils = ["heat", "rain", "wind", "drought"]
    run_id = str(uuid4())
    t0 = datetime.now(timezone.utc)
    df = load_features_frame(start_date, end_date)
    if df.empty:
        return {"run_id": run_id, "assets": {}}
    dq = evaluate_feature_dq(df)
    fire_events_df = load_fire_events_frame(
        datetime.combine(start_date, time.min, tzinfo=timezone.utc) - timedelta(days=7),
        datetime.combine(end_date, time.max, tzinfo=timezone.utc),
    )
    scored_map: dict[str, list[dict[str, Any]]] = {}
    rows_to_persist: list[AssetRiskScoreORM] = []
    for a in assets:
        records = _score_single_asset(
            a["asset_id"],
            a["lat"],
            a["lon"],
            df,
            climatology_version,
            include_perils=include_perils,
            fire_events_df=fire_events_df,
        )
        scored_map[a["asset_id"]] = [
            {
                "date": r.score_date.isoformat(),
                "peril": r.peril,
                "score_0_100": r.score_0_100,
                "band": r.band,
                "exposure": r.exposure,
                "drivers": r.drivers,
            }
            for r in records
        ]
        if persist:
            for r in records:
                rows_to_persist.append(
                    AssetRiskScoreORM(
                        asset_id=r.asset_id,
                        score_date=r.score_date,
                        peril=r.peril,
                        scenario="historical",
                        horizon="current",
                        likelihood="observed",
                        score_0_100=r.score_0_100,
                        band=r.band,
                        exposure_json=json.dumps(r.exposure),
                        drivers_json=json.dumps(r.drivers),
                        run_id=run_id,
                        climatology_version=climatology_version,
                        data_version="era5_daily_v1",
                    )
                )
    if persist and rows_to_persist:
        with SessionLocal() as db:
            asset_ids = [a["asset_id"] for a in assets]
            db.execute(
                delete(AssetRiskScoreORM).where(
                    AssetRiskScoreORM.asset_id.in_(asset_ids),
                    AssetRiskScoreORM.score_date >= start_date,
                    AssetRiskScoreORM.score_date <= end_date,
                    AssetRiskScoreORM.climatology_version == climatology_version,
                )
            )
            db.add_all(rows_to_persist)
            db.commit()
    LOG.info(
        json.dumps(
            {
                "event": "batch_score",
                "run_id": run_id,
                "asset_count": len(assets),
                "date_range": [start_date.isoformat(), end_date.isoformat()],
                "rows_scored": len(rows_to_persist),
                "persist": persist,
                "climatology_version": climatology_version,
                "perils": include_perils,
                "duration_seconds": (datetime.now(timezone.utc) - t0).total_seconds(),
                "dq_status": dq.get("dq_status"),
            }
        )
    )
    return {"run_id": run_id, "assets": scored_map, "dq": dq}


def get_portfolio_assets(portfolio_id: str) -> list[dict[str, Any]]:
    with SessionLocal() as db:
        rows = db.execute(select(PortfolioAssetORM).where(PortfolioAssetORM.portfolio_id == portfolio_id)).scalars().all()
    return [{"asset_id": r.asset_id, "lat": r.lat, "lon": r.lon} for r in rows]


def save_portfolio_assets(portfolio_id: str, assets: list[dict[str, Any]]) -> None:
    _upsert_portfolio_assets(portfolio_id, assets)


def portfolio_risk_summary(portfolio_id: str, start_date: date, end_date: date) -> dict[str, Any]:
    with SessionLocal() as db:
        portfolio_rows = db.execute(
            select(PortfolioAssetORM).where(PortfolioAssetORM.portfolio_id == portfolio_id)
        ).scalars().all()
        assets_meta = {r.asset_id: {"lat": float(r.lat), "lon": float(r.lon)} for r in portfolio_rows}
        rows = db.execute(
            select(AssetRiskScoreORM).where(
                AssetRiskScoreORM.asset_id.in_(assets_meta.keys()),
                AssetRiskScoreORM.score_date >= start_date,
                AssetRiskScoreORM.score_date <= end_date,
            )
        ).scalars().all()
    if not rows:
        return {
            "distribution": {},
            "top_10_assets": [],
            "trend_summary": {"message": "no scores"},
            "bands": {},
            "peril_averages": {},
            "top_assets": [],
            "trend": [],
        }
    records = [
        {
            "asset_id": r.asset_id,
            "date": r.score_date,
            "peril": r.peril,
            "score": r.score_0_100,
            "band": r.band,
        }
        for r in rows
    ]
    df = pd.DataFrame(records)
    dist: dict[str, dict[str, int]] = {}
    for peril, g in df.groupby("peril"):
        dist[peril] = g["band"].value_counts().to_dict()
    allhaz = (
        df.groupby(["asset_id", "date"])["score"].mean().reset_index().assign(
            band=lambda x: x["score"].apply(lambda s: _band(int(round(s))))
        )
    )
    dist["all_hazards"] = allhaz["band"].value_counts().to_dict()
    top_assets = (
        allhaz.groupby("asset_id")["score"].mean().reset_index().sort_values("score", ascending=False).head(10)
    )
    trend_daily = allhaz.groupby("date")["score"].mean().reset_index().sort_values("date")
    trend = "stable"
    if len(trend_daily) >= 2:
        delta = float(trend_daily.iloc[-1]["score"] - trend_daily.iloc[0]["score"])
        if delta > 2:
            trend = "up"
        elif delta < -2:
            trend = "down"
    # Contract-style overall bands from all-hazards
    band_counts = allhaz["band"].str.lower().value_counts().to_dict()
    for key in ["minimal", "minor", "moderate", "major", "extreme"]:
        band_counts.setdefault(key, 0)
    peril_avg = {p: round(float(v), 2) for p, v in df.groupby("peril")["score"].mean().to_dict().items()}
    if peril_avg:
        peril_avg["all"] = round(float(sum(peril_avg.values()) / len(peril_avg)), 2)
    trend_points = []
    by_date_peril = df.pivot_table(index="date", columns="peril", values="score", aggfunc="mean").reset_index()
    for _, row in by_date_peril.iterrows():
        scores = {k: round(float(v), 2) for k, v in row.items() if k != "date" and pd.notna(v)}
        if scores:
            scores["all"] = round(float(sum(scores.values()) / len(scores)), 2)
        trend_points.append({"date": row["date"].isoformat(), "scores": scores})

    top_contract = []
    allhaz_by_asset = allhaz.groupby("asset_id")["score"].mean().to_dict()
    per_asset_peril = df.groupby(["asset_id", "peril"])["score"].mean().unstack(fill_value=0.0)
    for asset_id, avg_all in sorted(allhaz_by_asset.items(), key=lambda x: x[1], reverse=True)[:10]:
        scores = {p: round(float(v), 2) for p, v in per_asset_peril.loc[asset_id].to_dict().items()}
        scores["all"] = round(float(avg_all), 2)
        top_contract.append(
            {
                "asset_id": asset_id,
                "name": asset_id,
                "lat": assets_meta.get(asset_id, {}).get("lat"),
                "lon": assets_meta.get(asset_id, {}).get("lon"),
                "band": _band(int(round(avg_all))).lower(),
                "scores": scores,
            }
        )

    return {
        "distribution": dist,
        "top_10_assets": [
            {"asset_id": r["asset_id"], "avg_all_hazards_score": round(float(r["score"]), 2)}
            for _, r in top_assets.iterrows()
        ],
        "trend_summary": {
            "direction": trend,
            "start_score": round(float(trend_daily.iloc[0]["score"]), 2) if len(trend_daily) else None,
            "end_score": round(float(trend_daily.iloc[-1]["score"]), 2) if len(trend_daily) else None,
        },
        "bands": band_counts,
        "peril_averages": peril_avg,
        "top_assets": top_contract,
        "trend": trend_points,
    }


def benchmark_batch_scoring(*, assets_count: int, start_date: date, end_date: date, climatology_version: str) -> dict[str, Any]:
    df = load_features_frame(start_date, end_date)
    if df.empty:
        raise RuntimeError("No feature data available for benchmark range")
    coords = df[["lat", "lng"]].drop_duplicates().reset_index(drop=True)
    if coords.empty:
        raise RuntimeError("No coordinates in feature frame")
    assets: list[dict[str, Any]] = []
    for i in range(assets_count):
        row = coords.iloc[i % len(coords)]
        assets.append({"asset_id": f"bench-{i+1}", "lat": float(row["lat"]), "lon": float(row["lng"])})
    t0 = datetime.now(timezone.utc)
    out = batch_score_assets(
        assets=assets,
        start_date=start_date,
        end_date=end_date,
        climatology_version=climatology_version,
        persist=False,
    )
    seconds = (datetime.now(timezone.utc) - t0).total_seconds()
    days = (end_date - start_date).days + 1
    return {
        "run_id": out["run_id"],
        "assets_count": assets_count,
        "days": days,
        "duration_seconds": round(seconds, 3),
        "per_asset_ms": round((seconds / max(assets_count, 1)) * 1000, 3),
        "per_asset_day_ms": round((seconds / max(assets_count * days, 1)) * 1000, 5),
    }
