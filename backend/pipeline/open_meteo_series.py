from __future__ import annotations

from datetime import date

import requests

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_open_meteo_daily(lat: float, lng: float, start_date: date, end_date: date) -> list[dict]:
    params = {
        "latitude": lat,
        "longitude": lng,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": "temperature_2m_mean,temperature_2m_max,precipitation_sum,wind_speed_10m_max",
        "timezone": "UTC",
    }
    res = requests.get(ARCHIVE_URL, params=params, timeout=60)
    res.raise_for_status()
    daily = res.json().get("daily", {})
    times = daily.get("time", [])
    tmean = daily.get("temperature_2m_mean", [])
    tmax = daily.get("temperature_2m_max", [])
    precip = daily.get("precipitation_sum", [])
    wind = daily.get("wind_speed_10m_max", [])

    out: list[dict] = []
    for i, d in enumerate(times):
        out.append(
            {
                "date": d,
                "temp_mean": float(tmean[i]) if i < len(tmean) and tmean[i] is not None else None,
                "temp_max": float(tmax[i]) if i < len(tmax) and tmax[i] is not None else None,
                "precip_sum": float(precip[i]) if i < len(precip) and precip[i] is not None else None,
                "wind_max": float(wind[i]) if i < len(wind) and wind[i] is not None else None,
                "soil_moisture_mean": None,
                "source": "open-meteo",
            }
        )
    return out


def fetch_open_meteo_today(lat: float, lng: float) -> dict | None:
    params = {
        "latitude": lat,
        "longitude": lng,
        "daily": "temperature_2m_mean,temperature_2m_max,precipitation_sum,wind_speed_10m_max",
        "timezone": "UTC",
        "forecast_days": 1,
    }
    res = requests.get(FORECAST_URL, params=params, timeout=30)
    res.raise_for_status()
    daily = res.json().get("daily", {})
    times = daily.get("time", [])
    if not times:
        return None
    return {
        "date": times[0],
        "temp_mean": float(daily.get("temperature_2m_mean", [None])[0]) if daily.get("temperature_2m_mean") else None,
        "temp_max": float(daily.get("temperature_2m_max", [None])[0]) if daily.get("temperature_2m_max") else None,
        "precip_sum": float(daily.get("precipitation_sum", [None])[0]) if daily.get("precipitation_sum") else None,
        "wind_max": float(daily.get("wind_speed_10m_max", [None])[0]) if daily.get("wind_speed_10m_max") else None,
        "soil_moisture_mean": None,
        "source": "open-meteo",
    }
