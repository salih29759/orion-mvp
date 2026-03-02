#!/usr/bin/env python3
"""
Robust ERA5-Land full backfill runner.

- Pulls 1950-2026 monthly data for Turkey bbox.
- Uses /jobs/era5/ingest endpoint.
- Splits variables into groups to reduce CDS request failures.
- Limits in-flight jobs (concurrency).
- Retries failed jobs.
- Persists state so it can resume after interruption.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path
import sys
import time
from typing import Any

import requests


ALL_VARIABLES = [
    "2m_dewpoint_temperature",
    "2m_temperature",
    "skin_temperature",
    "soil_temperature_level_1",
    "soil_temperature_level_2",
    "soil_temperature_level_3",
    "soil_temperature_level_4",
    "lake_bottom_temperature",
    "lake_ice_depth",
    "lake_ice_temperature",
    "lake_mix_layer_depth",
    "lake_mix_layer_temperature",
    "lake_shape_factor",
    "lake_total_layer_temperature",
    "snow_albedo",
    "snow_cover",
    "snow_density",
    "snow_depth",
    "snow_depth_water_equivalent",
    "snowfall",
    "snowmelt",
    "temperature_of_snow_layer",
    "skin_reservoir_content",
    "volumetric_soil_water_layer_1",
    "volumetric_soil_water_layer_2",
    "volumetric_soil_water_layer_3",
    "volumetric_soil_water_layer_4",
    "forecast_albedo",
    "surface_latent_heat_flux",
    "surface_net_solar_radiation",
    "surface_net_thermal_radiation",
    "surface_sensible_heat_flux",
    "surface_solar_radiation_downwards",
    "surface_thermal_radiation_downwards",
    "evaporation_from_bare_soil",
    "evaporation_from_open_water_surfaces_excluding_oceans",
    "evaporation_from_the_top_of_canopy",
    "evaporation_from_vegetation_transpiration",
    "potential_evaporation",
    "runoff",
    "snow_evaporation",
    "sub_surface_runoff",
    "surface_runoff",
    "total_evaporation",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "surface_pressure",
    "total_precipitation",
    "leaf_area_index_high_vegetation",
    "leaf_area_index_low_vegetation",
    "high_vegetation_cover",
    "glacier_mask",
    "lake_cover",
    "low_vegetation_cover",
    "lake_total_depth",
    "geopotential",
    "land_sea_mask",
    "soil_type",
    "type_of_high_vegetation",
    "type_of_low_vegetation",
]


SUCCESS_STATES = {"success", "success_with_warnings"}
FAILED_STATES = {"failed", "fail_dq"}


@dataclass(frozen=True)
class Task:
    month: str
    start_date: str
    end_date: str
    group_id: int
    variables: list[str]

    @property
    def key(self) -> str:
        return f"{self.month}|g{self.group_id}"


def month_iter(start_month: str, end_month: str) -> list[str]:
    sy, sm = [int(x) for x in start_month.split("-")]
    ey, em = [int(x) for x in end_month.split("-")]
    out: list[str] = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return out


def month_bounds(month: str) -> tuple[str, str]:
    y, m = [int(x) for x in month.split("-")]
    start = date(y, m, 1)
    if m == 12:
        end = date(y + 1, 1, 1) - date.resolution
    else:
        end = date(y, m + 1, 1) - date.resolution
    return start.isoformat(), end.isoformat()


def chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"tasks": {}}
    return json.loads(path.read_text())


def save_state(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True))


def submit_job(base_url: str, api_key: str, task: Task, bbox: dict[str, float]) -> dict[str, Any]:
    payload = {
        "dataset": "era5-land",
        "variables": task.variables,
        "start_date": task.start_date,
        "end_date": task.end_date,
        "bbox": bbox,
        "format": "netcdf",
    }
    r = requests.post(
        f"{base_url}/jobs/era5/ingest",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=60,
    )
    if r.status_code not in (200, 202):
        raise RuntimeError(f"submit failed {r.status_code}: {r.text}")
    return r.json()


def fetch_job(base_url: str, api_key: str, job_id: str) -> dict[str, Any]:
    r = requests.get(
        f"{base_url}/jobs/{job_id}",
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=60,
    )
    if r.status_code != 200:
        raise RuntimeError(f"status failed {r.status_code}: {r.text}")
    return r.json()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="https://orion-api-126886725893.europe-west1.run.app")
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--start-month", default="1950-01")
    parser.add_argument("--end-month", default="2026-12")
    parser.add_argument("--group-size", type=int, default=10)
    parser.add_argument("--max-inflight", type=int, default=2)
    parser.add_argument("--poll-seconds", type=int, default=15)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument(
        "--state-file",
        default="era5_full_backfill_state.json",
        help="Path for resumable local state JSON",
    )
    args = parser.parse_args()

    bbox = {"north": 42, "west": 26, "south": 36, "east": 45}
    state_path = Path(args.state_file).resolve()
    state = load_state(state_path)
    tasks_state: dict[str, Any] = state.setdefault("tasks", {})

    months = month_iter(args.start_month, args.end_month)
    groups = chunked(ALL_VARIABLES, args.group_size)

    all_tasks: list[Task] = []
    for month in months:
        start_date, end_date = month_bounds(month)
        for i, group in enumerate(groups, start=1):
            all_tasks.append(Task(month=month, start_date=start_date, end_date=end_date, group_id=i, variables=group))

    pending = [t for t in all_tasks if tasks_state.get(t.key, {}).get("status") not in ("success", "permanent_fail")]
    inflight: dict[str, str] = {}

    print(f"Total tasks={len(all_tasks)} pending={len(pending)} groups={len(groups)} months={len(months)}")

    while pending or inflight:
        while pending and len(inflight) < args.max_inflight:
            task = pending.pop(0)
            tstate = tasks_state.setdefault(task.key, {"retries": 0})
            if tstate.get("status") == "permanent_fail":
                continue
            try:
                out = submit_job(args.base_url, args.api_key, task, bbox)
                job_id = out["job_id"]
                inflight[task.key] = job_id
                tstate.update({"status": "submitted", "job_id": job_id, "last_submit_ts": int(time.time())})
                print(f"[SUBMIT] {task.key} -> {job_id} dedup={out.get('deduplicated')}")
            except Exception as exc:  # noqa: BLE001
                err = str(exc)
                if "submit failed 429" in err and "concurrency limit reached" in err.lower():
                    # Queue is full right now; do not count as retry/failure.
                    tstate["status"] = "queue_wait"
                    pending.append(task)
                    print(f"[WAIT] {task.key} queue full, will retry submit later")
                    time.sleep(args.poll_seconds)
                    save_state(state_path, state)
                    continue
                retries = int(tstate.get("retries", 0)) + 1
                tstate["retries"] = retries
                tstate["last_error"] = str(exc)
                if retries >= args.max_retries:
                    tstate["status"] = "permanent_fail"
                    print(f"[FAIL] {task.key} retries={retries} error={exc}")
                else:
                    tstate["status"] = "retry_pending"
                    pending.append(task)
                    print(f"[RETRY] {task.key} retries={retries} error={exc}")
            save_state(state_path, state)

        if not inflight:
            continue

        time.sleep(args.poll_seconds)
        done_keys: list[str] = []
        for key, job_id in list(inflight.items()):
            tstate = tasks_state[key]
            try:
                j = fetch_job(args.base_url, args.api_key, job_id)
                status = j.get("status")
                if status in SUCCESS_STATES:
                    tstate["status"] = "success"
                    tstate["finished_status"] = status
                    done_keys.append(key)
                    print(f"[OK] {key} job={job_id} status={status}")
                elif status in FAILED_STATES:
                    retries = int(tstate.get("retries", 0)) + 1
                    tstate["retries"] = retries
                    tstate["last_error"] = j.get("error")
                    done_keys.append(key)
                    if retries >= args.max_retries:
                        tstate["status"] = "permanent_fail"
                        print(f"[FAIL] {key} job={job_id} status={status} retries={retries}")
                    else:
                        tstate["status"] = "retry_pending"
                        month, group = key.split("|")
                        group_id = int(group.replace("g", ""))
                        # Rebuild task from key.
                        vars_group = groups[group_id - 1]
                        s, e = month_bounds(month)
                        pending.append(Task(month=month, start_date=s, end_date=e, group_id=group_id, variables=vars_group))
                        print(f"[RETRY] {key} job={job_id} status={status} retries={retries}")
                else:
                    tstate["status"] = status
            except Exception as exc:  # noqa: BLE001
                tstate["last_error"] = str(exc)
            save_state(state_path, state)

        for key in done_keys:
            inflight.pop(key, None)

    success = sum(1 for v in tasks_state.values() if v.get("status") == "success")
    permanent_fail = sum(1 for v in tasks_state.values() if v.get("status") == "permanent_fail")
    print(f"Done. success={success} permanent_fail={permanent_fail} total={len(all_tasks)} state={state_path}")
    return 0 if permanent_fail == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
