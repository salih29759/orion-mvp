from __future__ import annotations

from collections import defaultdict
from typing import Any

from pipeline.risk_scoring import batch_score_assets


def normalize_perils(perils: list[str] | None) -> list[str]:
    allowed = {"heat", "rain", "wind", "drought", "wildfire"}
    if not perils:
        return ["heat", "rain", "wind", "drought"]
    if "all" in perils:
        return ["heat", "rain", "wind", "drought", "wildfire"]
    out = [p for p in perils if p in allowed]
    if not out:
        return ["heat", "rain", "wind", "drought"]
    return out


def to_batch_results(assets_payload: dict[str, list[dict[str, Any]]], include_perils: list[str]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for asset_id, rows in assets_payload.items():
        by_date: dict[str, dict[str, Any]] = defaultdict(lambda: {"scores": {}, "bands": {}, "drivers": {}})
        for row in rows:
            dt = row["date"]
            peril = row["peril"]
            by_date[dt]["scores"][peril] = float(row["score_0_100"])
            by_date[dt]["bands"][peril] = row["band"].lower()
            by_date[dt]["drivers"][peril] = row.get("drivers", [])

        series: list[dict[str, Any]] = []
        for dt in sorted(by_date.keys()):
            point = by_date[dt]
            if include_perils:
                values = [point["scores"][p] for p in include_perils if p in point["scores"]]
                if values:
                    all_score = round(sum(values) / len(values), 2)
                    point["scores"]["all"] = all_score
                    point["bands"]["all"] = (
                        "extreme"
                        if all_score >= 80
                        else "major"
                        if all_score >= 60
                        else "moderate"
                        if all_score >= 40
                        else "minor"
                        if all_score >= 20
                        else "minimal"
                    )

            series.append(
                {
                    "date": dt,
                    "scores": point["scores"],
                    "bands": point["bands"],
                    "drivers": point["drivers"],
                }
            )
        results.append({"asset_id": asset_id, "series": series})
    return results


def run_batch_scores(
    *,
    assets: list[dict[str, Any]],
    start_date,
    end_date,
    climatology_version: str,
    include_perils: list[str] | None,
) -> dict[str, Any]:
    normalized = normalize_perils(include_perils)
    out = batch_score_assets(
        assets=assets,
        start_date=start_date,
        end_date=end_date,
        climatology_version=climatology_version,
        persist=True,
        include_perils=normalized,
    )
    return {
        "run_id": out["run_id"],
        "climatology_version": climatology_version,
        "results": to_batch_results(out["assets"], include_perils=normalized),
    }

