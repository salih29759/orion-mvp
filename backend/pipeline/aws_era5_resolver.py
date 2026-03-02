from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date

from app.config import settings
from pipeline.aws_era5_catalog import get_month_variables


@dataclass
class MonthProviderDecision:
    month_label: str
    start_date: date
    end_date: date
    provider: str  # aws|cds
    reason: str


def _iter_months(start_month: str, end_month: str) -> list[tuple[str, int, int]]:
    sy, sm = [int(x) for x in start_month.split("-")]
    ey, em = [int(x) for x in end_month.split("-")]
    y, m = sy, sm
    out: list[tuple[str, int, int]] = []
    while (y, m) <= (ey, em):
        out.append((f"{y:04d}-{m:02d}", y, m))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return out


def _month_range(y: int, m: int) -> tuple[date, date]:
    s = date(y, m, 1)
    e = date(y, m, monthrange(y, m)[1])
    return s, e


def _month_has_required_vars(month_vars: set[str], variables: list[str]) -> tuple[bool, str]:
    missing: list[str] = []
    for var in variables:
        if var == "total_precipitation":
            has_total = "total_precipitation" in month_vars
            has_components = {"large_scale_precipitation", "convective_precipitation"}.issubset(month_vars)
            if not (has_total or has_components):
                missing.append(var)
        elif var not in month_vars:
            missing.append(var)

    if missing:
        return False, f"missing_vars={','.join(missing)}"
    return True, "all_vars_available"


def resolve_months_provider(
    *,
    start_month: str,
    end_month: str,
    variables: list[str],
) -> list[MonthProviderDecision]:
    decisions: list[MonthProviderDecision] = []
    for month_label, y, m in _iter_months(start_month, end_month):
        s, e = _month_range(y, m)
        if not settings.era5_hybrid_enable:
            decisions.append(MonthProviderDecision(month_label=month_label, start_date=s, end_date=e, provider="cds", reason="hybrid_disabled"))
            continue

        month_vars = get_month_variables(y, m)
        ok, reason = _month_has_required_vars(month_vars, variables)
        if ok:
            decisions.append(MonthProviderDecision(month_label=month_label, start_date=s, end_date=e, provider="aws", reason=reason))
        else:
            provider = "cds" if settings.era5_cds_fallback_enable else "aws"
            decisions.append(MonthProviderDecision(month_label=month_label, start_date=s, end_date=e, provider=provider, reason=reason))
    return decisions
