from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any

import pydantic_monty
from loguru import logger


class ScriptExecutionError(RuntimeError):
    pass


# ── external helper functions provided to every script ──────────────

def _alert(
    alert_type: str,
    severity: str,
    title: str,
    summary: str,
    evidence: dict[str, Any],
    actions: list[Any],
) -> dict[str, Any]:
    """Construct a standardised alert dict inside the sandbox."""
    return {
        "alert_type": alert_type,
        "severity": severity,
        "title": title,
        "summary": summary,
        "evidence": evidence,
        "recommended_actions": [
            {"action_type": a[0], "params": a[1]} if isinstance(a, (list, tuple))
            else a
            for a in (actions or [])
        ][:3],
    }


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _stdev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    m = sum(values) / len(values)
    return math.sqrt(sum((x - m) ** 2 for x in values) / len(values))


def _z_score(value: float, values: list[float]) -> float:
    if not values:
        return 0.0
    m = sum(values) / len(values)
    sd = _stdev(values)
    if sd == 0.0:
        return 0.0 if value == m else (10.0 if value > m else -10.0)
    return (value - m) / sd


def _percentile(value: float, values: list[float]) -> float:
    if not values:
        return 0.5
    return sum(1 for v in values if v <= value) / len(values)


def _days_between(date1: str, date2: str) -> int:
    d1 = date.fromisoformat(date1)
    d2 = date.fromisoformat(date2)
    return (d2 - d1).days


def _date_add(dt: str, days: int) -> str:
    return (date.fromisoformat(dt) + timedelta(days=days)).isoformat()


EXTERNAL_FUNCTIONS: dict[str, Any] = {
    "alert": _alert,
    "mean": _mean,
    "stdev": _stdev,
    "z_score": _z_score,
    "percentile": _percentile,
    "days_between": _days_between,
    "date_add": _date_add,
}

# Names that scripts receive as input variables
INPUT_NAMES: list[str] = [
    "as_of_date",
    "location_id",
    "machine_id",
    "currency",
    "machine",
    "location",
    "baseline_dates",
    "daily_product_sales",
    "revenue",
    "ingredient_use",
    "price_discrepancies",
    "product_forecasts",
    "ingredient_forecasts",
    "hourly_product_sales",
    "cash_mix",
    "daily_totals",
]


def run_script(
    *,
    script_name: str,
    code: str,
    context: dict[str, Any],
    timeout_seconds: int = 8,
) -> list[dict[str, Any]]:
    """Execute an alert script in the Monty sandbox and return emitted alerts."""
    # Append bare `result` so Monty returns the variable as the last expression
    full_code = code.rstrip() + "\nresult\n"
    try:
        m = pydantic_monty.Monty(
            full_code,
            inputs=INPUT_NAMES,
            external_functions=list(EXTERNAL_FUNCTIONS.keys()),
            script_name=f"{script_name}.py",
        )
        output = m.run(
            inputs=context,
            external_functions=EXTERNAL_FUNCTIONS,
        )
    except Exception as exc:
        raise ScriptExecutionError(f"{script_name}: {exc}") from exc

    if not isinstance(output, list):
        raise ScriptExecutionError(
            f"{script_name}: script must set `result` to a list, got {type(output).__name__}"
        )
    for item in output:
        if not isinstance(item, dict):
            raise ScriptExecutionError(
                f"{script_name}: each alert must be a dict, got {type(item).__name__}"
            )
        item.setdefault("location_id", context["location_id"])
        item.setdefault("machine_id", context["machine_id"])
    return output
