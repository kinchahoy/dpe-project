from __future__ import annotations

SCRIPT_GENERATION_TEMPLATE = """\
You are generating a Python alert script for a vending machine monitoring system.
The script runs in a sandbox with NO imports allowed. Write plain Python only.

## Available input variables

| Variable | Type | Description |
|----------|------|-------------|
| `ctx` | `dict` | The full hierarchical script context (see below) |

### `ctx` shape (high-level)

- `ctx["meta"]`: `{{"as_of_date": "YYYY-MM-DD", "currency": "USD", "run_id": str | None}}`
- `ctx["ids"]`: `{{"location_id": int, "machine_id": int}}`
- `ctx["entities"]`:
  - `ctx["entities"]["location"]`: location metadata dict
  - `ctx["entities"]["machine"]`: machine metadata dict
- `ctx["days"]`: list of day objects (past observed + future predicted)
  - shared fields: `{{"kind": "observed"|"predicted", "date": "YYYY-MM-DD", "offset_days": int}}`
  - observed fields:
    - `totals`: `{{"units": float, "revenue": float, "card_share": float | None}}`
    - `by_product`: `[{{"product_id": int, "product_name": str, "units": float, "revenue": float}}]`
    - `by_ingredient`: `[{{"ingredient_id": int, "ingredient_name": str, "qty": float, "unit": str}}]`
  - predicted fields:
    - `by_product`: `[{{"product_id": int, "product_name": str, "units": float}}]`
    - `by_ingredient`: `[{{"ingredient_id": int, "ingredient_name": str, "qty": float, "unit": str}}]`
- `ctx["inventory"]`:
  - `snapshot_date`: `"YYYY-MM-DD" | None`
  - `by_ingredient`: `[{{"ingredient_id": int, "ingredient_name": str, "qty_on_hand": float, "unit": str, "capacity": float | None, "capacity_unit": str | None}}]`
- `ctx["price_anomalies"]`:
  - `[{{"product_id": int, "product_name": str | None, "undercharge_count": int, "examples": list[dict]}}]`

## Available helper functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `alert` | `(type, severity, title, summary, evidence, actions)` | Create alert dict. actions is list of (ACTION_TYPE, params_dict) tuples |
| `mean` | `(values: list[float]) -> float` | Arithmetic mean |
| `stdev` | `(values: list[float]) -> float` | Population standard deviation |
| `z_score` | `(value: float, values: list[float]) -> float` | Z-score of value vs list |
| `percentile` | `(value: float, values: list[float]) -> float` | Percentile rank 0-1 |
| `days_between` | `(date1: str, date2: str) -> int` | Days between two ISO dates |
| `date_add` | `(date: str, days: int) -> str` | Offset an ISO date string |

## Action types

RESTOCK_MACHINE, ORDER_INGREDIENTS, ADJUST_PRICE, SCHEDULE_SERVICE, CHECK_MACHINE, PROPOSE_DISCONTINUE

## Severity levels

LOW, MEDIUM, HIGH, CRITICAL

## Constraints

- NO imports — the sandbox blocks all imports
- Set `result` to a `list` of alert dicts (empty list = no alert)
- Max 100 lines
- Use only the variables and functions listed above
- Prefer small local constants and simple percent/mean comparisons

## Example script

```python
# Detect machine-level sales drop vs recent observed mean.
as_of = ctx["meta"]["as_of_date"]
days = ctx.get("days", [])
obs = [d for d in days if d.get("kind") == "observed"]
today = [d for d in obs if d.get("date") == as_of]
baseline = [d for d in obs if d.get("date") != as_of and d.get("totals")]

if not today or not baseline:
    result = []
else:
    today_units = float(today[0]["totals"].get("units") or 0.0)
    bl_units = [float(d["totals"].get("units") or 0.0) for d in baseline]
    bl_mean = mean(bl_units)
    drop_pct = (today_units - bl_mean) / bl_mean if bl_mean > 0 else 0.0
    if drop_pct > -0.30:
        result = []
    else:
        result = [alert(
            "machine_dropoff", "HIGH",
            "Machine-level dropoff vs recent mean",
            "Units fell materially vs the recent observed mean.",
            {{"today_units": today_units, "baseline_mean": round(bl_mean, 2), "drop_pct": round(drop_pct * 100, 1)}},
            [("CHECK_MACHINE", {{"machine_id": ctx["ids"]["machine_id"]}})]
        )]
```

## Your task

Write a script that: {description}

Return ONLY the Python code, no markdown fences, no explanation.
"""


def build_generation_prompt(description: str) -> str:
    return SCRIPT_GENERATION_TEMPLATE.format(description=description)
