from __future__ import annotations

SCRIPT_GENERATION_TEMPLATE = """\
You are generating a Python alert script for a coffee vending machine monitoring system.
The script runs in a sandbox with NO imports allowed. Write plain Python only.

## Available input variables

| Variable | Type | Description |
|----------|------|-------------|
| `as_of_date` | `str` | ISO date being analysed, e.g. "2024-01-15" |
| `location_id` | `int` | Location identifier |
| `machine_id` | `int` | Machine identifier |
| `currency` | `str` | Currency code, e.g. "USD" |
| `machine` | `dict` | Machine metadata: id, name, serial_number, model, installed_at, last_serviced_at, current_hours, location_id |
| `location` | `dict` | Location metadata: id, name, timezone, region, external_id |
| `baseline_dates` | `list[str]` | ISO dates for same-weekday baseline (past 8 weeks) |
| `daily_product_sales` | `list[dict]` | Columns: date, location_id, machine_id, product_id, currency, cash_type, units_sold |
| `revenue` | `list[dict]` | Columns: date, product_id, cash_type, currency, tx_count, revenue |
| `ingredient_use` | `list[dict]` | Columns: date, machine_id, ingredient_id, quantity_consumed, unit |
| `price_discrepancies` | `list[dict]` | Columns: date, product_id, cash_type, amount, expected_price, delta |
| `product_forecasts` | `list[dict]` | Columns: forecast_date, product_id, forecast_units, model_name |
| `ingredient_forecasts` | `list[dict]` | Columns: forecast_date, ingredient_id, forecast_quantity, unit, model_name |
| `hourly_product_sales` | `list[dict]` | Columns: date, hour_of_day, product_id, units_sold |
| `cash_mix` | `list[dict]` | Columns: date, cash_type, units, revenue |
| `daily_totals` | `list[dict]` | Columns: date, units_total, revenue_total, card_share |

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

## Example script

```python
# Detect machine-level sales drops vs same-weekday baseline.
today = [d for d in daily_totals if d["date"] == as_of_date]
if not today:
    result = []
else:
    today_units = today[0]["units_total"]
    bl_units = [d["units_total"] for d in daily_totals if d["date"] in baseline_dates]
    units_z = z_score(today_units, bl_units)

    if units_z > -2.0:
        result = []
    else:
        result = [alert(
            "machine_dropoff", "HIGH",
            "Machine-level dropoff vs baseline",
            "Sales fell materially vs same-weekday baseline.",
            {{"units_z": round(units_z, 2)}},
            [("CHECK_MACHINE", {{"machine_id": machine_id}})]
        )]
```

## Your task

Write a script that: {description}

Return ONLY the Python code, no markdown fences, no explanation.
"""


def build_generation_prompt(description: str) -> str:
    return SCRIPT_GENERATION_TEMPLATE.format(description=description)
