# Script Context (v2): `ctx`

Sandbox scripts receive exactly one input variable: `ctx` (a nested dict).

## Top-level keys

- `ctx["meta"]`
  - `as_of_date` (`str`): ISO date the script is analyzing (e.g. `"2025-03-23"`)
  - `currency` (`str`): currency code (e.g. `"USD"`)
  - `run_id` (`str | None`): simulation run id used for predicted rows (can be `None`)
- `ctx["ids"]`
  - `location_id` (`int`)
  - `machine_id` (`int`)
- `ctx["entities"]`
  - `location` (`dict`): location metadata (id, name, timezone, region, external_id, ...)
  - `machine` (`dict`): machine metadata (id, name, serial_number, model, installed_at, last_serviced_at, current_hours, location_id, ...)
- `ctx["days"]` (`list[dict]`): one timeline covering both observed and predicted days
- `ctx["inventory"]` (`dict`): current inventory snapshot (if available)

## `ctx["days"]`: observed + predicted timeline

Each entry in `ctx["days"]` has:

- `kind` (`"observed"` or `"predicted"`)
- `date` (`str`): ISO date for that day
- `offset_days` (`int`): day offset relative to `ctx["meta"]["as_of_date"]`
  - negative = in the past (observed window)
  - `0` = `as_of_date`
  - positive = in the future

### Observed day shape (`kind="observed"`)

- `totals`: `{"units": float, "revenue": float, "card_share": float | None}`
- `by_product`: rows like `{"product_id": int, "product_name": str | None, "units": float, "revenue": float}`
- `by_ingredient`: rows like `{"ingredient_id": int, "ingredient_name": str | None, "qty": float, "unit": str | None}`

### Predicted day shape (`kind="predicted"`)

- `by_product`: rows like `{"product_id": int, "product_name": str | None, "units": float}`
- `by_ingredient`: rows like `{"ingredient_id": int, "ingredient_name": str | None, "qty": float, "unit": str | None}`

Notes:

- Scripts typically filter by `kind` and/or `offset_days` (e.g. `offset_days == 1` for “tomorrow”, `1 <= offset_days <= 3` for “next 3 days”).
- Predicted rows can exist for the same dates as observed rows (the timeline can contain two entries with the same `date`, one per `kind`).
- Predicted rows may be empty when `ctx["meta"]["run_id"]` is `None` or projections are unavailable.

## `ctx["inventory"]`

- `snapshot_date` (`str | None`): ISO date of the inventory snapshot used
- `by_ingredient` (`list[dict]`): rows like:
  - `ingredient_id` (`int`)
  - `ingredient_name` (`str | None`)
  - `qty_on_hand` (`float`)
  - `unit` (`str | None`)
  - `capacity` (`float | None`)
  - `capacity_unit` (`str | None`)

Inventory is intentionally “raw”: scripts compute fill %, days-of-cover, and risk logic themselves.

## `ctx["price_anomalies"]`

Small, precomputed pricing signals derived from `sim_transaction_expanded` (when available).

- Each entry: `{"product_id": int, "product_name": str | None, "undercharge_count": int, "examples": list[dict]}`
- An “undercharge” means `amount <= expected_price * 0.95` (≥5% below expected).
- `examples` contains up to 3 rows like `{"date": str, "amount": float, "expected_price": float, "delta_pct": float, "currency": str | None}`.
