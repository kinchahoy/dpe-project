# Simplifying `build_script_context` (ideas + target shape)

## Goal

Make the sandbox script context **super simple**, **hierarchical**, and **easy to parse**, by:

- Sending only **past 7 days of observed data** + **next 7 days of predictions**
- Providing **product** and **ingredient** rows with **human names** already filled in
- Avoiding complex baseline logic, z-scores, per-script threshold configs, and derived “summary” objects
- Keeping the context small enough that scripts can be short, readable, and mostly “if % change > threshold: alert”

## What’s complex today (pain points)

Current context is a wide flat dict with many parallel tables + derived fields:

- `baseline_dates`, `history_summary`, z-score helpers (scripts rely on baseline + stats)
- Both `inventory_snapshot` and `inventory_current` plus an `inventory_by_ingredient` derived map
- `thresholds` is a big per-script config surface that encourages complexity and coordination
- Predictions are aggregated (e.g. `projected_sales_summary`) rather than being the raw “next 7” by product/ingredient

## A much simpler “v2” context (single hierarchical object)

**Expose exactly one input:** `ctx` (nested dict).

### Proposed shape

```json
{
  "meta": {
    "as_of_date": "YYYY-MM-DD",
    "currency": "USD",
    "run_id": null
  },
  "ids": {
    "location_id": 123,
    "machine_id": 456
  },
  "entities": {
    "location": {"id": 123, "name": "…", "timezone": "…", "region": "…", "external_id": "…"},
    "machine": {"id": 456, "name": "…", "serial_number": "…", "model": "…", "installed_at": "…", "last_serviced_at": "…", "current_hours": 12.3, "location_id": 123}
  },
  "days": [
    {
      "kind": "observed",
      "date": "YYYY-MM-DD",
      "offset_days": -2,
      "totals": {"units": 12.0, "revenue": 34.56, "card_share": 0.42},
      "by_product": [
        {"product_id": 1, "product_name": "Coke", "units": 5.0, "revenue": 12.5}
      ],
      "by_ingredient": [
        {"ingredient_id": 10, "ingredient_name": "Syrup", "qty": 1.2, "unit": "oz"}
      ]
    },
    {
      "kind": "predicted",
      "date": "YYYY-MM-DD",
      "offset_days": 1,
      "by_product": [
        {"product_id": 1, "product_name": "Coke", "units": 6.1}
      ],
      "by_ingredient": [
        {"ingredient_id": 10, "ingredient_name": "Syrup", "qty": 1.3, "unit": "oz"}
      ]
    }
  ],
  "inventory": {
    "snapshot_date": null,
    "by_ingredient": [
      {
        "ingredient_id": 10,
        "ingredient_name": "Syrup",
        "qty_on_hand": 12.3,
        "unit": "oz",
        "capacity": 30.0
      }
    ]
  }
}
```

Key properties:

- A single `days` timeline covers both observed and predicted; scripts filter by `kind`.
- Each day includes both an absolute `date` and `offset_days` relative to `meta.as_of_date` (so scripts can use whichever is simpler).
- By-product and by-ingredient rows include both `*_id` and `*_name`.
- Inventory is a single list; no duplicated `inventory_snapshot`/`inventory_current` surface.

## “No config” philosophy

To remove coordination overhead:

- **Remove `thresholds` from context entirely**
- Encourage scripts to embed small constants directly:
  - e.g. `DROP_PCT = 0.30`, `MIN_TODAY_UNITS = 10`

## Replace z-scores + baselines with a single percent threshold

Instead of “same weekday baseline, min baseline days, z-scores”, use patterns like:

- “Today revenue is down **> X%** vs average of last 7 days”
- “Next 7 days predicted units are up **> X%** vs last 7 days observed units”
- “Inventory on hand < **X days cover**, where days cover is computed in-script from predicted ingredient burn”

This reduces context to raw facts; scripts compute small aggregates.

## Simplification options (pick 1–3)

### Option A (recommended): Single day timeline with `kind` (as above)

Pros:
- Most “human” and hierarchical
- Easy for scripts to loop by day once, and branch on `kind`
- Natural place to attach both per-day totals + by-product/by-ingredient

Cons:
- Slightly larger payload than pure columnar tables

### Option B: Keep it minimal and columnar (still hierarchical, but unified with `kind`)

```json
{
  "daily_totals": [{"kind": "observed", "date": "...", "offset_days": 0, "units": 0, "revenue": 0, "card_share": 0}],
  "by_product": [{"kind": "observed", "date": "...", "offset_days": 0, "product_id": 1, "product_name": "...", "units": 0, "revenue": 0}],
  "by_ingredient": [{"kind": "observed", "date": "...", "offset_days": 0, "ingredient_id": 10, "ingredient_name": "...", "qty": 0, "unit": "..."}]
}
```

Note: predicted rows go into these same arrays with `kind="predicted"` (and some fields like `revenue`/`card_share` may be null or omitted).

Pros:
- Smaller transformation work inside `build_script_context`
- Easy to query/filter by date in scripts

Cons:
- Scripts do more “group by date” themselves

### Option C: Ship only “7d rolling sums” (ultra small)

If you truly want tiny payloads and ultra-short scripts:

- Rolling sums only, still tagged by `kind`:
  - `{"kind": "observed", "window_days": 7, "by_product": [...], "by_ingredient": [...]}`
  - `{"kind": "predicted", "window_days": 7, "by_product": [...], "by_ingredient": [...]}`

Pros:
- Tiny context and simplest scripts

Cons:
- Fewer diagnostics/evidence details; harder to explain alerts

## Query changes to enable “names filled in”

To fulfill “fill in product/ingredient name”:

- Join product dimension in observed-by-product and predicted-by-product queries:
  - `facts.product` (or whatever table holds product names)
- Join ingredient dimension in observed-by-ingredient and predicted-by-ingredient queries:
  - `facts.ingredient`

If names are in separate DBs today (facts vs observed/analysis), use `attachments={...}` like existing inventory query does.

## What to drop entirely from `build_script_context`

- `baseline_dates`
- `history_summary`
- `thresholds` and all `SCRIPT_THRESHOLDS`
- `projected_sales_summary`
- `inventory_by_ingredient` derived map (scripts can compute fill% and days cover from raw inventory + predicted ingredient burn)
- `projection_anchor_date` (optional; keep only if required to find predictions)

If a value is only used to *support z-score/baseline logic*, it should go.

## What to keep (minimal essentials)

- IDs + meta: `as_of_date`, `location_id`, `machine_id`, `currency`, `run_id`
- Entities: machine + location metadata (useful for alert titles/evidence)
- Observed: last 7 days totals + by-product + by-ingredient
- Predicted: next 7 days by-product + by-ingredient
- Inventory: one “current snapshot” by-ingredient list with names + capacity if available

## Script ergonomics: helper functions (optional)

If you want scripts to stay short without bloating context:

- Keep the existing math helpers (`mean`, etc.)
- Add 1–2 tiny helpers (still import-free) like:
  - `pct_change(new, old) -> float`
  - `sum_units(rows) -> float`

These helpers reduce copy/paste more effectively than big context precomputations.

## Script generation prompt should match the new model

Update `src/simple_agent_framework/script_prompt.py` to:

- Document only `ctx` as the input variable (and its nested fields)
- Provide 1–2 example scripts that compare last 7 vs next 7
- Encourage embedding constants directly inside scripts (“no shared thresholds”)

## Suggested migration path (low-risk)

1. Add a new builder: `build_script_context_v2(...) -> {"ctx": ...}`
2. Update `script_runner` to accept only `ctx` as `inputs`
3. Update scripts in `src/simple_agent_framework/scripts_sandbox/` to reference `ctx[...]`
4. Update the proof notebook to validate:
   - only one key is passed (`ctx`)
   - `ctx["days"]` contains both kinds, with `<= 7` rows of `kind="observed"` and `<= 7` rows of `kind="predicted"`
   - product/ingredient rows contain both id and name
5. Delete old context surface once scripts are migrated
