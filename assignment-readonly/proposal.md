# Daily Alert Engine + LLM-Evolving Script Stack

## 1) What We’re Building
A daily “ops brain” that continuously reads the database (now split across `artifacts/vending_machine_facts.db`, `artifacts/vending_sales_observed.db`, and `artifacts/vending_analysis.db`), analyzes the last day/week against prior weeks, and runs a small library of deterministic Python alert scripts (target: <=10). Those scripts emit location-specific alerts with concrete recommended actions. The scripts get access to a narrow subset of data cleanly pulled from the observed/sim DBs (so they cannot cause problems). The scripts run in a sandbox. Our agent framework keeps track of the scripts and only allows ~10. LLMs may generate these scripts for us. We may also run them as a backtest and provide the output to the LLM. 

The scripts trigger alerts to the manager proposing predefined actions for a location (and possibly machine). The manager can:
1. Accept the action and confirm it's been done
2. Send the script, its output, and recent other alerts of the script to review to an LLM, which will then provide two alerts A). A new action to take B). Optionally, a change to the script
3. Suppress the alert (from this script) for X days

Separately the manager can always view a dashboard / request a dashboard that summarizes performance of the last week or two with insights for a location.

The experience is primarily an inbox of “do this next” actions, powered by a small evolving alert stack.

The UI shows which day just ended (triggering the daily review) and allows the user to progress to the next day, skip to a specific day or reset to the start (the UI will work for a 30 day period going back from the latest data in the dataset)

### 1.1) Current Database Reality (from `init_db.py` + `db.py`)
This plan should use the *actual* table/view names we have today in `vending_sales_observed.db` / `vending_analysis.db`:

**Seeded core entities (tables)**:
- `location` (fields include `external_id`, `timezone`, `region`)
- `machine` (fields include `serial_number`, `model`, `installed_at`, `last_serviced_at`, `current_hours`, `location_id`)
- `product` (`name` is canonicalized + unique)
- `ingredient` (`name`, `unit`) and `productingredient` (recipe: `product_id`, `ingredient_id`, `quantity`)
- `transaction` (fields include `occurred_at`, `date`, `cash_type`, `card_token`, `amount`, `currency`, `source_file`, `source_row`)

**Pricing enrichment (tables)**:
- `pricechange` (inferred from modal transaction prices by `product_id` + `location_id` + `currency`)
- `enhancedtransaction` (one row per `transaction_id`, includes `expected_price`)

**Forecast outputs (tables)**:
- `daily_projections` (per `location_id` + `machine_id`; both per-product rows *and* a “total” row where `product_id = NULL`)
- `daily_ingredient_projections` (ingredient forecasts derived from the “total” rows in `daily_projections` + recipe rates)

**Friendly SQL names (views)**:
- Clean aliases: `transactions`, `products`, `ingredients`, `machines`, `locations`, `product_ingredients`, `price_changes`, `enhanced_transactions`
- Derived aggregates: `daily_product_sales`, `daily_ingredient_consumption`
- Compatibility aliases: `dailyproductsales`, `dailyingredientconsumption`

**Important data-generation facts (from `init_db.py`)**:
- The loader reads `index_*.csv` and inserts each CSV row twice: once into the UA location (`currency = "UAH"`) and once into the SF location (`currency = "USD"` with a fixed conversion).
- Each `index_*.csv` is mapped to a UA machine and an SF machine (so machine-specific alerts should key off `machine_id`).

### 1.2) Architectural Refinements

#### 1.2.1) Feature Bundle Augmentation
To keep scripts `<= 100` lines of code and computationally efficient, the `FeatureBundle` should include precomputed Z-scores and/or percentile ranks relative to the `prior_weeks_baseline` (computed by the Engine, not the scripts). Calculating these within each script would lead to code duplication across the stack.

The bundle should also include a small Temporal Context object:
- `is_holiday`: boolean (location-aware; relevant for SF vs. UA locales)
- `day_of_week`: integer `[0-6]`
- `hour_of_day`: integer `[0-23]`

#### 1.2.2) Sandboxed Execution Environment
To maintain security and idempotency, scripts should be executed in a sandbox (e.g. `RestrictedPython`, or a dedicated Python subprocess with no network and no filesystem write access).

- Input: read-only `FeatureBundle` (JSON or a read-only namespace)
- Output: a structured Alert object (JSON-compatible dict)
- State: scripts must be stateless; any “memory” of prior alerts is handled by the Engine via the `alerts` table (not by the script)

## 2) Core Loop (Every Day)
At `T = end of day`:
1. Load time windows per location and machine:
   - `last_day`: [T-24h, T)
   - `last_week`: [T-7d, T)
   - `prior_weeks_baseline`: same weekday/hour slices for last N weeks

2. Build a compact “feature bundle”:
   - daily units by product: view `daily_product_sales`
   - daily ingredient consumption: view `daily_ingredient_consumption`
   - daily revenue: aggregate `transactions` (`SUM(amount)` by date/location/machine/product/currency)
   - price discrepancies: rows in `enhanced_transactions` where `amount` differs from `expected_price`
   - machine counters: `machines.current_hours`, `machines.installed_at`, `machines.last_serviced_at`
   - prediction for product sales for next 10 days: table `daily_projections` / view `daily_product_projections`
   - prediction for ingredient consumption for next 10 days: table `daily_ingredient_projections`

   (Not in the DB yet, but referenced by the plan: “start-of-day inventory”, restock events, and on-hand quantities.)
3. Run deterministic scripts (<=10) against the feature bundle.
4. Persist emitted alerts to an `alerts` table (to be added).
5. If “LLM review” conditions met (triggered by script) or manager action:
   - ask LLM for one-off alert or for “new script idea”
6. Notify the manager UI (poll or push).

## 3) The Manager UI (Inbox + Actions)
The UI is a single screen that feels like triage:
- `Alerts Inbox`: newest first, grouped by location, filter by type/severity.
- `Alert Detail`: why it fired, the relevant slice of data, and “what to do next”.
- `Proposed action`: each alert ships with 1-3 recommended actions.
- `Review by AI`: sends alert + history + decision to the LLM improvement loop.

The manager is always in the loop: the system recommends; the manager decides; the system learns.

## 4) Action Taxonomy (Small, Powerful, Demoable)
Keep actions few and expressive. Every alert must map to one of these:

1. `RESTOCK_MACHINE`
   - Params: `machine_id`, `restock_level_by_ingredient`, `deadline`

2. `ORDER_INGREDIENTS`
   - Params: `location_id`, `ingredient_id`, `quantity`, `deadline`

3. `ADJUST_PRICE`
   - Params: `location_id`, `product_id`, `duration`, `new_price`, `rationale`

4. `SCHEDULE_SERVICE`
   - Params: `machine_id`, `priority`, `reason`, `suggested_date`

5. `CHECK_MACHINE`
   - Params: `location_id`, `machine_id`, `transaction_discrepancies`, `what_to_check`

6. `PROPOSE_DISCONTINUE`

7. `DEBUG_LLM_CALL`
   - Params: `location_id`, `machine_id`, `note_for_llm` (useful for debugging)

## 5) The <=10 Deterministic Scripts (Initial Stack)
Each script is short, clean, and returns structured alerts. Start with 8 scripts so there’s room to evolve.

1. `restock_predictor.py`
   - Detect/predict stockout risk for top ingredients/products during next days
   - Actions: `RESTOCK_MACHINE`, `ORDER_INGREDIENTS`

2. `systematic_demand_change_watch.py`
   - Product velocity spike vs prior-week baseline for that hour/day.
   - Actions: `RESTOCK_MACHINE`, optionally `ADJUST_PRICE`

3. `slow_mover_cleanup.py`
   - Persistent low demand items that tie up stock; suggests menu/stock changes.
   - Actions: `PROPOSE_DISCONTINUE`

4. `pricing_sanity_check.py`
   - Simple demand-index pricing opportunity: underpriced at peak, overpriced off-peak.
   - Actions: `ADJUST_PRICE`

5. `pricing_anomaly.py`
   - Price charge drift vs expected.
   - Actions: `CHECK_MACHINE`

6. `payment_outage_hint.py`
   - Sudden drop in card share (or “card missing entirely”) vs baseline.
   - Actions: `CHECK_MACHINE`

7. `machine_dropoff_monitor.py`
   - Machine-level sales/revenue drop vs baseline (possible fault, placement, downtime).
   - Actions: `CHECK_MACHINE`

8. `service_due_predictor.py`
   - Uses `installed_at`, `last_serviced_at`, `current_hours` + performance drift.
   - Actions: `SCHEDULE_SERVICE`

Script budget rule:
- If adding a 9th/10th script, the LLM must propose either consolidation or removal of an older one (with justification).

## 6) Script Interface (So They Stay Small)
Each script gets access to a limited view (ctx) of the database (cleanly copied so it cannot make mistakes). It's general structure is analyze -> generate proposed action -> set level -> LLM first or manager first

Where `ctx` is precomputed features, not raw SQL:
- `ctx.now`
- `ctx.location_id`, `ctx.machine_ids`
- `ctx.frames["hourly_product_sales"]`, `ctx.frames["cash_mix"]`, `ctx.frames["ingredient_use"]`, etc.

Alerts are strict JSON-compatible dicts:
- `alert_type`, `severity`, `title`, `summary`
- `location_id`, `machine_id?`, `product_id?`, `ingredient_id?`
- `evidence` (small: 3-10 key/value items)
- `next_step`: `llm_review` / `manager_inbox`
- `recommended_actions` (1-3 actions from the taxonomy)

## 7) Database Pipeline (What Exists Today)
The “daily loop” should be built around the same pipeline we already run in `init_db.py`:

1. `create_db()` creates tables and views (via `ensure_schema_views()`).
2. Load raw `transaction` rows from `index_*.csv` (seeded into *two* locations/currencies).
3. Seed `ingredient` + `productingredient` recipe rows (from `PRODUCT_INGREDIENTS`).
4. Pricing:
   - `init_price_schedule()` rebuilds `pricechange`
   - `update_expected_prices()` rebuilds `enhancedtransaction` (adds `expected_price`)
5. Forecasting:
   - `rebuild_daily_projections()` rebuilds `daily_projections` (10-day horizon per machine)
   - `rebuild_daily_ingredient_projections()` rebuilds `daily_ingredient_projections`

This gives us a clean “facts + enrichments + forecasts” base before we add alerts.

## 8) Feature Bundle (Minimal Concrete Implementation)
Implementation goal: scripts should read a small, stable bundle with *explicit* table/view sources.

Minimum `FeatureBundle` contents (per `location_id`, `machine_id`, `currency`, `as_of_date`):
- **Machine state**: `machines` row (`installed_at`, `last_serviced_at`, `current_hours`)
- **Sales facts**:
  - `daily_product_sales` filtered to the last N days (units by product)
  - revenue computed from `transactions` (`SUM(amount)` by day/product/cash_type)
- **Ingredient facts**: `daily_ingredient_consumption` filtered to last N days
- **Price discrepancies**: `enhanced_transactions` filtered to last N days where `ABS(amount - expected_price) > epsilon`
- **Forecasts**:
  - product forecasts from `daily_projections` for `forecast_date in (as_of_date+1 .. as_of_date+10)`
  - ingredient forecasts from `daily_ingredient_projections` for the same horizon

Important: until we add inventory snapshots, “restock” scripts should treat ingredient forecasts as *consumption forecasts* (not “on-hand remaining”).

## 9) Alerts (What To Add Next)
To make the plan executable, add an `alert` table with enough structure for inbox, dedup, snooze, and audit:

Suggested fields:
- Identity: `alert_id` (UUID primary key), `created_at`, `run_date`, `script_name`, `script_version`
- Idempotency key: `fingerprint` (SHA-256 of `(script_name, alert_type, location_id, machine_id, product_id, ingredient_id)`; used for dedup/cooldown)
- Routing: `severity`, `alert_type`, `location_id`, `machine_id`, optional `product_id`/`ingredient_id`
- Content: `title`, `summary`, `evidence_json` (JSON), `recommended_actions_json` (JSON list of actions + params)
- Workflow: `status` (`OPEN`/`ACKNOWLEDGED`/`SNOOZED`/`RESOLVED`/`DISMISSED`), `snoozed_until`, `decision`, `decision_note`, `decided_at`
- LLM evolution tracking: `feedback_loop_id` (UUID; foreign key to a `script_versions` table or similar)

Suggested dedup/cooldown (so the inbox stays usable):
- Dedup key: (`script_name`, `alert_type`, `location_id`, `machine_id`, `product_id`, `ingredient_id`)
- Cooldown: don’t create a new alert for the same key if there is an `OPEN` alert in the last X hours/days unless the evidence materially changes (store an `evidence_hash` if useful).

This is the minimal “manager-in-the-loop” spine for the rest of the system.

## 10) What Makes the Demo “Feel AI”
- The system writes short, crisp alerts tied to real actions.
- The manager can push back and the system adapts by proposing script updates.
- The script stack stays small and understandable (<=10), so it feels like “a curated playbook”, not a black box.

## 11) How to Demo It (Replay mode (1 month) + injected artificial data (1 week))

### Replay Mode (Recommended Default)
Goal: look realistic and prove the system works on “real” history.
- Make the engine run with a virtual clock (`now = demo_time`) instead of wall-clock time.
- Last 30 days

We'll also inject 7 days of new fake data that is guaranteed to trigger the remaining events.
Goal: guarantee each script fires at least once in a tight demo window.
- Insert 7 days of synthetic transactions into the “fake” San Francisco location (machines 3 and 4) with `source_file = "demo_synth"`.
- Make the injected patterns intentionally trigger each alert type at a known hour.

Why this is useful:
- Real datasets do not reliably contain payment outages, bursty cash patterns, or dramatic demand spikes on cue.
- The demo becomes deterministic and repeatable for leadership.
