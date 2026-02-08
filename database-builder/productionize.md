# Productionize Notes (100 Locations, 4-5 Countries)

This is a pragmatic list of improvements that move the system from a demo to a tool ops teams actually use day-to-day. The focus is workflow value and data correctness, not “four nines”.

## 1) Data Ingestion That Matches Reality
- Add a real ingestion pipeline:
- Pull hourly/daily exports per location or integrate with POS/vending telemetry.
- Store `ingestion_batch` metadata (source, checksum, row counts, run id) and link every transaction to a batch for traceability.
- Standardize identifiers:
- Stable `location.external_id`, `machine.serial_number`, `product.external_id` (SKU-like) to avoid name drift.
- Normalize timestamps:
- Store `occurred_at_utc` and `occurred_at_local` derived from location timezone.
- Add currency normalization:
- Keep original currency on the transaction.
- Add a daily FX table and store normalized amounts for cross-country reporting.

## 2) Inventory and “What’s Actually In the Machine”
The restock loop fails if inventory is hand-wavy.
- Add “inventory snapshots”:
- `machine_ingredient_on_hand` with `updated_at`, `source` (manual entry, IoT, route scan).
- Support partial restocks and waste:
- Record `restock_event` and optional `waste_event` to explain deltas.
- Build a simple “par levels” model:
- Per machine ingredient targets + preferred service cadence.
- Keep it editable by ops (not ML-only).

## 3) A Clean, Small Alert Catalog (Not Script Sprawl)
- Establish an alert taxonomy:
- ~10 core alert types (restock risk, demand spike, cash anomaly, card outage, machine dropoff, service due, etc.).
- Define severity and routing rules:
- Severity affects notifications.
- Routing maps to roles (location manager, regional manager, finance).
- Add de-dup + cooldown:
- Prevent the same alert from firing every hour unless something materially changed.
- Add “snooze” and “acknowledge until”:
- These are the real levers spreadsheet users expect.

## 4) Manager Workflow Improvements (Spreadsheet Replacements)
This is where adoption is won.
- Inbox-first UI:
- Alerts with 1-3 recommended actions, not charts.
- Quick filters: location, machine, severity, alert type, “needs me”.
- Action templates:
- Restock checklist with quantities.
- Service ticket template (what to check, why).
- Price change proposal with guardrails and a revert plan.
- “Explain this” button:
- Short explanation of why the alert fired (baseline comparison + top evidence points).
- Close the loop:
- Every alert requires a decision: `accept`, `reject`, `edit`, plus a note.
- Track “time to decision” and “dismiss reasons” to reduce noise.

## 5) Country/Region Differences (Practical, Not Theoretical)
- Per-country product catalog variants:
- Same product name can differ by size/recipe; use SKU-like IDs + versioned recipes.
- Per-country pricing rules:
- Rounding rules, tax/VAT treatment, allowed price bands.
- Local holidays + seasonality:
- Add a simple holiday calendar per country to improve baselines and avoid false spikes.

## 6) Script Evolution With Guardrails (So It Doesn’t Turn Into Chaos)
LLM-based script edits must be constrained.
- Script contracts:
- Fixed input feature bundle schema.
- Strict alert JSON schema output (validated).
- Diff-only proposals:
- LLM produces a patch against one script, not a free-form rewrite of the repo.
- Backtest gate:
- Before enabling, run on last 4-8 weeks and show:
- alert volume change by type/location
- manager accept rate proxy (if available) or manual review sampling
- Keep a “known good” version:
- One-click rollback if the new version gets noisy.
- Human approval required to activate a script change.

## 7) Better Baselines Before “Real ML”
Most value comes from robust baselines.
- Use “same weekday/hour” + trailing moving average as baseline.
- Add simple outlier resistance:
- winsorize or median baselines to avoid one weird day poisoning comparisons.
- Incorporate “open hours”:
- Many alerts should only be evaluated during business hours per location.

## 8) Pricing That Ops Can Trust
- Keep pricing conservative:
- Suggest changes within bands (e.g., +/- 3% to 10%).
- Avoid too-frequent changes:
- One change per product per day or per week by default.
- Always provide a “why” and an “undo”:
- If volume drops too much, revert next run.
- Segment by location:
- Different sites behave differently; avoid chain-wide changes unless proven.

## 9) Fraud / Cash Handling That Actually Helps Finance
- Produce a “reconciliation packet”:
- For each anomaly: window, counts, totals, repeated amounts, comparison baseline.
- Support attachments/notes:
- Finance teams live in evidence trails.
- Add “known patterns”:
- Some locations have predictable cash-only windows; encode as location settings.

## 10) Rollout Mechanics (100 Locations)
You need knobs and staged release.
- Start with 5-10 pilot locations.
- Add feature flags per location:
- alert types on/off
- severity thresholds
- notification channels
- Provide “quiet mode”:
- Observe-only alerts for a week before notifying managers.
- Create a weekly “tuning review”:
- Top noisy alerts, top ignored alerts, top accepted actions.

## 11) Integrations That Kill Spreadsheet Work
Pick 2-3 that matter, not 12.
- Ticketing for service: create service tasks automatically from accepted `SCHEDULE_SERVICE`.
- Ordering workflow: draft purchase orders from accepted `ORDER_INGREDIENTS`.
- Messaging: send high-severity alerts to Slack/Teams/Email with action links.

## 12) Multi-Tenant and Access (Practical Minimum)
- Role-based access:
- Location manager sees their location.
- Regional sees region.
- Finance sees cash anomalies chain-wide.
- Audit trail:
- Who accepted/rejected what, and when.

## 13) Data Quality and Monitoring (Useful, Not Overkill)
- Data freshness checks:
- “Last transaction received” per location/machine.
- Missing file/batch detection and simple alerts.
- Schema drift detection:
- New columns, missing columns, unexpected nulls.
- Basic metrics dashboards for the team:
- alert volume by type
- acceptance rate
- time-to-decision

## 14) Suggested “First Real” Targets
If you can only productionize a few things, these win:
1. Inventory snapshots + restock workflow.
2. De-dup/cooldown/snooze + manager decision capture.
3. Safe script evolution loop with backtests and rollback.
4. Multi-country timezone/currency normalization.
