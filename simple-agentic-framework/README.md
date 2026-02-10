# Simple Agentic Framework (Vending)

A simulation-aware operations assistant for vending machines:
- runs deterministic alert automation per machine/day
- shows alerts + inventory + performance in a web UI
- lets managers take actions and tune automation with AI-assistance


## Architecture

### Data sources (read-only)
- `vending_machine_facts.db`: machines, products, ingredients, capacities, locations
- `vending_sales_observed.db`: observed transactions + aggregates
- `vending_analysis.db`: simulation outputs (`sim_run`, projections, expanded transactions)

Configure via:
- env: `VENDING_DB_DIR=/path/to/database-builder/db`
- CLI: `--db-dir /path/to/database-builder/db`

### App-owned state (mutable)
- `agent.db` (default):
  - engine timeline state (`engine_state`)
  - alerts + suppression state
  - manager actions (`manager_action`)
  - agent-owned inventory state (`inventory_state`)
  - script settings/revisions

### Runtime flow
1. Engine selects current simulation day from `engine_state`.
2. For each machine, it builds script context (`ctx`) from facts + observed + analysis + agent inventory.
3. Sandbox scripts emit structured alerts.
4. Alerts are upserted by identity (same alert type/scope is replaced, not accumulated).
5. Manager actions (accept/snooze/AI review/script edits) update state.
6. UI reads state from FastAPI endpoints.

## Key features

- **Simulation day controls**: run current day, advance day, skip date, reset state.
- **Inventory model in agent DB**: day-by-day ingredient drawdown + restock actions.
- **Actionable alerts**:
  - `Take current action` resolves alert and schedules supported manager actions.
  - restock actions are applied to inventory progression.
- **AI review loop**:
  - structured LLM response in UI
  - visible queued/running state
  - optional script-change instruction output
- **Script operations**:
  - list/enable/disable scripts
  - generate AI edit draft
  - activate or revert revisions
- **Machine sales modal**:
  - click a machine to view current-day sales grouped by `product_group`
- **Role-scoped dashboard**:
  - overall / region manager filtering
  - revenue, alert patterns, and inventory

## API surface (high level)

- State: `/api/state`, `/api/state/next`, `/api/state/reset`, `/api/state/skip`
- Alerts: `/api/alerts`, `/api/alerts/{id}/accept`, `/snooze`, `/review-ai`
- Inventory & metrics: `/api/inventory`, `/api/dashboard`, `/api/machine-sales`
- Actions: `/api/restock-machine`
- Script management: `/api/scripts`, `/api/scripts/{name}`, `/enabled`, `/generate-edit`, `/activate`, `/revert`

## Run

From `simple-agentic-framework/`:

- Serve UI + API:
  - `uv run simple-agent serve` (runs `startover` on launch by default; disable with `--no-startover`)
- Run scripts for current day:
  - `uv run simple-agent run-current`
- Run + advance one day:
  - `uv run simple-agent advance`
- Reset agent state (start over):
  - `uv run simple-agent startover`

## Notes

- The engine is simulation-aware: LLM review uses simulation `current_date` context, not wall-clock alert creation time.
- `reset_state` resets **agent state only**; it does not rewrite source simulation DB tables.
