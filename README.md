# Vendagent Demo for d capital

This repo contains a proof-of-concept “Vendagent” vending operations assistant:
- a AI powered alert engine (sandboxed Python scripts)
- a FastAPI-backed dashboard UI for managers
- tooling to iterate on alert logic with AI-assisted reviews and controlled script edits

## Download

```bash
git clone https://github.com/kichahoy/dcapital.git
cd dcapital
```

## Prereqs

- `uv` (Python package manager): https://astral.sh/uv (install link)
- Python 3.13
  - will be installed with `uv sync`
- Node.js (only if you want to edit/rebuild the Slidev deck)

## Quickstart: Run The Dashboard

The main app lives in `simple-agentic-framework/`.

```bash
cd simple-agentic-framework
uv sync
uv run simple-agent serve
```

Open:
- http://127.0.0.1:8000/ (About deck modal opens by default)
- http://127.0.0.1:8000/?about=0 (skip About modal)

### Data Location

By default, the app reads demo SQLite databases from `database-builder/db/`.

Override the DB directory if needed:
- env: `VENDING_DB_DIR=/path/to/db`
- CLI: `uv run simple-agent --db-dir /path/to/db serve`

## Optional: Enable AI Features

AI-backed features (alert review + script edit draft/final-check) require an OpenAI API key.

Create a `.env` (repo root or `simple-agentic-framework/`) with:

```bash
OPENAI_API_KEY=...
```
(Uses GPT-5.2 by default)

## (Optional) Rebuild Demo Databases

If you want to regenerate the demo DBs in `database-builder/db/`:

```bash
cd database-builder
uv sync
uv run python init_db.py
```

## Presentation Deck (Slidev)

Source deck:
- `preso/vendagent-preso/slides.md`

Run the deck locally:

```bash
cd preso/vendagent-preso
npm install
npm run dev
```

Rebuild the deck into the app’s About modal (served from `/static/about-deck/`):

```bash
cd preso/vendagent-preso
npx slidev build slides.md --base /static/about-deck/ --out ../../simple-agentic-framework/src/simple_agent_framework/web/static/about-deck
```

## Repo Layout

- `simple-agentic-framework/`: FastAPI app + dashboard UI + alert engine
- `database-builder/`: scripts to build demo SQLite DBs from draw dataset
- `explore-dataset/`: exploratory analysis scripts/notebooks
- `preso/vendagent-preso/`: Slidev deck
