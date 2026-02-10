# ── Stage 1: Build the vending databases from CSV source data ──
FROM python:3.13-slim AS db-builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /build/database-builder
COPY database-builder/pyproject.toml database-builder/uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

COPY database-builder/ ./

# init_db.py writes DBs to ../db/
RUN mkdir -p /build/db && uv run python init_db.py


# ── Stage 2: Build the Slidev presentation ────────────────────
FROM node:22-slim AS preso-builder

WORKDIR /build/preso
COPY preso/vendagent-preso/package.json preso/vendagent-preso/package-lock.json ./
RUN npm ci

COPY preso/vendagent-preso/ ./
RUN npx slidev build slides.md --base /static/about-deck/ --out about-deck


# ── Stage 3: Application image ────────────────────────────────
FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app

# Install Python dependencies (layer-cached)
COPY simple-agentic-framework/pyproject.toml simple-agentic-framework/uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Copy source code
COPY simple-agentic-framework/src/ src/

# Install the project itself
RUN uv sync --frozen --no-dev

# Copy built databases from stage 1
COPY --from=db-builder /build/db/ /app/data/

# Copy built presentation into the static directory it's served from
COPY --from=preso-builder /build/preso/about-deck/ /app/src/simple_agent_framework/web/static/about-deck/

ENV VENDING_DB_DIR=/app/data
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["uv", "run", "simple-agent", "serve", "--host", "0.0.0.0", "--port", "8000"]
