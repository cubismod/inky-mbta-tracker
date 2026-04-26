FROM ghcr.io/astral-sh/uv:0.11.7@sha256:240fb85ab0f263ef12f492d8476aa3a2e4e1e333f7d67fbdd923d00a506a516a AS uv
FROM python:3.13-slim AS build
COPY --from=uv /uv /uvx /bin/

WORKDIR /app
COPY README.md pyproject.toml uv.lock ./

ENV HF_HOME=/app/hf
ENV KERAS_BACKEND=jax
ENV UV_LINK_MODE=copy

# Create HF dir and venv
RUN mkdir hf && uv venv

# Populate pip and uv caches and resolve wheels (no project install) using BuildKit cache mounts
# (requires BuildKit/Buildx in CI, which the workflow config already sets up)
RUN --mount=type=cache,target=/root/.cache/pip \
	--mount=type=cache,target=/root/.cache/uv \
	uv sync --link-mode=copy --frozen --no-install-project --no-dev

COPY inky-mbta-tracker ./inky-mbta-tracker
COPY child_stations.json ./

# Install the project (uses cached wheels from previous step) and verify lockfile
RUN --mount=type=cache,target=/root/.cache/pip \
	--mount=type=cache,target=/root/.cache/uv \
	uv sync --link-mode=copy --frozen --no-dev && uv lock --check

FROM python:3.13-slim AS main

WORKDIR /app

ENV HF_HOME=/app/hf
ENV KERAS_BACKEND=jax
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN mkdir hf

COPY --from=build /app/.venv /app/.venv
COPY --from=build /app/inky-mbta-tracker /app/inky-mbta-tracker
COPY --from=build /app/child_stations.json /app/

HEALTHCHECK --interval=15s --timeout=10s --start-period=60s --retries=2 \
	CMD python inky-mbta-tracker/healthcheck.py || exit 1

# replace for api server: ["uvicorn", "api_server:app", "--workers", "10", "--loop", "uvloop"]
CMD ["inky-mbta-tracker"]
