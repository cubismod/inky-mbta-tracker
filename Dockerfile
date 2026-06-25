FROM ghcr.io/astral-sh/uv:0.11.23@sha256:d0a0a753ab981624b49c97abc98821c1c09f4ca69d1ef5cee69c501be3d88479 AS uv
FROM python:3.14-slim@sha256:63a4c7f612a00f92042cbdcc7cdc6a306f38485af0a200b9c89de7d9b1607d15 AS build
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
COPY uvicorn_logging_config.json ./

# Install the project (uses cached wheels from previous step) and verify lockfile
RUN --mount=type=cache,target=/root/.cache/pip \
	--mount=type=cache,target=/root/.cache/uv \
	uv sync --link-mode=copy --frozen --no-dev && uv lock --check

FROM python:3.14-slim@sha256:63a4c7f612a00f92042cbdcc7cdc6a306f38485af0a200b9c89de7d9b1607d15 AS main

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
COPY --from=build /app/uvicorn_logging_config.json /app/

HEALTHCHECK --interval=15s --timeout=10s --start-period=60s --retries=2 \
	CMD python inky-mbta-tracker/healthcheck.py || exit 1

# replace for api server: ["uvicorn", "api_server:app", "--workers", "10", "--loop", "uvloop"]
CMD ["inky-mbta-tracker"]
