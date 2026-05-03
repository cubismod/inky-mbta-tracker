FROM python:3.13@sha256:135188571fdcd2d0627e8248803224e54bad50f843f182bbbaced1626f5f59dd AS main
COPY --from=ghcr.io/astral-sh/uv:0.11.7@sha256:240fb85ab0f263ef12f492d8476aa3a2e4e1e333f7d67fbdd923d00a506a516a /uv /uvx /bin/

WORKDIR /app
ADD README.md pyproject.toml uv.lock ./

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

ADD . .

# Install the project (uses cached wheels from previous step) and verify lockfile
RUN --mount=type=cache,target=/root/.cache/pip \
	--mount=type=cache,target=/root/.cache/uv \
	uv sync --link-mode=copy --no-dev && uv lock --check

HEALTHCHECK --interval=15s --timeout=10s --start-period=60s --retries=2 \
	CMD uv run python inky-mbta-tracker/healthcheck.py || exit 1

# API server command:
# CMD ["uv", "run", "uvicorn", "api_server:app", "--workers", "10", "--loop", "uvloop", "--proxy-headers", "--forwarded-allow-ips", "*", "--log-config", "uvicorn_logging_config.json"]
CMD ["uv", "run", "inky-mbta-tracker"]
