FROM python:3.13@sha256:02865b3929f3910fc2d6ebbf745bf00504d316478dacaea7d9e230e134411bcb AS main
COPY --from=ghcr.io/astral-sh/uv:0.9.24@sha256:816fdce3387ed2142e37d2e56e1b1b97ccc1ea87731ba199dc8a25c04e4997c5 /uv /uvx /bin/

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

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
	CMD uv run python inky-mbta-tracker/healthcheck.py || exit 1

# replace for api server: ["uvicorn", "api_server:app", "--workers", "10", "--loop", "uvloop"]
CMD ["uv", "run", "inky-mbta-tracker"]
