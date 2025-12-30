FROM python:3.13@sha256:a1218f6fd4c1b237a06830d2a651b33cf9f16acbeb7c83be55c830551da502ae AS main
COPY --from=ghcr.io/astral-sh/uv:0.9.18@sha256:5713fa8217f92b80223bc83aac7db36ec80a84437dbc0d04bbc659cae030d8c9 /uv /uvx /bin/

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


# replace for api server: ["uvicorn", "api_server:app", "--workers", "10", "--loop", "uvloop"]
CMD ["uv", "run", "inky-mbta-tracker"]
