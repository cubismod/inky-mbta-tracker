FROM python:3.13@sha256:28f60ab75da2183870846130cead1f6af30162148d3238348f78f89cf6160b5d AS main
COPY --from=ghcr.io/astral-sh/uv:0.8.0@sha256:5778d479c0fd7995fedd44614570f38a9d849256851f2786c451c220d7bd8ccd /uv /uvx /bin/

WORKDIR /app
ADD README.md pyproject.toml uv.lock ./

RUN uv venv && uv sync --frozen --no-cache --no-install-project

ADD . .

RUN uv sync && uv lock --check
RUN uv run ruff check && uv run mypy

CMD ["uv", "run", "inky-mbta-tracker"]
