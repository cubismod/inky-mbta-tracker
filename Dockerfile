FROM python:3.13@sha256:4ea77121eab13d9e71f2783d7505f5655b25bb7b2c263e8020aae3b555dbc0b2 AS main
COPY --from=ghcr.io/astral-sh/uv:0.8.3@sha256:ef11ed817e6a5385c02cd49fdcc99c23d02426088252a8eace6b6e6a2a511f36 /uv /uvx /bin/

WORKDIR /app
ADD README.md pyproject.toml uv.lock ./

RUN uv venv && uv sync --frozen --no-cache --no-install-project

ADD . .

RUN uv sync && uv lock --check
RUN uv run ruff check && uv run mypy

CMD ["uv", "run", "inky-mbta-tracker"]
