FROM python:3.13 AS main
COPY --from=ghcr.io/astral-sh/uv:0.6.0 /uv /uvx /bin/

WORKDIR /app
ADD pyproject.toml uv.lock ./

RUN uv sync --frozen --no-cache

ADD . .

RUN uv sync --frozen --no-cache
RUN uvx ruff check

CMD ["uv", "run", "inky-mbta-tracker/main.py"]
