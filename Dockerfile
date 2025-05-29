FROM python:3.13@sha256:a4b2b11a9faf847c52ad07f5e0d4f34da59bad9d8589b8f2c476165d94c6b377 AS main
COPY --from=ghcr.io/astral-sh/uv:0.7.5@sha256:69e13c7ae3a7649cbe0c912ca8afe00656966622a13f2db2d7eef7bb01118ccf /uv /uvx /bin/

WORKDIR /app
ADD pyproject.toml uv.lock ./

RUN uv sync --frozen --no-cache

ADD . .

RUN uv lock --check
RUN uvx ruff check

CMD ["uv", "run", "inky-mbta-tracker/main.py"]
