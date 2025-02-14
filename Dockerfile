FROM python:3.13 AS main
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
ADD . .

RUN uv sync --frozen
RUN uvx ruff check

CMD ["uv", "run", "inky-mbta-tracker/main.py"]