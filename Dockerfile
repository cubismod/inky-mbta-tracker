FROM python:3.13 AS main
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
COPY . .

RUN uv sync --frozen
RUN uvx ruff check

CMD ["python3", "inky-mbta-tracker/main.py"]