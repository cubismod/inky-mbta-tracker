FROM python:3.13@sha256:653b0cf8fc50366277a21657209ddd54edd125499d20f3520c6b277eb8c828d3 AS main
COPY --from=ghcr.io/astral-sh/uv:0.7.5@sha256:69e13c7ae3a7649cbe0c912ca8afe00656966622a13f2db2d7eef7bb01118ccf /uv /uvx /bin/

WORKDIR /app
ADD pyproject.toml uv.lock ./

RUN uv sync --frozen --no-cache

ADD . .

RUN uv lock --check
RUN uvx ruff check

CMD ["uv", "run", "inky-mbta-tracker/main.py"]
