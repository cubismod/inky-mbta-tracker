FROM python:3.13@sha256:bc336add24c507d3a11b68a08fe694877faae3eab2d0e18b0653097f1a0db9f3 AS main
COPY --from=ghcr.io/astral-sh/uv:0.6.6@sha256:031ddbc79275e351a43cbb66f64d8cd314cc78c3878898f4ab4f147b092e8e2d /uv /uvx /bin/

WORKDIR /app
ADD pyproject.toml uv.lock ./

RUN uv sync --frozen --no-cache

ADD . .

RUN uv lock --check
RUN uvx ruff check

CMD ["uv", "run", "inky-mbta-tracker/main.py"]
