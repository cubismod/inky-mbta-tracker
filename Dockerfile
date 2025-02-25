FROM python:3.13@sha256:08471c63c5fdf2644adc142a7fa8d0290eb405cda14c473fbe5b4cd0933af601 AS main
COPY --from=ghcr.io/astral-sh/uv:0.6.3@sha256:8257f3d17fd04794feaf89d83b4ccca3b2eaa5501de9399fa53929843c0a5b55 /uv /uvx /bin/

WORKDIR /app
ADD pyproject.toml uv.lock ./

RUN uv sync --frozen --no-cache

ADD . .

RUN uv sync --frozen --no-cache && uv lock --check
RUN uvx ruff check

CMD ["uv", "run", "inky-mbta-tracker/main.py"]
