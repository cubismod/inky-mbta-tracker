FROM python:3.13@sha256:ab70aee8e1bc6ffe4f95953ee2a98ca545123195a2b56d4338c7ea8ff5744a20 AS main
COPY --from=ghcr.io/astral-sh/uv:0.6.3@sha256:8257f3d17fd04794feaf89d83b4ccca3b2eaa5501de9399fa53929843c0a5b55 /uv /uvx /bin/

WORKDIR /app
ADD pyproject.toml uv.lock ./

RUN uv sync --frozen --no-cache

ADD . .

RUN uv sync --frozen --no-cache && uv lock --check
RUN uvx ruff check

CMD ["uv", "run", "inky-mbta-tracker/main.py"]
