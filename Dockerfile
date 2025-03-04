FROM python:3.13@sha256:385ccb8304f6330738a6d9e6fa0bd7608e006da7e15bc52b33b0398e1ba4a15b AS main
COPY --from=ghcr.io/astral-sh/uv:0.6.4@sha256:0d686193e6d06a262184e4367d00276e24a524357080868c1732c2718f75d4d9 /uv /uvx /bin/

WORKDIR /app
ADD pyproject.toml uv.lock ./

RUN uv sync --frozen --no-cache

ADD . .

RUN uv sync --frozen --no-cache && uv lock --check
RUN uvx ruff check

CMD ["uv", "run", "inky-mbta-tracker/main.py"]
