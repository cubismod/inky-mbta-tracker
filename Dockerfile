FROM python:3.13@sha256:a9710c44d58cb3fcba391e09b20e52147caf99be418aac2ba4ffa128435fc303 AS main
COPY --from=ghcr.io/astral-sh/uv:0.6.7@sha256:d25a9b42ecc3aac0d63782654dcb2ab4b304f86357794b05a5c39b17f4777339 /uv /uvx /bin/

WORKDIR /app
ADD pyproject.toml uv.lock ./

RUN uv sync --frozen --no-cache

ADD . .

RUN uv lock --check
RUN uvx ruff check

CMD ["uv", "run", "inky-mbta-tracker/main.py"]
