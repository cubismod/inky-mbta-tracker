FROM python:3.13@sha256:3480fd92acd9bb3d9db0d9c62a62350891434c282d2379f84597c3c3dd4beb2e AS main
COPY --from=ghcr.io/astral-sh/uv:0.7.12@sha256:4faec156e35a5f345d57804d8858c6ba1cf6352ce5f4bffc11b7fdebdef46a38 /uv /uvx /bin/

WORKDIR /app
ADD pyproject.toml uv.lock ./

RUN uv sync --frozen --no-cache

ADD . .

RUN uv lock --check
RUN uvx ruff check

CMD ["uv", "run", "inky-mbta-tracker/main.py"]
