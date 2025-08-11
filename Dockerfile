FROM python:3.13@sha256:68d0775234842868248bfe185eece56e725d3cb195f511a21233d0f564dee501 AS main
COPY --from=ghcr.io/astral-sh/uv:0.8.8@sha256:67b2bcccdc103d608727d1b577e58008ef810f751ed324715eb60b3f0c040d30 /uv /uvx /bin/

WORKDIR /app
ADD README.md pyproject.toml uv.lock ./

RUN uv venv && uv sync --frozen --no-cache --no-install-project --no-dev

ADD . .

RUN uv sync --no-dev && uv lock --check

CMD ["uv", "run", "inky-mbta-tracker"]
