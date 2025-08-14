FROM python:3.13@sha256:50cbf8e58ca53a806b99250b1ba2d16c19433e8c42e7eb4ac4ea924b095e280b AS main
COPY --from=ghcr.io/astral-sh/uv:0.8.8@sha256:67b2bcccdc103d608727d1b577e58008ef810f751ed324715eb60b3f0c040d30 /uv /uvx /bin/

WORKDIR /app
ADD README.md pyproject.toml uv.lock ./

RUN uv venv && uv sync --frozen --no-cache --no-install-project --no-dev

ADD . .

RUN uv sync --no-dev && uv lock --check

CMD ["uv", "run", "inky-mbta-tracker"]
