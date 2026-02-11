# Repository Guidelines

MBTA API available at <https://api-v3.mbta.com/docs/swagger/swagger.json>

## Project Structure & Module Organization
- App code lives under `inky-mbta-tracker/`.
  - `api/`: FastAPI app, middleware, endpoints, and services.
  - `main.py`, `api_server.py`: CLI entry and API bootstrap.
  - `tests/`: pytest tests.
- Config and assets: `config.json`, `profiles/`, `*shapes*.json`, `grafana-dashboard.json`.

## Build, Test, and Development Commands
- Setup (uses Astral UV): `uv run python -V` to confirm env; install handled on demand.
- Lint + type check: `task check` (ruff + pyright).
- Auto-fix + format: `task fix` then `task format`.
- Run app: `task run` or `uv run inky-mbta-tracker`.
- Run API server: `task api-server` (uvicorn via entrypoint).
- Tests: `task test` or `uv run pytest`.
- Full suite: `task check-all`.
- Docker (optional): `docker-compose up` for a containerized run.

## Coding Style & Naming Conventions
- Language: Python 3.13. Indent 4 spaces, line length 88, double quotes.
- Lint/format: Ruff enforces imports, errors, and formatting (`pyproject.toml`).
- Types: Pyright in basic mode; add precise `typing`/`pydantic` annotations for new code.
- Naming: modules_snake_case, functions_snake_case, ClassesCamelCase, constants UPPER_SNAKE.

## Testing Guidelines
- Framework: pytest. Place tests in `inky-mbta-tracker/tests/` as `test_*.py`.
- Keep tests deterministic; use fixtures/mocks for network and Redis.
- Run locally with `uv run pytest`; target a module with `-k`/path filters.

## Commit & Pull Request Guidelines
- Commits: short imperative subject (e.g., "refactor api server for async"), scoped changes per commit.
- PRs: include description, rationale, and any config/env changes; link issues.
  Add screenshots or `curl` examples (see `CURL.md`) for API-impacting changes.
- Passing `task check` and `task test` is required before review.

## Security & Configuration Tips
- Secrets via environment variables (see `.env` and tests): `IMT_REDIS_ENDPOINT`, `IMT_REDIS_PORT`, `IMT_REDIS_PASSWORD`, etc.
- Never commit real secrets or personal data. Prefer local `.env` and Docker secrets.
- Validate inputs on data ingestion paths; keep rate limits/middleware intact when modifying request handlers.
