# Repository Guidelines

MBTA API available at <https://api-v3.mbta.com/docs/swagger/swagger.json>

## Project Structure & Module Organization
- App code lives under `inky-mbta-tracker/`.
  - `api/`: FastAPI app, middleware, endpoints, and services.
  - `main.py`, `api_server.py`: CLI entry and API bootstrap.
  - `webhook/`: Discord webhook integration for MBTA alerts.
  - `shared_types/`: Shared Pydantic models and schema versioning.
  - `tests/`: pytest tests.
- Config and assets: `config.json`, `profiles/`, `grafana-dashboard.json`.

## Build, Test, and Development Commands
- Setup (uses Astral UV): `uv run python -V` to confirm env; install handled on demand.
- Lint + type check: `task check` (ruff + pyright).
- Auto-fix + format: `task fix` then `task format`.
- Run app: `task run` or `uv run inky-mbta-tracker`.
- Run API server: `task api-server` (uvicorn via entrypoint).
- Tests: `task test` or `uv run pytest`.
- Update class hashes: `task compute-class-hashes`.
- Full suite: `task check-all`.

## Architecture

Real-time MBTA transit tracker. Two entry points:
- **Worker** (`main.py`): Consumes MBTA SSE streams (vehicles, predictions, alerts), processes events through async queue to Redis. Publishes departures to MQTT for physical displays.
- **API server** (`api_server.py`): FastAPI exposing GeoJSON vehicle positions, shapes, alerts, predictions, stops, and SSE vehicle streaming.

Key modules:
- `mbta_client.py` / `mbta_client_extended.py`: MBTA API client and SSE watchers.
- `schedule_tracker.py`: Queue consumer, Redis data management, MQTT publishing.
- `geojson_utils.py`: GeoJSON feature construction, background refresh.
- `otel_config.py` / `otel_utils.py`: OpenTelemetry tracing.
- `redis_cache.py`: Redis get/set/delete with LRU caching.

## Coding Style & Naming Conventions
- Language: Python 3.13. Indent 4 spaces, line length 88, double quotes.
- Lint/format: Ruff enforces imports, errors, and formatting (`pyproject.toml`).
- Types: Pyright in basic mode; add precise `typing`/`pydantic` annotations for new code.
- Naming: modules_snake_case, functions_snake_case, ClassesCamelCase, constants UPPER_SNAKE.

## Testing Guidelines
- Framework: pytest. Place tests in `inky-mbta-tracker/tests/` as `test_*.py`.
- Keep tests deterministic; use fixtures/mocks for network and Redis.
- Run locally with `uv run pytest`; target a module with `-k`/path filters.

## Logging Guidelines
- Always use `exc_info` parameter when logging exceptions to capture stack traces.
- Use `exc_info=exception_variable` to pass the actual exception object.

  ```python
  # Good
  except ValueError as e:
      logger.error("Failed to process data", exc_info=e)

  # Bad - missing stack trace
  except ValueError as e:
      logger.error(f"Failed to process data: {e}")
  ```

## Commit & Pull Request Guidelines
- Commits: short imperative subject (e.g., "refactor api server for async"), scoped changes per commit.
- PRs: include description, rationale, and any config/env changes; link issues.
  Add screenshots or `curl` examples (see `CURL.md`) for API-impacting changes.
- Passing `task check` and `task test` is required before review.

## Security & Configuration Tips
- Secrets via environment variables (see `.env` and tests): `IMT_REDIS_ENDPOINT`, `IMT_REDIS_PORT`, `IMT_REDIS_PASSWORD`, etc.
- Never commit real secrets or personal data. Prefer local `.env` and Docker secrets.
- Validate inputs on data ingestion paths; keep rate limits/middleware intact when modifying request handlers.
