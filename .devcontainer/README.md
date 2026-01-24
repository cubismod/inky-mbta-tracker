# Devcontainer for inky-mbta-tracker

This directory contains a VS Code devcontainer configuration that builds the project image (from the repository `Dockerfile`) and starts the local development services used by the project (Redis and Mosquitto). The container is set up to make development quick and reproducible.

Quick summary
- Dev container image is built from the repo `Dockerfile`.
- A `dev` service (defined in `.devcontainer/docker-compose.devcontainer.yml`) is used as the development container and depends on the `tracker-redis` and `mosquitto` services defined in the repo `docker-compose.yaml`.
- The `postCreateCommand` creates a virtualenv and installs the project + dev dependencies using `uv` (`uv venv && uv sync --link-mode=copy --dev`).
- Python interpreter is set (by default) to `/app/.venv/bin/python`.
- Ports forwarded to the host: 8000 (Prometheus) and 8080 (track prediction API).
- Environment defaults inside the container:
  - `IMT_REDIS_ENDPOINT=tracker-redis`
  - `IMT_REDIS_PORT=6379`
  - `IMT_REDIS_PASSWORD=mbta`
  - `IMT_PROMETHEUS_ENABLE=true`
  - `IMT_PROM_PORT=8000`

Prerequisites
- Docker (Engine or Docker Desktop) installed and working.
- VS Code with the "Dev Containers" extension (or older "Remote - Containers" extension).
- (Optional) An MBTA API key set in a local `.env` file as `AUTH_TOKEN` if you want the app to talk to the MBTA API.

Quick start
1. Clone the repository and open the project folder in VS Code.
2. From the Command Palette (Ctrl/Cmd+Shift+P) choose:
   - "Dev Containers: Reopen Folder in Container" (or "Remote-Containers: Reopen Folder in Container").
3. Wait for the container and sidecars (Redis, Mosquitto) to build and start. The first build can take a few minutes.
4. After the devcontainer starts, dev dependencies will be installed automatically by the `postCreateCommand`. If that step fails, you can run the commands manually inside the container:
   ```bash
   uv venv
   uv sync --link-mode=copy --dev
   ```
5. Create a local `.env` with required runtime variables (do not commit secrets):
   ```env
   AUTH_TOKEN=<your_mbta_api_token>
   IMT_REDIS_PASSWORD=mbta
   # any other configuration you need...
   ```

Running the app & common tasks
- Start the tracker (recommended):
  - If you have the `task` CLI installed: `task run`
  - Or run directly: `uv run inky-mbta-tracker`
- Run the API server:
  - `task api-server` or `uv run uvicorn api_server:app --loop uvloop`
- Run tests:
  - `task test` or `uv run pytest`
- Formatting / linting / type checking:
  - `task format` (ruff format)
  - `task fix` / `task check` / `task pyright`
- See `Taskfile.yml` for more task shortcuts.

Ports & services
- Redis (container `tracker-redis`):
  - Inside container: `tracker-redis:6379`
  - From host: `localhost:6379` (the compose file publishes it)
  - Default dev password: `mbta` (development only)
- MQTT (mosquitto): ports 1883, 9001 (published on host by compose)
- Prometheus metrics: `http://localhost:8000` (forwarded)
- Track prediction API (if running): `http://localhost:8080` (forwarded)

VS Code & debugging
- The devcontainer will suggest/install recommended extensions (Python, Pylance, Docker).
- The repo already has launch configurations in `.vscode/launch.json` for:
  - Python Debugger: IMT Module (`.venv/bin/inky-mbta-tracker`)
  - Python Debugger: API
- If the Python interpreter isn't set automatically, point it to `/app/.venv/bin/python`.

Rebuilding / troubleshooting
- Rebuild from VS Code: Command Palette -> "Dev Containers: Rebuild Container".
- If `postCreateCommand` or `uv` commands fail, run them manually inside the container (see above).
- If the sidecars don't come up automatically, you can start them manually from the host:
  ```bash
  docker compose -f docker-compose.yaml up -d tracker-redis mosquitto
  ```
  (or use the combined dev compose files if you prefer)
- If you need to re-install dependencies after changing `pyproject.toml` or `uv.lock`:
  ```bash
  uv sync --link-mode=copy --dev
  ```
- If you want the `task` CLI inside the container, install it manually in the devcontainer. The project can be used with plain `uv run ...` commands without `task`.

Notes & caveats
- The configuration in this devcontainer is intentionally geared for local development and convenience. Default passwords and open services are for local/dev usage only and must not be relied upon for production.
- The dev container mounts the workspace and uses a named volume (`dev_venv`) to persist `.venv` across container rebuilds to speed up development iterations.
- If you run into permissions or volume issues, rebuild the container with the "Rebuild Container" command (this clears and recreates container state).

Need help?
If the devcontainer doesn't start or you hit unexpected errors, please open an issue with logs and a description of the behavior. Include output of the "Dev Containers: Show Log" command from the command palette to help diagnose startup/build steps.

Enjoy developing!