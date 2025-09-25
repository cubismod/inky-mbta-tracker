import logging
import os

import uvicorn
from api.app import app
from logging_setup import setup_logging


def run_main() -> None:
    """
    Start the FastAPI server using Uvicorn in a sync context.
    """
    # Ensure consistent logging per env vars (IMT_LOG_JSON, IMT_LOG_LEVEL, etc.)
    setup_logging()

    port = int(os.environ.get("IMT_API_PORT", "8080"))
    use_json = os.getenv("IMT_LOG_JSON", "false").lower() == "true"

    # When using JSON logs, let our root handlers format uvicorn logs
    # by disabling uvicorn's default log_config and enabling propagation.
    if use_json:
        for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
            logging.getLogger(name).propagate = True
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=port,
            loop="uvloop",
            http="h11",
            lifespan="auto",
            log_config=None,
        )
    else:
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=port,
            loop="uvloop",
            http="h11",
            lifespan="auto",
        )
