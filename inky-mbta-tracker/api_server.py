import os

import uvicorn
from api.app import app


def run_main() -> None:
    """Start the FastAPI server using Uvicorn in a sync context.

    Running Uvicorn inside anyio.run can interfere with its event loop and
    signal handling. Use the synchronous runner here and let Uvicorn manage
    the asyncio loop directly.
    """
    port = int(os.environ.get("IMT_API_PORT", "8080"))
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        loop="uvloop",
        http="h11",
        lifespan="auto",
    )
