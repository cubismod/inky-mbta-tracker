import os

import uvicorn
from api.app import app


async def run_main() -> None:
    port = int(os.environ.get("IMT_API_PORT", "8080"))
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=port))
    await server.serve()
