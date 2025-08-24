import os
import signal
import threading

import uvicorn
from api.app import app
from api.core import VEHICLES_QUEUE
from utils import bg_worker
from vehicles_background_worker import State


def signal_handler(_signum, _frame):
    VEHICLES_QUEUE.put(State.SHUTDOWN)


async def run_main() -> None:
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    thr = threading.Thread(
        target=bg_worker,
        kwargs={"queue": VEHICLES_QUEUE},
        name="vehicles_background_worker",
    )
    thr.start()
    port = int(os.environ.get("IMT_API_PORT", "8080"))
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=port))
    await server.serve()
