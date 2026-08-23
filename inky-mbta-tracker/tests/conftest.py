import os

import pytest

os.environ.setdefault("IMT_RATE_LIMITING_ENABLED", "false")


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    return "asyncio"
