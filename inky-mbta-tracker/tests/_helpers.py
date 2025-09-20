from typing import Any


async def _maybe_await(obj: Any) -> Any:
    """Await obj if it's awaitable, otherwise return it directly.

    This helper is used by tests to patch async helpers with plain return_value mocks.
    """
    try:
        if hasattr(obj, "__await__"):
            return await obj  # type: ignore[misc]
    except Exception:
        return obj
    return obj
