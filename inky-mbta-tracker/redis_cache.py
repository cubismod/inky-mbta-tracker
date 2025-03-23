import logging
from functools import wraps
from typing import Callable


class MissingKwargsError(Exception):
    pass


logger = logging.getLogger("decorators")


def redis_cache(fn: Callable):
    required_args = {"redis", "kwarg_keys", "exp_sec"}

    @wraps(fn)
    async def wrapper(*args, **kwargs):
        if not kwargs.keys() & required_args:
            raise MissingKwargsError(
                f"You need to define the following arguments for this decorator: {required_args}"
            )
