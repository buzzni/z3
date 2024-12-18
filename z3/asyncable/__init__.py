from __future__ import annotations

import asyncio
from concurrent.futures import Executor
from contextvars import copy_context
from functools import partial
from typing import (
    Callable,
    Optional,
    TypeVar,
    Union,
    cast,
)

from typing_extensions import ParamSpec

P = ParamSpec("P")

T = TypeVar("T")


async def run_in_executor(
    executor_or_config: Optional[Union[Executor]],
    func: Callable[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    def wrapper() -> T:
        try:
            return func(*args, **kwargs)
        except StopIteration as exc:
            raise RuntimeError from exc

    if executor_or_config is None or isinstance(executor_or_config, dict):
        return await asyncio.get_running_loop().run_in_executor(
            None,
            cast(Callable[..., T], partial(copy_context().run, wrapper)),
        )

    return await asyncio.get_running_loop().run_in_executor(executor_or_config, wrapper)
