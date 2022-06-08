import asyncio
from typing import Any, Callable, Coroutine, ParamSpec

P = ParamSpec("P")

Func = Callable[P, Coroutine[Any, Any, None]]


def execute_async_function(async_func: Func, *args: P.args, **kwargs: P.kwargs):
    async def async_func_wrapper():
        await async_func(*args, **kwargs)

    asyncio.run(async_func_wrapper())
