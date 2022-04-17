import asyncio

from asyncio import FIRST_COMPLETED, Task
from math import ceil
from random import random
from typing import Callable, Coroutine, Iterable, List, NamedTuple


async def exec_concurrently(coroutines: Iterable[Coroutine], concurrency: int) -> None:
    running: List[Task] = []

    for c in coroutines:
        running.append(asyncio.create_task(c))

        if len(running) < concurrency:
            continue

        await asyncio.wait(running, return_when=FIRST_COMPLETED)

        running = [t for t in running if not t.done()]


def get_ndist_random(upper_limit: int) -> int:
    return ceil(sum(random() for _ in range(upper_limit)))


class ProbabilityRangeFn(NamedTuple):
    """
    Store function alongside a valid range for it to fire.

    Intended to be used with random().
    """

    min: float
    max: float
    fn: Callable[..., Coroutine]


def decide_on_random(
    range_fns: Iterable[ProbabilityRangeFn], default_fn: Callable
) -> Coroutine:
    r = random()

    fn = next((rf.fn for rf in range_fns if rf.min <= r and r < rf.max), None)

    if fn == None:
        return default_fn()
    else:
        return fn()
