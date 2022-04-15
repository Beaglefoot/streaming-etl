import asyncio

from asyncio import FIRST_COMPLETED, Task
from typing import Coroutine, Iterable, List


async def exec_concurrently(coroutines: Iterable[Coroutine], concurrency: int) -> None:
    running: List[Task] = []

    for c in coroutines:
        running.append(asyncio.create_task(c))

        if len(running) < concurrency:
            continue

        await asyncio.wait(running, return_when=FIRST_COMPLETED)

        running = [t for t in running if not t.done()]
