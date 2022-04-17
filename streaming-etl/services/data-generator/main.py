import asyncio

from modules.db_connection import get_db_connection_pool
from modules.emulate_call import emulate_call
from modules.emulate_registration import emulate_registration
from modules.populate import populate_tables
from modules.utils import ProbabilityRangeFn, decide_on_random, exec_concurrently


async def main():
    pool = await get_db_connection_pool(10)

    await populate_tables(pool)

    decisions = [
        ProbabilityRangeFn(0, 0.2, lambda: emulate_call(pool)),
        ProbabilityRangeFn(0.2, 0.215, lambda: emulate_registration(pool)),
    ]

    infinite_gen = (
        decide_on_random(decisions, lambda: asyncio.sleep(1)) for _ in iter(int, 1)
    )

    await exec_concurrently(infinite_gen, pool.get_size())

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
