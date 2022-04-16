import asyncio
from random import random

from modules.db_connection import get_db_connection_pool
from modules.emulate_call import emulate_call
from modules.populate import populate_tables
from modules.utils import exec_concurrently


async def main():
    pool = await get_db_connection_pool(10)

    await populate_tables(pool)

    infinite_gen = (
        emulate_call(pool) if random() > 0.8 else asyncio.sleep(1) for _ in iter(int, 1)
    )

    await exec_concurrently(infinite_gen, pool.get_size())

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
