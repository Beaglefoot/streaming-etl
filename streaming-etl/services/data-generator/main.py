import asyncio

from modules.db_connection import get_db_connection_pool
from modules.user import generate_user, upload_user
from modules.populate import populate_tables
from modules.utils import exec_concurrently


async def main():
    pool = await get_db_connection_pool()

    await populate_tables(pool)

    infinite_gen = (upload_user(generate_user(), pool) for _ in iter(int, 1))

    # await exec_concurrently(infinite_gen, pool.get_size())

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
