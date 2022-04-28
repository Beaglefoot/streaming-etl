import asyncio

from modules.process_call import process_call
from modules.process_user import process_user
from modules.staging_db import get_db_connection_pool


async def main() -> None:
    staging_db_conn_pool = await get_db_connection_pool(10)

    await process_user(staging_db_conn_pool)
    # await process_call()


if __name__ == "__main__":
    asyncio.run(main())
