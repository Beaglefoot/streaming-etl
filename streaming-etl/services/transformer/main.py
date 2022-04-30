import asyncio

from modules.process_room import process_room
from modules.process_user import process_user
from modules.staging_db import get_db_connection_pool


async def main() -> None:
    staging_db_conn_pool = await get_db_connection_pool(10)

    processing_coroutines = {
        process_user(staging_db_conn_pool),
        process_room(staging_db_conn_pool),
    }

    await asyncio.wait(processing_coroutines)


if __name__ == "__main__":
    asyncio.run(main())
