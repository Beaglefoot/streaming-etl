import asyncio
from modules.process_call import init_process_call_fn

from modules.process_call_participant import init_process_call_participant_fn
from modules.process_room import init_process_room_fn
from modules.process_room_call import init_process_room_call_fn
from modules.process_user import init_process_user_fn
from modules.staging_db import get_db_connection_pool
from modules.utils import with_delay


async def main() -> None:
    staging_db_conn_pool = await get_db_connection_pool(25)

    process_user = await init_process_user_fn()
    process_room = await init_process_room_fn()
    process_call_participant = await init_process_call_participant_fn()
    process_room_call = await init_process_room_call_fn()
    process_call = await init_process_call_fn()

    processing_coroutines = {
        process_user(staging_db_conn_pool),
        process_room(staging_db_conn_pool),
        process_call_participant(staging_db_conn_pool),
        process_room_call(staging_db_conn_pool),
        # Have to delay calls processing in absence of separate initial historical load
        with_delay(process_call(staging_db_conn_pool), 10),
    }

    await asyncio.wait(processing_coroutines, return_when=asyncio.FIRST_EXCEPTION)


if __name__ == "__main__":
    asyncio.run(main())
