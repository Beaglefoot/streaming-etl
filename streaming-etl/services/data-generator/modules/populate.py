from asyncpg import Pool
from modules.utils import exec_concurrently, get_ndist_random
from modules.user import (
    fetch_random_users,
    fetch_user_count,
    generate_user,
    upload_user,
)
from modules.room import (
    fetch_random_room_before_date,
    fetch_room_count,
    generate_room,
    upload_room,
)
from modules.call import (
    fetch_all_calls,
    fetch_call_count,
    fetch_random_calls,
    generate_call,
    upload_call,
)
from modules.call_participant import (
    fetch_call_participant_empty,
    generate_call_participant,
    upload_call_participant,
)
from modules.room_call import fetch_room_call_empty, upload_room_call
from modules.logger import LOGGER


async def populate_user(pool: Pool) -> None:
    target_user_count = 1000
    existing_user_count = await fetch_user_count(pool)

    user_count = target_user_count - existing_user_count

    uploads_gen = (upload_user(generate_user(), pool) for _ in range(user_count))

    LOGGER.info("Populating user table. Records to add: %s", user_count)

    await exec_concurrently(uploads_gen, pool.get_size())


async def populate_room(pool: Pool) -> None:
    target_room_count = 100
    existing_room_count = await fetch_room_count(pool)

    room_count = target_room_count - existing_room_count

    uploads_gen = (upload_room(generate_room(), pool) for _ in range(room_count))

    LOGGER.info("Populating room table. Records to add: %s", room_count)

    await exec_concurrently(uploads_gen, pool.get_size())


async def populate_call(pool: Pool) -> None:
    target_call_count = 1000
    existing_call_count = await fetch_call_count(pool)

    call_count = target_call_count - existing_call_count

    uploads_gen = (upload_call(generate_call(), pool) for _ in range(call_count))

    LOGGER.info("Populating call table. Records to add: %s", call_count)

    await exec_concurrently(uploads_gen, pool.get_size())


async def populate_call_participant(pool: Pool) -> None:
    if not await fetch_call_participant_empty(pool):
        LOGGER.info("call_participant table is already populted")
        return

    LOGGER.info("Populating call_participant table for existing calls")

    async for call in fetch_all_calls(pool):
        participants_count = get_ndist_random(4)

        users = await fetch_random_users(participants_count, pool)

        uploads_gen = (
            upload_call_participant(generate_call_participant(call, user), pool)
            for user in users
        )

        await exec_concurrently(uploads_gen, participants_count)


async def populate_room_call(pool: Pool) -> None:
    if not await fetch_room_call_empty(pool):
        LOGGER.info("room_call table is already populted")
        return

    slice_size = 500

    LOGGER.info(
        "Populating room_call table for random calls (capped with %s calls)", slice_size
    )

    async for call in fetch_random_calls(slice_size, pool):
        room = await fetch_random_room_before_date(call.start_time, pool)

        if room == None:
            continue

        await upload_room_call(room, call, pool)


async def populate_tables(pool: Pool) -> None:
    await populate_user(pool)
    await populate_room(pool)
    await populate_call(pool)
    await populate_call_participant(pool)
    await populate_room_call(pool)
