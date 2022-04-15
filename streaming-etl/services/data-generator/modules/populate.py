from math import ceil
from random import random
from asyncpg import Pool
from modules.utils import exec_concurrently
from modules.user import (
    fetch_random_users,
    fetch_user_count,
    generate_user,
    upload_user,
)
from modules.room import fetch_room_count, generate_room, upload_room
from modules.call import fetch_all_calls, fetch_call_count, generate_call, upload_call
from modules.call_participant import generate_call_participant, upload_call_participant


async def populate_user(pool: Pool) -> None:
    target_user_count = 1000
    existing_user_count = await fetch_user_count(pool)

    user_count = target_user_count - existing_user_count

    uploads_gen = (upload_user(generate_user(), pool) for _ in range(user_count))

    await exec_concurrently(uploads_gen, pool.get_size())


async def populate_room(pool: Pool) -> None:
    target_room_count = 100
    existing_room_count = await fetch_room_count(pool)

    room_count = target_room_count - existing_room_count

    uploads_gen = (upload_room(generate_room(), pool) for _ in range(room_count))

    await exec_concurrently(uploads_gen, pool.get_size())


async def populate_call(pool: Pool) -> None:
    target_call_count = 1000
    existing_call_count = await fetch_call_count(pool)

    call_count = target_call_count - existing_call_count

    uploads_gen = (upload_call(generate_call(), pool) for _ in range(call_count))

    await exec_concurrently(uploads_gen, pool.get_size())


async def populate_call_participant(pool: Pool) -> None:
    async for call in fetch_all_calls(pool):
        participants_count = ceil(sum(random() for _ in range(4)))

        users = await fetch_random_users(participants_count, pool)

        uploads_gen = (
            upload_call_participant(generate_call_participant(call, user), pool)
            for user in users
        )

        await exec_concurrently(uploads_gen, participants_count)


async def populate_tables(pool: Pool) -> None:
    await populate_user(pool)
    await populate_room(pool)
    await populate_call(pool)
    await populate_call_participant(pool)
