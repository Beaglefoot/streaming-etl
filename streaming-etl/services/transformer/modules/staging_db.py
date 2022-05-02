import os
from typing import List, Optional
import asyncpg

from asyncpg import ClientCannotConnectError, Connection, NoDataFoundError, Pool
from modules.logger import LOGGER

_host = os.getenv("POSTGRES_HOST")
_port = os.getenv("POSTGRES_PORT")
_database = os.getenv("POSTGRES_DB")
_user = os.getenv("POSTGRES_USER")
_password = os.getenv("POSTGRES_PASSWORD")


class NoValueError(Exception):
    pass


async def get_db_connection_pool(size: int) -> Pool:
    pool = await asyncpg.create_pool(
        host=_host,
        port=_port,
        database=_database,
        user=_user,
        password=_password,
        min_size=size,
        max_size=size,
    )

    if pool == None:
        raise ClientCannotConnectError

    LOGGER.info("Established DB connection pool: %s", _host)

    async with pool.acquire() as conn:
        db_version = await conn.fetchrow("SELECT version()")

        if db_version == None:
            raise NoDataFoundError

        LOGGER.info("DB version: %s", db_version["version"])

    return pool


async def generate_user_key(user_id: int, pool: Pool) -> int:
    update = """
    UPDATE user_lookup
    SET user_key = nextval('user_key_seq')
    WHERE user_id = $1
    RETURNING user_key
    """

    insert = """
    INSERT INTO user_lookup (user_id)
    VALUES ($1)
    RETURNING user_key
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        user_key = await db_conn.fetchval(update, user_id)

        if user_key == None:
            user_key = await db_conn.fetchval(insert, user_id)

        if user_key == None:
            raise NoValueError

        return user_key


async def generate_room_key(room_id: int, pool: Pool) -> int:
    update = """
    UPDATE room_lookup
    SET room_key = nextval('room_key_seq')
    WHERE room_id = $1
    RETURNING room_key
    """

    insert = """
    INSERT INTO room_lookup (room_id)
    VALUES ($1)
    RETURNING room_key
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        room_key = await db_conn.fetchval(update, room_id)

        if room_key == None:
            room_key = await db_conn.fetchval(insert, room_id)

        if room_key == None:
            raise NoValueError

        return room_key


async def upload_call_participant(call_id: int, user_id: int, pool: Pool) -> None:
    insert = """
    INSERT INTO call_participant_buffer (call_id, user_id)
    VALUES ($1, $2)
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        await db_conn.execute(insert, call_id, user_id)


async def upload_room_call(room_id: int, call_id: int, pool: Pool) -> None:
    insert = """
    INSERT INTO room_call_buffer (room_id, call_id)
    VALUES ($1, $2)
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        await db_conn.execute(insert, room_id, call_id)


async def fetch_room_key(room_id: int, pool: Pool) -> Optional[int]:
    query = """
    SELECT room_key
    FROM room_lookup
    WHERE room_id = $1
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        room_key = await db_conn.fetchval(query, room_id)

        return room_key


async def fetch_room_id_for_call(call_id: int, pool: Pool) -> Optional[int]:
    query = """
    SELECT room_id
    FROM room_call_buffer
    WHERE call_id = $1
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        room_id = await db_conn.fetchval(query, call_id)

        return room_id


async def fetch_user_keys(user_ids: List[int], pool: Pool) -> List[int]:
    if len(user_ids) == 0:
        return []

    query = f"""
    SELECT user_key
    FROM user_lookup
    WHERE user_id IN ({", ".join((f'${i + 1}' for i, _ in enumerate(user_ids)))})
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        rows = await db_conn.fetch(query, *user_ids)

        return [r["user_key"] for r in rows]


async def fetch_participant_ids_for_call(call_id: int, pool: Pool) -> List[int]:
    query = """
    SELECT user_id
    FROM call_participant_buffer
    WHERE call_id = $1
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        rows = await db_conn.fetch(query, call_id)

        return [r["user_id"] for r in rows]


async def delete_participant_ids_for_call(call_id: int, pool: Pool) -> None:
    query = """
    DELETE FROM call_participant_buffer
    WHERE call_id = $1
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        await db_conn.execute(query, call_id)


async def delete_room_id_for_call(call_id: int, pool: Pool) -> None:
    query = """
    DELETE FROM room_call_buffer
    WHERE call_id = $1
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        await db_conn.execute(query, call_id)
