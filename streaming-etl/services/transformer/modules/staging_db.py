import os
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


async def fetch_user_key(user_id: int, pool: Pool) -> int:
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


async def fetch_room_key(room_id: int, pool: Pool) -> int:
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
