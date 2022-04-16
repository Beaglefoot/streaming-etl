import os
import asyncpg

from asyncpg import ClientCannotConnectError, NoDataFoundError, Pool
from modules.logger import LOGGER

_host = os.getenv("POSTGRES_HOST")
_database = os.getenv("POSTGRES_DB")
_user = os.getenv("POSTGRES_USER")
_password = os.getenv("POSTGRES_PASSWORD")


async def get_db_connection_pool(size: int) -> Pool:
    pool = await asyncpg.create_pool(
        host=_host,
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
