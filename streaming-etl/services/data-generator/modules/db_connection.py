import os
import asyncpg

from asyncpg import ClientCannotConnectError, NoDataFoundError, Pool

_host = os.getenv("POSTGRES_HOST")
_database = os.getenv("POSTGRES_DB")
_user = os.getenv("POSTGRES_USER")
_password = os.getenv("POSTGRES_PASSWORD")


async def get_db_connection_pool() -> Pool:
    pool = await asyncpg.create_pool(
        host=_host,
        database=_database,
        user=_user,
        password=_password,
        min_size=10,
        max_size=10,
    )

    if pool == None:
        raise ClientCannotConnectError

    print(f"Established DB connection pool: {_host}")

    async with pool.acquire() as conn:
        db_version = await conn.fetchrow("SELECT version()")

        if db_version == None:
            raise NoDataFoundError

        print("DB version:", db_version["version"])

    return pool
