from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import AsyncGenerator, Optional
from faker import Faker
from asyncpg import NoDataFoundError, Pool, Connection
from modules.logger import LOGGER

_fake = Faker()


@dataclass
class CallPartial:
    start_time: datetime
    end_time: Optional[datetime]


@dataclass
class Call(CallPartial):
    call_id: int


def generate_call() -> CallPartial:
    date1 = _fake.date_time_this_year()
    date2 = _fake.date_time_between(date1, date1 + timedelta(hours=2))

    return CallPartial(start_time=date1, end_time=date2)


async def upload_call(call: CallPartial, pool: Pool) -> Call:
    sql = """
    INSERT INTO "call" (start_time, end_time)
    VALUES ($1, $2) RETURNING call_id;
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        row = await db_conn.fetchrow(sql, call.start_time, call.end_time)

        if row == None:
            raise NoDataFoundError

        LOGGER.debug("uploaded new call with id: %s", row["call_id"])

    return Call(call.start_time, call.end_time, call_id=row["call_id"])


async def fetch_call_count(pool: Pool) -> int:
    async with pool.acquire() as db_conn:
        db_conn: Connection

        val = await db_conn.fetchval('SELECT count(*) FROM "call"')

        if val == None:
            raise NoDataFoundError

        return val


async def fetch_all_calls(pool: Pool) -> AsyncGenerator[Call, None]:
    sql = """
    SELECT
        call_id,
        start_time,
        end_time
    FROM "call"
    """

    async with pool.acquire() as db_conn, db_conn.transaction():
        db_conn: Connection

        async for row in db_conn.cursor(sql):
            yield Call(row["start_time"], row["end_time"], call_id=row["call_id"])


async def fetch_random_calls(amount: int, pool: Pool) -> AsyncGenerator[Call, None]:
    sql = """
    SELECT
        call_id,
        start_time,
        end_time
    FROM "call" TABLESAMPLE BERNOULLI(50)
    LIMIT $1;
    """

    async with pool.acquire() as db_conn, db_conn.transaction():
        db_conn: Connection

        async for row in db_conn.cursor(sql, amount):
            yield Call(
                row["start_time"],
                row["end_time"],
                call_id=row["call_id"],
            )


async def update_call_end_time(call_id: int, end_time: datetime, pool: Pool) -> None:
    sql = """
    UPDATE "call"
    SET end_time = $2
    WHERE call_id = $1
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        await db_conn.execute(sql, call_id, end_time)
