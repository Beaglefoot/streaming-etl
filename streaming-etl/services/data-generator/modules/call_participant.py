from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from faker import Faker
from asyncpg import NoDataFoundError, Pool, Connection
from modules.call import Call
from modules.user import User
from modules.logger import LOGGER

_fake = Faker()


@dataclass
class CallParticipantPartial:
    call_id: int
    user_id: int
    join_time: datetime
    leave_time: Optional[datetime]


@dataclass
class CallParticipant(CallParticipantPartial):
    call_participant_id: int


def generate_call_participant(call: Call, user: User) -> CallParticipantPartial:
    join_time = _fake.date_time_between(call.start_time, call.end_time)
    leave_time = _fake.date_time_between(join_time, call.end_time)

    return CallParticipantPartial(call.call_id, user.user_id, join_time, leave_time)


async def upload_call_participant(cp: CallParticipantPartial, pool: Pool) -> None:
    sql = """
    INSERT INTO call_participant (call_id, user_id, join_time, leave_time)
    VALUES ($1, $2, $3, $4) RETURNING call_id, user_id;
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        row = await db_conn.fetchrow(
            sql, cp.call_id, cp.user_id, cp.join_time, cp.leave_time
        )

        if row == None:
            raise NoDataFoundError

        LOGGER.debug(
            "uploaded new call_participant with call_id: %s and user_id: %s",
            row["call_id"],
            row["user_id"],
        )


async def fetch_call_participant_empty(pool: Pool) -> bool:
    async with pool.acquire() as db_conn:
        db_conn: Connection

        row = await db_conn.fetchrow("SELECT call_id FROM call_participant LIMIT 1;")

        return not bool(row)


async def update_call_participant_leave_time(
    call_id: int, user_id: int, leave_time: datetime, pool: Pool
) -> None:
    sql = """
    UPDATE call_participant
    SET leave_time = $3
    WHERE call_id = $1 AND user_id = $2
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        await db_conn.execute(sql, call_id, user_id, leave_time)
