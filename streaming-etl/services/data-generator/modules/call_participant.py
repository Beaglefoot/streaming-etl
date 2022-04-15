from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal, Optional, Union
from faker import Faker
from asyncpg import NoDataFoundError, Pool, Connection
from modules.call import Call
from modules.user import User

_fake = Faker()


@dataclass
class CallParticipant:
    call_id: int
    user_id: int
    join_time: Union[datetime, Literal["DEFAULT"]] = "DEFAULT"
    leave_time: Optional[datetime] = None


def generate_call_participant(call: Call, user: User) -> CallParticipant:
    join_time = _fake.date_time_between(call.start_time, call.end_time)
    leave_time = _fake.date_time_between(join_time, call.end_time)

    return CallParticipant(call.call_id, user.user_id, join_time, leave_time)


async def upload_call_participant(cp: CallParticipant, pool: Pool) -> None:
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

        print(
            f'uploaded new call_participant with call_id: {row["call_id"]} and user_id: {row["user_id"]}'
        )


async def fetch_call_participant_empty(pool: Pool) -> bool:
    async with pool.acquire() as db_conn:
        db_conn: Connection

        row = await db_conn.fetchrow("SELECT call_id FROM call_participant LIMIT 1;")

        return not bool(row)
