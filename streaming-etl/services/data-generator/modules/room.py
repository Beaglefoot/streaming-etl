from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
from faker import Faker
from asyncpg import NoDataFoundError, Pool, Connection

_fake = Faker()


@dataclass
class RoomPartial:
    title: str
    description: str
    foundation_time: datetime


@dataclass
class Room(RoomPartial):
    room_id: int


def generate_room() -> RoomPartial:
    date1 = datetime(datetime.now().year, 1, 1) - timedelta(weeks=8)
    date2 = datetime.now() - timedelta(weeks=4)

    return RoomPartial(_fake.bs(), _fake.text(), _fake.date_time_between(date1, date2))


async def upload_room(room: RoomPartial, pool: Pool) -> None:
    sql = """
    INSERT INTO room (title, description, foundation_time)
    VALUES ($1, $2, $3) RETURNING room_id;
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        row = await db_conn.fetchrow(
            sql, room.title, room.description, room.foundation_time
        )

        if row == None:
            raise NoDataFoundError

        print("uploaded new room with id: ", row["room_id"])


async def fetch_room_count(pool: Pool) -> int:
    async with pool.acquire() as db_conn:
        db_conn: Connection

        val = await db_conn.fetchval("SELECT count(*) FROM room")

        if val == None:
            raise NoDataFoundError

        return val


async def fetch_random_room_before_date(date: datetime, pool: Pool) -> Optional[Room]:
    sql = """
    SELECT
        room_id,
        title,
        description,
        foundation_time
    FROM room TABLESAMPLE BERNOULLI(1)
    WHERE foundation_time < $1
    LIMIT 1;
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        row = await db_conn.fetchrow(sql, date)

        if row == None:
            return None

        return Room(
            row["title"],
            row["description"],
            row["foundation_time"],
            room_id=row["room_id"],
        )
