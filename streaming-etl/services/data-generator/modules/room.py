from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from faker import Faker
from asyncpg import NoDataFoundError, Pool, Connection

_fake = Faker()


@dataclass
class Room:
    title: str
    description: str
    room_id: Optional[int] = None
    foundation_time: Optional[datetime] = None


def generate_room() -> Room:
    return Room(_fake.bs(), _fake.text())


async def upload_room(room: Room, pool: Pool) -> None:
    sql = """
    INSERT INTO room (title, description)
    VALUES ($1, $2) RETURNING room_id, foundation_time;
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        row = await db_conn.fetchrow(sql, room.title, room.description)

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
