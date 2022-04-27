from asyncpg import Pool, Connection
from modules.room import Room
from modules.user import User
from modules.logger import LOGGER


async def upload_room_member(room: Room, user: User, pool: Pool) -> None:
    sql = """
    INSERT INTO room_member (room_id, user_id)
    VALUES ($1, $2);
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        await db_conn.fetchrow(sql, room.room_id, user.user_id)

        LOGGER.debug(
            "uploaded new room_member with room_id: %s and user_id: %s",
            room.room_id,
            user.user_id,
        )


async def fetch_room_member_empty(pool: Pool) -> bool:
    async with pool.acquire() as db_conn:
        db_conn: Connection

        row = await db_conn.fetchrow("SELECT user_id FROM room_member LIMIT 1;")

        return not bool(row)
