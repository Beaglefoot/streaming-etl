from asyncpg import Pool, Connection
from modules.room import Room
from modules.call import Call
from modules.logger import LOGGER


async def upload_room_call(room: Room, call: Call, pool: Pool) -> None:
    sql = """
    INSERT INTO room_call (room_id, call_id)
    VALUES ($1, $2);
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        await db_conn.fetchrow(sql, room.room_id, call.call_id)

        LOGGER.debug(
            "uploaded new room_call with room_id: %s and call_id: %s",
            room.room_id,
            call.call_id,
        )


async def fetch_room_call_empty(pool: Pool) -> bool:
    async with pool.acquire() as db_conn:
        db_conn: Connection

        row = await db_conn.fetchrow("SELECT call_id FROM room_call LIMIT 1;")

        return not bool(row)
