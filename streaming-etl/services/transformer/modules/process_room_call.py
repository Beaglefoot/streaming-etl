import functools
from typing import Any, Callable, Coroutine
from aiokafka import AIOKafkaConsumer, ConsumerRecord
from asyncpg import Pool
from models.generated.room_call import AppDbPublicRoomCallEnvelope

from modules.env import BOOTSTRAP_SERVERS, GROUP_ID
from modules.logger import LOGGER
from modules.staging_db import upload_room_call
from modules.utils import get_deserialize_fn


IN_TOPIC = "app-db.public.room_call"


async def _process_room_call(consumer: AIOKafkaConsumer, pool: Pool) -> None:
    async with consumer:
        while True:
            rec: ConsumerRecord[
                bytes, AppDbPublicRoomCallEnvelope
            ] = await consumer.getone()

            if rec.value == None or rec.value.after == None:
                continue

            call_id = rec.value.after.call_id
            room_id = rec.value.after.room_id

            if call_id == None or room_id == None:
                continue

            await upload_room_call(room_id, call_id, pool)

            await consumer.commit()


async def init_process_room_call_fn() -> Callable[[Pool], Coroutine[Any, Any, None]]:
    LOGGER.info("Initializing %s topic processor", IN_TOPIC)

    consumer = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        isolation_level="read_committed",
        value_deserializer=get_deserialize_fn(AppDbPublicRoomCallEnvelope),
    )

    @functools.wraps(_process_room_call)
    async def wrapper(pool: Pool) -> None:
        return await _process_room_call(consumer, pool)

    return wrapper
