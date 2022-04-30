from datetime import datetime
from typing import AsyncGenerator, Dict, Iterable, List, Tuple
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
from asyncpg import Pool
from pydantic import BaseModel
from models.generated.room import AppDbPublicRoomEnvelope
from models.room_dim import RoomDim
from modules.env import BOOTSTRAP_SERVERS, GROUP_ID
from modules.logger import LOGGER
from modules.staging_db import fetch_room_key
from modules.utils import (
    get_schema_id,
    get_deserialize_fn,
    get_serialize_fn,
    get_timestamp,
)


IN_TOPIC = "app-db.public.room"
OUT_TOPIC = "room_dim"
TRANSACTIONAL_ID = "room-transaction"

POLL_TIMEOUT = 5_000

RoomRecord = ConsumerRecord[bytes, AppDbPublicRoomEnvelope]


async def process_batch(
    msgs: Iterable[RoomRecord], pool: Pool
) -> AsyncGenerator[Tuple[bytes, BaseModel, int], None]:
    for m in msgs:
        if not (m.key and m.value):
            continue

        if m.value.op == "d":
            room = m.value.before
        else:
            room = m.value.after

        if room == None:
            continue

        if (
            room.room_id == None
            or room.title == None
            or room.description == None
            or room.foundation_time == None
        ):
            LOGGER.info("Record is invalid: %s", room)
            continue

        if m.value.op == "d":
            row_effective_time = datetime.now()
            row_expiration_time = datetime.now()
            current_row_indicator = "Expired"
        else:
            row_effective_time = datetime.fromtimestamp(
                get_timestamp(room.foundation_time)
            )
            row_expiration_time = datetime.strptime("9999-01-01", "%Y-%m-%d")
            current_row_indicator = "Current"

        room_key = await fetch_room_key(room.room_id, pool)

        new_value = RoomDim(
            room_key=room_key,
            room_id=room.room_id,
            title=room.title,
            description=room.description,
            foundation_time=datetime.fromtimestamp(get_timestamp(room.foundation_time)),
            row_effective_time=row_effective_time,
            row_expiration_time=row_expiration_time,
            current_row_indicator=current_row_indicator,
        )

        yield (m.key, new_value, m.timestamp)


async def process_room(pool: Pool) -> None:
    out_schema_id = await get_schema_id(RoomDim, OUT_TOPIC)

    consumer = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        isolation_level="read_committed",
        value_deserializer=get_deserialize_fn(AppDbPublicRoomEnvelope),
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        transactional_id=TRANSACTIONAL_ID,
        value_serializer=get_serialize_fn(out_schema_id),
    )

    batch_count = 0

    async with consumer, producer:
        while True:
            msg_batch: Dict[TopicPartition, List[RoomRecord]]
            msg_batch = await consumer.getmany(timeout_ms=POLL_TIMEOUT, max_records=200)

            if not msg_batch:
                LOGGER.info("No new messages on topic: %s", IN_TOPIC)
                continue

            batch_count += 1

            LOGGER.info("Got batch: %s", batch_count)

            async with producer.transaction():
                commit_offsets: Dict[TopicPartition, int] = {}
                in_msgs: List[RoomRecord] = []

                for tp, msgs in msg_batch.items():
                    in_msgs.extend(msgs)
                    commit_offsets[tp] = msgs[-1].offset + 1

                out_msgs = process_batch(in_msgs, pool)

                async for key, value, timestamp in out_msgs:
                    await producer.send(
                        OUT_TOPIC, value=value, key=key, timestamp_ms=timestamp
                    )

                await producer.send_offsets_to_transaction(commit_offsets, GROUP_ID)
