from datetime import datetime
import functools
from typing import Any, AsyncGenerator, Callable, Coroutine, Dict, Iterable, List, Tuple
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
from asyncpg import Pool
from pydantic import BaseModel
from models.generated.user import AppDbPublicUserEnvelope
from models.user_dim import UserDim
from modules.env import BOOTSTRAP_SERVERS, GROUP_ID
from modules.logger import LOGGER
from modules.staging_db import generate_user_key
from modules.utils import (
    get_schema_id,
    get_deserialize_fn,
    get_serialize_fn,
    get_timestamp,
)


IN_TOPIC = "app-db.public.user"
OUT_TOPIC = "user_dim"
TRANSACTIONAL_ID = "user_transaction"

POLL_TIMEOUT = 10000

UserRecord = ConsumerRecord[bytes, AppDbPublicUserEnvelope]


async def process_batch(
    msgs: Iterable[UserRecord], pool: Pool
) -> AsyncGenerator[Tuple[bytes, BaseModel, int], None]:
    for m in msgs:
        if not (m.key and m.value):
            continue

        if m.value.op == "d":
            user = m.value.before
        else:
            user = m.value.after

        if user == None:
            continue

        if (
            user.user_id == None
            or user.first_name == None
            or user.last_name == None
            or user.registration_time == None
            or user.email == None
            or user.username == None
        ):
            LOGGER.info("Record is invalid: %s", user)
            continue

        if m.value.op == "d":
            row_effective_time = datetime.now()
            row_expiration_time = datetime.now()
            current_row_indicator = "Expired"
        else:
            row_effective_time = datetime.fromtimestamp(
                get_timestamp(user.registration_time)
            )
            row_expiration_time = datetime.strptime("9999-01-01", "%Y-%m-%d")
            current_row_indicator = "Current"

        user_key = await generate_user_key(user.user_id, pool)

        new_value = UserDim(
            user_key=user_key,
            user_id=user.user_id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
            email=user.email,
            registration_time=datetime.fromtimestamp(
                get_timestamp(user.registration_time)
            ),
            row_effective_time=row_effective_time,
            row_expiration_time=row_expiration_time,
            current_row_indicator=current_row_indicator,
        )

        yield (m.key, new_value, m.timestamp)


async def _process_user(
    consumer: AIOKafkaConsumer, producer: AIOKafkaProducer, pool: Pool
) -> None:
    batch_count = 0

    async with consumer, producer:
        while True:
            msg_batch: Dict[TopicPartition, List[UserRecord]]
            msg_batch = await consumer.getmany(timeout_ms=POLL_TIMEOUT, max_records=200)

            if not msg_batch:
                LOGGER.info("No new messages on topic: %s", IN_TOPIC)
                continue

            batch_count += 1

            LOGGER.info("Got batch: %s", batch_count)

            async with producer.transaction():
                commit_offsets: Dict[TopicPartition, int] = {}
                in_msgs: List[UserRecord] = []

                for tp, msgs in msg_batch.items():
                    in_msgs.extend(msgs)
                    commit_offsets[tp] = msgs[-1].offset + 1

                out_msgs = process_batch(in_msgs, pool)

                async for key, value, timestamp in out_msgs:
                    await producer.send(
                        OUT_TOPIC, value=value, key=key, timestamp_ms=timestamp
                    )

                await producer.send_offsets_to_transaction(commit_offsets, GROUP_ID)


async def init_process_user_fn() -> Callable[[Pool], Coroutine[Any, Any, None]]:
    LOGGER.info("Initializing %s topic processor", IN_TOPIC)

    out_schema_id = await get_schema_id(UserDim, OUT_TOPIC)

    consumer = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        isolation_level="read_committed",
        value_deserializer=get_deserialize_fn(AppDbPublicUserEnvelope),
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        transactional_id=TRANSACTIONAL_ID,
        value_serializer=get_serialize_fn(out_schema_id),
    )

    @functools.wraps(_process_user)
    async def wrapper(pool: Pool) -> None:
        return await _process_user(consumer, producer, pool)

    return wrapper
