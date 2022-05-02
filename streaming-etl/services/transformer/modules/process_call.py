import asyncio
import functools
from datetime import datetime
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Coroutine,
    Dict,
    Iterable,
    List,
    Literal,
    Tuple,
)

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
from asyncpg import Pool
from pydantic import BaseModel
from models.call_fact import CallFact

from models.generated.call import AppDbPublicCallEnvelope
from models.participant_bridge import ParticipantBridge
from models.participant_group_dim import ParticipantGroupDim
from models.processing_error import ProcessingError
from modules.enums import PredefinedRoomIds
from modules.env import BOOTSTRAP_SERVERS, GROUP_ID
from modules.logger import LOGGER
from modules.staging_db import (
    delete_participant_ids_for_call,
    delete_room_id_for_call,
    fetch_participant_ids_for_call,
    fetch_room_id_for_call,
    fetch_room_key,
    fetch_user_keys,
)
from modules.utils import (
    get_schema_id,
    get_deserialize_fn,
    get_serialize_fn,
    get_timestamp,
)

IN_TOPIC = "app-db.public.call"
TRANSACTIONAL_ID = "call_transaction"

POLL_TIMEOUT = 5000

CallRecord = ConsumerRecord[bytes, AppDbPublicCallEnvelope]

OutTopic = Literal[
    "call_fact", "participant_group_dim", "participant_bridge", "processing_error"
]


async def init_schemas() -> Dict[OutTopic, int]:
    result: Dict[OutTopic, int] = {}

    result["call_fact"] = await get_schema_id(CallFact, "call_fact")
    result["participant_group_dim"] = await get_schema_id(
        ParticipantGroupDim, "participant_group_dim"
    )
    result["participant_bridge"] = await get_schema_id(
        ParticipantBridge, "participant_bridge"
    )
    result["processing_error"] = await get_schema_id(
        ProcessingError, "processing_error"
    )

    return result


async def process_batch(
    msgs: Iterable[CallRecord], pool: Pool
) -> AsyncGenerator[Tuple[OutTopic, bytes, BaseModel, int], None]:
    for m in msgs:
        if m.key == None or m.value == None or m.value.after == None:
            continue

        call = m.value.after

        if call.call_id == None or call.start_time == None or call.end_time == None:
            continue

        room_id = await fetch_room_id_for_call(call.call_id, pool)

        if room_id == None:
            room_key = PredefinedRoomIds.NO_ROOM.value
        else:
            room_key = (
                await fetch_room_key(room_id, pool) or PredefinedRoomIds.NO_ROOM.value
            )

        start_time = datetime.fromtimestamp(get_timestamp(call.start_time))
        end_time = datetime.fromtimestamp(get_timestamp(call.end_time))

        call_fact_value = CallFact(
            participant_group_key=call.call_id,
            room_key=room_key,
            start_date_key=int(start_time.strftime("%Y%m%d")),
            end_date_key=int(end_time.strftime("%Y%m%d")),
            start_time=start_time,
            end_time=end_time,
        )

        participant_ids = await fetch_participant_ids_for_call(call.call_id, pool)

        if len(participant_ids) == 0:
            yield (
                "processing_error",
                m.key,
                ProcessingError(
                    message=f"No participants for call: {call.call_id} in staging db"
                ),
                m.timestamp,
            )
            continue

        yield (
            "participant_group_dim",
            m.key,
            ParticipantGroupDim(participant_group_key=call.call_id),
            m.timestamp,
        )

        for key in await fetch_user_keys(participant_ids, pool):
            yield (
                "participant_bridge",
                m.key,
                ParticipantBridge(participant_group_key=call.call_id, user_key=key),
                m.timestamp,
            )

        yield ("call_fact", m.key, call_fact_value, m.timestamp)


async def clean_up_staging_leftovers(call_ids: Iterable[int], pool) -> None:
    for call_id in call_ids:
        await asyncio.gather(
            delete_participant_ids_for_call(call_id, pool),
            delete_room_id_for_call(call_id, pool),
        )


async def _process_call(
    consumer: AIOKafkaConsumer,
    producer: AIOKafkaProducer,
    serializers: Dict[OutTopic, Callable[[BaseModel], bytes]],
    pool: Pool,
) -> None:
    batch_count = 0

    async with consumer, producer:
        while True:
            msg_batch: Dict[TopicPartition, List[CallRecord]]
            msg_batch = await consumer.getmany(timeout_ms=POLL_TIMEOUT, max_records=200)

            if not msg_batch:
                LOGGER.info("No new messages on topic: %s", IN_TOPIC)
                continue

            batch_count += 1

            LOGGER.info("Got batch: %s", batch_count)

            async with producer.transaction():
                commit_offsets: Dict[TopicPartition, int] = {}
                in_msgs: List[CallRecord] = []

                for tp, msgs in msg_batch.items():
                    in_msgs.extend(msgs)
                    commit_offsets[tp] = msgs[-1].offset + 1

                async for topic, key, value, timestamp in process_batch(in_msgs, pool):
                    await producer.send(
                        topic,
                        value=serializers[topic](value),
                        key=key,
                        timestamp_ms=timestamp,
                    )

                await producer.send_offsets_to_transaction(commit_offsets, GROUP_ID)

                await clean_up_staging_leftovers(
                    (
                        msg.value.after.call_id
                        for msg in in_msgs
                        if msg.value and msg.value.after and msg.value.after.call_id
                    ),
                    pool,
                )


async def init_process_call_fn() -> Callable[[Pool], Coroutine[Any, Any, None]]:
    LOGGER.info("Initializing %s topic processor", IN_TOPIC)

    schema_ids = await init_schemas()
    serializers: Dict[OutTopic, Callable[[BaseModel], bytes]] = {
        t: get_serialize_fn(i) for t, i in schema_ids.items()
    }

    consumer = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        isolation_level="read_committed",
        value_deserializer=get_deserialize_fn(AppDbPublicCallEnvelope),
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        transactional_id=TRANSACTIONAL_ID,
    )

    @functools.wraps(_process_call)
    async def wrapper(pool: Pool) -> None:
        return await _process_call(consumer, producer, serializers, pool)

    return wrapper
