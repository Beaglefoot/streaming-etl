import asyncio
import functools
from typing import Any, Callable, Coroutine, Dict, List, Tuple
from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from asyncpg import Pool
from models.generated.call_participant import AppDbPublicCallParticipantEnvelope

from modules.env import BOOTSTRAP_SERVERS, GROUP_ID
from modules.logger import LOGGER
from modules.process_call import POLL_TIMEOUT
from modules.staging_db import upload_call_participant
from modules.utils import get_deserialize_fn


IN_TOPIC = "app-db.public.call_participant"

POLL_TIMEOUT = 2500

CallParticipantRecord = ConsumerRecord[bytes, AppDbPublicCallParticipantEnvelope]


async def _process_call_participant(consumer: AIOKafkaConsumer, pool: Pool) -> None:
    batch_count = 0

    async with consumer:
        while True:
            msg_batch: Dict[TopicPartition, List[CallParticipantRecord]]
            msg_batch = await consumer.getmany(timeout_ms=POLL_TIMEOUT, max_records=20)

            if not msg_batch:
                LOGGER.info("No new messages on topic: %s", IN_TOPIC)
                continue

            batch_count += 1

            LOGGER.info("Got batch: %s", batch_count)

            commit_offsets: Dict[TopicPartition, int] = {}
            uploading_batch: List[Tuple[int, int]] = []

            for tp, msgs in msg_batch.items():
                commit_offsets[tp] = msgs[-1].offset + 1

                for msg in msgs:
                    if msg.value == None or msg.value.after == None:
                        continue

                    call_id = msg.value.after.call_id
                    user_id = msg.value.after.user_id
                    leave_time = msg.value.after.leave_time

                    if call_id == None or user_id == None or leave_time == None:
                        continue

                    uploading_batch.append((call_id, user_id))

            await asyncio.gather(
                *(
                    upload_call_participant(call_id, user_id, pool)
                    for call_id, user_id in uploading_batch
                )
            )

            await consumer.commit(commit_offsets)


async def init_process_call_participant_fn() -> Callable[
    [Pool], Coroutine[Any, Any, None]
]:
    LOGGER.info("Initializing %s topic processor", IN_TOPIC)

    consumer = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        isolation_level="read_committed",
        value_deserializer=get_deserialize_fn(AppDbPublicCallParticipantEnvelope),
    )

    @functools.wraps(_process_call_participant)
    async def wrapper(pool: Pool) -> None:
        return await _process_call_participant(consumer, pool)

    return wrapper
