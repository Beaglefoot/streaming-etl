from typing import Dict, Iterable, List, Generator, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition

from models.generated.call import AppDbPublicCallEnvelope
from models.test import TestCallModel
from modules.env import BOOTSTRAP_SERVERS, GROUP_ID
from modules.utils import ensure_schema_exists, get_deserialize_fn

IN_TOPIC = "app-db.public.call"
OUT_TOPIC = "test_call"
TRANSACTIONAL_ID = "call-tran"

POLL_TIMEOUT = 5_000

CallRecord = ConsumerRecord[bytes, AppDbPublicCallEnvelope]


def process_batch(
    msgs: Iterable[CallRecord],
) -> Generator[Tuple[bytes, bytes, int], None, None]:
    for m in msgs:
        if not (
            m.key
            and m.value
            and m.value.after
            and m.value.after.start_time
            and m.value.after.call_id
        ):
            continue

        new_value = TestCallModel(
            start_time=m.value.after.start_time,
            end_time=m.value.after.end_time,
            call_id=m.value.after.call_id,
        )

        yield (m.key, new_value.json().encode(), m.timestamp)


async def process_call() -> None:
    await ensure_schema_exists(TestCallModel, OUT_TOPIC)

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
        bootstrap_servers=BOOTSTRAP_SERVERS, transactional_id=TRANSACTIONAL_ID
    )

    batch_count = 0

    async with consumer, producer:
        while True:
            msg_batch: Dict[TopicPartition, List[CallRecord]]
            msg_batch = await consumer.getmany(timeout_ms=POLL_TIMEOUT, max_records=200)

            if not msg_batch:
                print("No new messages on topic: ", IN_TOPIC)
                continue

            batch_count += 1

            print("Got batch: ", batch_count)

            async with producer.transaction():
                commit_offsets: Dict[TopicPartition, int] = {}
                in_msgs: List[CallRecord] = []

                for tp, msgs in msg_batch.items():
                    in_msgs.extend(msgs)
                    commit_offsets[tp] = msgs[-1].offset + 1

                out_msgs = process_batch(in_msgs)

                for key, value, timestamp in out_msgs:
                    await producer.send(
                        OUT_TOPIC, value=value, key=key, timestamp_ms=timestamp
                    )

                await producer.send_offsets_to_transaction(commit_offsets, GROUP_ID)
