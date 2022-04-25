import asyncio
import json
from aiokafka import AIOKafkaConsumer, ConsumerRecord

from models.call import AppDbPublicCallEnvelope


def deserialize(data: bytes) -> AppDbPublicCallEnvelope:
    # First 5 bytes do not belong to JSON and probably signify schemaId
    return AppDbPublicCallEnvelope(**json.loads(data[5:]))


async def consume() -> None:
    async with AIOKafkaConsumer(
        "app-db.public.call",
        bootstrap_servers="localhost:9092",
        group_id="transformer",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=deserialize,
    ) as consumer:

        r: ConsumerRecord[bytes, AppDbPublicCallEnvelope] = await consumer.getone()

        if r.value == None or r.value.after == None:
            raise Exception

        print(r.value.after.call_id)


if __name__ == "__main__":
    asyncio.run(consume())
