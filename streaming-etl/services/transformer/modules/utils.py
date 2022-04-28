import json
from typing import Callable, Type
from pydantic import BaseModel
from schema_registry.client import AsyncSchemaRegistryClient

from modules.env import SCHEMA_REGISTRY_URL


def get_deserialize_fn(model: Type[BaseModel]) -> Callable[[bytes], BaseModel]:
    def deserialize(data: bytes) -> BaseModel:
        # First 5 bytes do not belong to JSON and probably signify schemaId
        return model(**json.loads(data[5:]))

    return deserialize


async def ensure_schema_exists(model: Type[BaseModel], topic: str) -> None:
    client = AsyncSchemaRegistryClient(SCHEMA_REGISTRY_URL)

    schema = await client.check_version(
        subject=topic, schema=model.schema_json(), schema_type="JSON"
    )

    if schema:
        return

    await client.register(
        subject=f"{topic}-value", schema=model.schema_json(), schema_type="JSON"
    )
