import json
from typing import Callable, Type
from pydantic import BaseModel
from schema_registry.client import AsyncSchemaRegistryClient

from modules.env import SCHEMA_REGISTRY_URL


def get_serialize_fn(schema_id: int) -> Callable[[BaseModel], bytes]:
    def serialize(model: BaseModel) -> bytes:
        # https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
        magic_byte = (0).to_bytes(1, "big")
        schema_id_encoded = schema_id.to_bytes(4, "big")
        return magic_byte + schema_id_encoded + model.json().encode()

    return serialize


def get_deserialize_fn(model_class: Type[BaseModel]) -> Callable[[bytes], BaseModel]:
    def deserialize(data: bytes) -> BaseModel:
        # https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
        return model_class(**json.loads(data[5:]))

    return deserialize


async def get_schema_id(model: Type[BaseModel], topic: str) -> int:
    client = AsyncSchemaRegistryClient(SCHEMA_REGISTRY_URL)

    schema = model.schema_json()

    schema_meta = await client.check_version(
        subject=topic, schema=schema, schema_type="JSON"
    )

    if schema_meta:
        return schema_meta.schema_id

    return await client.register(
        subject=f"{topic}-value", schema=schema, schema_type="JSON"
    )


def get_timestamp(microseconds: int) -> float:
    return microseconds / 1000000
