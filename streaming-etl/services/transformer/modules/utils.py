import json
from typing import Callable, Type
from pydantic import BaseModel


def get_deserialize_fn(model: Type[BaseModel]) -> Callable[[bytes], BaseModel]:
    def deserialize(data: bytes) -> BaseModel:
        # First 5 bytes do not belong to JSON and probably signify schemaId
        return model(**json.loads(data[5:]))

    return deserialize
