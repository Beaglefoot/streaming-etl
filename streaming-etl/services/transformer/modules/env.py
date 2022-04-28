import os
from typing_extensions import Never


class UndefinedEnvVariableError(Exception):
    pass


def _raise() -> Never:
    raise UndefinedEnvVariableError


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or _raise()
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL") or _raise()
GROUP_ID = os.getenv("KAFKA_GROUP_ID") or _raise()
