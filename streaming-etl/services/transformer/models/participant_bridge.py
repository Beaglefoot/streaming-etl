from pydantic import BaseModel


class ParticipantBridge(BaseModel):
    participant_group_key: int
    user_key: int
