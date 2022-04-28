from pydantic import BaseModel


class ParticipantGroupDim(BaseModel):
    participant_group_key: int
