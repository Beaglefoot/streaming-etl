from datetime import datetime
from pydantic import BaseModel


class CallFact(BaseModel):
    participant_group_key: int
    room_key: int
    start_date_key: int
    end_date_key: int
    start_time: datetime
    end_time: datetime
