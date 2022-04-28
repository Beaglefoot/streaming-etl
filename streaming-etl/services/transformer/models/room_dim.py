from datetime import datetime
from pydantic import BaseModel


class RoomDim(BaseModel):
    room_key: int
    title: str
    description: str
    foundation_time: datetime
    row_effective_time: datetime
    row_expiration_time: datetime
    current_row_indicator: str
