from datetime import datetime
from typing import Literal
from pydantic import BaseModel


class RoomDim(BaseModel):
    room_key: int
    room_id: int
    title: str
    description: str
    foundation_time: datetime
    row_effective_time: datetime
    row_expiration_time: datetime
    current_row_indicator: Literal["Current", "Expired"]
