from datetime import datetime
from typing import Literal
from pydantic import BaseModel


class UserDim(BaseModel):
    user_key: int
    user_id: int
    username: str
    first_name: str
    last_name: str
    email: str
    registration_time: datetime
    row_effective_time: datetime
    row_expiration_time: datetime
    current_row_indicator: Literal["Current", "Expired"]
