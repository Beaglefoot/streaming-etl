from typing import Literal, Optional
from pydantic import BaseModel


class UserDim(BaseModel):
    user_key: int
    username: str
    first_name: str
    last_name: str
    email: str
    registration_time: int
    row_effective_time: int
    row_expiration_time: Optional[int]
    current_row_indicator: Literal["Current", "Expired"]
