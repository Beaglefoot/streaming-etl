from typing import Optional
from pydantic import BaseModel


class TestCallModel(BaseModel):
    start_time: int
    end_time: Optional[int]
    call_id: int = 0
