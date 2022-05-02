from pydantic import BaseModel


class ProcessingError(BaseModel):
    message: str
