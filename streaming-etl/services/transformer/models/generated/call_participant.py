# generated by datamodel-codegen:
#   filename:  http://localhost:8081/subjects/app-db.public.call_participant-value/versions/latest/schema
#   timestamp: 2022-04-29T14:52:46+00:00

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class BeforeItem(BaseModel):
    leave_time: Optional[Optional[int]]
    user_id: Optional[int]
    join_time: Optional[int] = Field(0, title='io.debezium.time.MicroTimestamp')
    call_id: Optional[int]


class AfterItem(BaseModel):
    leave_time: Optional[Optional[int]]
    user_id: Optional[int]
    join_time: Optional[int] = Field(0, title='io.debezium.time.MicroTimestamp')
    call_id: Optional[int]


class Source(BaseModel):
    schema_: Optional[str] = Field(None, alias='schema')
    sequence: Optional[Optional[str]]
    xmin: Optional[Optional[int]]
    connector: Optional[str]
    lsn: Optional[Optional[int]]
    name: Optional[str]
    txId: Optional[Optional[int]]
    version: Optional[str]
    ts_ms: Optional[int]
    snapshot: Optional[Optional[str]]
    db: Optional[str]
    table: Optional[str]


class TransactionItem(BaseModel):
    data_collection_order: Optional[int]
    id: Optional[str]
    total_order: Optional[int]


class AppDbPublicCallParticipantEnvelope(BaseModel):
    op: Optional[str]
    before: Optional[Optional[BeforeItem]]
    after: Optional[Optional[AfterItem]]
    source: Optional[Source] = Field(
        None, title='io.debezium.connector.postgresql.Source'
    )
    ts_ms: Optional[Optional[int]]
    transaction: Optional[Optional[TransactionItem]]
