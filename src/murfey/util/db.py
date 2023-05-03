from __future__ import annotations

from typing import Optional

from sqlmodel import Field, SQLModel, create_engine


class ClientEnvironment(SQLModel, table=True):  # type: ignore
    client_id: Optional[int] = Field(primary_key=True, unique=True)
    visit: str = Field(default="")
    connected: bool


class DataCollectionGroup(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True, unique=True)
    client: int = Field(primary_key=True)
    tag: str


class DataCollection(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True, unique=True)
    client: int = Field(primary_key=True)
    tag: str
    dcg_id: int = Field(foreign_key="datacollectiongroup.id")


class ProcessingJob(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True, unique=True)
    client: int = Field(primary_key=True)
    recipe: str = Field(primary_key=True)
    tag: str
    dc_id: int = Field(foreign_key="datacollection.id")


def setup(url: str):
    engine = create_engine(url)
    SQLModel.metadata.create_all(engine)
