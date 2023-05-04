# from __future__ import annotations

from __future__ import annotations

from typing import List, Optional

from sqlmodel import Field, Relationship, SQLModel, create_engine


class ClientEnvironment(SQLModel, table=True):  # type: ignore
    client_id: Optional[int] = Field(primary_key=True, unique=True)
    visit: str = Field(default="")
    connected: bool
    rsync_instances: List["RsyncInstance"] = Relationship(
        back_populates="client", sa_relationship_kwargs={"cascade": "delete"}
    )


class RsyncInstance(SQLModel, table=True):  # type: ignore
    source: str = Field(primary_key=True)
    destination: str = Field(primary_key=True, default="")
    client_id: int = Field(foreign_key="clientenvironment.client_id", primary_key=True)
    files_transferred: int = Field(default=0)
    files_counted: int = Field(default=0)
    transferring: bool = Field(default=False)
    client: Optional[ClientEnvironment] = Relationship(back_populates="rsync_instances")


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
