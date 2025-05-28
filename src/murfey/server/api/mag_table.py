from typing import List

from fastapi import APIRouter, Depends
from sqlmodel import select

from murfey.server.api.auth import validate_token
from murfey.server.murfey_db import murfey_db
from murfey.util.db import MagnificationLookup

router = APIRouter(
    prefix="/mag_table",
    dependencies=[Depends(validate_token)],
    tags=["Magnification Table"],
)


@router.get("/mag_table/")
def get_mag_table(db=murfey_db) -> List[MagnificationLookup]:
    return db.exec(select(MagnificationLookup)).all()


@router.post("/mag_table/")
def add_to_mag_table(rows: List[MagnificationLookup], db=murfey_db):
    for r in rows:
        db.add(r)
    db.commit()


@router.delete("/mag_table/{mag}")
def remove_mag_table_row(mag: int, db=murfey_db):
    row = db.exec(
        select(MagnificationLookup).where(MagnificationLookup.magnification == mag)
    ).one()
    db.delete(row)
    db.commit()
