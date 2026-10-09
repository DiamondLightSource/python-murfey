from logging import getLogger
from typing import List

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlmodel import select

from murfey.server.murfey_db import murfey_db
from murfey.util.config import get_machine_config
from murfey.util.db import MagnificationImageShift

logger = getLogger("murfey.server.api.hub")

config = get_machine_config()

router = APIRouter(tags=["Murfey Hub"])


class InstrumentInfo(BaseModel):
    instrument_name: str
    display_name: str
    instrument_url: str


@router.get("/instruments")
def get_instrument_info() -> List[InstrumentInfo]:
    return [
        InstrumentInfo(
            instrument_name=k, display_name=v.display_name, instrument_url=v.murfey_url
        )
        for k, v in config.items()
    ]


@router.get("/instrument/{instrument_name}/image")
def get_instrument_image(instrument_name: str) -> FileResponse:
    if config.get(instrument_name):
        return FileResponse(config[instrument_name].image_path)
    return FileResponse()


class ImageShift(BaseModel):
    x: float
    y: float


@router.get("/instrument/{instrument_name}/mag/{mag}/shifts")
def get_instrument_mag_image_shift(
    instrument_name: str, mag: int, db=murfey_db
) -> ImageShift:
    shifts = db.exec(
        select(MagnificationImageShift)
        .where(MagnificationImageShift.instrument_name == instrument_name)
        .where(MagnificationImageShift.magnification == mag)
    ).one_or_none()
    if shifts is None:
        raise HTTPException(status_code=404, detail="Image shifts not found")
    return ImageShift(x=shifts.x_shift, y=shifts.y_shift)
