from logging import getLogger
from typing import List

from fastapi import APIRouter
from fastapi.responses import FileResponse
from pydantic import BaseModel

from murfey.util.config import get_machine_config

logger = getLogger("murfey.api.hub")

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
