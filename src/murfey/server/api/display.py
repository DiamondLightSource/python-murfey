from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import FileResponse

from murfey.util.config import get_machine_config

# Create APIRouter class object
router = APIRouter(prefix="/display", tags=["display"])
machine_config = get_machine_config()


@router.get("/microscope_image/")
def get_mic_image():
    if machine_config.image_path:
        return FileResponse(machine_config.image_path)
    return None
