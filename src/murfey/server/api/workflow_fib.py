import json
import logging
import os
from pathlib import Path

import numpy as np
import PIL.Image
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import Session, select

import murfey.util.db as MurfeyDB
from murfey.server import _transport_object
from murfey.server.api.auth import validate_instrument_token
from murfey.server.murfey_db import murfey_db
from murfey.util import sanitise_path
from murfey.util.config import get_machine_config
from murfey.util.models import LamellaSiteInfo

logger = logging.getLogger("murfey.server.api.workflow_fib")

router = APIRouter(
    prefix="/workflow/fib",
    dependencies=[Depends(validate_instrument_token)],
    tags=["Workflows: FIB milling"],
)


class FIBAtlasFile(BaseModel):
    file: Path


@router.post("/sessions/{session_id}/register_atlas")
def register_fib_atlas(
    session_id: int,
    fib_atlas: FIBAtlasFile,
):
    if _transport_object is None:
        logger.error("No Transport Manager object was set up")
        return None
    _transport_object.send(
        _transport_object.feedback_queue,
        {
            "register": "fib.register_atlas",
            "session_id": session_id,
            "atlas_file": str(fib_atlas.file),
        },
    )


@router.post("/sessions/{session_id}/register_milling_progress")
def register_fib_milling_progress(
    session_id: int,
    site_info: LamellaSiteInfo,
    db: Session = murfey_db,
):
    logger.debug(
        "Received the following FIB metadata for registration:\n"
        f"{json.dumps(site_info.model_dump(exclude_none=True), indent=2, default=str)}"
    )


class FIBGIFParameters(BaseModel):
    lamella_number: int
    images: list[Path]
    output_file: Path


@router.post("/sessions/{session_id}/make_gif")
async def make_gif(
    session_id: int,
    gif_params: FIBGIFParameters,
    db=murfey_db,
):
    # Load machine config and session info
    session_entry = db.exec(
        select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
    ).one()
    instrument_name = session_entry.instrument_name
    visit_name = session_entry.visit
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    rsync_basepath = machine_config.rsync_basepath or Path(".").resolve()

    # Sanitise and verify that the output directory is relative to rsync basepath
    output_file = sanitise_path(gif_params.output_file)
    if not output_file.is_relative_to(rsync_basepath):
        logger.error("Output file path is not permitted")
        raise ValueError

    # Create folders in the visit directory and onwards and change permissions
    visit_index = output_file.parts.index(visit_name)
    for current_path in list(reversed(output_file.parents))[visit_index + 1 :]:
        if not current_path.exists():
            current_path.mkdir(parents=True)
            logger.debug(f"Created output directory {current_path}")
            try:
                os.chmod(current_path, mode=machine_config.mkdir_chmod)
            except PermissionError:
                logger.warning(
                    f"Insufficient permissions to modify directory {current_path}"
                )
                continue

    # Load the images as PIL Image objects
    arr: list[np.ndarray] = []
    for f in gif_params.images:
        with PIL.Image.open(f) as im:
            im.thumbnail((512, 512))
            frame = np.array(im, dtype=np.float32)
            vmin, vmax = np.percentile(frame, (0.5, 99.5))
            scale = 255 / ((vmax - vmin) or 1)
            np.clip(frame, a_min=vmin, a_max=vmax, out=frame)
            np.subtract(frame, vmin, out=frame)
            np.multiply(frame, scale, out=frame)
            arr.append(frame.astype(np.uint8))
    arr = np.array(arr).astype(np.uint8)

    # Convert back to PIL.Image objects and save as GIF
    try:
        converted = [PIL.Image.fromarray(a, mode="L") for a in arr]
        converted[0].save(
            output_file,
            format="GIF",
            append_images=converted[1:],
            save_all=True,
            duration=30,
            loop=0,
        )
        logger.info(f"Created GIF file {output_file}")
        return {"output_gif": str(output_file)}
    finally:
        for im in converted:
            im.close()
