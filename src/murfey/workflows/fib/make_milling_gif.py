import logging
import os
from pathlib import Path
from typing import Any

import numpy as np
import PIL.Image
from sqlmodel import Session as SQLModelSession, select

import murfey.util.db as MurfeyDB
from murfey.server import _transport_object
from murfey.util import sanitise_path
from murfey.util.config import get_machine_config
from murfey.util.models import FIBGIFParameters

logger = logging.getLogger(__name__)


def run(message: dict[str, Any], murfey_db: SQLModelSession):
    # Early exit if no TransportManager was set up
    if _transport_object is None:
        logger.error("No TransportManager object was configured")
        return {"success": False, "requeue": False}

    try:
        # Parse and unpack incoming message
        session_id = int(message["session_id"])
        gif_params = FIBGIFParameters(**message["gif_params"])
    except Exception:
        logger.error("Error parsing contents of message", exc_info=True)
        return {"success": False, "requeue": False}
    # Load machine config and session info
    session_entry = murfey_db.exec(
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
